package indexing

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
	"magworkspace/internal/indexqueue"
	"magworkspace/internal/observability"
	"magworkspace/internal/redisstate"
	"magworkspace/internal/reranking"
	"magworkspace/internal/store"
)

type Options struct {
	Queue          *indexqueue.Queue
	MaxRetries     int
	RetryBaseDelay time.Duration
	Redis          *redisstate.Client
	Workers        int
	PollInterval   time.Duration
	JobTimeout     time.Duration
	// Execution changes scheduling only; both strategies share retrieval and
	// authorization semantics. The zero value selects the serial baseline.
	Execution     string
	Telemetry     *observability.Telemetry
	Reranker      reranking.Model
	RerankOptions reranking.Options
	// Model identity includes the pinned model revision and operational policy. Never include credentials.
	RerankModelID string
}

type Service struct {
	db       *store.Store
	parser   Parser
	embedder Embedder
	search   SearchStore
	objects  blob.Storage
	profile  domain.IndexProfile
	opts     Options
}

// Profile also changes when the configured provider/storage endpoint changes.
// Credentials are intentionally excluded. Changing model content under a fixed
// model tag requires a new model tag or pipeline revision before rebuilding.
func Profile(model string, dimensions int, parserURL, embeddingURL, vectorURL string, avgLen float64) domain.IndexProfile {
	profile, _ := ProfileWithChunking(model, dimensions, parserURL, embeddingURL, vectorURL, avgLen, ChunkingStructure)
	return profile
}

// ProfileWithChunking keeps the original profile hash for an explicit legacy
// rollout. New chunk boundaries require a distinct immutable index namespace.
func ProfileWithChunking(model string, dimensions int, parserURL, embeddingURL, vectorURL string, avgLen float64, strategy string) (domain.IndexProfile, error) {
	if strategy == "" {
		strategy = ChunkingStructure
	}
	pipeline := "mw-index-v3/minio-qdrant-1.16.2-native-bm25-rrf61/tika-3.3.3/chunk-800-100"
	switch strategy {
	case ChunkingLegacy:
	case ChunkingStructure:
		pipeline = "mw-index-v4/minio-qdrant-1.16.2-native-bm25-rrf61/tika-3.3.3/structure-v2-800-100/manifest-2"
	default:
		return domain.IndexProfile{}, &Failure{Code: "artifact_invalid"}
	}
	data, _ := json.Marshal([]any{pipeline, model, dimensions,
		strings.TrimRight(parserURL, "/"), strings.TrimRight(embeddingURL, "/"), strings.TrimRight(vectorURL, "/"), bm25Options(avgLen)})
	hash := sha256.Sum256(data)
	return domain.IndexProfile{ID: hex.EncodeToString(hash[:]), Model: model, Dimensions: dimensions, ChunkingStrategy: strategy}, nil
}

func NewService(db *store.Store, parser Parser, embedder Embedder, search SearchStore, objects blob.Storage, profile domain.IndexProfile, opts Options) *Service {
	if opts.Reranker != nil && opts.RerankOptions.MaxCandidates == 0 {
		opts.RerankOptions = reranking.DefaultOptions()
	}
	if opts.RetryBaseDelay <= 0 {
		opts.RetryBaseDelay = 5 * time.Second
	}
	if opts.Workers <= 0 {
		opts.Workers = 1
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = time.Second
	}
	if opts.JobTimeout <= 0 {
		opts.JobTimeout = 5 * time.Minute
	}
	if opts.Execution != "optimized" {
		opts.Execution = "baseline"
	}
	return &Service{db: db, parser: parser, embedder: embedder, search: search, objects: objects, profile: profile, opts: opts}
}

func (s *Service) IndexProfile() domain.IndexProfile { return s.profile }

// Execution identifies the frozen scheduling strategy without changing index data.
func (s *Service) Execution() string {
	if s.nativeFusion() {
		return "qdrant_native"
	}
	return s.opts.Execution
}
func (s *Service) nativeFusion() bool {
	native, ok := s.search.(interface{ NativeFusion() bool })
	return ok && native.NativeFusion()
}

func (s *Service) Run(ctx context.Context) {
	var wg sync.WaitGroup
	if s.opts.Queue != nil {
		for ctx.Err() == nil {
			if err := s.opts.Queue.Start(ctx, s.ProcessDelivery); err == nil {
				break
			}
			slog.Warn("index queue startup pending")
			if !pause(ctx, s.opts.PollInterval) {
				return
			}
		}
		defer s.opts.Queue.Shutdown()
	}
	if s.opts.Redis != nil || s.opts.Queue != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				if err := s.DispatchOnce(ctx); err != nil && ctx.Err() == nil {
					s.opts.Telemetry.ObserveCount("redis.dispatch", "error", 1)
				}
				if !pause(ctx, 2*time.Second) {
					return
				}
			}
		}()
	}
	workers := s.opts.Workers
	if s.opts.Queue != nil {
		workers = 0
	}
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				worked, err := s.ProcessOne(ctx)
				if err != nil && ctx.Err() == nil {
					slog.Warn("document indexing failed", "code", ErrorCode(err))
				}
				if !worked && !pause(ctx, s.opts.PollInterval) {
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			if err := s.CleanupOnce(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("index cleanup pending", "code", ErrorCode(err))
			}
			if !pause(ctx, 2*s.opts.PollInterval) {
				return
			}
		}
	}()
	wg.Wait()
}

func pause(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// ProcessOne is also used by integration tests to exercise the production worker.
func (s *Service) ProcessOne(ctx context.Context) (bool, error) {
	var input *domain.IndexInput
	var err error
	lease := s.opts.JobTimeout + 30*time.Second
	if s.opts.Redis != nil {
		var id string
		id, err = s.opts.Redis.Pop(ctx, s.profile.ID)
		if err == nil && id != "" {
			input, err = s.db.ClaimIndexJobByID(ctx, s.profile.ID, id, lease)
		} else if err != nil || id == "" {
			if err != nil {
				s.opts.Telemetry.ObserveCount("redis.dispatch", "error", 1)
			}
			input, err = s.db.ClaimIndexJob(ctx, s.profile.ID, lease)
		}
	} else {
		input, err = s.db.ClaimIndexJob(ctx, s.profile.ID, lease)
	}
	if err != nil || input == nil {
		return false, err
	}
	processErr, persistErr := s.executeClaimed(ctx, input)
	if persistErr != nil {
		return true, persistErr
	}
	return true, processErr
}

// ProcessDelivery ACKs only after success or a durable retry/terminal decision.
// Duplicate and stale deliveries are harmless because the DB owns admission.
func (s *Service) ProcessDelivery(ctx context.Context, jobID, deliveryID string) error {
	input, err := s.db.ClaimIndexDelivery(ctx, s.profile.ID, jobID, deliveryID, s.opts.JobTimeout+30*time.Second)
	if err != nil || input == nil {
		return err
	}
	_, persistErr := s.executeClaimed(ctx, input)
	return persistErr
}

func (s *Service) executeClaimed(ctx context.Context, input *domain.IndexInput) (error, error) {
	jobCtx, cancel := context.WithTimeout(ctx, s.opts.JobTimeout)
	err := s.process(jobCtx, input)
	cancel()
	if err == nil {
		return nil, nil
	}
	// Cancellation leaves a fenced lease; outbox and Asynq both recover it.
	cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if ctx.Err() != nil {
		_ = s.db.QueueIndexCleanup(cleanupCtx, input.Job.ProfileID, input.Job.Generation)
		return err, ctx.Err()
	}
	code := ErrorCode(err)
	if errors.Is(err, domain.ErrForbidden) || errors.Is(err, domain.ErrNotFound) {
		code = "authorization_changed"
	}
	var persistErr error
	if retryableIndexError(err) && s.opts.MaxRetries > 0 {
		delay := s.opts.RetryBaseDelay
		for i := 1; i < input.Job.Attempts && delay < 5*time.Minute; i++ {
			delay *= 2
		}
		if delay > 5*time.Minute {
			delay = 5 * time.Minute
		}
		_, persistErr = s.db.RetryDocumentIndex(cleanupCtx, input.Job, code, s.opts.MaxRetries, delay)
	} else {
		persistErr = s.db.FailDocumentIndex(cleanupCtx, input.Job, code)
	}
	// A changed generation, deleted workspace or committed ready state wins.
	if errors.Is(persistErr, domain.ErrConflict) || errors.Is(persistErr, domain.ErrNotFound) {
		persistErr = s.db.QueueIndexCleanup(cleanupCtx, input.Job.ProfileID, input.Job.Generation)
	}
	return err, persistErr
}

func retryableIndexError(err error) bool {
	switch ErrorCode(err) {
	case "source_unavailable", "parser_unavailable", "embedding_unavailable", "embedding_timeout", "index_unavailable", "qdrant_unavailable", "artifact_unavailable", "publication_unavailable", "processing_timeout":
		return true
	default:
		return false
	}
}

func (s *Service) process(ctx context.Context, input *domain.IndexInput) (resultErr error) {
	ctx, finish := s.opts.Telemetry.Start(ctx, "index.process")
	started := time.Now()
	defer func() {
		finish(resultErr)
		observability.Logger(ctx).InfoContext(ctx, "document index finished", "outcome", executionOutcome(resultErr), "duration_ms", time.Since(started).Milliseconds())
	}()
	s.progress(ctx, input.Job, "loading", 0)
	content, err := s.db.LoadIndexSource(ctx, *input)
	if err != nil {
		if errors.Is(err, domain.ErrForbidden) || errors.Is(err, domain.ErrNotFound) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, blob.ErrIntegrity) {
			return &Failure{Code: "source_integrity_failed"}
		}
		return &Failure{Code: "source_unavailable", Cause: err}
	}
	digest := sha256.Sum256(content)
	if hex.EncodeToString(digest[:]) != input.Job.SourceSHA256 || input.Version.SHA256 != input.Job.SourceSHA256 || input.Version.ID != input.Job.VersionID || input.Version.SizeBytes != len(content) {
		return &Failure{Code: "source_integrity_failed"}
	}
	s.progress(ctx, input.Job, "parsing", 0)
	parseCtx, finishParse := s.opts.Telemetry.Start(ctx, "index.parse")
	text, err := s.parser.Parse(parseCtx, input.Version, content)
	finishParse(err)
	if err != nil {
		return err
	}
	s.progress(ctx, input.Job, "chunking", 0)
	_, finishChunk := s.opts.Telemetry.Start(ctx, "index.chunk")
	chunks, err := SplitWithStrategy(text, s.profile.ChunkingStrategy)
	finishChunk(err)
	if err != nil {
		return err
	}
	s.opts.Telemetry.ObserveCount("index.chunk", "success", len(chunks))
	texts := make([]string, 0, len(chunks))
	searchChunks := make([]domain.IndexChunk, 0, len(chunks))
	for i := range chunks {
		var id [16]byte
		if _, err = rand.Read(id[:]); err != nil {
			return err
		}
		chunks[i].ID = hex.EncodeToString(id[:])
		chunks[i].IndexID = input.Job.ID
		chunks[i].DocumentID = input.Job.DocumentID
		chunks[i].VersionID = input.Job.VersionID
		chunks[i].SourceSHA256 = input.Job.SourceSHA256
		// Keep every canonical span for immutable references, but never send
		// whitespace-only spans to embedding or search providers.
		if strings.TrimSpace(chunks[i].Content) != "" {
			texts = append(texts, chunks[i].Content)
			searchChunks = append(searchChunks, chunks[i])
		}
	}
	s.progress(ctx, input.Job, "embedding", len(chunks))
	embedCtx, finishEmbed := s.opts.Telemetry.Start(ctx, "index.embed")
	vectors, err := s.embedder.Embed(embedCtx, texts)
	finishEmbed(err)
	if err != nil {
		return err
	}
	s.opts.Telemetry.ObserveCount("index.embed", "success", len(vectors))
	s.progress(ctx, input.Job, "writing", len(chunks))
	writeCtx, finishWrite := s.opts.Telemetry.Start(ctx, "index.write")
	key := artifactKey(input.Job.ProfileID, input.Job.Generation)
	var artifact []byte
	err = func() error {
		if err := s.search.Ensure(writeCtx, s.profile.ID, s.profile.Dimensions); err != nil {
			return err
		}
		if err := s.search.Upsert(writeCtx, s.profile.ID, input.Job.Generation, input.Job.WorkspaceID, searchChunks, vectors); err != nil {
			return err
		}
		var err error
		artifact, err = encodeManifest(input.Job, text, chunks, s.profile.ChunkingStrategy)
		if err != nil {
			return err
		}
		if err = s.objects.Put(writeCtx, key, artifact); err != nil {
			return &Failure{Code: "artifact_unavailable"}
		}
		return nil
	}()
	finishWrite(err)
	if err != nil {
		return err
	}
	s.opts.Telemetry.ObserveCount("index.write", "success", len(searchChunks))
	s.progress(ctx, input.Job, "publishing", len(chunks))
	publishCtx, finishPublish := s.opts.Telemetry.Start(ctx, "index.publish")
	err = s.db.FinishDocumentIndex(publishCtx, input.Job, key, digestBytes(artifact), len(chunks))
	finishPublish(err)
	if err != nil && !errors.Is(err, domain.ErrForbidden) && !errors.Is(err, domain.ErrNotFound) && !errors.Is(err, domain.ErrInvalid) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		// This includes DB deadlock/lock-timeout, mapped to Conflict by store.
		// Retry's generation/status guard protects an already committed ready.
		err = &Failure{Code: "publication_unavailable", Cause: err}
	}
	if err == nil {
		s.opts.Telemetry.ObserveCount("index.process", "success", len(chunks))
	}
	return err
}

func (s *Service) CleanupOnce(ctx context.Context) error {
	items, err := s.db.PendingIndexCleanup(ctx, 16)
	if err != nil {
		return err
	}
	var firstErr error
	for _, item := range items {
		cleanCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		err = s.search.DeleteGeneration(cleanCtx, item.ProfileID, item.Generation)
		if err == nil {
			err = s.objects.Delete(cleanCtx, artifactKey(item.ProfileID, item.Generation))
		}
		if err == nil {
			err = s.db.CompleteIndexCleanup(cleanCtx, item.ID)
		}
		cancel()
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if deferErr := s.db.DeferIndexCleanup(ctx, item.ID, 30*time.Second); deferErr != nil {
				return deferErr
			}
		}
	}
	return firstErr
}

func (s *Service) Search(ctx context.Context, userID, workspaceID, query, mode string, limit int) ([]domain.SearchHit, error) {
	return s.SearchScoped(ctx, userID, workspaceID, query, mode, limit, nil)
}

// SearchScoped applies a server-authorized document selection throughout search.
// nil selects the workspace; [] explicitly selects no documents.
func (s *Service) SearchScoped(ctx context.Context, userID, workspaceID, query, mode string, limit int, documentIDs []string) ([]domain.SearchHit, error) {
	result, err := s.SearchScopedDetailed(ctx, userID, workspaceID, query, mode, limit, documentIDs, nil)
	return result.Hits, err
}

func (s *Service) searchScoped(ctx context.Context, userID, workspaceID, query, mode string, limit int, documentIDs []string, rerank bool, diagnostic *reranking.Diagnostics) (results []domain.SearchHit, resultErr error) {
	ctx, finish := s.opts.Telemetry.Start(ctx, "retrieval.search")
	started := time.Now()
	defer func() {
		finish(resultErr)
		if resultErr == nil {
			s.opts.Telemetry.ObserveCount("retrieval.search", "success", len(results))
		}
		execution := s.Execution()
		observability.Logger(ctx).InfoContext(ctx, "retrieval finished", "execution", execution, "outcome", executionOutcome(resultErr), "results", len(results), "duration_ms", time.Since(started).Milliseconds())
	}()
	query = strings.TrimSpace(query)
	if !utf8.ValidString(query) || utf8.RuneCountInString(query) < 2 || len(query) > 512 || strings.IndexFunc(query, unicode.IsControl) >= 0 || limit < 1 || limit > 50 || (mode != "hybrid" && mode != "vector" && mode != "lexical") {
		return nil, domain.ErrInvalid
	}
	// Authorization precedes external calls; final hydration repeats it so a
	// revoked member or changed current version cannot use a stale search scope.
	scopeCtx, finishScope := s.opts.Telemetry.Start(ctx, "retrieval.scope")
	generations, err := s.db.SearchIndexScope(scopeCtx, userID, workspaceID, s.profile.ID, documentIDs)
	finishScope(err)
	if err != nil {
		return nil, err
	}
	if len(generations) == 0 {
		return []domain.SearchHit{}, nil
	}
	candidateLimit := 100
	if rerank {
		candidateLimit = max(candidateLimit, s.opts.RerankOptions.MaxCandidates)
	}
	var scores map[string]float64
	var candidatesByID map[string]domain.IndexCandidate
	if s.nativeFusion() {
		candidates, err := s.nativeCandidates(ctx, workspaceID, generations, query, mode, candidateLimit)
		if err != nil {
			return nil, err
		}
		scores = make(map[string]float64)
		candidatesByID = make(map[string]domain.IndexCandidate)
		for _, c := range candidates {
			scores[c.ChunkID] = c.Score
			candidatesByID[c.ChunkID] = c
		}
	} else {
		lexical, vector, err := s.retrieveCandidates(ctx, workspaceID, generations, query, mode, candidateLimit)
		if err != nil {
			return nil, err
		}
		_, finishMerge := s.opts.Telemetry.Start(ctx, "retrieval.merge")
		scores, candidatesByID = mergeCandidates(lexical, vector)
		finishMerge(nil)
	}
	normalizeRetrievalScores(scores, mode, s.nativeFusion())
	// Bound the globally fused set before hydration. Both branches can each
	// return 200 rows, while the database resolver accepts at most 200 hints.
	candidateIDs := make([]string, 0, len(candidatesByID))
	for id := range candidatesByID {
		candidateIDs = append(candidateIDs, id)
	}
	sort.Slice(candidateIDs, func(i, j int) bool {
		if scores[candidateIDs[i]] == scores[candidateIDs[j]] {
			return candidateIDs[i] < candidateIDs[j]
		}
		return scores[candidateIDs[i]] > scores[candidateIDs[j]]
	})
	for _, id := range candidateIDs[min(len(candidateIDs), 200):] {
		delete(candidatesByID, id)
	}
	s.opts.Telemetry.ObserveCount("retrieval.merge", "success", len(candidatesByID))
	ids := make([]string, 0, len(candidatesByID))
	for _, c := range candidatesByID {
		ids = append(ids, c.IndexID)
	}
	resolveCtx, finishResolve := s.opts.Telemetry.Start(ctx, "retrieval.resolve")
	refs, err := s.db.ResolveIndexes(resolveCtx, userID, workspaceID, s.profile.ID, ids, documentIDs)
	finishResolve(err)
	if err != nil {
		return nil, err
	}
	hits := []domain.SearchHit{}
	published := make(map[string]domain.IndexJob)
	headers := make(map[string]string)
	hydrateCtx, finishHydrate := s.opts.Telemetry.Start(ctx, "retrieval.hydrate")
	for _, ref := range refs {
		chunks, err := s.loadManifest(hydrateCtx, ref.Job)
		if err != nil {
			finishHydrate(err)
			return nil, err
		}
		published[ref.Job.ID] = ref.Job
		if rerank {
			for id, header := range contextHeaders(chunks) {
				headers[id] = header
			}
		}
		for _, chunk := range chunks {
			c, ok := candidatesByID[chunk.ID]
			if ok && c.IndexID == ref.Job.ID && c.Generation == ref.Job.Generation {
				hits = append(hits, domain.SearchHit{Chunk: chunk, Title: ref.Title, Score: scores[chunk.ID]})
			}
		}
	}
	finishHydrate(nil)
	s.opts.Telemetry.ObserveCount("retrieval.hydrate", "success", len(hits))
	// Recheck before sending trusted passages to the model, then again after
	// the external call (including a successful empty result or model fallback).
	hits, err = s.currentHits(ctx, userID, workspaceID, ids, documentIDs, published, hits)
	if err != nil {
		return nil, err
	}
	sortRetrievalHits(hits)
	if rerank {
		hits, err = s.rerankHits(ctx, query, hits, headers, limit, len(documentIDs) > 0, diagnostic)
		if err != nil {
			return nil, err
		}
		hits, err = s.currentHits(ctx, userID, workspaceID, ids, documentIDs, published, hits)
		if err != nil {
			return nil, err
		}
	}
	if len(hits) > limit {
		hits = hits[:limit]
	}
	s.opts.Telemetry.ObserveCount("retrieval.filter", "success", len(hits))
	return hits, nil
}

// Native BM25 and RRF run in one Qdrant query with identical prefilters.
func (s *Service) nativeCandidates(ctx context.Context, workspace string, generations []string, query, mode string, candidateLimit int) ([]domain.IndexCandidate, error) {
	var vector []float32
	if mode != "lexical" {
		embedCtx, finish := s.opts.Telemetry.Start(ctx, "retrieval.embed")
		vectors, err := s.embedder.Embed(embedCtx, []string{query})
		if err == nil && (len(vectors) != 1 || !validVector(vectors[0]) || len(vectors[0]) != s.profile.Dimensions) {
			err = &Failure{Code: "embedding_invalid"}
		}
		finish(err)
		if err != nil {
			return nil, err
		}
		vector = vectors[0]
	}
	stage := "retrieval.merge"
	if mode == "lexical" {
		stage = "retrieval.lexical"
	}
	if mode == "vector" {
		stage = "retrieval.vector"
	}
	queryCtx, finish := s.opts.Telemetry.Start(ctx, stage)
	items, err := s.search.Search(queryCtx, s.profile.ID, workspace, generations, query, vector, mode, candidateLimit)
	finish(err)
	return items, err
}

// Both execution strategies use the same retrievers and fusion. Optimized only
// overlaps independent external calls; source resolution and trusted hydration
// retain the same ordering and final authorization checks as the baseline.
func (s *Service) retrieveCandidates(ctx context.Context, workspaceID string, generations []string, query, mode string, candidateLimit int) (lexical, vector []domain.IndexCandidate, err error) {
	lexicalSearch := func(ctx context.Context) ([]domain.IndexCandidate, error) {
		ctx, finish := s.opts.Telemetry.Start(ctx, "retrieval.lexical")
		items, err := s.search.Search(ctx, s.profile.ID, workspaceID, generations, query, nil, "lexical", candidateLimit)
		finish(err)
		if err == nil {
			s.opts.Telemetry.ObserveCount("retrieval.lexical", "success", len(items))
		}
		return items, err
	}
	vectorSearch := func(ctx context.Context) ([]domain.IndexCandidate, error) {
		embedCtx, finishEmbed := s.opts.Telemetry.Start(ctx, "retrieval.embed")
		vectors, err := s.embedder.Embed(embedCtx, []string{query})
		if err == nil && len(vectors) != 1 {
			err = &Failure{Code: "embedding_invalid"}
		}
		finishEmbed(err)
		if err != nil {
			return nil, err
		}
		ctx, finish := s.opts.Telemetry.Start(ctx, "retrieval.vector")
		items, err := s.search.Search(ctx, s.profile.ID, workspaceID, generations, query, vectors[0], "vector", candidateLimit)
		finish(err)
		if err == nil {
			s.opts.Telemetry.ObserveCount("retrieval.vector", "success", len(items))
		}
		return items, err
	}
	if mode == "hybrid" && s.opts.Execution == "optimized" {
		searchCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		var lexicalErr, vectorErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			lexical, lexicalErr = lexicalSearch(searchCtx)
			if lexicalErr != nil {
				// Baseline returns lexical failures before running the vector
				// branch. Preserve that error priority and stop unused work.
				cancel()
			}
		}()
		go func() {
			defer wg.Done()
			vector, vectorErr = vectorSearch(searchCtx)
		}()
		wg.Wait()
		if lexicalErr != nil {
			return nil, nil, lexicalErr
		}
		if vectorErr != nil {
			return nil, nil, vectorErr
		}
		return lexical, vector, nil
	}
	if mode != "vector" {
		lexical, err = lexicalSearch(ctx)
		if err != nil {
			return nil, nil, err
		}
	}
	if mode != "lexical" {
		vector, err = vectorSearch(ctx)
		if err != nil {
			return nil, nil, err
		}
	}
	return lexical, vector, nil
}

func mergeCandidates(lexical, vector []domain.IndexCandidate) (map[string]float64, map[string]domain.IndexCandidate) {
	scores := make(map[string]float64)
	byID := make(map[string]domain.IndexCandidate)
	for _, candidates := range [][]domain.IndexCandidate{lexical, vector} {
		seen := make(map[string]bool)
		for rank, candidate := range candidates {
			if !seen[candidate.ChunkID] {
				scores[candidate.ChunkID] += 1.0 / float64(61+rank)
				seen[candidate.ChunkID] = true
				byID[candidate.ChunkID] = candidate
			}
		}
	}
	return scores, byID
}

// Logging shares bounded outcomes with telemetry instead of serializing an
// arbitrary storage/provider error, which can contain private input or secrets.
func executionOutcome(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, domain.ErrForbidden), errors.Is(err, domain.ErrNotFound), errors.Is(err, domain.ErrInvalid), errors.Is(err, domain.ErrConflict):
		return "rejected"
	default:
		return "error"
	}
}

// DispatchOnce reconciles all upload/update/approval paths through committed
// MySQL intents. Crash before/after Redis publication is safe to repeat.
func (s *Service) DispatchOnce(ctx context.Context) error {
	if s.opts.Queue != nil {
		deliveries, err := s.db.PendingIndexDeliveries(ctx, s.profile.ID, 256)
		if err != nil {
			return err
		}
		for _, d := range deliveries {
			if err := s.opts.Queue.Enqueue(ctx, d.ID, d.JobID); err != nil {
				return err
			}
			if err := s.db.MarkIndexDeliverySent(ctx, d); err != nil {
				return err
			}
		}
		return nil
	}
	if s.opts.Redis == nil {
		return nil
	}
	jobs, err := s.db.PendingIndexJobs(ctx, s.profile.ID)
	if err != nil {
		return err
	}
	return s.opts.Redis.Dispatch(ctx, s.profile.ID, jobs)
}
func (s *Service) progress(ctx context.Context, job domain.IndexJob, stage string, chunks int) {
	if s.opts.Redis != nil {
		s.opts.Redis.PutProgress(ctx, job, stage, chunks)
	}
}
