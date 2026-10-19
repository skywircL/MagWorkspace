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
	"magworkspace/internal/observability"
	"magworkspace/internal/redisstate"
	"magworkspace/internal/store"
)

type Options struct {
	Redis        *redisstate.Client
	Workers      int
	PollInterval time.Duration
	JobTimeout   time.Duration
	// Execution changes scheduling only; both strategies share retrieval and
	// authorization semantics. The zero value selects the serial baseline.
	Execution string
	Telemetry *observability.Telemetry
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
	data, _ := json.Marshal([]any{"mw-index-v3/minio-qdrant-1.16.2-native-bm25-rrf61/tika-3.3.3/chunk-800-100", model, dimensions,
		strings.TrimRight(parserURL, "/"), strings.TrimRight(embeddingURL, "/"), strings.TrimRight(vectorURL, "/"), bm25Options(avgLen)})
	hash := sha256.Sum256(data)
	return domain.IndexProfile{ID: hex.EncodeToString(hash[:]), Model: model, Dimensions: dimensions}
}

func NewService(db *store.Store, parser Parser, embedder Embedder, search SearchStore, objects blob.Storage, profile domain.IndexProfile, opts Options) *Service {
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
	if s.opts.Redis != nil {
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
	for range s.opts.Workers {
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
	jobCtx, cancel := context.WithTimeout(ctx, s.opts.JobTimeout)
	err = s.process(jobCtx, input)
	cancel()
	if err == nil {
		return true, nil
	}
	// Shutdown leaves a fenced lease for recovery. A finished/expired old worker
	// still queues its generation, but cleanup excludes any live published one.
	cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if ctx.Err() == nil {
		code := ErrorCode(err)
		if errors.Is(err, domain.ErrForbidden) || errors.Is(err, domain.ErrNotFound) {
			code = "authorization_changed"
		}
		if failErr := s.db.FailDocumentIndex(cleanupCtx, input.Job, code); failErr != nil {
			slog.Warn("index failure persistence pending", "job_id", input.Job.ID)
		}
	}
	if cleanupErr := s.db.QueueIndexCleanup(cleanupCtx, input.Job.ProfileID, input.Job.Generation); cleanupErr != nil {
		slog.Warn("index cleanup persistence pending", "job_id", input.Job.ID)
	}
	return true, err
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
		if errors.Is(err, domain.ErrForbidden) || errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrConflict) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, blob.ErrIntegrity) {
			return &Failure{Code: "source_integrity_failed"}
		}
		return &Failure{Code: "source_unavailable"}
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
	chunks, err := Split(text)
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
		artifact, err = encodeManifest(input.Job, text, chunks)
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
			return err
		}
	}
	return nil
}

func (s *Service) Search(ctx context.Context, userID, workspaceID, query, mode string, limit int) ([]domain.SearchHit, error) {
	return s.SearchScoped(ctx, userID, workspaceID, query, mode, limit, nil)
}

// SearchScoped applies a server-authorized document selection throughout search.
// nil selects the workspace; [] explicitly selects no documents.
func (s *Service) SearchScoped(ctx context.Context, userID, workspaceID, query, mode string, limit int, documentIDs []string) (results []domain.SearchHit, resultErr error) {
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
	var scores map[string]float64
	var candidatesByID map[string]domain.IndexCandidate
	if s.nativeFusion() {
		candidates, err := s.nativeCandidates(ctx, workspaceID, generations, query, mode)
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
		lexical, vector, err := s.retrieveCandidates(ctx, workspaceID, generations, query, mode)
		if err != nil {
			return nil, err
		}
		_, finishMerge := s.opts.Telemetry.Start(ctx, "retrieval.merge")
		scores, candidatesByID = mergeCandidates(lexical, vector)
		finishMerge(nil)
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
	hydrateCtx, finishHydrate := s.opts.Telemetry.Start(ctx, "retrieval.hydrate")
	for _, ref := range refs {
		chunks, err := s.loadManifest(hydrateCtx, ref.Job)
		if err != nil {
			finishHydrate(err)
			return nil, err
		}
		published[ref.Job.ID] = ref.Job
		for _, chunk := range chunks {
			c, ok := candidatesByID[chunk.ID]
			if ok && c.IndexID == ref.Job.ID && c.Generation == ref.Job.Generation {
				hits = append(hits, domain.SearchHit{Chunk: chunk, Title: ref.Title, Score: scores[chunk.ID]})
			}
		}
	}
	finishHydrate(nil)
	s.opts.Telemetry.ObserveCount("retrieval.hydrate", "success", len(hits))
	// Repeat authorization and current-head filtering after all remote reads.
	filterCtx, finishFilter := s.opts.Telemetry.Start(ctx, "retrieval.filter")
	refs, err = s.db.ResolveIndexes(filterCtx, userID, workspaceID, s.profile.ID, ids, documentIDs)
	if err != nil {
		finishFilter(err)
		return nil, err
	}
	current := make(map[string]domain.IndexReference)
	for _, ref := range refs {
		prior := published[ref.Job.ID]
		if prior.Generation == ref.Job.Generation && prior.ArtifactSHA256 == ref.Job.ArtifactSHA256 {
			current[ref.Job.ID] = ref
		}
	}
	filtered := hits[:0]
	for _, hit := range hits {
		if ref, ok := current[hit.Chunk.IndexID]; ok {
			hit.Title = ref.Title
			filtered = append(filtered, hit)
		}
	}
	hits = filtered
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score == hits[j].Score {
			return hits[i].Chunk.ID < hits[j].Chunk.ID
		}
		return hits[i].Score > hits[j].Score
	})
	if len(hits) > limit {
		hits = hits[:limit]
	}
	finishFilter(nil)
	s.opts.Telemetry.ObserveCount("retrieval.filter", "success", len(hits))
	return hits, nil
}

// Native BM25 and RRF run in one Qdrant query with identical prefilters.
func (s *Service) nativeCandidates(ctx context.Context, workspace string, generations []string, query, mode string) ([]domain.IndexCandidate, error) {
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
	items, err := s.search.Search(queryCtx, s.profile.ID, workspace, generations, query, vector, mode, 100)
	finish(err)
	return items, err
}

// Both execution strategies use the same retrievers and fusion. Optimized only
// overlaps independent external calls; source resolution and trusted hydration
// retain the same ordering and final authorization checks as the baseline.
func (s *Service) retrieveCandidates(ctx context.Context, workspaceID string, generations []string, query, mode string) (lexical, vector []domain.IndexCandidate, err error) {
	lexicalSearch := func(ctx context.Context) ([]domain.IndexCandidate, error) {
		ctx, finish := s.opts.Telemetry.Start(ctx, "retrieval.lexical")
		items, err := s.search.Search(ctx, s.profile.ID, workspaceID, generations, query, nil, "lexical", 100)
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
		items, err := s.search.Search(ctx, s.profile.ID, workspaceID, generations, query, vectors[0], "vector", 100)
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
