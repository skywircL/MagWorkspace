package httpapi

import (
	"context"
	"errors"
	"net/http"
	"time"

	"magworkspace/internal/indexing"
)

func (s *server) indexingEnabled(w http.ResponseWriter) bool {
	if s.cfg.Indexing == nil {
		problem(w, 503, "indexing_disabled", "document indexing is not configured")
		return false
	}
	return true
}

func (s *server) enqueueDocumentIndex(w http.ResponseWriter, r *http.Request) {
	var in struct{}
	if !decode(w, r, &in) || !s.indexingEnabled(w) {
		return
	}
	job, queued, err := s.db.EnqueueDocumentIndex(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("documentID"), r.PathValue("versionID"), s.cfg.Indexing.IndexProfile())
	if err != nil {
		indexFailure(w, err)
		return
	}
	status := http.StatusOK
	if queued {
		status = http.StatusAccepted
	}
	respond(w, status, job)
}

func (s *server) documentIndex(w http.ResponseWriter, r *http.Request) {
	if !s.indexingEnabled(w) {
		return
	}
	job, err := s.db.DocumentIndex(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("documentID"), r.PathValue("versionID"), s.cfg.Indexing.IndexProfile().ID)
	if err != nil {
		indexFailure(w, err)
		return
	}
	if s.cfg.Redis != nil {
		job.Progress = s.cfg.Redis.Progress(r.Context(), job)
	}
	respond(w, 200, job)
}

func (s *server) indexChunks(w http.ResponseWriter, r *http.Request) {
	if !s.indexingEnabled(w) {
		return
	}
	p, n, ok := pagination(w, r)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	kind := r.URL.Query().Get("kind")
	if kind == "" {
		kind = "parent"
	}
	chunks, err := s.cfg.Indexing.ChunksByKind(ctx, userID(r), r.PathValue("workspaceID"), r.PathValue("documentID"), r.PathValue("versionID"), r.PathValue("indexID"), kind)
	if err != nil {
		indexFailure(w, err)
		return
	}
	total := len(chunks)
	start := min((p-1)*n, total)
	page(w, chunks[start:min(start+n, total)], total, p, n)
}

func (s *server) indexChunk(w http.ResponseWriter, r *http.Request) {
	if !s.indexingEnabled(w) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	chunk, err := s.cfg.Indexing.Chunk(ctx, userID(r), r.PathValue("workspaceID"), r.PathValue("documentID"), r.PathValue("versionID"), r.PathValue("indexID"), r.PathValue("chunkID"))
	if err != nil {
		indexFailure(w, err)
		return
	}
	respond(w, 200, chunk)
}

func indexFailure(w http.ResponseWriter, err error) {
	var external *indexing.Failure
	if errors.As(err, &external) {
		problem(w, 503, external.Code, "document index dependency unavailable")
		return
	}
	fail(w, err)
}

func (s *server) searchDocuments(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Query  string `json:"query"`
		Mode   string `json:"mode"`
		Limit  int    `json:"limit"`
		Rerank *bool  `json:"rerank"`
	}
	if !decode(w, r, &in) || !s.indexingEnabled(w) {
		return
	}
	if in.Mode == "" {
		in.Mode = "hybrid"
	}
	if in.Limit == 0 {
		in.Limit = 10
	}
	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()
	result, err := s.cfg.Indexing.SearchScopedDetailed(ctx, userID(r), r.PathValue("workspaceID"), in.Query, in.Mode, in.Limit, nil, in.Rerank)
	if err != nil {
		var external *indexing.Failure
		if errors.As(err, &external) {
			problem(w, 503, external.Code, "document search dependency unavailable")
			return
		}
		indexFailure(w, err)
		return
	}
	respond(w, 200, map[string]any{"mode": in.Mode, "execution": s.cfg.Indexing.Execution(), "profile_id": s.cfg.Indexing.IndexProfile().ID, "hits": result.Hits, "rerank": result.Rerank, "retrieval_policy_id": result.RetrievalPolicyID})
}
