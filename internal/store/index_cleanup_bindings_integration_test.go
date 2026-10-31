package store

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

func TestMySQLIndexCleanupClaimsBindStorageAndFenceOldWorkers(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	p := testIndexProfile()
	generation := strings.Repeat("1", 32)
	unknown := strings.Repeat("2", 32)
	must(t, s.QueueIndexCleanup(ctx, "", p.ID, unknown))
	must(t, s.QueueIndexCleanup(ctx, p.StorageID, p.ID, generation))
	expireTestIndexCleanup(t, s, unknown)
	expireTestIndexCleanup(t, s, generation)
	// Configured bindings take priority even behind a backlog of unknown tasks.
	claimed, err := s.ClaimIndexCleanup(ctx, []string{p.StorageID}, 1, time.Minute)
	must(t, err)
	if len(claimed) != 1 || claimed[0].Generation != generation {
		t.Fatal("unknown binding blocked configured cleanup")
	}
	first := claimed[0]
	var wg sync.WaitGroup
	results := make(chan []domain.IndexCleanup, 8)
	errors := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rows, err := s.ClaimIndexCleanup(ctx, []string{p.StorageID}, 1, time.Minute)
			results <- rows
			errors <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		must(t, err)
	}
	count := 0
	for rows := range results {
		for _, row := range rows {
			if row.Generation != unknown {
				t.Fatal("same live cleanup lease claimed twice")
			}
			count++
		}
	}
	if count != 1 {
		t.Fatalf("concurrent unknown claims=%d", count)
	}
	_, err = s.pool.ExecContext(ctx, `UPDATE index_cleanup SET lease_expires_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE id=?`, first.ID)
	must(t, err)
	claimed, err = s.ClaimIndexCleanup(ctx, []string{p.StorageID}, 1, time.Minute)
	must(t, err)
	if len(claimed) != 1 || claimed[0].ID != first.ID || claimed[0].ClaimToken == first.ClaimToken {
		t.Fatal("expired cleanup lease was not fenced")
	}
	second := claimed[0]
	must(t, s.CompleteIndexCleanup(ctx, first.ID, first.ClaimToken))
	must(t, s.DeferIndexCleanup(ctx, first.ID, first.ClaimToken, "qdrant_unavailable", time.Hour))
	var token, code string
	var attempts int
	must(t, s.pool.QueryRowContext(ctx, `SELECT claim_token,last_error_code,attempts FROM index_cleanup WHERE id=?`, second.ID).Scan(&token, &code, &attempts))
	if token != second.ClaimToken || code != "" || attempts != 2 {
		t.Fatal("stale worker changed the current cleanup claim")
	}
	must(t, s.CompleteIndexCleanup(ctx, second.ID, second.ClaimToken))
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM index_cleanup WHERE id=?`, second.ID).Scan(&count))
	if count != 0 {
		t.Fatal("valid owner could not finish cleanup")
	}
}

func TestMySQLIndexCleanupSameGenerationDifferentStorageAndRetirement(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	p := testIndexProfile()
	bindingB := strings.Repeat("c", 64)
	generation := strings.Repeat("3", 32)
	must(t, s.QueueIndexCleanup(ctx, p.StorageID, p.ID, generation))
	must(t, s.QueueIndexCleanup(ctx, bindingB, p.ID, generation))
	expireTestIndexCleanup(t, s, generation)
	claimed, err := s.ClaimIndexCleanup(ctx, []string{p.StorageID, bindingB}, 2, time.Minute)
	must(t, err)
	if len(claimed) != 2 || claimed[0].StorageID == claimed[1].StorageID {
		t.Fatal("different storage targets collapsed into one cleanup")
	}
	for _, item := range claimed {
		must(t, s.CompleteIndexCleanup(ctx, item.ID, item.ClaimToken))
	}
	var count int
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM cleanup_retirements WHERE kind='index' AND profile_id=? AND resource_key=?`, p.ID, generation).Scan(&count))
	if count != 2 {
		t.Fatal("cleanup completion discarded late-write retirement responsibility")
	}
	must(t, s.RequeueRetiredCleanup(ctx, "index"))
	pending, err := s.PendingIndexCleanup(ctx, 10)
	must(t, err)
	if len(pending) != 0 {
		t.Fatal("retirement retried before its scheduled check")
	}
	_, err = s.pool.ExecContext(ctx, `UPDATE cleanup_retirements SET next_check_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE kind='index'`)
	must(t, err)
	must(t, s.RequeueRetiredCleanup(ctx, "index"))
	claimed, err = s.ClaimIndexCleanup(ctx, []string{p.StorageID, bindingB}, 2, time.Minute)
	must(t, err)
	if len(claimed) != 2 {
		t.Fatal("late remote writes would have no scheduled cleanup")
	}
	// A scheduled sweep must not revoke another cleaner's existing token.
	_, err = s.pool.ExecContext(ctx, `UPDATE cleanup_retirements SET next_check_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE kind='index'`)
	must(t, err)
	must(t, s.RequeueRetiredCleanup(ctx, "index"))
	for _, item := range claimed {
		var token string
		must(t, s.pool.QueryRowContext(ctx, `SELECT claim_token FROM index_cleanup WHERE id=?`, item.ID).Scan(&token))
		if token != item.ClaimToken {
			t.Fatal("retirement sweep invalidated an active cleanup")
		}
		must(t, s.CompleteIndexCleanup(ctx, item.ID, item.ClaimToken))
	}
}

func TestMySQLIndexCleanupProtectionAndRetrievalRequireStorageBinding(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "cleanup-binding-publication")
	p := testIndexProfile()
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "bound source", "bound.txt", []byte("source"))
	must(t, err)
	job := publishTestIndex(t, s, u.ID, w.ID, write, p)
	bindingB := strings.Repeat("c", 64)
	must(t, s.QueueIndexCleanup(ctx, p.StorageID, p.ID, job.Generation))
	must(t, s.QueueIndexCleanup(ctx, bindingB, p.ID, job.Generation))
	expireTestIndexCleanup(t, s, job.Generation)
	claimed, err := s.ClaimIndexCleanup(ctx, []string{p.StorageID, bindingB}, 2, time.Minute)
	must(t, err)
	if len(claimed) != 1 || claimed[0].StorageID != bindingB {
		t.Fatal("published generation protected the wrong storage or lost its own protection")
	}
	must(t, s.CompleteIndexCleanup(ctx, claimed[0].ID, claimed[0].ClaimToken))
	chunks, err := s.ReadIndexChunks(ctx, u.ID, w.ID, job, nil)
	must(t, err)
	if len(chunks) != 2 {
		t.Fatal("cleanup on a different store deleted published SQL bodies")
	}
	var child domain.IndexChunk
	for _, chunk := range chunks {
		if chunk.Kind == "child" {
			child = chunk
		}
	}
	candidates := []domain.IndexCandidate{{IndexID: job.ID, Generation: job.Generation, ChunkID: child.ID, Score: 1}}
	hits, err := s.ResolveIndexChunks(ctx, u.ID, w.ID, p.ID, candidates)
	must(t, err)
	if len(hits) != 1 {
		t.Fatal("bound publication was not retrievable")
	}
	_, err = s.pool.ExecContext(ctx, `UPDATE document_indexes SET storage_id='' WHERE id=?`, job.ID)
	must(t, err)
	generations, err := s.SearchIndexScope(ctx, u.ID, w.ID, p.ID)
	must(t, err)
	refs, err := s.ResolveIndexes(ctx, u.ID, w.ID, p.ID, []string{job.ID})
	must(t, err)
	hits, err = s.ResolveIndexChunks(ctx, u.ID, w.ID, p.ID, candidates)
	must(t, err)
	if len(generations)+len(refs)+len(hits) != 0 {
		t.Fatal("unbound publication remained available to retrieval")
	}
}
