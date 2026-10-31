package httpapi_test

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"magworkspace/internal/httpapi"
	"magworkspace/internal/indexing"
	"magworkspace/internal/indexqueue"
	"magworkspace/internal/store"
)

func outboxService(x *indexFixture, opts indexing.Options) {
	x.service = indexing.NewService(x.store, x.parser, x.embedder, x.search, x.objects, x.profile, opts)
	x.config.Indexing = x.service
	x.server.Close()
	x.server = httptest.NewServer(httpapi.New(x.store, x.agent, x.config))
}
func indexDelivery(t *testing.T, x *indexFixture) store.IndexDelivery {
	t.Helper()
	ds, err := x.store.PendingIndexDeliveries(context.Background(), x.profile.ID, 256)
	if err != nil || len(ds) != 1 {
		t.Fatalf("deliveries=%v err=%v", ds, err)
	}
	return ds[0]
}
func outboxSQL(t *testing.T, x *indexFixture, query string, args ...any) {
	t.Helper()
	db, err := sql.Open("mysql", x.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}
func TestHTTPIndexOutboxPartialWriteRetryAndDuplicate(t *testing.T) {
	x := newIndexFixture(t)
	outboxService(x, indexing.Options{MaxRetries: 2, RetryBaseDelay: time.Hour})
	owner := x.register(t, "outbox-retry")
	doc := x.upload(t, owner, "结构化知识库可靠索引")
	d := indexDelivery(t, x)
	ctx := context.Background()
	outboxSQL(t, x, `CREATE TRIGGER fail_chunk_publish BEFORE INSERT ON chunks FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='chunk write outage'`)
	if err := x.service.ProcessDelivery(ctx, d.JobID, d.ID); err != nil {
		t.Fatal(err)
	}
	job := x.job(t, owner, doc)
	if job.Status != "queued" || job.ErrorCode != "publication_unavailable" || job.Attempts != 1 {
		t.Fatalf("retry state %+v", job)
	}
	if got := x.find(t, owner.Token, owner.Workspaces[0].ID, "hybrid").Hits; len(got) != 0 {
		t.Fatal("unpublished partial data searchable")
	}
	if work, err := x.service.ProcessOne(ctx); err != nil || work {
		t.Fatal("backoff bypassed", work, err)
	}
	if err := x.service.ProcessDelivery(ctx, d.JobID, d.ID); err != nil {
		t.Fatal(err)
	}
	if x.embedder.calls.Load() != 1 {
		t.Fatal("stale delivery rebuilt")
	}
	outboxSQL(t, x, "DROP TRIGGER fail_chunk_publish")
	outboxSQL(t, x, "UPDATE index_outbox SET next_attempt_at=UTC_TIMESTAMP(6),next_dispatch_at=UTC_TIMESTAMP(6)")
	next := indexDelivery(t, x)
	if next.ID == d.ID {
		t.Fatal("retry reused delivery identity")
	}
	if err := x.service.ProcessDelivery(ctx, next.JobID, next.ID); err != nil {
		t.Fatal(err)
	}
	job = x.job(t, owner, doc)
	if job.Status != "ready" || job.Attempts != 2 {
		t.Fatal(job)
	}
	if err := x.service.ProcessDelivery(ctx, next.JobID, next.ID); err != nil {
		t.Fatal(err)
	}
	if x.embedder.calls.Load() != 2 {
		t.Fatal("lost ACK rebuilt ready index")
	}
	if got := x.find(t, owner.Token, owner.Workspaces[0].ID, "hybrid").Hits; len(got) != 1 {
		t.Fatal("published generation missing", got)
	}
	writes, _, _ := x.search.snapshot()
	if len(writes) != 2 || writes[0].generation == writes[1].generation {
		t.Fatal("generation not fenced")
	}
	x.expireCleanup(t, writes[0].generation)
	if err := x.service.CleanupOnce(ctx); err != nil {
		t.Fatal(err)
	}
	writes, _, _ = x.search.snapshot()
	if len(writes) != 1 || writes[0].generation != job.Generation {
		t.Fatal("cleanup lost current generation")
	}
}

func TestHTTPIndexOutboxTerminalAndDeletedRetry(t *testing.T) {
	for _, code := range []string{"embedding_unavailable", "embedding_invalid"} {
		t.Run(code, func(t *testing.T) {
			x := newIndexFixture(t)
			outboxService(x, indexing.Options{MaxRetries: 1, RetryBaseDelay: time.Millisecond})
			owner := x.register(t, "outbox-terminal")
			doc := x.upload(t, owner, "知识库失败预算")
			x.search.setUpsert(func(context.Context) error { return &indexing.Failure{Code: code} })
			ctx := context.Background()
			d := indexDelivery(t, x)
			if err := x.service.ProcessDelivery(ctx, d.JobID, d.ID); err != nil {
				t.Fatal(err)
			}
			if code == "embedding_unavailable" {
				outboxSQL(t, x, "UPDATE index_outbox SET next_attempt_at=UTC_TIMESTAMP(6),next_dispatch_at=UTC_TIMESTAMP(6)")
				d = indexDelivery(t, x)
				if err := x.service.ProcessDelivery(ctx, d.JobID, d.ID); err != nil {
					t.Fatal(err)
				}
			}
			job := x.job(t, owner, doc)
			if job.Status != "failed" || job.ErrorCode != code {
				t.Fatal(job)
			}
			ds, err := x.store.PendingIndexDeliveries(ctx, x.profile.ID, 256)
			if err != nil || len(ds) != 0 {
				t.Fatal("terminal revived", ds, err)
			}
		})
	}
	x := newIndexFixture(t)
	outboxService(x, indexing.Options{MaxRetries: 3})
	owner := x.register(t, "outbox-delete")
	doc := x.upload(t, owner, "删除后不能恢复旧文档")
	d := indexDelivery(t, x)
	x.request(t, "DELETE", documentsPath(doc.Document.WorkspaceID)+"/"+doc.Document.ID, owner.Token, nil, 204)
	if err := x.service.ProcessDelivery(context.Background(), d.JobID, d.ID); err != nil {
		t.Fatal(err)
	}
	if x.embedder.calls.Load() != 0 {
		t.Fatal("deleted doc processed")
	}
}

func TestHTTPIndexOutboxAsynqProductionLoop(t *testing.T) {
	addr := os.Getenv("MAGWORKSPACE_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("isolated Redis required")
	}
	x := newIndexFixture(t)
	q := indexqueue.New(addr, "", x.profile.ID, 2, 15*time.Second)
	t.Cleanup(func() { q.Close() })
	outboxService(x, indexing.Options{Queue: q, MaxRetries: 2, RetryBaseDelay: time.Millisecond, PollInterval: 20 * time.Millisecond, JobTimeout: 10 * time.Second})
	owner := x.register(t, "asynq-loop")
	doc := x.upload(t, owner, "Asynq可靠异步索引")
	var failures atomic.Int32
	x.search.setUpsert(func(context.Context) error {
		if failures.Add(1) == 1 {
			return &indexing.Failure{Code: "qdrant_unavailable"}
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); x.service.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			t.Error("worker did not stop")
		}
	})
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		job := x.job(t, owner, doc)
		if job.Status == "ready" {
			if job.Attempts != 2 {
				t.Fatal(job)
			}
			return
		}
		if job.Status == "failed" {
			t.Fatal(job)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("production Asynq worker did not recover transient failure")
}

func TestHTTPIndexOutboxPublicationFailureRecovers(t *testing.T) {
	x := newIndexFixture(t)
	outboxService(x, indexing.Options{MaxRetries: 2, RetryBaseDelay: time.Millisecond})
	owner := x.register(t, "outbox-publish")
	doc := x.upload(t, owner, "知识库发布事务恢复")
	outboxSQL(t, x, `CREATE TRIGGER fail_index_publish BEFORE UPDATE ON document_indexes FOR EACH ROW BEGIN IF NEW.status='ready' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='publication test outage'; END IF; END`)
	d := indexDelivery(t, x)
	if err := x.service.ProcessDelivery(context.Background(), d.JobID, d.ID); err != nil {
		t.Fatal(err)
	}
	job := x.job(t, owner, doc)
	if job.Status != "queued" || job.ErrorCode != "publication_unavailable" {
		t.Fatalf("publication error incorrectly terminal: %+v", job)
	}
	outboxSQL(t, x, "DROP TRIGGER fail_index_publish")
	outboxSQL(t, x, "UPDATE index_outbox SET next_attempt_at=UTC_TIMESTAMP(6),next_dispatch_at=UTC_TIMESTAMP(6)")
	d = indexDelivery(t, x)
	if err := x.service.ProcessDelivery(context.Background(), d.JobID, d.ID); err != nil {
		t.Fatal(err)
	}
	if job = x.job(t, owner, doc); job.Status != "ready" || job.Attempts != 2 {
		t.Fatal(job)
	}
}

type oneFailedCleanup struct {
	*indexTestSearch
	bad string
}

func (s *oneFailedCleanup) DeleteGeneration(ctx context.Context, profile, generation string) error {
	if generation == s.bad {
		return &indexing.Failure{Code: "qdrant_unavailable"}
	}
	return s.indexTestSearch.DeleteGeneration(ctx, profile, generation)
}
func TestHTTPIndexOutboxCleanupContinuesAfterFailure(t *testing.T) {
	x := newIndexFixture(t)
	ctx := context.Background()
	bad, good := strings.Repeat("a", 32), strings.Repeat("b", 32)
	if err := x.store.QueueIndexCleanup(ctx, x.profile.StorageID, x.profile.ID, bad); err != nil {
		t.Fatal(err)
	}
	if err := x.store.QueueIndexCleanup(ctx, x.profile.StorageID, x.profile.ID, good); err != nil {
		t.Fatal(err)
	}
	outboxSQL(t, x, "UPDATE index_cleanup SET not_before=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 2 SECOND) WHERE generation=?", bad)
	outboxSQL(t, x, "UPDATE index_cleanup SET not_before=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE generation=?", good)
	s := indexing.NewService(x.store, x.parser, x.embedder, &oneFailedCleanup{x.search, bad}, x.objects, x.profile, indexing.Options{})
	if err := s.CleanupOnce(ctx); err == nil {
		t.Fatal("failure hidden")
	}
	_, _, deleted := x.search.snapshot()
	if len(deleted) != 1 || deleted[0] != good {
		t.Fatal("bad cleanup blocked next", deleted)
	}
	pending, err := x.store.PendingIndexCleanup(ctx, 16)
	if err != nil || len(pending) != 0 {
		t.Fatal("failed cleanup hot loops", pending, err)
	}
}
