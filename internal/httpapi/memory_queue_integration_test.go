package httpapi_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hibiken/asynq"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
	"magworkspace/internal/memorycapture"
	"magworkspace/internal/store"
	"magworkspace/internal/taskqueue"
)

// The HTTP and SQL boundaries are real. Every test uses its own queue so loss
// injection and cleanup never flush Redis or affect another worker's deliveries.
type memoryQueueFixture struct {
	addr, name string
	inspector  *asynq.Inspector
}

func newMemoryQueueFixture(t *testing.T) *memoryQueueFixture {
	t.Helper()
	addr := os.Getenv("MAGWORKSPACE_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("MAGWORKSPACE_TEST_REDIS_ADDR is unset; memory Asynq integration not run")
	}
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		t.Fatal(err)
	}
	mq := &memoryQueueFixture{addr: addr, name: "mw-memory-test-" + hex.EncodeToString(entropy[:]), inspector: asynq.NewInspector(asynq.RedisClientOpt{Addr: addr})}
	t.Cleanup(func() {
		defer mq.inspector.Close()
		if err := mq.inspector.DeleteQueue(mq.name, true); err != nil && !errors.Is(err, asynq.ErrQueueNotFound) {
			t.Error("could not remove isolated memory queue")
		}
	})
	return mq
}

func (mq *memoryQueueFixture) newQueue(t *testing.T) *taskqueue.Queue {
	t.Helper()
	q := taskqueue.New(mq.addr, "", mq.name, "magworkspace:memory-capture", 1, 5*time.Second)
	t.Cleanup(func() {
		if err := q.Close(); err != nil {
			t.Error(err)
		}
	})
	return q
}

func startMemoryQueueWorker(t *testing.T, worker *memorycapture.Service) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); worker.Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(15 * time.Second):
				t.Error("memory worker did not shut down")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func waitMemoryCaptureStatus(t *testing.T, f *integrationFixture, path, token, want string) domain.MemoryCaptureStatus {
	t.Helper()
	var last domain.MemoryCaptureStatus
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		last = integrationData[domain.MemoryCaptureStatus](t, f.request(t, "GET", path, token, nil, 200))
		if last.Status == want {
			return last
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("memory capture status=%+v, want %s", last, want)
	return last
}

func memoryQueueDB(t *testing.T, f *integrationFixture) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func memoryQueueExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

func memoryQueueCount(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func memoryQueueDelivery(t *testing.T, f *integrationFixture) store.MemoryCaptureDelivery {
	t.Helper()
	deliveries, err := f.store.PendingMemoryCaptureDeliveries(context.Background(), 32)
	if err != nil || len(deliveries) != 1 {
		t.Fatalf("pending memory deliveries=%d err=%v", len(deliveries), err)
	}
	return deliveries[0]
}

func (mq *memoryQueueFixture) waitAbsent(t *testing.T, id string) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		_, err := mq.inspector.GetTaskInfo(mq.name, id)
		if errors.Is(err, asynq.ErrTaskNotFound) || errors.Is(err, asynq.ErrQueueNotFound) {
			return
		}
		if err != nil {
			t.Fatal("memory queue inspection failed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("delivery was not acknowledged and removed from Asynq")
}

func TestHTTPMemoryQueueLostDeliveryRepublishedAfterRestart(t *testing.T) {
	mq := newMemoryQueueFixture(t)
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	db := memoryQueueDB(t, f)
	owner := f.register(t, "memory-queue-loss")
	wid := owner.Workspaces[0].ID
	session := f.session(t, owner, wid, "durable memory source")
	const query = "我平时使用 Go 编写个人文档工具。"
	run := integrationData[recoveryHTTPResult](t, f.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": query}, 201)).Run
	delivery := memoryQueueDelivery(t, f)
	var calls atomic.Int32
	extractor := captureExtractorFunc(func(_ context.Context, source string) ([]agent.ExtractionItem, error) {
		calls.Add(1)
		if source != query {
			t.Error("capture included assistant text or unrelated context")
		}
		return []agent.ExtractionItem{{Kind: "fact", Content: "用户使用 Go 编写个人文档工具。", Topic: "development", Importance: 3, Evidence: query}}, nil
	})
	q := mq.newQueue(t)
	worker := memorycapture.NewService(f.store, extractor, memorycapture.Options{Queue: q})
	if err := worker.DispatchOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	info, err := mq.inspector.GetTaskInfo(mq.name, delivery.ID)
	if err != nil {
		t.Fatal("durable delivery was not published")
	}
	var payload map[string]string
	if json.Unmarshal(info.Payload, &payload) != nil || len(payload) != 2 || payload["job_id"] != run.ID || payload["delivery_id"] != delivery.ID || strings.Contains(string(info.Payload), query) {
		t.Fatal("Redis task contains more than the durable job and delivery identifiers")
	}
	if worked, err := worker.ProcessOne(context.Background()); err != nil || worked || calls.Load() != 0 {
		t.Fatal("queue-configured service bypassed Asynq with direct database polling")
	}
	if err := mq.inspector.DeleteTask(mq.name, delivery.ID); err != nil {
		t.Fatal(err)
	}
	if count := memoryQueueCount(t, db, "SELECT COUNT(*) FROM memory_capture_outbox WHERE run_id=?", run.ID); count != 1 {
		t.Fatal("enqueue consumed durable dispatch intent")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	// Redis lost its pending task after an acknowledged enqueue. The database
	// reconciliation deadline, rather than a fresh chat turn, makes it eligible.
	memoryQueueExec(t, db, "UPDATE memory_capture_outbox SET next_dispatch_at=UTC_TIMESTAMP(6) WHERE run_id=?", run.ID)
	q = mq.newQueue(t)
	worker = memorycapture.NewService(f.store, extractor, memorycapture.Options{Queue: q, PollInterval: 20 * time.Millisecond})
	startMemoryQueueWorker(t, worker)
	status := waitMemoryCaptureStatus(t, f, captureStatusPath(session, run), owner.Token, "done")
	if status.SavedCount != 1 || calls.Load() != 1 {
		t.Fatalf("capture status=%+v calls=%d", status, calls.Load())
	}
	mq.waitAbsent(t, delivery.ID)
	// A lost completion ACK can replay the same transport delivery. MySQL must
	// refuse it even after Redis has discarded its task-level deduplication key.
	if err := q.Enqueue(context.Background(), delivery.ID, delivery.JobID); err != nil {
		t.Fatal(err)
	}
	mq.waitAbsent(t, delivery.ID)
	items := integrationData[[]domain.Memory](t, f.request(t, "GET", memoryPath(wid), owner.Token, nil, 200))
	if calls.Load() != 1 || len(items) != 1 || items[0].SourceRunID != run.ID || items[0].Origin != "automatic" {
		t.Fatal("duplicate delivery repeated extraction or changed authoritative provenance")
	}
	if count := memoryQueueCount(t, db, "SELECT COUNT(*) FROM memory_capture_outbox WHERE run_id=?", run.ID); count != 0 {
		t.Fatal("completed capture retained a dispatch intent")
	}
}

func TestHTTPMemoryQueueModelFailureUsesDatabaseRetryBudget(t *testing.T) {
	mq := newMemoryQueueFixture(t)
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	db := memoryQueueDB(t, f)
	owner := f.register(t, "memory-queue-budget")
	session := f.session(t, owner, owner.Workspaces[0].ID, "retry source")
	const query = "我偏好简短的中文解释，并使用 Go 开发。"
	run := integrationData[recoveryHTTPResult](t, f.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": query}, 201)).Run
	var calls atomic.Int32
	q := mq.newQueue(t)
	worker := memorycapture.NewService(f.store, captureExtractorFunc(func(context.Context, string) ([]agent.ExtractionItem, error) {
		calls.Add(1)
		return nil, fmt.Errorf("%w: provider echoed private input %s", agent.ErrUnavailable, query)
	}), memorycapture.Options{Queue: q, PollInterval: time.Hour})
	delivery := memoryQueueDelivery(t, f)
	if err := worker.DispatchOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	startMemoryQueueWorker(t, worker)
	for attempt := 1; attempt <= 3; attempt++ {
		var status, code string
		var attempts int
		for deadline := time.Now().Add(10 * time.Second); ; {
			if err := db.QueryRow("SELECT status,error_code,attempt FROM memory_capture_jobs WHERE run_id=?", run.ID).Scan(&status, &code, &attempts); err != nil {
				t.Fatal(err)
			}
			if attempts == attempt && code == "extraction_unavailable" && status != "running" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("model failure receipt: status=%s code=%s attempt=%d", status, code, attempts)
			}
			time.Sleep(20 * time.Millisecond)
		}
		mq.waitAbsent(t, delivery.ID)
		want := "queued"
		if attempt == 3 {
			want = "failed"
		}
		response := f.request(t, "GET", captureStatusPath(session, run), owner.Token, nil, 200)
		if strings.Contains(string(response.Data), query) || strings.Contains(string(response.Data), "provider echoed private input") {
			t.Fatal("capture status exposed private model error text")
		}
		receipt := integrationData[domain.MemoryCaptureStatus](t, response)
		if receipt.Status != want || receipt.ErrorCode != "extraction_unavailable" || calls.Load() != int32(attempt) {
			t.Fatalf("API/SQL retry state disagrees: %+v calls=%d", receipt, calls.Load())
		}
		if err := q.Enqueue(context.Background(), delivery.ID, delivery.JobID); err != nil {
			t.Fatal(err)
		}
		mq.waitAbsent(t, delivery.ID)
		if calls.Load() != int32(attempt) {
			t.Fatal("Asynq redelivery spent another database retry attempt")
		}
		if attempt < 3 {
			memoryQueueExec(t, db, "UPDATE memory_capture_jobs SET next_attempt_at=UTC_TIMESTAMP(6) WHERE run_id=?", run.ID)
			memoryQueueExec(t, db, "UPDATE memory_capture_outbox SET next_dispatch_at=UTC_TIMESTAMP(6) WHERE run_id=?", run.ID)
			next := memoryQueueDelivery(t, f)
			if next.ID == delivery.ID {
				t.Fatal("database retry reused an old delivery token")
			}
			delivery = next
			if err := worker.DispatchOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
	info, err := mq.inspector.GetQueueInfo(mq.name)
	if err != nil || info.Retry != 0 || info.Archived != 0 || info.FailedTotal != 0 {
		t.Fatal("handled model failures created an independent Asynq retry budget")
	}
	if count := memoryQueueCount(t, db, "SELECT COUNT(*) FROM memory_capture_outbox WHERE run_id=?", run.ID); count != 0 {
		t.Fatal("terminal model failure retained a delivery")
	}
	persisted := integrationData[domain.Run](t, f.request(t, "GET", sessionPath(session)+"/runs/"+run.ID, owner.Token, nil, 200))
	if persisted.Status != "completed" {
		t.Fatal("memory failure changed the successful chat response")
	}
}

func TestHTTPMemoryQueueLateOutputCannotRestoreInvalidatedMemory(t *testing.T) {
	for _, action := range []string{"disable", "clear", "forget", "revoke", "delete_session"} {
		t.Run(action, func(t *testing.T) {
			mq := newMemoryQueueFixture(t)
			f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
			db := memoryQueueDB(t, f)
			owner := f.register(t, "memory-queue-authority")
			actor := owner
			wid := owner.Workspaces[0].ID
			if action == "revoke" {
				actor = f.register(t, "memory-queue-member")
				f.request(t, "POST", workspacePath(wid)+"/members", owner.Token, map[string]string{"email": actor.User.Email, "role": "viewer"}, 201)
			}
			const remembered = "用户平时使用 Go 开发工具。"
			var forgotten domain.Memory
			if action == "forget" {
				forgotten = integrationData[domain.Memory](t, f.request(t, "POST", memoryPath(wid), actor.Token, map[string]any{"kind": "fact", "content": remembered, "topic": "development"}, 201))
			}
			session := f.session(t, actor, wid, "revocable source")
			const query = "我平时使用 Go 开发工具。"
			run := integrationData[recoveryHTTPResult](t, f.request(t, "POST", sessionPath(session)+"/messages", actor.Token, map[string]string{"content": query}, 201)).Run
			delivery := memoryQueueDelivery(t, f)
			started, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			worker := memorycapture.NewService(f.store, captureExtractorFunc(func(ctx context.Context, source string) ([]agent.ExtractionItem, error) {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				return []agent.ExtractionItem{{Kind: "fact", Content: remembered, Topic: "development", Importance: 3, Evidence: query}}, nil
			}), memorycapture.Options{Queue: mq.newQueue(t), PollInterval: 20 * time.Millisecond})
			startMemoryQueueWorker(t, worker)
			select {
			case <-started:
			case <-time.After(10 * time.Second):
				t.Fatal("queued extraction did not start")
			}
			switch action {
			case "disable":
				f.request(t, "PUT", workspacePath(wid)+"/memory-settings", actor.Token, map[string]bool{"enabled": false}, 200)
			case "clear":
				f.request(t, "DELETE", memoryPath(wid), actor.Token, map[string]bool{"confirm": true}, 204)
			case "forget":
				f.request(t, "DELETE", memoryPath(wid)+"/"+forgotten.ID, actor.Token, nil, 204)
			case "revoke":
				f.request(t, "DELETE", workspacePath(wid)+"/members/"+actor.User.ID, owner.Token, nil, 204)
			case "delete_session":
				f.request(t, "DELETE", sessionPath(session), actor.Token, nil, 204)
			}
			releaseOnce.Do(func() { close(release) })
			mq.waitAbsent(t, delivery.ID)
			if count := memoryQueueCount(t, db, "SELECT COUNT(*) FROM user_memories WHERE workspace_id=? AND user_id=? AND status='active'", wid, actor.User.ID); count != 0 {
				t.Fatal("late queued extraction restored invalidated personal memory")
			}
			if count := memoryQueueCount(t, db, "SELECT COUNT(*) FROM memory_capture_outbox WHERE run_id=?", run.ID); count != 0 {
				t.Fatal("invalidated capture retained a dispatch intent")
			}
			path := captureStatusPath(session, run)
			if action == "delete_session" || action == "revoke" {
				f.request(t, "GET", path, actor.Token, nil, 404)
				if action == "delete_session" {
					if count := memoryQueueCount(t, db, "SELECT COUNT(*) FROM memory_capture_jobs WHERE run_id=?", run.ID); count != 0 {
						t.Fatal("deleted source session retained a capture job")
					}
				} else if count := memoryQueueCount(t, db, "SELECT COUNT(*) FROM memory_capture_jobs WHERE run_id=? AND status='cancelled' AND saved_count=0", run.ID); count != 1 {
					t.Fatal("revocation did not cancel the SQL capture receipt")
				}
			} else {
				receipt := waitMemoryCaptureStatus(t, f, path, actor.Token, "cancelled")
				if receipt.SavedCount != 0 {
					t.Fatal("cancelled capture reported saved memories")
				}
			}
		})
	}
}

func TestHTTPMemoryQueueShutdownLeaseRecovery(t *testing.T) {
	mq := newMemoryQueueFixture(t)
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	db := memoryQueueDB(t, f)
	owner := f.register(t, "memory-queue-shutdown")
	wid := owner.Workspaces[0].ID
	session := f.session(t, owner, wid, "shutdown source")
	const query = "我使用 Go 维护自己的知识库。"
	run := integrationData[recoveryHTTPResult](t, f.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": query}, 201)).Run
	delivery := memoryQueueDelivery(t, f)
	started := make(chan struct{})
	var calls atomic.Int32
	extractor := captureExtractorFunc(func(ctx context.Context, source string) ([]agent.ExtractionItem, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return []agent.ExtractionItem{{Kind: "fact", Content: "用户使用 Go 维护自己的知识库。", Topic: "development", Importance: 3, Evidence: query}}, nil
	})
	q := mq.newQueue(t)
	worker := memorycapture.NewService(f.store, extractor, memorycapture.Options{Queue: q, PollInterval: 20 * time.Millisecond})
	stop := startMemoryQueueWorker(t, worker)
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("first worker did not start extraction")
	}
	stop()
	var status string
	var attempts int
	var generation uint64
	if err := db.QueryRow("SELECT status,attempt,generation FROM memory_capture_jobs WHERE run_id=?", run.ID).Scan(&status, &attempts, &generation); err != nil {
		t.Fatal(err)
	}
	if status != "running" || attempts != 1 || generation != 1 || memoryQueueCount(t, db, "SELECT COUNT(*) FROM memory_capture_outbox WHERE run_id=?", run.ID) != 1 {
		t.Fatalf("shutdown lost recoverable lease: status=%s attempts=%d generation=%d", status, attempts, generation)
	}
	if count := memoryQueueCount(t, db, "SELECT COUNT(*) FROM user_memories WHERE source_run_id=?", run.ID); count != 0 {
		t.Fatal("cancelled worker committed an extraction")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	// Simulate elapsed lease time, then reconstruct the worker on the same queue.
	// The canceled transport delivery may still be pending/retrying in Asynq.
	memoryQueueExec(t, db, "UPDATE memory_capture_jobs SET lease_expires_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE run_id=?", run.ID)
	memoryQueueExec(t, db, "UPDATE memory_capture_outbox SET next_dispatch_at=UTC_TIMESTAMP(6) WHERE run_id=?", run.ID)
	q = mq.newQueue(t)
	worker = memorycapture.NewService(f.store, extractor, memorycapture.Options{Queue: q, PollInterval: 20 * time.Millisecond})
	startMemoryQueueWorker(t, worker)
	receipt := waitMemoryCaptureStatus(t, f, captureStatusPath(session, run), owner.Token, "done")
	mq.waitAbsent(t, delivery.ID)
	if err := db.QueryRow("SELECT attempt,generation FROM memory_capture_jobs WHERE run_id=?", run.ID).Scan(&attempts, &generation); err != nil {
		t.Fatal(err)
	}
	if receipt.SavedCount != 1 || calls.Load() != 2 || attempts != 2 || generation != 2 || memoryQueueCount(t, db, "SELECT COUNT(*) FROM user_memories WHERE source_run_id=?", run.ID) != 1 {
		t.Fatalf("restart did not fence and recover once: receipt=%+v calls=%d attempts=%d generation=%d", receipt, calls.Load(), attempts, generation)
	}
}
