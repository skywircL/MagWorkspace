package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/memorycapture"
)

// Use the real FastAPI extraction semaphore: its authenticated admission refusal
// must defer an Asynq delivery without spending the MySQL model-attempt budget.
func TestHTTPMemoryQueueHarnessBusyPreservesModelBudget(t *testing.T) {
	mq := newMemoryQueueFixture(t)
	x := newIndexFixture(t)
	db := memoryQueueDB(t, x.integrationFixture)
	owner := x.register(t, "memory-harness-busy")
	wid := owner.Workspaces[0].ID
	const blockerQuery = "我平时使用 Rust 维护一个独立项目。"
	const query = "我偏好简短的中文解释，并使用 Go 维护知识库。"
	const remembered = "用户偏好简短的中文解释。"
	const answer = "本轮已回答，记忆采集由后台继续处理。"
	started, release := make(chan struct{}), make(chan struct{})
	var startOnce, releaseOnce sync.Once
	var blockers, extractions, chats atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		in := harnessHTTPModelInput(t, r)
		if len(in.Messages) > 0 && strings.Contains(in.Messages[0].Content, "MEMORY_EXTRACTION_V1") {
			if len(in.Messages) != 2 || in.Messages[1].Role != "user" {
				t.Error("extraction received unrelated context")
				http.Error(w, "invalid fixture input", http.StatusBadRequest)
				return
			}
			switch in.Messages[1].Content {
			case blockerQuery:
				blockers.Add(1)
				startOnce.Do(func() { close(started) })
				select {
				case <-release:
					harnessHTTPModelReply(w, in, `{"items":[]}`, "", "")
				case <-r.Context().Done():
				}
			case query:
				extractions.Add(1)
				raw, _ := json.Marshal(map[string]any{"items": []agent.ExtractionItem{{Kind: "preference", Content: remembered, Topic: "response_style", Importance: 4, Evidence: "我偏好简短的中文解释"}}})
				harnessHTTPModelReply(w, in, string(raw), "", "")
			default:
				t.Error("unexpected extraction source")
				http.Error(w, "invalid fixture source", http.StatusBadRequest)
			}
			return
		}
		chats.Add(1)
		harnessHTTPModelReply(w, in, answer, "", "")
	}))
	defer provider.Close()
	defer releaseOnce.Do(func() { close(release) })
	callback := "http://" + harnessHTTPAddress(t)
	service := newHarnessHTTPService(t, provider.URL, callback)
	client := harnessHTTPClient(t, service.url, callback)
	installHarnessHTTPClient(x, client)
	// This second client models another Go instance using the same independent
	// FastAPI extraction slot. Extraction does not need a callback capability.
	blockingClient, err := agent.NewHTTPClient(agent.HTTPConfig{
		URL: service.url, APIKey: harnessHTTPTestKey, Mode: "live", Timeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer blockingClient.Close()
	blockedCtx, cancelBlocker := context.WithCancel(context.Background())
	defer cancelBlocker()
	blocked := make(chan error, 1)
	go func() {
		items, err := blockingClient.ExtractMemories(blockedCtx, blockerQuery)
		if err == nil && len(items) != 0 {
			t.Error("blocking request returned unexpected memories")
		}
		blocked <- err
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("standalone extraction did not acquire the FastAPI slot")
	}
	// Interactive execution has a separate slot and remains available while
	// extraction is busy. Its successful answer atomically creates the outbox.
	session := x.session(t, owner, wid, "capture during extraction congestion")
	completed := integrationData[recoveryHTTPResult](t, x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": query}, 201))
	if completed.Run.Status != "completed" || completed.Answer != answer || chats.Load() != 1 {
		t.Fatal("busy extraction slot blocked or changed the successful chat")
	}
	delivery := memoryQueueDelivery(t, x.integrationFixture)
	q := mq.newQueue(t)
	worker := memorycapture.NewService(x.store, client, memorycapture.Options{Queue: q, PollInterval: time.Hour})
	if err := worker.DispatchOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	startMemoryQueueWorker(t, worker)
	const busyRounds = 3
	for round := 1; round <= busyRounds; round++ {
		var status, code, nextID string
		var attempt, generation, cooldownMicros int
		var releasedLease bool
		for deadline := time.Now().Add(10 * time.Second); ; {
			if err := db.QueryRow(`SELECT j.status,j.error_code,j.attempt,j.generation,o.id,
				TIMESTAMPDIFF(MICROSECOND,j.updated_at,j.next_attempt_at),j.lease_expires_at IS NULL
				FROM memory_capture_jobs j JOIN memory_capture_outbox o ON o.run_id=j.run_id WHERE j.run_id=?`, completed.Run.ID).
				Scan(&status, &code, &attempt, &generation, &nextID, &cooldownMicros, &releasedLease); err != nil {
				t.Fatal(err)
			}
			if status == "queued" && code == "extraction_busy" && generation == round {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("busy receipt round=%d status=%s code=%s attempt=%d generation=%d", round, status, code, attempt, generation)
			}
			time.Sleep(20 * time.Millisecond)
		}
		if attempt != 0 || nextID == delivery.ID || cooldownMicros != 5_000_000 || !releasedLease || extractions.Load() != 0 || blockers.Load() != 1 {
			t.Fatalf("admission refusal consumed model work or lost fencing: attempt=%d generation=%d changed_token=%t cooldown_us=%d lease_released=%t calls=%d blockers=%d", attempt, generation, nextID != delivery.ID, cooldownMicros, releasedLease, extractions.Load(), blockers.Load())
		}
		mq.waitAbsent(t, delivery.ID)
		statusAPI := integrationData[domain.MemoryCaptureStatus](t, x.request(t, "GET", captureStatusPath(session, completed.Run), owner.Token, nil, 200))
		if statusAPI.Status != "queued" || statusAPI.ErrorCode != "extraction_busy" || statusAPI.SavedCount != 0 {
			t.Fatalf("busy receipt is not visible through the authenticated status API: %+v", statusAPI)
		}
		// Redelivery of the already handled task must not consume another claim,
		// even after Redis has removed its task ID deduplication state.
		if err := q.Enqueue(context.Background(), delivery.ID, delivery.JobID); err != nil {
			t.Fatal(err)
		}
		mq.waitAbsent(t, delivery.ID)
		if count := memoryQueueCount(t, db, "SELECT COUNT(*) FROM memory_capture_jobs WHERE run_id=? AND status='queued' AND attempt=0 AND generation=?", completed.Run.ID, round); count != 1 {
			t.Fatal("stale busy delivery acquired the replacement claim")
		}
		delivery.ID = nextID
		if round < busyRounds {
			// Advance only this test job's SQL clock instead of sleeping through
			// three real cooldowns. The production five-second value was checked.
			memoryQueueExec(t, db, "UPDATE memory_capture_jobs SET next_attempt_at=UTC_TIMESTAMP(6) WHERE run_id=?", completed.Run.ID)
			memoryQueueExec(t, db, "UPDATE memory_capture_outbox SET next_dispatch_at=UTC_TIMESTAMP(6) WHERE run_id=?", completed.Run.ID)
			if err := worker.DispatchOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
	info, err := mq.inspector.GetQueueInfo(mq.name)
	if err != nil || info.Retry != 0 || info.Archived != 0 || info.FailedTotal != 0 {
		t.Fatal("handled admission refusals created an independent Asynq retry budget")
	}
	if extractions.Load() != 0 || memoryQueueCount(t, db, "SELECT COUNT(*) FROM user_memories WHERE source_run_id=?", completed.Run.ID) != 0 {
		t.Fatal("busy job invoked the model or saved a memory before admission")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-blocked:
		if err != nil {
			t.Fatalf("blocking extraction did not finish normally: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("released extraction did not free its service slot")
	}
	memoryQueueExec(t, db, "UPDATE memory_capture_jobs SET next_attempt_at=UTC_TIMESTAMP(6) WHERE run_id=?", completed.Run.ID)
	memoryQueueExec(t, db, "UPDATE memory_capture_outbox SET next_dispatch_at=UTC_TIMESTAMP(6) WHERE run_id=?", completed.Run.ID)
	if err := worker.DispatchOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	status := waitMemoryCaptureStatus(t, x.integrationFixture, captureStatusPath(session, completed.Run), owner.Token, "done")
	mq.waitAbsent(t, delivery.ID)
	if status.SavedCount != 1 || extractions.Load() != 1 || memoryQueueCount(t, db, "SELECT COUNT(*) FROM memory_capture_jobs WHERE run_id=? AND status='done' AND attempt=1 AND generation=?", completed.Run.ID, busyRounds+1) != 1 {
		t.Fatalf("available slot did not run one actual model attempt: status=%+v calls=%d", status, extractions.Load())
	}
	if memoryQueueCount(t, db, "SELECT COUNT(*) FROM memory_capture_outbox WHERE run_id=?", completed.Run.ID) != 0 || memoryQueueCount(t, db, "SELECT COUNT(*) FROM user_memories WHERE source_run_id=?", completed.Run.ID) != 1 {
		t.Fatal("completed capture did not atomically publish once and remove its intent")
	}
	memories := integrationData[[]domain.Memory](t, x.request(t, "GET", memoryPath(wid), owner.Token, nil, 200))
	if len(memories) != 1 || memories[0].Content != remembered || memories[0].SourceRunID != completed.Run.ID || memories[0].Origin != "automatic" {
		t.Fatal("busy recovery lost memory contents or authenticated provenance")
	}
	persisted := integrationData[domain.Run](t, x.request(t, "GET", sessionPath(session)+"/runs/"+completed.Run.ID, owner.Token, nil, 200))
	messages := integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, 200))
	if persisted.Status != "completed" || len(messages) != 2 || messages[0].Content != query || messages[1].Content != answer || chats.Load() != 1 {
		t.Fatal("memory admission retries changed or repeated the completed conversation")
	}
}
