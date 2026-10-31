package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
	"magworkspace/internal/memorycapture"
	"magworkspace/internal/taskqueue"
)

type captureExtractorFunc func(context.Context, string) ([]agent.ExtractionItem, error)

func (fn captureExtractorFunc) ExtractMemories(ctx context.Context, q string) ([]agent.ExtractionItem, error) {
	return fn(ctx, q)
}

func captureStatusPath(s domain.Session, r domain.Run) string {
	return sessionPath(s) + "/runs/" + r.ID + "/memory"
}

func TestHTTPMemoryCaptureFastAPIWorkerAndCrossSessionRecall(t *testing.T) {
	mq := newMemoryQueueFixture(t)
	x := newIndexFixture(t)
	owner := x.register(t, "capture-owner")
	other := x.register(t, "capture-other")
	wid := owner.Workspaces[0].ID
	x.request(t, "POST", workspacePath(wid)+"/members", owner.Token, map[string]string{"email": other.User.Email, "role": "viewer"}, 201)
	const query = "我偏好简短的中文回答，先给结论。"
	const remembered = "用户偏好简短中文回答，先给结论。"
	var extractions, recalled atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		in := harnessHTTPModelInput(t, r)
		if len(in.Messages) > 0 && strings.Contains(in.Messages[0].Content, "MEMORY_EXTRACTION_V1") {
			extractions.Add(1)
			if len(in.Messages) != 2 || in.Messages[1].Role != "user" || in.Messages[1].Content != query {
				t.Error("extractor received history or non-user input")
			}
			raw, _ := json.Marshal(map[string]any{"items": []agent.ExtractionItem{{Kind: "preference", Content: remembered, Topic: "response_style", Importance: 4, Evidence: query}}})
			harnessHTTPModelReply(w, in, string(raw), "", "")
			return
		}
		for _, m := range in.Messages {
			if m.Role == "system" && strings.Contains(m.Content, remembered) {
				recalled.Add(1)
			}
		}
		harnessHTTPModelReply(w, in, "本轮已回答；记忆状态以后台处理结果为准。", "", "")
	}))
	defer provider.Close()
	callback := "http://" + harnessHTTPAddress(t)
	service := newHarnessHTTPService(t, provider.URL, callback)
	client := harnessHTTPClient(t, service.url, callback)
	installHarnessHTTPClient(x, client)
	first := x.session(t, owner, wid, "capture first")
	result := integrationData[recoveryHTTPResult](t, x.request(t, "POST", sessionPath(first)+"/messages", owner.Token, map[string]string{"content": query}, 201))
	path := captureStatusPath(first, result.Run)
	status := integrationData[domain.MemoryCaptureStatus](t, x.request(t, "GET", path, owner.Token, nil, 200))
	if status.Status != "queued" || extractions.Load() != 0 {
		t.Fatal("capture blocked response or missing durable job")
	}
	x.request(t, "GET", path, other.Token, nil, 404)
	x.request(t, "GET", path, "", nil, 401)
	worker := memorycapture.NewService(x.store, client, memorycapture.Options{Queue: mq.newQueue(t), PollInterval: 20 * time.Millisecond})
	stop := startMemoryQueueWorker(t, worker)
	status = waitMemoryCaptureStatus(t, x.integrationFixture, path, owner.Token, "done")
	if status.Status != "done" || status.SavedCount != 1 || extractions.Load() != 1 {
		t.Fatalf("capture receipt: %+v calls=%d", status, extractions.Load())
	}
	items := integrationData[[]domain.Memory](t, x.request(t, "GET", memoryPath(wid), owner.Token, nil, 200))
	if len(items) != 1 || items[0].Origin != "automatic" || items[0].SourceRunID != result.Run.ID {
		t.Fatal("missing authoritative provenance")
	}
	// Same database, new Go router and Python process; a different Session recalls it.
	stop()
	service.stop()
	service.start()
	x.restart(t)
	installHarnessHTTPClient(x, client)
	second := x.session(t, owner, wid, "capture recall")
	secondResult := harnessHTTPCompletedSSE(t, x, sessionPath(second)+"/messages", owner.Token, map[string]string{"content": "介绍一下工作台。"})
	if recalled.Load() != 1 {
		t.Fatal("automatic memory not injected into fresh session after restart")
	}
	if s := integrationData[domain.MemoryCaptureStatus](t, x.request(t, "GET", captureStatusPath(second, secondResult.Run), owner.Token, nil, 200)); s.Status != "queued" {
		t.Fatal("SSE completion did not enqueue capture")
	}
}

func TestHTTPMemoryCaptureExplicitBypassAndLateClear(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	owner := f.register(t, "capture-explicit")
	wid := owner.Workspaces[0].ID
	session := f.session(t, owner, wid, "capture lifecycle")
	run := integrationData[recoveryHTTPResult](t, f.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "请记住：回答优先使用中文。"}, 201)).Run
	var calls atomic.Int32
	worker := memorycapture.NewService(f.store, captureExtractorFunc(func(context.Context, string) ([]agent.ExtractionItem, error) {
		calls.Add(1)
		return nil, errors.New("extractor must not be called")
	}), memorycapture.Options{})
	if worked, err := worker.ProcessOne(context.Background()); !worked || err != nil || calls.Load() != 0 {
		t.Fatalf("explicit used extractor: %v %v", worked, err)
	}
	status := integrationData[domain.MemoryCaptureStatus](t, f.request(t, "GET", captureStatusPath(session, run), owner.Token, nil, 200))
	if status.Status != "done" || status.SavedCount != 1 {
		t.Fatalf("explicit not persisted: %+v", status)
	}
	items := integrationData[[]domain.Memory](t, f.request(t, "GET", memoryPath(wid), owner.Token, nil, 200))
	if len(items) != 1 || items[0].Origin != "explicit" {
		t.Fatal("explicit provenance absent")
	}
	const query = "我长期使用 Go 开发知识库。"
	run = integrationData[recoveryHTTPResult](t, f.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": query}, 201)).Run
	started, release := make(chan struct{}), make(chan struct{})
	worker = memorycapture.NewService(f.store, captureExtractorFunc(func(ctx context.Context, q string) ([]agent.ExtractionItem, error) {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return []agent.ExtractionItem{{Kind: "fact", Content: "用户长期用 Go 开发知识库。", Topic: "development", Importance: 3, Evidence: query}}, nil
	}), memorycapture.Options{})
	done := make(chan error, 1)
	go func() { _, err := worker.ProcessOne(context.Background()); done <- err }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}
	f.request(t, "DELETE", memoryPath(wid), owner.Token, map[string]bool{"confirm": true}, 204)
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not finish")
	}
	status = integrationData[domain.MemoryCaptureStatus](t, f.request(t, "GET", captureStatusPath(session, run), owner.Token, nil, 200))
	if status.Status != "cancelled" {
		t.Fatalf("late extraction not invalidated: %+v", status)
	}
	items = integrationData[[]domain.Memory](t, f.request(t, "GET", memoryPath(wid), owner.Token, nil, 200))
	if len(items) != 0 {
		t.Fatal("late paraphrase restored cleared memory")
	}
}

func TestHTTPMemoryCaptureModelFailurePreservesAnswerAndPollingRetry(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	owner := f.register(t, "capture-failure")
	session := f.session(t, owner, owner.Workspaces[0].ID, "capture failure")
	run := integrationData[recoveryHTTPResult](t, f.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "我主要使用 Go 开发个人项目。"}, 201)).Run
	worker := memorycapture.NewService(f.store, captureExtractorFunc(func(context.Context, string) ([]agent.ExtractionItem, error) { return nil, agent.ErrUnavailable }), memorycapture.Options{})
	if worked, err := worker.ProcessOne(context.Background()); !worked || !errors.Is(err, agent.ErrUnavailable) {
		t.Fatalf("polling retry failed: %v %v", worked, err)
	}
	status := integrationData[domain.MemoryCaptureStatus](t, f.request(t, "GET", captureStatusPath(session, run), owner.Token, nil, 200))
	if status.Status != "queued" || status.ErrorCode != "extraction_unavailable" {
		t.Fatalf("missing retry state: %+v", status)
	}
	persisted := integrationData[domain.Run](t, f.request(t, "GET", sessionPath(session)+"/runs/"+run.ID, owner.Token, nil, 200))
	if persisted.Status != "completed" {
		t.Fatal("memory extraction failure changed completed chat")
	}
}

func TestHTTPMemoryCaptureRedisOutageRetainsDurableIntent(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	owner := f.register(t, "capture-outage")
	session := f.session(t, owner, owner.Workspaces[0].ID, "queued during Redis outage")
	run := integrationData[recoveryHTTPResult](t, f.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "我主要使用 Go 开发个人项目。"}, 201)).Run
	before := memoryQueueDelivery(t, f)
	q := taskqueue.New("127.0.0.1:1", "", "mw-memory-outage-test", "magworkspace:memory-capture", 1, 30*time.Second)
	defer q.Close()
	var calls atomic.Int32
	worker := memorycapture.NewService(f.store, captureExtractorFunc(func(context.Context, string) ([]agent.ExtractionItem, error) {
		calls.Add(1)
		return nil, nil
	}), memorycapture.Options{Queue: q})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := worker.DispatchOnce(ctx); err == nil {
		t.Fatal("unavailable queue unexpectedly accepted delivery")
	}
	if worked, err := worker.ProcessOne(context.Background()); err != nil || worked || calls.Load() != 0 {
		t.Fatal("Redis outage bypassed configured task queue")
	}
	after := memoryQueueDelivery(t, f)
	if before != after {
		t.Fatal("failed enqueue consumed or replaced durable intent")
	}
	status := integrationData[domain.MemoryCaptureStatus](t, f.request(t, "GET", captureStatusPath(session, run), owner.Token, nil, 200))
	if status.Status != "queued" || status.ErrorCode != "" {
		t.Fatalf("unexpected status during outage: %+v", status)
	}
	persisted := integrationData[domain.Run](t, f.request(t, "GET", sessionPath(session)+"/runs/"+run.ID, owner.Token, nil, 200))
	if persisted.Status != "completed" {
		t.Fatal("queue outage changed completed answer")
	}
}
