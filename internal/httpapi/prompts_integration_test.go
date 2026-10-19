package httpapi_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
	"magworkspace/internal/indexing"
	"magworkspace/internal/store"
)

const promptSnapshotTestSecret = "private-snapshot-content-not-for-api"

type promptHTTPResult struct {
	Run    domain.Run       `json:"run"`
	Prompt domain.RunPrompt `json:"prompt"`
	Answer string           `json:"answer"`
}

func promptTestResult(req agent.Request, id string) agent.Result {
	return agent.Result{RunID: req.RunID, Status: "suspended", Mode: "demo", Prompt: &agent.UserPrompt{
		ID: id, ToolCallID: "ask-" + id, Question: "你指的是国内还是海外差旅？", Options: []string{"国内", "海外"},
		Snapshot: `{"opaque":"` + promptSnapshotTestSecret + `"}`, ExpiresAt: float64(time.Now().Add(time.Hour).Unix()),
	}}
}

func promptPath(session domain.Session, runID, promptID string) string {
	return sessionPath(session) + "/runs/" + runID + "/prompts/" + promptID
}

func promptRequirePrivate(t *testing.T, raw []byte) {
	t.Helper()
	for _, forbidden := range []string{promptSnapshotTestSecret, `"snapshot"`, `"snapshot_key"`, `"snapshot_sha256"`} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("public prompt response leaked private snapshot field %q", forbidden)
		}
	}
}

func promptSuspendHTTP(t *testing.T, x *indexFixture, owner integrationAuth, session domain.Session) promptHTTPResult {
	t.Helper()
	response := x.call("POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "查住宿标准", "document_ids": []string{}})
	promptRequirePrivate(t, response.body)
	result := integrationData[promptHTTPResult](t, checkIntegrationResponse(t, response, 202))
	if result.Run.Status != "suspended" || result.Prompt.ID == "" || result.Prompt.RunID != result.Run.ID || result.Prompt.SessionID != session.ID || result.Prompt.Status != "pending" || result.Answer != "" {
		t.Fatalf("invalid suspended response: %+v", result)
	}
	return result
}

func TestHTTPPromptsDurablePrivateResumeAfterRestart(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "promptrestart")
	session := x.session(t, owner, owner.Workspaces[0].ID, "等待澄清")
	const promptID = "11111111111111111111111111111111"
	x.agent.set(func(_ context.Context, req agent.Request) (agent.Result, error) {
		return promptTestResult(req, promptID), nil
	})
	pending := promptSuspendHTTP(t, x, owner, session)
	path := promptPath(session, pending.Run.ID, promptID)
	x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "不可重叠"}, 409)
	for _, getPath := range []string{path, sessionPath(session) + "/runs/" + pending.Run.ID + "/prompt"} {
		response := x.call("GET", getPath, owner.Token, nil)
		promptRequirePrivate(t, response.body)
		got := integrationData[domain.RunPrompt](t, checkIntegrationResponse(t, response, 200))
		if got.ID != promptID || got.Status != "pending" || len(got.Options) != 2 {
			t.Fatalf("prompt lookup differs from durable question: %+v", got)
		}
	}
	for _, answer := range []any{map[string]string{}, map[string]string{"answer": "   "}, map[string]string{"answer": strings.Repeat("a", 4097)}, map[string]string{"answer": "国内", "user_id": owner.User.ID}} {
		x.request(t, "POST", path+"/answer", owner.Token, answer, 400)
	}
	// Reopen both the HTTP server and database handle. Object storage is the
	// only external boundary retained, as it is in the deployed service.
	x.server.Close()
	x.server = nil
	x.store.Close()
	x.store = nil
	db, err := store.Open(context.Background(), x.dsn, x.profile)
	if err != nil {
		t.Fatal(err)
	}
	x.store = db
	db.SetObjectStorage(x.objects)
	x.service = indexing.NewService(db, x.parser, x.embedder, x.search, x.objects, x.profile, indexing.Options{JobTimeout: 10 * time.Second})
	x.config.Indexing = x.service
	x.agent.set(func(_ context.Context, req agent.Request) (agent.Result, error) {
		if req.Resume == nil || req.RunID != pending.Run.ID || req.UserID != owner.User.ID || req.SessionID != session.ID || req.WorkspaceID != session.WorkspaceID || req.Query != "查住宿标准" || req.DocumentIDs == nil || len(*req.DocumentIDs) != 0 {
			return agent.Result{}, errors.New("resume request lost authenticated identity, original query or explicit empty scope")
		}
		var native struct {
			ID, Kind, Status, Snapshot string
			RunID                      string `json:"run_id"`
			UserID                     string `json:"user_id"`
			Response                   struct{ Answer string }
		}
		if json.Unmarshal(req.Resume.Prompt, &native) != nil || native.ID != promptID || native.Kind != "ask_user" || native.Status != "answered" || native.RunID != pending.Run.ID || native.UserID != owner.User.ID || native.Response.Answer != "国内" || !strings.Contains(native.Snapshot, promptSnapshotTestSecret) {
			return agent.Result{}, errors.New("resume was not reconstructed from private persisted snapshot and user answer")
		}
		return agent.Result{RunID: req.RunID, Mode: "demo", Answer: "国内住宿标准已确认"}, nil
	})
	x.server = httptest.NewServer(httpapi.New(db, x.agent, x.config))
	response := x.call("POST", path+"/answer", owner.Token, map[string]string{"answer": "国内"})
	promptRequirePrivate(t, response.body)
	completed := integrationData[promptHTTPResult](t, checkIntegrationResponse(t, response, 201))
	if completed.Run.ID != pending.Run.ID || completed.Run.Status != "completed" || completed.Answer != "国内住宿标准已确认" {
		t.Fatalf("resume created a different run or failed to commit: %+v", completed)
	}
	got := integrationData[domain.RunPrompt](t, x.request(t, "GET", path, owner.Token, nil, 200))
	if got.Status != "done" || got.Answer != "国内" {
		t.Fatalf("answered prompt not resolved: %+v", got)
	}
	messages := integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, 200))
	if len(messages) != 2 || messages[0].RunID != pending.Run.ID || messages[1].RunID != pending.Run.ID || messages[0].Content != "查住宿标准" || messages[1].Content != completed.Answer {
		t.Fatalf("resume duplicated or replaced persisted messages: %+v", messages)
	}
	x.request(t, "POST", path+"/answer", owner.Token, map[string]string{"answer": "海外"}, 409)
	if len(x.agent.calls()) != 2 {
		t.Fatalf("invalid/replayed answers reached agent: %d calls", len(x.agent.calls()))
	}
	x.agent.set(func(_ context.Context, req agent.Request) (agent.Result, error) {
		if req.Resume != nil || len(req.History) != 2 || req.History[0].Role != "user" || !strings.Contains(req.History[0].Content, "用户答复：国内") || strings.Contains(req.History[0].Content, promptSnapshotTestSecret) {
			return agent.Result{}, errors.New("completed clarification was omitted from later user history or exposed a private snapshot")
		}
		return agent.Result{RunID: req.RunID, Mode: "demo", Answer: "沿用已确认的国内范围"}, nil
	})
	x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "继续刚才的话题"}, 201)
}

func TestHTTPPromptsConcurrentAnswerClaimsOnce(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "promptconcurrent")
	session := x.session(t, owner, owner.Workspaces[0].ID, "仅恢复一次")
	x.agent.set(func(_ context.Context, req agent.Request) (agent.Result, error) {
		return promptTestResult(req, "22222222222222222222222222222222"), nil
	})
	pending := promptSuspendHTTP(t, x, owner, session)
	path := promptPath(session, pending.Run.ID, pending.Prompt.ID) + "/answer"
	entered, release := make(chan struct{}), make(chan struct{})
	var once, enterOnce sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	x.agent.set(func(ctx context.Context, req agent.Request) (agent.Result, error) {
		if req.Resume == nil {
			return agent.Result{}, errors.New("expected claimed resume")
		}
		enterOnce.Do(func() { close(entered) })
		select {
		case <-release:
			return agent.Result{RunID: req.RunID, Mode: "demo", Answer: "唯一完成结果"}, nil
		case <-ctx.Done():
			return agent.Result{}, ctx.Err()
		}
	})
	first := make(chan integrationResponse, 1)
	go func() { first <- x.call("POST", path, owner.Token, map[string]string{"answer": "国内"}) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first answer did not reach resumed execution")
	}
	x.request(t, "POST", path, owner.Token, map[string]string{"answer": "海外"}, 409)
	unblock()
	select {
	case response := <-first:
		checkIntegrationResponse(t, response, 201)
	case <-time.After(5 * time.Second):
		t.Fatal("first resume did not complete")
	}
	if len(x.agent.calls()) != 2 {
		t.Fatalf("concurrent answer invoked agent twice: %+v", x.agent.calls())
	}
}

func TestHTTPPromptsRepeatQuestionCancelAndFreshRun(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "promptcancel")
	session := x.session(t, owner, owner.Workspaces[0].ID, "连续澄清")
	x.agent.set(func(_ context.Context, req agent.Request) (agent.Result, error) {
		id := "33333333333333333333333333333333"
		if req.Resume != nil {
			id = "44444444444444444444444444444444"
		}
		return promptTestResult(req, id), nil
	})
	first := promptSuspendHTTP(t, x, owner, session)
	firstPath := promptPath(session, first.Run.ID, first.Prompt.ID)
	response := x.call("POST", firstPath+"/answer", owner.Token, map[string]string{"answer": "国内"})
	promptRequirePrivate(t, response.body)
	second := integrationData[promptHTTPResult](t, checkIntegrationResponse(t, response, 202))
	if second.Run.ID != first.Run.ID || second.Run.Status != "suspended" || second.Prompt.ID == first.Prompt.ID {
		t.Fatalf("follow-up question lost execution identity: %+v", second)
	}
	old := integrationData[domain.RunPrompt](t, x.request(t, "GET", firstPath, owner.Token, nil, 200))
	if old.Status != "done" || old.Answer != "国内" {
		t.Fatalf("previous answer not preserved: %+v", old)
	}
	x.request(t, "POST", firstPath+"/answer", owner.Token, map[string]string{"answer": "海外"}, 409)
	secondPath := promptPath(session, second.Run.ID, second.Prompt.ID)
	x.request(t, "POST", secondPath+"/cancel", owner.Token, map[string]string{}, 204)
	run := integrationData[domain.Run](t, x.request(t, "GET", sessionPath(session)+"/runs/"+second.Run.ID, owner.Token, nil, 200))
	if run.Status != "failed" || run.ErrorCode != "question_canceled" || run.FinishedAt == nil {
		t.Fatalf("cancel did not persist failure: %+v", run)
	}
	x.request(t, "POST", secondPath+"/answer", owner.Token, map[string]string{"answer": "国内"}, 409)
	x.agent.set(func(_ context.Context, req agent.Request) (agent.Result, error) {
		if len(req.History) != 0 || req.Resume != nil {
			return agent.Result{}, errors.New("canceled partial turn entered later model history")
		}
		return agent.Result{RunID: req.RunID, Mode: "demo", Answer: "新的独立回答"}, nil
	})
	x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "重新开始"}, 201)
}

func TestHTTPPromptsPrivateOwnerAndRevocationOnResume(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "promptworkspaceowner")
	member := x.register(t, "promptmember")
	base := workspacePath(owner.Workspaces[0].ID)
	x.request(t, "POST", base+"/members", owner.Token, map[string]string{"email": member.User.Email, "role": "contributor"}, 201)
	session := x.session(t, member, owner.Workspaces[0].ID, "成员私有问题")
	x.agent.set(func(_ context.Context, req agent.Request) (agent.Result, error) {
		return promptTestResult(req, "55555555555555555555555555555555"), nil
	})
	pending := promptSuspendHTTP(t, x, member, session)
	path := promptPath(session, pending.Run.ID, pending.Prompt.ID)
	for _, lookup := range []string{path, sessionPath(session) + "/runs/" + pending.Run.ID + "/prompt"} {
		x.request(t, "GET", lookup, owner.Token, nil, 404)
	}
	x.request(t, "POST", path+"/answer", owner.Token, map[string]string{"answer": "国内"}, 404)
	x.request(t, "POST", path+"/cancel", owner.Token, map[string]string{}, 404)
	// A suspended run holds no executing tools, so removing its caller's
	// membership is allowed and must prevent later snapshot hydration/resume.
	x.request(t, "DELETE", base+"/members/"+member.User.ID, owner.Token, nil, 204)
	x.request(t, "GET", path, member.Token, nil, 404)
	x.request(t, "POST", path+"/answer", member.Token, map[string]string{"answer": "国内"}, 404)
	if len(x.agent.calls()) != 1 {
		t.Fatal("private or revoked caller reached resumed agent")
	}
}

func TestHTTPPromptsSnapshotIOFailuresDoNotPublishOrConsumeAnswer(t *testing.T) {
	t.Run("upload failure", func(t *testing.T) {
		x := newIndexFixture(t)
		owner := x.register(t, "promptuploadfail")
		session := x.session(t, owner, owner.Workspaces[0].ID, "存储失败")
		x.agent.set(func(_ context.Context, req agent.Request) (agent.Result, error) {
			return promptTestResult(req, "66666666666666666666666666666666"), nil
		})
		x.objects.putHook(func(context.Context, string) error { return blob.ErrUnavailable })
		x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "等待"}, 503)
		calls := x.agent.calls()
		if len(calls) != 1 {
			t.Fatalf("unexpected agent calls: %d", len(calls))
		}
		run := integrationData[domain.Run](t, x.request(t, "GET", sessionPath(session)+"/runs/"+calls[0].RunID, owner.Token, nil, 200))
		if run.Status != "failed" {
			t.Fatalf("failed snapshot upload exposed a resumable run: %+v", run)
		}
		x.request(t, "GET", sessionPath(session)+"/runs/"+run.ID+"/prompt", owner.Token, nil, 404)
	})
	t.Run("read failure and integrity", func(t *testing.T) {
		x := newIndexFixture(t)
		owner := x.register(t, "promptreadfail")
		session := x.session(t, owner, owner.Workspaces[0].ID, "恢复存储校验")
		x.agent.set(func(_ context.Context, req agent.Request) (agent.Result, error) {
			return promptTestResult(req, "77777777777777777777777777777777"), nil
		})
		pending := promptSuspendHTTP(t, x, owner, session)
		path := promptPath(session, pending.Run.ID, pending.Prompt.ID)
		x.objects.readHook(func(context.Context, string) error { return blob.ErrUnavailable })
		x.request(t, "POST", path+"/answer", owner.Token, map[string]string{"answer": "国内"}, 503)
		x.objects.readHook(nil)
		x.objects.mu.Lock()
		for key := range x.objects.data {
			x.objects.data[key] = []byte(`{"opaque":"tampered"}`)
		}
		x.objects.mu.Unlock()
		x.request(t, "POST", path+"/answer", owner.Token, map[string]string{"answer": "国内"}, 503)
		got := integrationData[domain.RunPrompt](t, x.request(t, "GET", path, owner.Token, nil, 200))
		if got.Status != "pending" || got.Answer != "" || len(x.agent.calls()) != 1 {
			t.Fatalf("failed snapshot read consumed answer or ran agent: %+v", got)
		}
	})
}

func TestHTTPPromptsExpiryAndSourceDeletionRejectResume(t *testing.T) {
	t.Run("expired prompt", func(t *testing.T) {
		x := newIndexFixture(t)
		owner := x.register(t, "promptexpired")
		session := x.session(t, owner, owner.Workspaces[0].ID, "过期问题")
		x.agent.set(func(_ context.Context, req agent.Request) (agent.Result, error) {
			return promptTestResult(req, "88888888888888888888888888888888"), nil
		})
		pending := promptSuspendHTTP(t, x, owner, session)
		db, err := sql.Open("mysql", x.dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		for _, statement := range []string{`UPDATE runs SET lease_expires_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE id=?`, `UPDATE run_prompts SET expires_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE run_id=?`} {
			if _, err := db.ExecContext(context.Background(), statement, pending.Run.ID); err != nil {
				t.Fatal(err)
			}
		}
		path := promptPath(session, pending.Run.ID, pending.Prompt.ID)
		x.request(t, "POST", path+"/answer", owner.Token, map[string]string{"answer": "国内"}, 409)
		got := integrationData[domain.RunPrompt](t, x.request(t, "GET", path, owner.Token, nil, 200))
		run := integrationData[domain.Run](t, x.request(t, "GET", sessionPath(session)+"/runs/"+pending.Run.ID, owner.Token, nil, 200))
		if got.Status != "expired" || run.Status != "failed" || run.ErrorCode != "prompt_expired" || len(x.agent.calls()) != 1 {
			t.Fatalf("expired prompt lifecycle: prompt=%+v run=%+v", got, run)
		}
	})
	t.Run("source deleted while waiting", func(t *testing.T) {
		x := newIndexFixture(t)
		owner := x.register(t, "promptsourcegone")
		write := x.upload(t, owner, "住宿标准")
		session := x.session(t, owner, owner.Workspaces[0].ID, "来源不可用")
		x.agent.set(func(ctx context.Context, req agent.Request) (agent.Result, error) {
			if req.ToolHandler == nil {
				return agent.Result{}, errors.New("expected metadata tools")
			}
			if _, err := req.ToolHandler(ctx, "list_documents", json.RawMessage(`{"limit":1}`)); err != nil {
				return agent.Result{}, err
			}
			return promptTestResult(req, "99999999999999999999999999999999"), nil
		})
		pending := integrationData[promptHTTPResult](t, x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "查住宿标准", "document_ids": []string{write.Document.ID}}, 202))
		x.request(t, "DELETE", documentsPath(session.WorkspaceID)+"/"+write.Document.ID, owner.Token, nil, 204)
		x.request(t, "POST", promptPath(session, pending.Run.ID, pending.Prompt.ID)+"/answer", owner.Token, map[string]string{"answer": "国内"}, 409)
		if len(x.agent.calls()) != 1 {
			t.Fatal("deleted document metadata was hydrated into resumed execution")
		}
	})
}

func TestHTTPPromptsSSESuspensionAndResumeTerminalEvents(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "promptsse")
	session := x.session(t, owner, owner.Workspaces[0].ID, "流式澄清")
	client := &sseIntegrationAgent{stream: func(_ context.Context, req agent.Request, emit func(agent.Event) error) (agent.Result, error) {
		if err := emitSSETestMessage(req, emit, 1, "正在确认差旅范围"); err != nil {
			return agent.Result{}, err
		}
		if req.Resume == nil {
			return promptTestResult(req, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), nil
		}
		return agent.Result{RunID: req.RunID, Mode: "demo", Answer: "国内差旅规则已确认"}, nil
	}}
	x.server.Close()
	x.server = httptest.NewServer(httpapi.New(x.store, client, x.config))
	response, _, cancel := sseTestRequest(t, x.integrationFixture, sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "确认范围"}, "")
	raw, err := io.ReadAll(response.Body)
	cancel()
	response.Body.Close()
	if err != nil || response.StatusCode != 200 {
		t.Fatalf("SSE suspension status=%d err=%v body=%s", response.StatusCode, err, raw)
	}
	promptRequirePrivate(t, raw)
	if !strings.Contains(string(raw), "event: run.started") || !strings.Contains(string(raw), "event: message.delta") || !strings.Contains(string(raw), "event: run.suspended") || strings.Contains(string(raw), "event: run.completed") || strings.Contains(string(raw), "event: run.failed") {
		t.Fatalf("invalid suspended SSE events: %s", raw)
	}
	runID := response.Header.Get("X-Run-ID")
	stored := integrationData[domain.Run](t, x.request(t, "GET", sessionPath(session)+"/runs/"+runID, owner.Token, nil, 200))
	if stored.Status != "suspended" {
		t.Fatalf("SSE terminal preceded durable suspension: %+v", stored)
	}
	path := promptPath(session, runID, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	response, _, cancel = sseTestRequest(t, x.integrationFixture, path+"/answer", owner.Token, map[string]string{"answer": "国内"}, "")
	raw, err = io.ReadAll(response.Body)
	cancel()
	response.Body.Close()
	if err != nil || response.StatusCode != 200 || !strings.Contains(string(raw), "event: run.started") || !strings.Contains(string(raw), "event: run.completed") || strings.Contains(string(raw), "event: run.suspended") || response.Header.Get("X-Run-ID") != runID {
		t.Fatalf("SSE resumed terminal status=%d err=%v body=%s", response.StatusCode, err, raw)
	}
	promptRequirePrivate(t, raw)
	stored = integrationData[domain.Run](t, x.request(t, "GET", sessionPath(session)+"/runs/"+runID, owner.Token, nil, 200))
	if stored.Status != "completed" {
		t.Fatalf("resume completed event not durable: %+v", stored)
	}
	_, streams, runs := client.calls()
	if streams != 2 || runs != 0 {
		t.Fatalf("SSE resume did not use streaming client: streams=%d runs=%d", streams, runs)
	}
}
