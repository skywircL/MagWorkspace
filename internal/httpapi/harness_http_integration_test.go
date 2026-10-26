package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
)

const harnessHTTPTestKey = "a17ba17ba17ba17ba17ba17ba17ba17ba17ba17ba17ba17ba17ba17ba17ba17ba17b"

// This starts the actual FastAPI entry point; missing service dependencies fail
// once the usual isolated MySQL fixture is configured. It never reads .env.
type harnessHTTPService struct {
	t       *testing.T
	url     string
	args    []string
	env     []string
	command *exec.Cmd
	done    chan error
	log     *os.File
}

func harnessHTTPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func newHarnessHTTPService(t *testing.T, provider, callback string) *harnessHTTPService {
	t.Helper()
	python := os.Getenv("MAGENT_TEST_PYTHON")
	if python == "" {
		python = "python3"
	}
	path, err := exec.LookPath(python)
	if err != nil {
		t.Fatal("FastAPI integration requires Python 3.11+; set MAGENT_TEST_PYTHON")
	}
	script, err := filepath.Abs(filepath.Join("..", "..", "bridges", "magent", "service.py"))
	if err != nil {
		t.Fatal(err)
	}
	harness := os.Getenv("MAGENT_TEST_HARNESS_PATH")
	if harness == "" {
		harness = filepath.Join("..", "..", "..", "Magent Harness")
	}
	harness, err = filepath.Abs(harness)
	if err != nil {
		t.Fatal(err)
	}
	address := harnessHTTPAddress(t)
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	s := &harnessHTTPService{t: t, url: "http://" + address, args: []string{path, "-S", "-B", script}, env: []string{
		"PYTHONPATH=" + filepath.Join(harness, "src"), "PYTHONIOENCODING=utf-8",
		"MAGENT_SERVICE_HOST=127.0.0.1", "MAGENT_SERVICE_PORT=" + port,
		"MAGENT_SERVICE_API_KEY=" + harnessHTTPTestKey, "MAGENT_SERVICE_MAX_CONCURRENCY=1",
		"MAGENT_CALLBACK_ORIGIN=" + callback, "MAGENT_MODE=live",
		"MAGENT_BASE_URL=" + provider, "MAGENT_API_KEY=local-test-key",
		"MAGENT_MODEL=local-controlled-provider", "MAGENT_HTTP_TIMEOUT=20",
	}}
	for _, key := range []string{"PATH", "LANG", "LC_ALL", "SYSTEMROOT", "WINDIR"} {
		if value, ok := os.LookupEnv(key); ok {
			s.env = append(s.env, key+"="+value)
		}
	}
	t.Cleanup(s.stop)
	s.start()
	return s
}

func (s *harnessHTTPService) start() {
	s.t.Helper()
	var err error
	s.log, err = os.OpenFile(filepath.Join(s.t.TempDir(), "service.log"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		s.t.Fatal(err)
	}
	s.command = exec.Command(s.args[0], s.args[1:]...)
	s.command.Env = s.env
	s.command.Stdout, s.command.Stderr = s.log, s.log
	if err := s.command.Start(); err != nil {
		s.t.Fatal(err)
	}
	s.done = make(chan error, 1)
	go func() { s.done <- s.command.Wait() }()
	client := &http.Client{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-s.done:
			raw, _ := os.ReadFile(s.log.Name())
			s.command = nil
			s.t.Fatalf("FastAPI service exited: %v; install bridges/magent/service_requirements.txt in .deps: %.6000s", err, raw)
		default:
		}
		response, err := client.Get(s.url + "/healthz")
		if response != nil {
			response.Body.Close()
			if err == nil && response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	s.t.Fatal("actual FastAPI service did not become healthy")
}

func (s *harnessHTTPService) stop() {
	if s.command != nil {
		_ = s.command.Process.Signal(os.Interrupt)
		select {
		case <-s.done:
		case <-time.After(5 * time.Second):
			_ = s.command.Process.Kill()
			<-s.done
		}
		s.command = nil
	}
	if s.log != nil {
		_ = s.log.Close()
		s.log = nil
	}
}

func harnessHTTPClient(t *testing.T, endpoint, callback string) *agent.HTTPClient {
	t.Helper()
	client, err := agent.NewHTTPClient(agent.HTTPConfig{
		URL: endpoint, APIKey: harnessHTTPTestKey, Mode: "live", Timeout: 20 * time.Second,
		CallbackListenAddr: strings.TrimPrefix(callback, "http://"), CallbackBaseURL: callback,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func installHarnessHTTPClient(x *indexFixture, client agent.Client) {
	x.server.Close()
	x.server = httptest.NewServer(httpapi.New(x.store, client, x.config))
}

type harnessHTTPModelRequest struct {
	Stream   bool `json:"stream"`
	Messages []struct {
		Role, Content string
		ToolCallID    string `json:"tool_call_id"`
	} `json:"messages"`
}

func harnessHTTPModelInput(t *testing.T, r *http.Request) harnessHTTPModelRequest {
	t.Helper()
	if r.Method != http.MethodPost || r.URL.Path != "/chat/completions" || r.Header.Get("Authorization") != "Bearer local-test-key" {
		t.Error("unexpected controlled provider endpoint or credential")
	}
	var request harnessHTTPModelRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		t.Error(err)
	}
	return request
}

func harnessHTTPModelReply(w http.ResponseWriter, request harnessHTTPModelRequest, answer, tool, arguments string) {
	message := map[string]any{"role": "assistant", "content": answer}
	finish := "stop"
	if tool != "" {
		call := map[string]any{"id": "http-" + tool, "type": "function", "function": map[string]string{"name": tool, "arguments": arguments}}
		if request.Stream {
			call["index"] = 0
		}
		message["tool_calls"] = []any{call}
		finish = "tool_calls"
	}
	if request.Stream {
		w.Header().Set("Content-Type", "text/event-stream")
		chunk, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": message, "finish_reason": finish}}})
		_, _ = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", chunk)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": message, "finish_reason": finish}}})
}

func harnessHTTPCompletedSSE(t *testing.T, x *indexFixture, path, token string, input any) recoveryHTTPResult {
	t.Helper()
	response, _, cancel := sseTestRequest(t, x.integrationFixture, path, token, input, "")
	raw, err := io.ReadAll(response.Body)
	cancel()
	_ = response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("HTTP Harness SSE failed: status=%d error=%v body=%s", response.StatusCode, err, raw)
	}
	assertJournalResponsePrivate(t, raw)
	var completed recoveryHTTPResult
	terminals := 0
	for _, block := range strings.Split(string(raw), "\n\n") {
		if strings.Contains(block, "event: run.failed") || strings.Contains(block, "event: run.suspended") {
			t.Fatalf("unexpected HTTP Harness terminal: %s", raw)
		}
		if strings.Contains(block, "event: run.completed\n") {
			terminals++
			for _, line := range strings.Split(block, "\n") {
				if strings.HasPrefix(line, "data: ") {
					if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &completed); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
	}
	if terminals != 1 || completed.Run.Status != "completed" {
		t.Fatalf("expected one persisted completion: %s", raw)
	}
	return completed
}

// Go HTTP, MySQL, callback handlers, FastAPI and the actual Harness are real.
// Only the model provider and external object/index services are local fixtures.
func TestHTTPHarnessServiceToolsMemoryPromptsAndApproval(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "serviceowner")
	other := x.register(t, "serviceother")
	wid := owner.Workspaces[0].ID
	x.request(t, "POST", workspacePath(wid)+"/members", owner.Token, map[string]string{"email": other.User.Email, "role": "contributor"}, 201)
	write := x.upload(t, owner, "HTTP 服务链路验收文档")
	x.process(t)
	const remembered = "HTTP private preference: concise Markdown."
	x.request(t, "POST", memoryPath(wid), owner.Token, map[string]string{"kind": "preference", "content": remembered, "topic": "format"}, 201)
	session := x.session(t, owner, wid, "独立 Harness 连续会话")
	var calls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := harnessHTTPModelInput(t, r)
		if queryUnderstandingRequest(payload) {
			harnessHTTPModelReply(w, payload, "not-json", "", "")
			return
		}
		step := calls.Add(1)
		visible, _ := json.Marshal(payload.Messages)
		if strings.Contains(string(visible), harnessHTTPTestKey) || strings.Contains(string(visible), "127.0.0.1") {
			t.Error("callback/service capability entered model context")
		}
		if strings.Contains(string(visible), remembered) != (step <= 7) {
			t.Errorf("personal memory isolation failed at model step %d", step)
		}
		find := func(id, text string) {
			for _, m := range payload.Messages {
				if m.Role == "tool" && m.ToolCallID == "http-"+id && strings.Contains(m.Content, text) && !strings.HasPrefix(m.Content, "Error:") {
					return
				}
			}
			t.Errorf("model step %d omitted successful %s callback", step, id)
		}
		switch step {
		case 1:
			harnessHTTPModelReply(w, payload, "", "list_documents", `{}`)
		case 2:
			find("list_documents", write.Document.ID)
			harnessHTTPModelReply(w, payload, "", "search_memory", `{"query":"Markdown"}`)
		case 3:
			find("search_memory", remembered)
			harnessHTTPModelReply(w, payload, "", "ask_user", `{"question":"选择部署范围","options":["国内","海外"]}`)
		case 4:
			find("ask_user", "国内")
			harnessHTTPModelReply(w, payload, "第一轮已确认国内。", "", "")
		case 5:
			if !strings.Contains(string(visible), "第一轮已确认国内") {
				t.Error("second turn omitted completed first-turn history")
			}
			harnessHTTPModelReply(w, payload, "", "create_document", `{"title":"审批文档","filename":"approved.md","content":"精确批准的正文"}`)
		case 6:
			find("create_document", "approved.md")
			harnessHTTPModelReply(w, payload, "第二轮文档已创建。", "", "")
		case 7:
			if !strings.Contains(string(visible), "第一轮已确认国内") || !strings.Contains(string(visible), "第二轮文档已创建") {
				t.Error("third turn lost earlier completed turns")
			}
			harnessHTTPModelReply(w, payload, "连续会话正常。", "", "")
		case 8:
			harnessHTTPModelReply(w, payload, "其他用户没有该记忆。", "", "")
		default:
			t.Errorf("unexpected model replay: %d", step)
			http.Error(w, "unexpected call", 400)
		}
	}))
	t.Cleanup(provider.Close)
	callback := "http://" + harnessHTTPAddress(t)
	service := newHarnessHTTPService(t, provider.URL, callback)
	client := harnessHTTPClient(t, service.url, callback)
	installHarnessHTTPClient(x, client)
	response := x.call("POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "查看目录与记忆，确认范围"})
	pending := integrationData[promptHTTPResult](t, checkIntegrationResponse(t, response, 202))
	assertJournalResponsePrivate(t, response.body)
	if pending.Run.Status != "suspended" || pending.Prompt.Kind != "ask_user" {
		t.Fatalf("question did not suspend: %+v", pending)
	}
	question := promptPath(session, pending.Run.ID, pending.Prompt.ID)
	x.request(t, "POST", question+"/answer", other.Token, map[string]string{"answer": "国内"}, 404)
	service.stop()
	service.start()
	recoveryRestart(t, x)
	installHarnessHTTPClient(x, client)
	completed := harnessHTTPCompletedSSE(t, x, question+"/answer", owner.Token, map[string]string{"answer": "国内"})
	if completed.Run.ID != pending.Run.ID || completed.Answer != "第一轮已确认国内。" {
		t.Fatal("restart changed the suspended run or answer")
	}
	response = x.call("POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "创建一个经过审批的文档"})
	approval := integrationData[promptHTTPResult](t, checkIntegrationResponse(t, response, 202))
	if approval.Prompt.Kind != "tool_approval" || approval.Prompt.Approval == nil || approval.Prompt.Approval.Tool != "create_document" {
		t.Fatalf("write did not produce an exact approval: %+v", approval)
	}
	_, total, err := x.store.Documents(context.Background(), owner.User.ID, wid, 20, 0)
	if err != nil || total != 1 {
		t.Fatalf("document written before approval: total=%d err=%v", total, err)
	}
	approvalPath := promptPath(session, approval.Run.ID, approval.Prompt.ID)
	x.request(t, "POST", approvalPath+"/approve", other.Token, map[string]bool{"approved": true}, 404)
	completed = harnessHTTPCompletedSSE(t, x, approvalPath+"/approve", owner.Token, map[string]bool{"approved": true})
	if completed.Run.ID != approval.Run.ID {
		t.Fatal("approval started a replacement run")
	}
	x.request(t, "POST", approvalPath+"/approve", owner.Token, map[string]bool{"approved": true}, 409)
	receipt := integrationData[domain.RunPrompt](t, x.request(t, "GET", approvalPath, owner.Token, nil, 200))
	if receipt.Approval == nil || receipt.Approval.Result == nil {
		t.Fatal("approved write has no durable receipt")
	}
	result := receipt.Approval.Result
	_, content, err := x.store.DocumentContent(context.Background(), owner.User.ID, wid, result.Document.ID, result.Version.ID)
	if err != nil || string(content) != "精确批准的正文" {
		t.Fatalf("approved bytes differ: %v", err)
	}
	_, total, err = x.store.Documents(context.Background(), owner.User.ID, wid, 20, 0)
	if err != nil || total != 2 {
		t.Fatal("approval did not create exactly one document", total, err)
	}
	x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "检查连续会话"}, 201)
	otherSession := x.session(t, other, wid, "私有记忆隔离")
	x.request(t, "POST", sessionPath(otherSession)+"/messages", other.Token, map[string]string{"content": "检查我的记忆"}, 201)
	x.request(t, "GET", sessionPath(session)+"/messages", other.Token, nil, 404)
	messages := integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, 200))
	if len(messages) != 6 || calls.Load() != 8 {
		t.Fatalf("HTTP Harness repeated messages or model work: messages=%d model_calls=%d", len(messages), calls.Load())
	}
}

func TestHTTPHarnessServiceCommittedJournalSurvivesLostResponseAndRestart(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "servicerecovery")
	write := x.upload(t, owner, "恢复时不得重复查询和生成")
	x.process(t)
	session := x.session(t, owner, owner.Workspaces[0].ID, "服务重启恢复")
	var calls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := harnessHTTPModelInput(t, r)
		if calls.Add(1) == 1 {
			harnessHTTPModelReply(w, payload, "", "list_documents", `{}`)
			return
		}
		found := false
		for _, m := range payload.Messages {
			found = found || (m.Role == "tool" && strings.Contains(m.Content, write.Document.ID))
		}
		if !found {
			t.Error("actual callback did not supply the uploaded document")
		}
		harnessHTTPModelReply(w, payload, "恢复已提交的答案。", "", "")
	}))
	t.Cleanup(provider.Close)
	callback := "http://" + harnessHTTPAddress(t)
	service := newHarnessHTTPService(t, provider.URL, callback)
	target, _ := url.Parse(service.url)
	proxy := httputil.NewSingleHostReverseProxy(target)
	var dropped atomic.Bool
	fault := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !dropped.CompareAndSwap(false, true) {
			proxy.ServeHTTP(w, r)
			return
		}
		request := r.Clone(r.Context())
		request.URL.Scheme, request.URL.Host, request.RequestURI = target.Scheme, target.Host, ""
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Error(err)
			http.Error(w, "controlled transport failed", 502)
			return
		}
		raw, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != 200 || !strings.Contains(string(raw), `"status":"completed"`) {
			t.Errorf("lost response injection missed completed journal: status=%d err=%v", response.StatusCode, err)
		}
		w.Header().Set("Content-Type", response.Header.Get("Content-Type"))
		w.Header().Set("X-Magent-Protocol", response.Header.Get("X-Magent-Protocol"))
		w.Header().Set("Content-Length", fmt.Sprint(len(raw)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(raw[:len(raw)/2])
		w.(http.Flusher).Flush()
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = connection.Close() // Truncate a real HTTP response after the remote commit.
	}))
	t.Cleanup(fault.Close)
	client := harnessHTTPClient(t, fault.URL, callback)
	installHarnessHTTPClient(x, client)
	failed := x.call("POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "读取目录"})
	if checkIntegrationResponse(t, failed, 502).Error.Code != "agent_failed" {
		t.Fatal("lost HTTP response did not fail the original delivery")
	}
	messages := integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, 200))
	if len(messages) != 1 || calls.Load() != 2 {
		t.Fatalf("HTTP client replayed a POST or committed a missing response: messages=%d calls=%d", len(messages), calls.Load())
	}
	runID := messages[0].RunID
	assertHarnessJournalFacts(t, x, runID, true)
	service.stop()
	service.start()
	recoveryRestart(t, x)
	installHarnessHTTPClient(x, client)
	completed := harnessHTTPCompletedSSE(t, x, recoveryPath(session, runID), owner.Token, map[string]any{})
	if completed.Run.ID != runID || completed.Answer != "恢复已提交的答案。" || calls.Load() != 2 {
		t.Fatalf("journal recovery regenerated the model or changed run: %+v calls=%d", completed, calls.Load())
	}
	messages = integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, 200))
	if len(messages) != 2 || messages[1].RunID != runID || messages[1].Content != completed.Answer {
		t.Fatal("recovery did not persist exactly one original conversation turn")
	}
	x.request(t, "POST", recoveryPath(session, runID), owner.Token, map[string]any{}, 409)
	assertHarnessJournalFacts(t, x, runID, true)
}

func TestHTTPHarnessServiceSSEStopAfterDisconnectCancelsProviderAndReleasesSlot(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "servicecancel")
	session := x.session(t, owner, owner.Workspaces[0].ID, "服务取消")
	var calls atomic.Int32
	providerClosed := make(chan struct{})
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := harnessHTTPModelInput(t, r)
		if calls.Add(1) != 1 {
			harnessHTTPModelReply(w, payload, "取消后可以继续。", "", "")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"临时内容\"},\"finish_reason\":null}]}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(providerClosed)
	}))
	t.Cleanup(provider.Close)
	callback := "http://" + harnessHTTPAddress(t)
	service := newHarnessHTTPService(t, provider.URL, callback)
	client := harnessHTTPClient(t, service.url, callback)
	installHarnessHTTPClient(x, client)
	connection := openSSETestConnection(t, x.integrationFixture, sessionPath(session)+"/messages", owner.Token, "生成并中途取消")
	started := connection.next(t, "run.started")
	connection.next(t, "message.start")
	connection.next(t, "message.delta")
	connection.close()
	select {
	case <-providerClosed:
		t.Fatal("browser disconnect canceled the actual Harness provider")
	case <-time.After(150 * time.Millisecond):
	}
	x.request(t, "POST", sessionPath(session)+"/runs/"+started.RunID+"/stop", owner.Token, map[string]any{}, 202)
	select {
	case <-providerClosed:
	case <-time.After(5 * time.Second):
		t.Fatal("explicit stop left the actual Harness provider request alive")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		run, err := x.store.Run(context.Background(), owner.User.ID, session.WorkspaceID, session.ID, started.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != "running" {
			if run.Status == "completed" {
				t.Fatal("provisional text was committed after disconnect")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("canceled HTTP execution retained its backend lease")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The service has one execution slot: this cannot finish if cancel leaked it.
	x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "继续新问题"}, 201)
	messages := integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, 200))
	if len(messages) != 3 || messages[2].Content != "取消后可以继续。" || calls.Load() != 2 {
		t.Fatalf("cancellation committed partial text or prevented a new turn: messages=%d calls=%d", len(messages), calls.Load())
	}
}
