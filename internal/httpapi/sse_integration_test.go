package httpapi_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
)

type sseIntegrationAgent struct {
	mu       sync.Mutex
	requests []agent.Request
	streams  int
	runs     int
	stream   func(context.Context, agent.Request, func(agent.Event) error) (agent.Result, error)
}

func (a *sseIntegrationAgent) record(req agent.Request, streaming bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	req.History = append([]agent.Message(nil), req.History...)
	a.requests = append(a.requests, req)
	if streaming {
		a.streams++
	} else {
		a.runs++
	}
}

func (a *sseIntegrationAgent) Run(_ context.Context, req agent.Request) (agent.Result, error) {
	a.record(req, false)
	return agent.Result{RunID: req.RunID, Answer: "answer: " + req.Query, Mode: "demo"}, nil
}

func (a *sseIntegrationAgent) Stream(ctx context.Context, req agent.Request, emit func(agent.Event) error) (agent.Result, error) {
	a.record(req, true)
	if a.stream != nil {
		return a.stream(ctx, req, emit)
	}
	answer := "answer: " + req.Query
	if err := emitSSETestMessage(req, emit, 1, "partial answer"); err != nil {
		return agent.Result{}, err
	}
	return agent.Result{RunID: req.RunID, Answer: answer, Mode: "demo"}, nil
}

func (a *sseIntegrationAgent) calls() ([]agent.Request, int, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]agent.Request(nil), a.requests...), a.streams, a.runs
}

func emitSSETestMessage(req agent.Request, emit func(agent.Event) error, round int, delta string) error {
	if err := emit(agent.Event{Type: "message.start", RunID: req.RunID, SessionID: req.SessionID, Round: round}); err != nil {
		return err
	}
	return emit(agent.Event{Type: "message.delta", RunID: req.RunID, SessionID: req.SessionID, Round: round, Delta: delta})
}

func newSSEIntegrationFixture(t *testing.T, config httpapi.Config, client *sseIntegrationAgent) *integrationFixture {
	t.Helper()
	f := newIntegrationFixture(t, config)
	f.server.Close()
	f.server = httptest.NewServer(httpapi.New(f.store, client, config))
	return f
}

type sseTestPayload struct {
	RunID     string     `json:"run_id"`
	SessionID string     `json:"session_id"`
	Run       domain.Run `json:"run"`
	Round     int        `json:"round"`
	Delta     string     `json:"delta"`
	Answer    string     `json:"answer"`
	Error     struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type sseTestEvent struct {
	typeName string
	data     []byte
	err      error
}

type sseTestConnection struct {
	response *http.Response
	events   <-chan sseTestEvent
	comments <-chan string
	cancel   context.CancelFunc
}

func sseTestRequest(t *testing.T, f *integrationFixture, path, token string, body any, lastEventID string) (*http.Response, context.Context, context.CancelFunc) {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.server.URL+path, bytes.NewReader(encoded))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Authorization", "Bearer "+token)
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("open SSE request: %v", err)
	}
	t.Cleanup(func() { cancel(); response.Body.Close() })
	return response, ctx, cancel
}

func openSSETestConnection(t *testing.T, f *integrationFixture, path, token, content string) *sseTestConnection {
	t.Helper()
	response, ctx, cancel := sseTestRequest(t, f, path, token, map[string]string{"content": content}, "")
	if response.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("SSE status=%d, want 200; body=%s", response.StatusCode, data)
	}
	if !strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") || !strings.Contains(response.Header.Get("Cache-Control"), "no-cache") || response.Header.Get("X-Accel-Buffering") != "no" {
		t.Fatalf("invalid SSE headers: %v", response.Header)
	}
	events := make(chan sseTestEvent, 16)
	comments := make(chan string, 16)
	go func() {
		defer close(events)
		defer close(comments)
		scanner := bufio.NewScanner(response.Body)
		scanner.Buffer(make([]byte, 4096), 2<<20)
		var name string
		var data []byte
		send := func(event sseTestEvent) bool {
			select {
			case events <- event:
				return true
			case <-ctx.Done():
				return false
			}
		}
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				if name != "" || len(data) != 0 {
					if !send(sseTestEvent{typeName: name, data: append([]byte(nil), data...)}) {
						return
					}
				}
				name, data = "", nil
			} else if strings.HasPrefix(line, ":") {
				select {
				case comments <- strings.TrimSpace(strings.TrimPrefix(line, ":")):
				default:
				}
			} else if strings.HasPrefix(line, "event:") {
				name = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			} else if strings.HasPrefix(line, "data:") {
				if len(data) != 0 {
					data = append(data, '\n')
				}
				data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " ")...)
			}
		}
		if err := scanner.Err(); err != nil {
			send(sseTestEvent{err: err})
		}
	}()
	return &sseTestConnection{response: response, events: events, comments: comments, cancel: cancel}
}

func (c *sseTestConnection) next(t *testing.T, expected string) sseTestPayload {
	t.Helper()
	select {
	case event, ok := <-c.events:
		if !ok {
			t.Fatalf("SSE ended before %s", expected)
		}
		if event.err != nil {
			t.Fatalf("SSE read before %s: %v", expected, event.err)
		}
		if event.typeName != expected {
			t.Fatalf("SSE event=%q, want %q; data=%s", event.typeName, expected, event.data)
		}
		var payload sseTestPayload
		if err := json.Unmarshal(event.data, &payload); err != nil {
			t.Fatalf("invalid %s payload: %v", expected, err)
		}
		return payload
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", expected)
		return sseTestPayload{}
	}
}

func (c *sseTestConnection) close() { c.cancel(); c.response.Body.Close() }

func (c *sseTestConnection) ended(t *testing.T) {
	t.Helper()
	select {
	case event, ok := <-c.events:
		if ok {
			t.Fatalf("unexpected event after terminal: %+v", event)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SSE did not close after terminal event")
	}
}

func checkSSEIdentity(t *testing.T, payload sseTestPayload, runID, sessionID string) {
	t.Helper()
	if payload.RunID != runID || payload.SessionID != sessionID {
		t.Fatalf("wrong SSE identity: %+v", payload)
	}
}

func TestHTTPSSEFlushesTextDeltaBeforeCompletionAndPersistsTerminal(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	releaseAgent := func() { once.Do(func() { close(release) }) }
	client := &sseIntegrationAgent{stream: func(ctx context.Context, req agent.Request, emit func(agent.Event) error) (agent.Result, error) {
		if err := emitSSETestMessage(req, emit, 1, "partial answer"); err != nil {
			return agent.Result{}, err
		}
		select {
		case <-release:
			return agent.Result{RunID: req.RunID, Answer: "final answer", Mode: "demo"}, nil
		case <-ctx.Done():
			return agent.Result{}, ctx.Err()
		}
	}}
	f := newSSEIntegrationFixture(t, httpapi.Config{AllowRegistration: true}, client)
	t.Cleanup(releaseAgent)
	auth := f.register(t, "streaming")
	session := f.session(t, auth, auth.Workspaces[0].ID, "Streaming turn")
	path := sessionPath(session)
	connection := openSSETestConnection(t, f, path+"/messages", auth.Token, "show progress")
	started := connection.next(t, "run.started")
	if started.RunID == "" || started.Run.ID != started.RunID || started.Run.Status != "running" {
		t.Fatalf("invalid run.started: %+v", started)
	}
	checkSSEIdentity(t, started, started.RunID, session.ID)
	if connection.response.Header.Get("X-Run-ID") != started.RunID {
		t.Fatal("X-Run-ID disagrees with stream")
	}
	message := connection.next(t, "message.start")
	checkSSEIdentity(t, message, started.RunID, session.ID)
	if message.Round != 1 || message.Delta != "" {
		t.Fatalf("invalid message start: %+v", message)
	}
	delta := connection.next(t, "message.delta")
	checkSSEIdentity(t, delta, started.RunID, session.ID)
	if delta.Round != 1 || delta.Delta != "partial answer" || delta.Answer != "" {
		t.Fatalf("invalid provisional text delta: %+v", delta)
	}
	// The agent is still blocked on release: observing the delta here proves
	// that answer text is flushed before the final answer exists.
	current := integrationData[domain.Run](t, f.request(t, http.MethodGet, path+"/runs/"+started.RunID, auth.Token, nil, http.StatusOK))
	if current.Status != "running" {
		t.Fatalf("run finished before agent release: %+v", current)
	}
	pending := integrationData[[]domain.Message](t, f.request(t, http.MethodGet, path+"/messages", auth.Token, nil, http.StatusOK))
	if len(pending) != 1 || pending[0].Role != "user" || pending[0].Status != "running" {
		t.Fatalf("provisional text was persisted before completion: %+v", pending)
	}
	response, _, cancel := sseTestRequest(t, f, path+"/messages", auth.Token, map[string]string{"content": "overlap"}, "")
	data, err := io.ReadAll(response.Body)
	cancel()
	response.Body.Close()
	checkIntegrationResponse(t, integrationResponse{status: response.StatusCode, body: data, err: err}, http.StatusConflict)
	if !strings.HasPrefix(response.Header.Get("Content-Type"), "application/json") {
		t.Fatal("pre-run conflict must remain JSON")
	}
	releaseAgent()
	completed := connection.next(t, "run.completed")
	checkSSEIdentity(t, completed, started.RunID, session.ID)
	if completed.Run.Status != "completed" || completed.Run.FinishedAt == nil || completed.Answer != "final answer" {
		t.Fatalf("invalid completed payload: %+v", completed)
	}
	stored := integrationData[domain.Run](t, f.request(t, http.MethodGet, path+"/runs/"+started.RunID, auth.Token, nil, http.StatusOK))
	messages := integrationData[[]domain.Message](t, f.request(t, http.MethodGet, path+"/messages", auth.Token, nil, http.StatusOK))
	if stored.Status != "completed" || len(messages) != 2 || messages[1].Role != "assistant" || messages[1].Content != completed.Answer || messages[1].Status != "completed" {
		t.Fatalf("terminal preceded durable completion: run=%+v messages=%+v", stored, messages)
	}
	connection.ended(t)
	requests, streams, runs := client.calls()
	if streams != 1 || runs != 0 || len(requests) != 1 || requests[0].UserID != auth.User.ID || requests[0].WorkspaceID != session.WorkspaceID || requests[0].Role != "owner" {
		t.Fatalf("invalid streaming invocation: streams=%d runs=%d requests=%+v", streams, runs, requests)
	}
}

func TestHTTPSSEFailureDoesNotPersistAssistant(t *testing.T) {
	client := &sseIntegrationAgent{stream: func(_ context.Context, req agent.Request, emit func(agent.Event) error) (agent.Result, error) {
		if err := emitSSETestMessage(req, emit, 1, "partial answer"); err != nil {
			return agent.Result{}, err
		}
		return agent.Result{}, errors.New("private provider diagnostic")
	}}
	f := newSSEIntegrationFixture(t, httpapi.Config{AllowRegistration: true}, client)
	auth := f.register(t, "streamfailure")
	session := f.session(t, auth, auth.Workspaces[0].ID, "Failed stream")
	path := sessionPath(session)
	connection := openSSETestConnection(t, f, path+"/messages", auth.Token, "failed query")
	started := connection.next(t, "run.started")
	connection.next(t, "message.start")
	connection.next(t, "message.delta")
	failed := connection.next(t, "run.failed")
	checkSSEIdentity(t, failed, started.RunID, session.ID)
	if failed.Error.Code != "agent_failed" || failed.Error.Message == "" || strings.Contains(failed.Error.Message, "private provider") {
		t.Fatalf("invalid failure payload: %+v", failed)
	}
	connection.ended(t)
	run := integrationData[domain.Run](t, f.request(t, http.MethodGet, path+"/runs/"+started.RunID, auth.Token, nil, http.StatusOK))
	messages := integrationData[[]domain.Message](t, f.request(t, http.MethodGet, path+"/messages", auth.Token, nil, http.StatusOK))
	if run.Status != "failed" || run.ErrorCode != "agent_failed" || len(messages) != 1 || messages[0].Role != "user" || messages[0].Status != "failed" {
		t.Fatalf("assistant persisted for a failed run or failure not recorded: run=%+v messages=%+v", run, messages)
	}
	f.request(t, http.MethodPost, path+"/messages", auth.Token, map[string]string{"content": "recover normally"}, http.StatusCreated)
	requests, streams, runs := client.calls()
	if streams != 1 || runs != 1 || len(requests) != 2 || len(requests[1].History) != 0 {
		t.Fatalf("failed turn contaminated history: %+v", requests)
	}
}

func TestHTTPSSEDiscardedDraftIsReplacedAndNeverPersisted(t *testing.T) {
	client := &sseIntegrationAgent{stream: func(_ context.Context, req agent.Request, emit func(agent.Event) error) (agent.Result, error) {
		if err := emitSSETestMessage(req, emit, 1, "discarded draft"); err != nil {
			return agent.Result{}, err
		}
		if err := emit(agent.Event{Type: "message.discard", RunID: req.RunID, SessionID: req.SessionID, Round: 1}); err != nil {
			return agent.Result{}, err
		}
		if err := emitSSETestMessage(req, emit, 2, "accepted draft"); err != nil {
			return agent.Result{}, err
		}
		return agent.Result{RunID: req.RunID, Answer: "authoritative final answer", Mode: "demo"}, nil
	}}
	f := newSSEIntegrationFixture(t, httpapi.Config{AllowRegistration: true}, client)
	auth := f.register(t, "discardretry")
	session := f.session(t, auth, auth.Workspaces[0].ID, "Discard and retry")
	path := sessionPath(session)
	connection := openSSETestConnection(t, f, path+"/messages", auth.Token, "retry a draft")
	started := connection.next(t, "run.started")
	connection.next(t, "message.start")
	first := connection.next(t, "message.delta")
	if first.Round != 1 || first.Delta != "discarded draft" {
		t.Fatalf("wrong first draft: %+v", first)
	}
	discarded := connection.next(t, "message.discard")
	checkSSEIdentity(t, discarded, started.RunID, session.ID)
	if discarded.Round != 1 || discarded.Delta != "" || discarded.Answer != "" {
		t.Fatalf("discard leaked text or selected the wrong draft: %+v", discarded)
	}
	second := connection.next(t, "message.start")
	if second.Round != 2 {
		t.Fatalf("replacement did not identify a new attempt: %+v", second)
	}
	delta := connection.next(t, "message.delta")
	if delta.Round != 2 || delta.Delta != "accepted draft" {
		t.Fatalf("wrong replacement delta: %+v", delta)
	}
	completed := connection.next(t, "run.completed")
	if completed.Answer != "authoritative final answer" || completed.Run.Status != "completed" {
		t.Fatalf("final result did not replace provisional drafts: %+v", completed)
	}
	connection.ended(t)
	messages := integrationData[[]domain.Message](t, f.request(t, http.MethodGet, path+"/messages", auth.Token, nil, http.StatusOK))
	if len(messages) != 2 || messages[1].Role != "assistant" || messages[1].Content != completed.Answer || messages[1].Status != "completed" {
		t.Fatalf("drafts entered durable history: %+v", messages)
	}
	f.request(t, http.MethodPost, path+"/messages", auth.Token, map[string]string{"content": "next turn"}, http.StatusCreated)
	requests, streams, runs := client.calls()
	if streams != 1 || runs != 1 || len(requests) != 2 || len(requests[1].History) != 2 || requests[1].History[1].Role != "assistant" || requests[1].History[1].Content != completed.Answer {
		t.Fatalf("next turn used provisional text: %+v", requests)
	}
}

func TestHTTPSSEDiscardThenFailureDoesNotPersistDraft(t *testing.T) {
	client := &sseIntegrationAgent{stream: func(_ context.Context, req agent.Request, emit func(agent.Event) error) (agent.Result, error) {
		if err := emitSSETestMessage(req, emit, 1, "draft before model failure"); err != nil {
			return agent.Result{}, err
		}
		if err := emit(agent.Event{Type: "message.discard", RunID: req.RunID, SessionID: req.SessionID, Round: 1}); err != nil {
			return agent.Result{}, err
		}
		return agent.Result{}, errors.New("private provider detail after discard")
	}}
	f := newSSEIntegrationFixture(t, httpapi.Config{AllowRegistration: true}, client)
	auth := f.register(t, "discardfailure")
	session := f.session(t, auth, auth.Workspaces[0].ID, "Discard then fail")
	path := sessionPath(session)
	connection := openSSETestConnection(t, f, path+"/messages", auth.Token, "failed attempt")
	started := connection.next(t, "run.started")
	connection.next(t, "message.start")
	connection.next(t, "message.delta")
	connection.next(t, "message.discard")
	failed := connection.next(t, "run.failed")
	if failed.Error.Code != "agent_failed" || strings.Contains(failed.Error.Message, "private provider") {
		t.Fatalf("discarded failure leaked provider details: %+v", failed)
	}
	connection.ended(t)
	run := integrationData[domain.Run](t, f.request(t, http.MethodGet, path+"/runs/"+started.RunID, auth.Token, nil, http.StatusOK))
	messages := integrationData[[]domain.Message](t, f.request(t, http.MethodGet, path+"/messages", auth.Token, nil, http.StatusOK))
	if run.Status != "failed" || run.ErrorCode != "agent_failed" || len(messages) != 1 || messages[0].Role != "user" || messages[0].Status != "failed" {
		t.Fatalf("discarded failure retained draft or active run: run=%+v messages=%+v", run, messages)
	}
	f.request(t, http.MethodPost, path+"/messages", auth.Token, map[string]string{"content": "after discard failure"}, http.StatusCreated)
	requests, _, _ := client.calls()
	if len(requests) != 2 || len(requests[1].History) != 0 {
		t.Fatalf("discarded failure contaminated next history: %+v", requests)
	}
}

func TestHTTPSSEDiscardCanCompleteWithAuthoritativeFallback(t *testing.T) {
	client := &sseIntegrationAgent{stream: func(_ context.Context, req agent.Request, emit func(agent.Event) error) (agent.Result, error) {
		if err := emitSSETestMessage(req, emit, 1, "filtered provisional text"); err != nil {
			return agent.Result{}, err
		}
		if err := emit(agent.Event{Type: "message.discard", RunID: req.RunID, SessionID: req.SessionID, Round: 1}); err != nil {
			return agent.Result{}, err
		}
		return agent.Result{RunID: req.RunID, Answer: "safe fallback answer", Mode: "demo"}, nil
	}}
	f := newSSEIntegrationFixture(t, httpapi.Config{AllowRegistration: true}, client)
	auth := f.register(t, "discardfallback")
	session := f.session(t, auth, auth.Workspaces[0].ID, "Discard and fallback")
	path := sessionPath(session)
	connection := openSSETestConnection(t, f, path+"/messages", auth.Token, "fallback turn")
	started := connection.next(t, "run.started")
	connection.next(t, "message.start")
	connection.next(t, "message.delta")
	connection.next(t, "message.discard")
	completed := connection.next(t, "run.completed")
	checkSSEIdentity(t, completed, started.RunID, session.ID)
	if completed.Answer != "safe fallback answer" || completed.Run.Status != "completed" {
		t.Fatalf("discard prevented authoritative fallback completion: %+v", completed)
	}
	connection.ended(t)
	messages := integrationData[[]domain.Message](t, f.request(t, http.MethodGet, path+"/messages", auth.Token, nil, http.StatusOK))
	if len(messages) != 2 || messages[1].Role != "assistant" || messages[1].Content != completed.Answer || messages[1].Status != "completed" {
		t.Fatalf("fallback was not persisted or discarded text survived: %+v", messages)
	}
}

func TestHTTPSSERejectsInvalidDiscardTransitions(t *testing.T) {
	tests := []struct {
		name   string
		events []agent.Event
		public []string
	}{
		{
			name: "delta after discard",
			events: []agent.Event{
				{Type: "message.start", Round: 1},
				{Type: "message.delta", Round: 1, Delta: "provisional"},
				{Type: "message.discard", Round: 1},
				{Type: "message.delta", Round: 1, Delta: "must not escape"},
			},
			public: []string{"message.start", "message.delta", "message.discard"},
		},
		{
			name:   "discard before start",
			events: []agent.Event{{Type: "message.discard", Round: 1}},
		},
		{
			name: "duplicate discard",
			events: []agent.Event{
				{Type: "message.start", Round: 1},
				{Type: "message.discard", Round: 1},
				{Type: "message.discard", Round: 1},
			},
			public: []string{"message.start", "message.discard"},
		},
		{
			name: "discard wrong attempt",
			events: []agent.Event{
				{Type: "message.start", Round: 1},
				{Type: "message.discard", Round: 2},
			},
			public: []string{"message.start"},
		},
		{
			name: "discard with text",
			events: []agent.Event{
				{Type: "message.start", Round: 1},
				{Type: "message.discard", Round: 1, Delta: "private diagnostic"},
			},
			public: []string{"message.start"},
		},
	}
	client := &sseIntegrationAgent{stream: func(_ context.Context, req agent.Request, emit func(agent.Event) error) (agent.Result, error) {
		for _, test := range tests {
			if req.Query != test.name {
				continue
			}
			for _, event := range test.events {
				event.RunID, event.SessionID = req.RunID, req.SessionID
				if err := emit(event); err != nil {
					return agent.Result{}, err
				}
			}
			return agent.Result{RunID: req.RunID, Answer: "must not be committed", Mode: "demo"}, nil
		}
		return agent.Result{}, errors.New("unmatched test case")
	}}
	f := newSSEIntegrationFixture(t, httpapi.Config{AllowRegistration: true}, client)
	auth := f.register(t, "invaliddiscard")
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := f.session(t, auth, auth.Workspaces[0].ID, test.name)
			path := sessionPath(session)
			connection := openSSETestConnection(t, f, path+"/messages", auth.Token, test.name)
			started := connection.next(t, "run.started")
			for _, expected := range test.public {
				connection.next(t, expected)
			}
			failed := connection.next(t, "run.failed")
			if failed.Error.Code != "agent_failed" || strings.Contains(failed.Error.Message, "private diagnostic") {
				t.Fatalf("invalid transition did not fail safely: %+v", failed)
			}
			connection.ended(t)
			run := integrationData[domain.Run](t, f.request(t, http.MethodGet, path+"/runs/"+started.RunID, auth.Token, nil, http.StatusOK))
			messages := integrationData[[]domain.Message](t, f.request(t, http.MethodGet, path+"/messages", auth.Token, nil, http.StatusOK))
			if run.Status != "failed" || run.ErrorCode != "agent_failed" || len(messages) != 1 || messages[0].Status != "failed" {
				t.Fatalf("invalid transition persisted answer or active run: run=%+v messages=%+v", run, messages)
			}
		})
	}
}

func TestHTTPSSETimeoutSendsTerminalFailure(t *testing.T) {
	client := &sseIntegrationAgent{stream: func(ctx context.Context, req agent.Request, emit func(agent.Event) error) (agent.Result, error) {
		if err := emitSSETestMessage(req, emit, 1, "partial answer"); err != nil {
			return agent.Result{}, err
		}
		<-ctx.Done()
		return agent.Result{}, ctx.Err()
	}}
	// The real authorization/provenance transactions run before streaming.
	// Exercise the agent deadline without assuming an idle database or CPU.
	f := newSSEIntegrationFixture(t, httpapi.Config{AllowRegistration: true, RunTimeout: 2 * time.Second}, client)
	auth := f.register(t, "streamtimeout")
	session := f.session(t, auth, auth.Workspaces[0].ID, "Timed out stream")
	path := sessionPath(session)
	connection := openSSETestConnection(t, f, path+"/messages", auth.Token, "timeout")
	started := connection.next(t, "run.started")
	connection.next(t, "message.start")
	connection.next(t, "message.delta")
	failed := connection.next(t, "run.failed")
	checkSSEIdentity(t, failed, started.RunID, session.ID)
	if failed.Error.Code != "agent_timeout" {
		t.Fatalf("timeout error=%+v", failed.Error)
	}
	connection.ended(t)
	run := integrationData[domain.Run](t, f.request(t, http.MethodGet, path+"/runs/"+started.RunID, auth.Token, nil, http.StatusOK))
	if run.Status != "failed" || run.ErrorCode != "agent_timeout" || run.FinishedAt == nil {
		t.Fatalf("timeout not durable: %+v", run)
	}
}

func TestHTTPSSEDisconnectCancelsAgentAndReleasesSession(t *testing.T) {
	canceled := make(chan struct{})
	client := &sseIntegrationAgent{stream: func(ctx context.Context, req agent.Request, emit func(agent.Event) error) (agent.Result, error) {
		if err := emitSSETestMessage(req, emit, 1, "partial answer"); err != nil {
			return agent.Result{}, err
		}
		<-ctx.Done()
		close(canceled)
		return agent.Result{}, ctx.Err()
	}}
	f := newSSEIntegrationFixture(t, httpapi.Config{AllowRegistration: true}, client)
	auth := f.register(t, "disconnect")
	session := f.session(t, auth, auth.Workspaces[0].ID, "Disconnect")
	path := sessionPath(session)
	connection := openSSETestConnection(t, f, path+"/messages", auth.Token, "disconnect")
	started := connection.next(t, "run.started")
	connection.next(t, "message.start")
	connection.next(t, "message.delta")
	connection.close()
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("client disconnect did not cancel agent context")
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		run := integrationData[domain.Run](t, f.request(t, http.MethodGet, path+"/runs/"+started.RunID, auth.Token, nil, http.StatusOK))
		if run.Status == "failed" {
			if run.ErrorCode != "agent_canceled" || run.FinishedAt == nil {
				t.Fatalf("disconnect failed with wrong terminal state: %+v", run)
			}
			break
		}
		if run.Status != "running" {
			t.Fatalf("unexpected state after disconnect: %+v", run)
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("disconnected run stayed active")
		}
	}
	f.request(t, http.MethodPost, path+"/messages", auth.Token, map[string]string{"content": "after disconnect"}, http.StatusCreated)
	requests, _, _ := client.calls()
	if len(requests) != 2 || len(requests[1].History) != 0 {
		t.Fatalf("disconnected turn entered history: %+v", requests)
	}
}

func TestHTTPSSERejectsBeforeOpeningStreamOrCreatingRun(t *testing.T) {
	client := &sseIntegrationAgent{}
	f := newSSEIntegrationFixture(t, httpapi.Config{AllowRegistration: true}, client)
	owner, member := f.register(t, "streamowner"), f.register(t, "streammember")
	workspaceID := owner.Workspaces[0].ID
	f.request(t, http.MethodPost, workspacePath(workspaceID)+"/members", owner.Token, map[string]string{"email": member.User.Email, "role": "owner"}, http.StatusCreated)
	session := f.session(t, owner, workspaceID, "Private stream")
	path := sessionPath(session)
	for _, test := range []struct {
		name, token, lastEventID string
		body                     map[string]string
		status                   int
	}{
		{"private owner", member.Token, "", map[string]string{"content": "forbidden"}, http.StatusNotFound},
		{"injected identity", owner.Token, "", map[string]string{"content": "bad", "user_id": member.User.ID}, http.StatusBadRequest},
		{"unsupported replay", owner.Token, "previous-event", map[string]string{"content": "retry"}, http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			response, _, cancel := sseTestRequest(t, f, path+"/messages", test.token, test.body, test.lastEventID)
			defer cancel()
			data, err := io.ReadAll(response.Body)
			checkIntegrationResponse(t, integrationResponse{status: response.StatusCode, body: data, err: err}, test.status)
			if !strings.HasPrefix(response.Header.Get("Content-Type"), "application/json") || response.Header.Get("X-Run-ID") != "" {
				t.Fatalf("rejected request opened a stream/run: %v", response.Header)
			}
		})
	}
	requests, streams, runs := client.calls()
	if len(requests) != 0 || streams != 0 || runs != 0 {
		t.Fatal("invalid request reached the agent")
	}
	messages := integrationData[[]domain.Message](t, f.request(t, http.MethodGet, path+"/messages", owner.Token, nil, http.StatusOK))
	if len(messages) != 0 {
		t.Fatalf("invalid requests created turns: %+v", messages)
	}
}

func TestHTTPSSEHeartbeatsKeepIdleStreamOpen(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	releaseAgent := func() { once.Do(func() { close(release) }) }
	client := &sseIntegrationAgent{stream: func(ctx context.Context, req agent.Request, emit func(agent.Event) error) (agent.Result, error) {
		select {
		case <-release:
			if err := emitSSETestMessage(req, emit, 1, "partial answer"); err != nil {
				return agent.Result{}, err
			}
			return agent.Result{RunID: req.RunID, Answer: "after idle", Mode: "demo"}, nil
		case <-ctx.Done():
			return agent.Result{}, ctx.Err()
		}
	}}
	f := newSSEIntegrationFixture(t, httpapi.Config{AllowRegistration: true, SSEHeartbeat: 20 * time.Millisecond}, client)
	t.Cleanup(releaseAgent)
	auth := f.register(t, "heartbeat")
	session := f.session(t, auth, auth.Workspaces[0].ID, "Idle stream")
	path := sessionPath(session)
	connection := openSSETestConnection(t, f, path+"/messages", auth.Token, "wait for model")
	started := connection.next(t, "run.started")
	for range 2 {
		select {
		case _, ok := <-connection.comments:
			if !ok {
				t.Fatal("stream ended before heartbeat")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("idle SSE connection did not receive a flushed heartbeat comment")
		}
	}
	run := integrationData[domain.Run](t, f.request(t, http.MethodGet, path+"/runs/"+started.RunID, auth.Token, nil, http.StatusOK))
	if run.Status != "running" {
		t.Fatalf("heartbeat incorrectly terminated idle run: %+v", run)
	}
	releaseAgent()
	connection.next(t, "message.start")
	connection.next(t, "message.delta")
	connection.next(t, "run.completed")
	connection.ended(t)
}

func TestHTTPSSEAgentPanicBecomesFailedRunAndAllowsNextTurn(t *testing.T) {
	client := &sseIntegrationAgent{stream: func(_ context.Context, req agent.Request, emit func(agent.Event) error) (agent.Result, error) {
		if err := emitSSETestMessage(req, emit, 1, "partial answer"); err != nil {
			return agent.Result{}, err
		}
		panic("private implementation panic")
	}}
	f := newSSEIntegrationFixture(t, httpapi.Config{AllowRegistration: true}, client)
	auth := f.register(t, "streampanic")
	session := f.session(t, auth, auth.Workspaces[0].ID, "Panicking stream")
	path := sessionPath(session)
	connection := openSSETestConnection(t, f, path+"/messages", auth.Token, "before panic")
	started := connection.next(t, "run.started")
	connection.next(t, "message.start")
	connection.next(t, "message.delta")
	failed := connection.next(t, "run.failed")
	checkSSEIdentity(t, failed, started.RunID, session.ID)
	if failed.Error.Code != "agent_failed" || strings.Contains(failed.Error.Message, "private implementation") {
		t.Fatalf("panic escaped the stable failure contract: %+v", failed)
	}
	connection.ended(t)
	run := integrationData[domain.Run](t, f.request(t, http.MethodGet, path+"/runs/"+started.RunID, auth.Token, nil, http.StatusOK))
	messages := integrationData[[]domain.Message](t, f.request(t, http.MethodGet, path+"/messages", auth.Token, nil, http.StatusOK))
	if run.Status != "failed" || run.ErrorCode != "agent_failed" || run.FinishedAt == nil || len(messages) != 1 || messages[0].Role != "user" || messages[0].Status != "failed" {
		t.Fatalf("panic left an active run or committed an answer: run=%+v messages=%+v", run, messages)
	}
	f.request(t, http.MethodPost, path+"/messages", auth.Token, map[string]string{"content": "after panic"}, http.StatusCreated)
	requests, _, _ := client.calls()
	if len(requests) != 2 || len(requests[1].History) != 0 {
		t.Fatalf("panicked turn contaminated next history: %+v", requests)
	}
}

// This wrapper fails a specific protocol frame at the actual handler boundary.
// Unwrap lets ResponseController discover recorder capabilities, while
// FlushError exposes failures that a plain http.Flusher cannot return.
type failingSSETestWriter struct {
	recorder      *httptest.ResponseRecorder
	target        string
	failOnFlush   bool
	targetPending bool
	fired         bool
}

func (w *failingSSETestWriter) Header() http.Header         { return w.recorder.Header() }
func (w *failingSSETestWriter) WriteHeader(status int)      { w.recorder.WriteHeader(status) }
func (w *failingSSETestWriter) Unwrap() http.ResponseWriter { return w.recorder }
func (w *failingSSETestWriter) Write(data []byte) (int, error) {
	w.targetPending = bytes.HasPrefix(data, []byte("event: "+w.target+"\n"))
	if w.targetPending && !w.failOnFlush {
		w.fired = true
		return 0, errors.New("injected SSE transport write failure")
	}
	return w.recorder.Write(data)
}
func (w *failingSSETestWriter) FlushError() error {
	if w.targetPending && w.failOnFlush {
		w.fired = true
		return errors.New("injected SSE transport flush failure")
	}
	w.recorder.Flush()
	return nil
}

func TestHTTPSSEWriterFailuresRespectCommitBoundary(t *testing.T) {
	for _, test := range []struct {
		name, event string
		flush       bool
	}{
		{"delta write", "message.delta", false},
		{"delta flush", "message.delta", true},
		{"completed write", "run.completed", false},
		{"completed flush", "run.completed", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			beforeCommit := test.event == "message.delta"
			canceled := make(chan struct{})
			client := &sseIntegrationAgent{stream: func(ctx context.Context, req agent.Request, emit func(agent.Event) error) (agent.Result, error) {
				if err := emitSSETestMessage(req, emit, 1, "partial answer"); err != nil {
					return agent.Result{}, err
				}
				if beforeCommit {
					<-ctx.Done()
					close(canceled)
					return agent.Result{}, ctx.Err()
				}
				return agent.Result{RunID: req.RunID, Answer: "final answer", Mode: "demo"}, nil
			}}
			f := newSSEIntegrationFixture(t, httpapi.Config{AllowRegistration: true}, client)
			auth := f.register(t, "transport")
			session := f.session(t, auth, auth.Workspaces[0].ID, "Transport boundary")
			path := sessionPath(session)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			request := httptest.NewRequest(http.MethodPost, path+"/messages", strings.NewReader(`{"content":"transport test"}`)).WithContext(ctx)
			request.Header.Set("Authorization", "Bearer "+auth.Token)
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Accept", "text/event-stream")
			writer := &failingSSETestWriter{recorder: httptest.NewRecorder(), target: test.event, failOnFlush: test.flush}
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				httpapi.New(f.store, client, f.config).ServeHTTP(writer, request)
			}()
			select {
			case <-finished:
			case <-time.After(5 * time.Second):
				t.Fatal("handler did not stop after injected transport failure")
			}
			if !writer.fired {
				t.Fatalf("did not reach transport failure on %s: %s", test.event, writer.recorder.Body.String())
			}
			requests, streams, _ := client.calls()
			if streams != 1 || len(requests) != 1 {
				t.Fatalf("expected one streaming worker, got %d", streams)
			}
			run := integrationData[domain.Run](t, f.request(t, http.MethodGet, path+"/runs/"+requests[0].RunID, auth.Token, nil, http.StatusOK))
			messages := integrationData[[]domain.Message](t, f.request(t, http.MethodGet, path+"/messages", auth.Token, nil, http.StatusOK))
			if beforeCommit {
				select {
				case <-canceled:
				default:
					t.Fatal("failed text delta write did not cancel and join the worker")
				}
				if run.Status != "failed" || run.ErrorCode != "agent_canceled" || run.FinishedAt == nil || len(messages) != 1 || messages[0].Role != "user" || messages[0].Status != "failed" {
					t.Fatalf("pre-commit write failure persisted an answer or left run active: run=%+v messages=%+v", run, messages)
				}
				f.request(t, http.MethodPost, path+"/messages", auth.Token, map[string]string{"content": "retry after writer failure"}, http.StatusCreated)
			} else if run.Status != "completed" || run.FinishedAt == nil || len(messages) != 2 || messages[1].Role != "assistant" || messages[1].Content != "final answer" || messages[1].Status != "completed" {
				t.Fatalf("terminal transport failure reverted durable completion: run=%+v messages=%+v", run, messages)
			}
		})
	}
}
