package httpapi_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
	"magworkspace/internal/store"
)

const integrationPassword = "correct-horse-42"

type integrationAgent struct {
	mu       sync.Mutex
	requests []agent.Request
	run      func(context.Context, agent.Request) (agent.Result, error)
}

func (a *integrationAgent) Run(ctx context.Context, req agent.Request) (agent.Result, error) {
	a.mu.Lock()
	req.History = append([]agent.Message(nil), req.History...)
	a.requests = append(a.requests, req)
	fn := a.run
	a.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return agent.Result{RunID: req.RunID, Answer: "answer: " + req.Query, Mode: "demo"}, nil
}

func (a *integrationAgent) calls() []agent.Request {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]agent.Request(nil), a.requests...)
}

func (a *integrationAgent) set(fn func(context.Context, agent.Request) (agent.Result, error)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.run = fn
}

type integrationFixture struct {
	dsn    string
	store  *store.Store
	server *httptest.Server
	agent  *integrationAgent
	config httpapi.Config
}

// Each test owns a fresh MySQL database; no application data or shared tables
// are truncated. Missing infrastructure is an explicit skip, never a fake pass.
func newIntegrationFixture(t *testing.T, config httpapi.Config) *integrationFixture {
	t.Helper()
	dsn := os.Getenv("MAGWORKSPACE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MAGWORKSPACE_TEST_DATABASE_URL is unset; MySQL HTTP integration test not run")
	}
	parsed, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal("MAGWORKSPACE_TEST_DATABASE_URL must be a MySQL driver DSN")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	adminConfig := *parsed
	adminConfig.DBName = ""
	admin, err := sql.Open("mysql", adminConfig.FormatDSN())
	if err != nil {
		t.Fatalf("open integration database: %v", err)
	}
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		admin.Close()
		t.Fatalf("generate schema name: %v", err)
	}
	database := "mw_api_test_" + hex.EncodeToString(random[:])
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+database+" CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"); err != nil {
		admin.Close()
		t.Fatalf("create isolated database: %v", err)
	}
	parsed.DBName = database
	parsed.ParseTime = true
	f := &integrationFixture{dsn: parsed.FormatDSN(), agent: &integrationAgent{}, config: config}
	t.Cleanup(func() {
		if f.server != nil {
			f.server.Close()
		}
		if f.store != nil {
			f.store.Close()
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.ExecContext(cleanupCtx, "DROP DATABASE "+database); err != nil {
			t.Errorf("drop isolated database: %v", err)
		}
		admin.Close()
	})
	f.start(t)
	return f
}

func (f *integrationFixture) start(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := store.Open(ctx, f.dsn)
	if err != nil {
		t.Fatalf("open application store: %v", err)
	}
	f.store = db
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate isolated database: %v", err)
	}
	f.server = httptest.NewServer(httpapi.New(db, f.agent, f.config))
}

func (f *integrationFixture) restart(t *testing.T) {
	t.Helper()
	f.server.Close()
	f.server = nil
	f.store.Close()
	f.store = nil
	f.start(t)
}

type integrationEnvelope struct {
	Success  bool            `json:"success"`
	Data     json.RawMessage `json:"data"`
	Total    int             `json:"total"`
	Page     int             `json:"page"`
	PageSize int             `json:"page_size"`
	Error    struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type integrationResponse struct {
	status int
	body   []byte
	err    error
}

func (f *integrationFixture) call(method, path, token string, body any) integrationResponse {
	var encoded []byte
	var err error
	if body != nil {
		encoded, err = json.Marshal(body)
		if err != nil {
			return integrationResponse{err: err}
		}
	}
	req, err := http.NewRequest(method, f.server.URL+path, bytes.NewReader(encoded))
	if err != nil {
		return integrationResponse{err: err}
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return integrationResponse{err: err}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return integrationResponse{status: resp.StatusCode, body: data, err: err}
}

func checkIntegrationResponse(t *testing.T, resp integrationResponse, want ...int) integrationEnvelope {
	t.Helper()
	if resp.err != nil {
		t.Fatalf("HTTP request failed: %v", resp.err)
	}
	matched := false
	for _, status := range want {
		matched = matched || resp.status == status
	}
	if !matched {
		t.Fatalf("HTTP status = %d, want %v; body = %s", resp.status, want, resp.body)
	}
	if resp.status == http.StatusNoContent {
		if len(resp.body) != 0 {
			t.Errorf("204 response contains a body: %s", resp.body)
		}
		return integrationEnvelope{}
	}
	var envelope integrationEnvelope
	if err := json.Unmarshal(resp.body, &envelope); err != nil {
		t.Fatalf("decode response envelope: %v; body = %s", err, resp.body)
	}
	if resp.status >= 400 {
		if envelope.Success || envelope.Error.Code == "" || envelope.Error.Message == "" {
			t.Fatalf("invalid error envelope: %s", resp.body)
		}
	} else if !envelope.Success {
		t.Fatalf("successful response missing success=true: %s", resp.body)
	}
	return envelope
}

func (f *integrationFixture) request(t *testing.T, method, path, token string, body any, status ...int) integrationEnvelope {
	t.Helper()
	return checkIntegrationResponse(t, f.call(method, path, token, body), status...)
}

func integrationData[T any](t *testing.T, envelope integrationEnvelope) T {
	t.Helper()
	var result T
	if err := json.Unmarshal(envelope.Data, &result); err != nil {
		t.Fatalf("decode response data: %v; data = %s", err, envelope.Data)
	}
	return result
}

type integrationAuth struct {
	User         domain.User        `json:"user"`
	Workspaces   []domain.Workspace `json:"workspaces"`
	Token        string             `json:"token"`
	RefreshToken string             `json:"refresh_token"`
	ExpiresAt    time.Time          `json:"expires_at"`
}

func (f *integrationFixture) register(t *testing.T, name string) integrationAuth {
	t.Helper()
	response := f.request(t, http.MethodPost, "/api/v1/auth/register", "", map[string]string{
		"name": name, "email": name + "@example.test", "password": integrationPassword,
	}, http.StatusCreated)
	auth := integrationData[integrationAuth](t, response)
	if auth.User.ID == "" || auth.Token == "" || auth.RefreshToken == "" || len(auth.Workspaces) != 1 {
		t.Fatalf("registration omitted identity, tokens, or default workspace: %+v", auth.User)
	}
	if auth.Workspaces[0].Role != "owner" || auth.Workspaces[0].ID == "" {
		t.Fatalf("default workspace must belong to registered owner: %+v", auth.Workspaces)
	}
	if !auth.ExpiresAt.After(time.Now()) {
		t.Fatal("access token expiry must be in the future")
	}
	return auth
}

func workspacePath(workspaceID string) string { return "/api/v1/workspaces/" + workspaceID }

func (f *integrationFixture) session(t *testing.T, auth integrationAuth, workspaceID, title string) domain.Session {
	t.Helper()
	envelope := f.request(t, http.MethodPost, workspacePath(workspaceID)+"/sessions", auth.Token,
		map[string]string{"title": title}, http.StatusCreated)
	session := integrationData[domain.Session](t, envelope)
	if session.ID == "" || session.UserID != auth.User.ID || session.WorkspaceID != workspaceID {
		t.Fatalf("session identity not derived from authenticated caller: %+v", session)
	}
	return session
}

func sessionPath(session domain.Session) string {
	return workspacePath(session.WorkspaceID) + "/sessions/" + session.ID
}

func TestHTTPRegistrationAndTokenLifecycle(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	for _, path := range []string{"/healthz", "/readyz"} {
		resp := f.call(http.MethodGet, path, "", nil)
		if resp.err != nil || resp.status != http.StatusOK {
			t.Fatalf("%s unavailable: status=%d error=%v", path, resp.status, resp.err)
		}
	}
	alice := f.register(t, "alice")
	members := f.request(t, http.MethodGet, workspacePath(alice.Workspaces[0].ID)+"/members", alice.Token, nil, http.StatusOK)
	list := integrationData[[]domain.Member](t, members)
	if len(list) != 1 || list[0].UserID != alice.User.ID || list[0].Role != "owner" {
		t.Fatalf("registration did not atomically provision Owner membership: %+v", list)
	}
	f.request(t, http.MethodPost, "/api/v1/auth/register", "", map[string]string{
		"name": "duplicate", "email": " ALICE@EXAMPLE.TEST ", "password": integrationPassword,
	}, http.StatusConflict)
	f.request(t, http.MethodPost, "/api/v1/auth/login", "", map[string]string{
		"email": "alice@example.test", "password": "wrong-password-42",
	}, http.StatusUnauthorized)
	login := integrationData[integrationAuth](t, f.request(t, http.MethodPost, "/api/v1/auth/login", "", map[string]string{
		"email": " ALICE@EXAMPLE.TEST ", "password": integrationPassword,
	}, http.StatusOK))
	if login.User.ID != alice.User.ID {
		t.Fatalf("normalized login returned wrong identity: %+v", login.User)
	}
	f.request(t, http.MethodGet, "/api/v1/auth/me", login.RefreshToken, nil, http.StatusUnauthorized)
	f.request(t, http.MethodPost, "/api/v1/auth/refresh", "", map[string]string{"refresh_token": login.Token}, http.StatusUnauthorized)
	rotated := integrationData[integrationAuth](t, f.request(t, http.MethodPost, "/api/v1/auth/refresh", "",
		map[string]string{"refresh_token": login.RefreshToken}, http.StatusOK))
	if rotated.Token == "" || rotated.RefreshToken == "" || rotated.Token == login.Token || rotated.RefreshToken == login.RefreshToken {
		t.Fatal("refresh must rotate both tokens")
	}
	f.request(t, http.MethodGet, "/api/v1/auth/me", login.Token, nil, http.StatusUnauthorized)
	f.request(t, http.MethodPost, "/api/v1/auth/refresh", "", map[string]string{"refresh_token": login.RefreshToken}, http.StatusUnauthorized)
	f.request(t, http.MethodGet, "/api/v1/auth/me", rotated.Token, nil, http.StatusOK)
	f.request(t, http.MethodPost, "/api/v1/auth/logout", rotated.Token, nil, http.StatusNoContent)
	f.request(t, http.MethodGet, "/api/v1/auth/me", rotated.Token, nil, http.StatusUnauthorized)
	f.request(t, http.MethodPost, "/api/v1/auth/refresh", "", map[string]string{"refresh_token": rotated.RefreshToken}, http.StatusUnauthorized)
}

func TestHTTPWorkspacePermissionsAndPrivateSessions(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	owner, viewer, admin, outsider := f.register(t, "owner"), f.register(t, "viewer"), f.register(t, "admin"), f.register(t, "outsider")
	workspace := integrationData[domain.Workspace](t, f.request(t, http.MethodPost, "/api/v1/workspaces", owner.Token,
		map[string]string{"name": "Research", "description": "Private notes"}, http.StatusCreated))
	base := workspacePath(workspace.ID)
	for _, member := range []struct {
		auth integrationAuth
		role string
	}{{viewer, "viewer"}, {admin, "admin"}} {
		f.request(t, http.MethodPost, base+"/members", owner.Token,
			map[string]string{"email": member.auth.User.Email, "role": member.role}, http.StatusCreated)
	}
	f.request(t, http.MethodGet, base, viewer.Token, nil, http.StatusOK)
	f.request(t, http.MethodPatch, base, viewer.Token, map[string]string{"name": "stolen"}, http.StatusForbidden)
	f.request(t, http.MethodDelete, base, viewer.Token, nil, http.StatusForbidden)
	f.request(t, http.MethodPost, base+"/members", admin.Token,
		map[string]string{"email": outsider.User.Email, "role": "owner"}, http.StatusForbidden)
	f.request(t, http.MethodPatch, base+"/members/"+owner.User.ID, owner.Token, map[string]string{"role": "viewer"}, http.StatusConflict)
	f.request(t, http.MethodDelete, base+"/members/"+owner.User.ID, owner.Token, nil, http.StatusConflict)
	f.request(t, http.MethodGet, base, outsider.Token, nil, http.StatusForbidden, http.StatusNotFound)
	f.request(t, http.MethodPatch, base, outsider.Token, map[string]string{"name": "stolen"}, http.StatusForbidden, http.StatusNotFound)
	session := f.session(t, owner, workspace.ID, "Owner private session")
	path := sessionPath(session)
	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		f.request(t, method, path, admin.Token, nil, http.StatusNotFound)
	}
	f.request(t, http.MethodPatch, path, admin.Token, map[string]string{"title": "stolen"}, http.StatusNotFound)
	f.request(t, http.MethodGet, path+"/messages", viewer.Token, nil, http.StatusNotFound)
	f.request(t, http.MethodPost, path+"/messages", admin.Token, map[string]string{"content": "inject"}, http.StatusNotFound)
	visible := integrationData[[]domain.Session](t, f.request(t, http.MethodGet, base+"/sessions", admin.Token, nil, http.StatusOK))
	if len(visible) != 0 {
		t.Fatalf("workspace admin can list another user's private sessions: %+v", visible)
	}
	f.request(t, http.MethodGet, workspacePath(outsider.Workspaces[0].ID)+"/sessions/"+session.ID, owner.Token, nil, http.StatusForbidden, http.StatusNotFound)
	f.request(t, http.MethodDelete, base+"/members/"+viewer.User.ID, owner.Token, nil, http.StatusNoContent)
	f.request(t, http.MethodGet, base, viewer.Token, nil, http.StatusForbidden, http.StatusNotFound)
	f.request(t, http.MethodPost, base+"/sessions", viewer.Token, map[string]string{"title": "revoked"}, http.StatusForbidden, http.StatusNotFound)
	if len(f.agent.calls()) != 0 {
		t.Fatal("unauthorized operations reached the agent")
	}
}

func TestHTTPAgentIdentityHistoryAndPersistence(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	auth := f.register(t, "writer")
	session := f.session(t, auth, auth.Workspaces[0].ID, "Saved conversation")
	path := sessionPath(session)
	for _, identity := range []string{"user_id", "workspace_id", "session_id", "role", "history"} {
		f.request(t, http.MethodPost, path+"/messages", auth.Token,
			map[string]any{"content": "injected", identity: "attacker"}, http.StatusBadRequest)
	}
	for _, content := range []string{"first question", "follow up"} {
		envelope := f.request(t, http.MethodPost, path+"/messages", auth.Token,
			map[string]string{"content": content}, http.StatusCreated)
		payload := integrationData[struct {
			Run domain.Run `json:"run"`
		}](t, envelope)
		if payload.Run.ID == "" || payload.Run.Status != "completed" {
			t.Fatalf("turn did not return a completed run: %+v", payload.Run)
		}
		loaded := integrationData[domain.Run](t, f.request(t, http.MethodGet, path+"/runs/"+payload.Run.ID, auth.Token, nil, http.StatusOK))
		if loaded.ID != payload.Run.ID || loaded.Status != "completed" || loaded.FinishedAt == nil {
			t.Fatalf("completed run was not persisted: %+v", loaded)
		}
	}
	calls := f.agent.calls()
	if len(calls) != 2 {
		t.Fatalf("agent calls = %d, want 2", len(calls))
	}
	for _, call := range calls {
		if call.UserID != auth.User.ID || call.WorkspaceID != session.WorkspaceID || call.SessionID != session.ID || call.Role != "owner" || call.RunID == "" {
			t.Fatalf("agent identity is not authoritative: %+v", call)
		}
	}
	if len(calls[0].History) != 0 || len(calls[1].History) != 2 || calls[1].History[0].Role != "user" || calls[1].History[0].Content != "first question" || calls[1].History[1].Role != "assistant" || calls[1].History[1].Content != "answer: first question" {
		t.Fatalf("agent history must contain only previous completed turns: %+v", calls)
	}
	f.restart(t)
	messages := integrationData[[]domain.Message](t, f.request(t, http.MethodGet, path+"/messages", auth.Token, nil, http.StatusOK))
	if len(messages) != 4 {
		t.Fatalf("messages after server/store reconnect = %d, want 4", len(messages))
	}
	for _, message := range messages {
		if message.SessionID != session.ID || message.RunID == "" || message.Status != "completed" {
			t.Fatalf("invalid persisted message: %+v", message)
		}
	}
	page := f.request(t, http.MethodGet, path+"/messages?page=1&page_size=1", auth.Token, nil, http.StatusOK)
	if len(integrationData[[]domain.Message](t, page)) != 1 || page.Total != 4 || page.Page != 1 || page.PageSize != 1 {
		t.Fatalf("invalid paged message response: %+v", page)
	}
	for _, query := range []string{"page=-1", "page_size=101", "page_size=invalid"} {
		f.request(t, http.MethodGet, path+"/messages?"+query, auth.Token, nil, http.StatusBadRequest)
	}
}

func TestHTTPFailedTurnDoesNotContaminateAgentHistory(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	auth := f.register(t, "failure")
	session := f.session(t, auth, auth.Workspaces[0].ID, "Failure recovery")
	path := sessionPath(session)
	f.agent.set(func(context.Context, agent.Request) (agent.Result, error) {
		return agent.Result{}, errors.New("sensitive upstream transport diagnostic")
	})
	response := f.call(http.MethodPost, path+"/messages", auth.Token, map[string]string{"content": "failed question"})
	checkIntegrationResponse(t, response, http.StatusBadGateway)
	if strings.Contains(string(response.body), "sensitive upstream") {
		t.Fatal("HTTP error leaked upstream diagnostics")
	}
	failedID := f.agent.calls()[0].RunID
	failed := integrationData[domain.Run](t, f.request(t, http.MethodGet, path+"/runs/"+failedID, auth.Token, nil, http.StatusOK))
	if failed.Status != "failed" || failed.FinishedAt == nil || failed.ErrorCode == "" {
		t.Fatalf("failed run was left active: %+v", failed)
	}
	f.agent.set(nil)
	f.request(t, http.MethodPost, path+"/messages", auth.Token, map[string]string{"content": "retry safely"}, http.StatusCreated)
	calls := f.agent.calls()
	if len(calls) != 2 || len(calls[1].History) != 0 {
		t.Fatalf("failed turn contaminated subsequent model history: %+v", calls)
	}
}

func TestHTTPConcurrentTurnRejectedWhileAgentIsRunning(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	auth := f.register(t, "concurrent")
	session := f.session(t, auth, auth.Workspaces[0].ID, "One active run")
	path := sessionPath(session)
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseAgent := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseAgent)
	f.agent.set(func(ctx context.Context, req agent.Request) (agent.Result, error) {
		close(started)
		select {
		case <-release:
			return agent.Result{RunID: req.RunID, Answer: "finished", Mode: "demo"}, nil
		case <-ctx.Done():
			return agent.Result{}, ctx.Err()
		}
	})
	finished := make(chan integrationResponse, 1)
	go func() {
		finished <- f.call(http.MethodPost, path+"/messages", auth.Token, map[string]string{"content": "first"})
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first request did not reach the agent")
	}
	f.request(t, http.MethodPost, path+"/messages", auth.Token, map[string]string{"content": "overlapping"}, http.StatusConflict)
	if len(f.agent.calls()) != 1 {
		t.Fatal("second concurrent turn reached the agent")
	}
	releaseAgent()
	select {
	case response := <-finished:
		checkIntegrationResponse(t, response, http.StatusCreated)
	case <-time.After(5 * time.Second):
		t.Fatal("released agent request did not complete")
	}
	messages := integrationData[[]domain.Message](t, f.request(t, http.MethodGet, path+"/messages", auth.Token, nil, http.StatusOK))
	if len(messages) != 2 {
		t.Fatalf("rejected overlapping turn persisted messages: %+v", messages)
	}
}

func TestHTTPAgentTimeoutPersistsFailureAndReleasesSession(t *testing.T) {
	// This deadline verifies cancellation, not database speed. Leave enough time
	// for the real authorization/provenance transactions under the race detector.
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true, RunTimeout: 2 * time.Second})
	auth := f.register(t, "timeout")
	session := f.session(t, auth, auth.Workspaces[0].ID, "Timeout recovery")
	path := sessionPath(session)
	f.agent.set(func(ctx context.Context, _ agent.Request) (agent.Result, error) {
		<-ctx.Done()
		return agent.Result{}, ctx.Err()
	})
	f.request(t, http.MethodPost, path+"/messages", auth.Token, map[string]string{"content": "will timeout"}, http.StatusGatewayTimeout)
	calls := f.agent.calls()
	if len(calls) != 1 {
		t.Fatalf("timeout request reached agent %d times", len(calls))
	}
	run := integrationData[domain.Run](t, f.request(t, http.MethodGet, path+"/runs/"+calls[0].RunID, auth.Token, nil, http.StatusOK))
	if run.Status != "failed" || run.ErrorCode != "agent_timeout" || run.FinishedAt == nil {
		t.Fatalf("timeout failed to persist terminal state independently of canceled context: %+v", run)
	}
	f.agent.set(nil)
	// Recovery verifies that the failed run released the session. It need not
	// race the deliberately short timeout used to trigger the first failure.
	f.config.RunTimeout = 0
	f.restart(t)
	f.request(t, http.MethodPost, path+"/messages", auth.Token, map[string]string{"content": "after timeout"}, http.StatusCreated)
}

func TestHTTPLastOwnerInvariantSurvivesConcurrentDemotions(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	first, second := f.register(t, "firstowner"), f.register(t, "secondowner")
	base := workspacePath(first.Workspaces[0].ID)
	f.request(t, http.MethodPost, base+"/members", first.Token,
		map[string]string{"email": second.User.Email, "role": "owner"}, http.StatusCreated)
	start := make(chan struct{})
	results := make(chan integrationResponse, 2)
	for _, auth := range []integrationAuth{first, second} {
		go func(auth integrationAuth) {
			<-start
			results <- f.call(http.MethodPatch, base+"/members/"+auth.User.ID, auth.Token, map[string]string{"role": "contributor"})
		}(auth)
	}
	close(start)
	statuses := map[int]int{}
	for range 2 {
		select {
		case response := <-results:
			checkIntegrationResponse(t, response, http.StatusOK, http.StatusConflict)
			statuses[response.status]++
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent Owner demotions did not complete")
		}
	}
	if statuses[http.StatusOK] != 1 || statuses[http.StatusConflict] != 1 {
		t.Fatalf("expected exactly one accepted Owner demotion, got %v", statuses)
	}
	members := integrationData[[]domain.Member](t, f.request(t, http.MethodGet, base+"/members", first.Token, nil, http.StatusOK))
	owners := 0
	for _, member := range members {
		if member.Role == "owner" {
			owners++
		}
	}
	if owners != 1 {
		t.Fatalf("concurrent demotions left %d Owners", owners)
	}
}
