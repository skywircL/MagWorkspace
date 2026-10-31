package httpapi_test

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
	"magworkspace/internal/stream"
)

func TestHTTPStreamManagerStopPrivateAcrossInstances(t *testing.T) {
	for _, backend := range []string{"memory", "redis"} {
		t.Run(backend, func(t *testing.T) {
			var manager stream.Manager = stream.NewMemory(stream.DefaultTTL)
			var peerManager stream.Manager
			if backend == "redis" {
				c := httpRedis(t)
				manager = c.StreamManager()
				peerManager = c.StreamManager()
			}
			canceled := make(chan struct{})
			client := &sseIntegrationAgent{stream: func(ctx context.Context, req agent.Request, emit func(agent.Event) error) (agent.Result, error) {
				if err := emitSSETestMessage(req, emit, 1, "private draft"); err != nil {
					return agent.Result{}, err
				}
				<-ctx.Done()
				close(canceled)
				return agent.Result{}, ctx.Err()
			}}
			config := httpapi.Config{Streams: manager, AllowRegistration: true}
			f := newSSEIntegrationFixture(t, config, client)
			owner := f.register(t, "stopowner")
			other := f.register(t, "stopother")
			session := f.session(t, owner, owner.Workspaces[0].ID, "stop")
			f.request(t, "POST", workspacePath(session.WorkspaceID)+"/members", owner.Token, map[string]string{"email": other.User.Email, "role": "owner"}, 201)
			connection := openSSETestConnection(t, f, sessionPath(session)+"/messages", owner.Token, "question")
			started := connection.next(t, "run.started")
			connection.next(t, "message.start")
			connection.next(t, "message.delta")
			connection.close()
			// Distinct HTTP runtime and, for Redis, distinct Manager instance sharing storage.
			if backend == "redis" {
				config.Streams = peerManager
			}
			peer := httpapi.New(f.store, client, config)
			peerServer := httptest.NewServer(peer)
			defer peerServer.Close()
			defer peer.Shutdown(context.Background())
			original := f.server
			f.server = peerServer
			defer func() { f.server = original }()
			path := sessionPath(session) + "/runs/" + started.RunID
			f.request(t, "POST", path+"/stop", other.Token, map[string]any{}, 404)
			f.request(t, "GET", path+"/events", other.Token, nil, 404)
			f.request(t, "POST", path+"/stop", owner.Token, map[string]string{"user_id": other.User.ID}, 400)
			f.request(t, "POST", path+"/stop", owner.Token, map[string]any{}, 202)
			select {
			case <-canceled:
			case <-time.After(5 * time.Second):
				t.Fatal("stop not consumed by producer")
			}
			run := waitStreamRun(t, f, owner.Token, path, "failed")
			if run.ErrorCode != "agent_canceled" {
				t.Fatal(run)
			}
			response := readStreamHTTP(t, f, path+"/events", owner.Token)
			if strings.Contains(response, `"type":"stop"`) || !strings.Contains(response, "run.failed") {
				t.Fatal("control leaked or failed terminal missing", response)
			}
			f.request(t, "POST", path+"/stop", owner.Token, map[string]any{}, 409)
			_, streams, _ := client.calls()
			if streams != 1 {
				t.Fatal(streams)
			}
		})
	}
}

func TestHTTPStreamManagerShutdownJoinsDetachedWorker(t *testing.T) {
	canceled := make(chan struct{})
	client := &sseIntegrationAgent{stream: func(ctx context.Context, req agent.Request, emit func(agent.Event) error) (agent.Result, error) {
		if err := emitSSETestMessage(req, emit, 1, "draft"); err != nil {
			return agent.Result{}, err
		}
		<-ctx.Done()
		close(canceled)
		return agent.Result{}, ctx.Err()
	}}
	f := newSSEIntegrationFixture(t, httpapi.Config{AllowRegistration: true}, client)
	owner := f.register(t, "shutdown")
	session := f.session(t, owner, owner.Workspaces[0].ID, "shutdown")
	connection := openSSETestConnection(t, f, sessionPath(session)+"/messages", owner.Token, "question")
	started := connection.next(t, "run.started")
	connection.next(t, "message.start")
	connection.next(t, "message.delta")
	connection.close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f.server.Config.Handler.(*httpapi.Handler).Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-canceled:
	default:
		t.Fatal("shutdown returned before worker cancellation")
	}
	run := waitStreamRun(t, f, owner.Token, sessionPath(session)+"/runs/"+started.RunID, "failed")
	if run.ErrorCode != "agent_canceled" {
		t.Fatal(run)
	}
}

type failingStreamManager struct {
	stream.Manager
	failAppend bool
}

func (m failingStreamManager) AppendEvent(ctx context.Context, k stream.Key, e stream.Event) error {
	if m.failAppend {
		return stream.ErrUnavailable
	}
	return m.Manager.AppendEvent(ctx, k, e)
}
func TestHTTPStreamManagerInitializationFailureDoesNotExecute(t *testing.T) {
	m := failingStreamManager{Manager: stream.NewMemory(stream.DefaultTTL), failAppend: true}
	client := &sseIntegrationAgent{}
	f := newSSEIntegrationFixture(t, httpapi.Config{AllowRegistration: true, Streams: m}, client)
	owner := f.register(t, "cachefailure")
	session := f.session(t, owner, owner.Workspaces[0].ID, "cache failure")
	response, _, cancel := sseTestRequest(t, f, sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "question"}, "")
	defer cancel()
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	if response.StatusCode != 503 || !strings.Contains(string(raw), "events_unavailable") {
		t.Fatal(response.StatusCode, string(raw))
	}
	_, streams, _ := client.calls()
	if streams != 0 {
		t.Fatal("executed without buffer")
	}
	run := waitStreamRun(t, f, owner.Token, sessionPath(session)+"/runs/"+response.Header.Get("X-Run-ID"), "failed")
	if run.ErrorCode != "stream_unavailable" {
		t.Fatal(run)
	}
}
func TestHTTPStreamManagerPersistenceFailureNeverPublishesSuccess(t *testing.T) {
	client := &sseIntegrationAgent{}
	f := newSSEIntegrationFixture(t, httpapi.Config{AllowRegistration: true}, client)
	owner := f.register(t, "persistfailure")
	session := f.session(t, owner, owner.Workspaces[0].ID, "persist failure")
	db, err := sql.Open("mysql", f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TRIGGER reject_assistant BEFORE INSERT ON messages FOR EACH ROW BEGIN IF NEW.role='assistant' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected failure'; END IF; END`)
	if err != nil {
		t.Fatal(err)
	}
	connection := openSSETestConnection(t, f, sessionPath(session)+"/messages", owner.Token, "question")
	started := connection.next(t, "run.started")
	connection.next(t, "message.start")
	connection.next(t, "message.delta")
	failed := connection.next(t, "run.failed")
	connection.ended(t)
	if failed.Error.Code != "persistence_failed" {
		t.Fatal(failed)
	}
	replay := readStreamHTTP(t, f, sessionPath(session)+"/runs/"+started.RunID+"/events", owner.Token)
	if strings.Contains(replay, "run.completed") {
		t.Fatal("false success", replay)
	}
	messages := integrationData[[]domain.Message](t, f.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, 200))
	if len(messages) != 1 || messages[0].Status != "failed" {
		t.Fatal(messages)
	}
}

func readStreamHTTP(t *testing.T, f *integrationFixture, path, token string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", f.server.URL+path, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != 200 {
		t.Fatalf("stream status=%d body=%s err=%v", response.StatusCode, raw, err)
	}
	return string(raw)
}
