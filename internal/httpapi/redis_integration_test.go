package httpapi_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/httpapi"
	"magworkspace/internal/indexing"
	"magworkspace/internal/redisstate"
)

func httpRedis(t *testing.T) *redisstate.Client {
	t.Helper()
	addr := os.Getenv("MAGWORKSPACE_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("isolated Redis required")
	}
	c := redisstate.New(addr, "", fmt.Sprintf("http:%d:", time.Now().UnixNano()))
	t.Cleanup(func() { c.Close() })
	if err := c.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	return c
}
func TestHTTPRedisIndexDispatchProgressRecovery(t *testing.T) {
	c := httpRedis(t)
	x := newIndexFixture(t)
	x.server.Close()
	x.service = indexing.NewService(x.store, x.parser, x.embedder, x.search, x.objects, x.profile, indexing.Options{Redis: c, JobTimeout: 10 * time.Second})
	x.config.Redis = c
	x.config.Indexing = x.service
	x.server = httptest.NewServer(httpapi.New(x.store, x.agent, x.config))
	auth := x.register(t, "redis-index")
	other := x.register(t, "redis-outsider")
	write := x.upload(t, auth, "Redis queue durable source")
	ctx := context.Background()
	if err := x.service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := c.Pop(ctx, x.profile.ID)
	if err != nil || id == "" {
		t.Fatal(id, err)
	} // Simulate crash after pop, before claim.
	if err = x.service.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	// Simulate a crashed worker with an expired fenced lease.
	abandoned, err := x.store.ClaimIndexJobByID(ctx, x.profile.ID, id, time.Millisecond)
	if err != nil || abandoned == nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	x.search.setUpsert(func(context.Context) error {
		job := x.job(t, auth, write)
		if job.Progress == nil || job.Progress.Stage != "writing" || job.Progress.Chunks != 1 {
			t.Errorf("missing live progress: %+v", job.Progress)
		}
		if job.Generation == abandoned.Job.Generation {
			t.Error("expired generation reused")
		}
		x.request(t, "GET", indexJobPath(write), other.Token, nil, 404)
		return nil
	})
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := x.service.ProcessOne(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	job := x.job(t, auth, write)
	if job.Status != "ready" || job.Attempts != 2 || job.Progress != nil {
		t.Fatalf("job %+v", job)
	}
	if x.embedder.calls.Load() != 1 {
		t.Fatal("duplicate processing", x.embedder.calls.Load())
	}
	x.search.setUpsert(nil)
	c.Close() // Connection loss falls back to durable DB claim, not failed document.
	next := x.upload(t, auth, "Redis outage fallback")
	x.process(t)
	if got := x.job(t, auth, next); got.Status != "ready" {
		t.Fatal(got)
	}
}
func TestHTTPRedisSSEReplayPrivateGenerationAndSources(t *testing.T) {
	c := httpRedis(t)
	x := newIndexFixture(t)
	x.server.Close()
	auth := integrationAuth{} // register after replacing the server
	client := &sseIntegrationAgent{}
	x.config.Redis = c
	x.server = httptest.NewServer(httpapi.New(x.store, client, x.config))
	auth = x.register(t, "redis-stream")
	other := x.register(t, "redis-stream-other")
	doc := x.upload(t, auth, "source content")
	client.stream = func(ctx context.Context, req agent.Request, emit func(agent.Event) error) (agent.Result, error) {
		if _, err := req.ToolHandler(ctx, "list_documents", json.RawMessage(`{}`)); err != nil {
			return agent.Result{}, err
		}
		if err := emitSSETestMessage(req, emit, 1, "partial answer"); err != nil {
			return agent.Result{}, err
		}
		return agent.Result{RunID: req.RunID, Answer: "final answer", Mode: "demo"}, nil
	}
	session := x.session(t, auth, auth.Workspaces[0].ID, "Redis stream")
	res, _, cancel := sseTestRequest(t, x.integrationFixture, sessionPath(session)+"/messages", auth.Token, map[string]string{"content": "question"}, "")
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	cancel()
	if err != nil || res.StatusCode != 200 || !strings.Contains(string(body), "id: 1:1") || !strings.Contains(string(body), "run.completed") {
		t.Fatalf("original stream %s %v", body, err)
	}
	rid := res.Header.Get("X-Run-ID")
	path := sessionPath(session) + "/runs/" + rid + "/events"
	get := func(token, cursor string) (int, string) {
		t.Helper()
		req, _ := http.NewRequest("GET", x.server.URL+path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Last-Event-ID", cursor)
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, string(data)
	}
	if code, data := get(auth.Token, "1:1"); code != 200 || strings.Contains(data, "run.started") || !strings.Contains(data, "final answer") {
		t.Fatal(code, data)
	}
	if code, data := get(other.Token, ""); code == 200 || strings.Contains(data, "final answer") {
		t.Fatal("private replay", code, data)
	}
	if code, _ := get(auth.Token, "2:1"); code != 409 {
		t.Fatal("old execution accepted", code)
	}
	if code, _ := get(auth.Token, "1:999"); code != 409 {
		t.Fatal("invalid cursor accepted", code)
	}
	_, streams, runs := client.calls()
	if streams != 1 || runs != 0 {
		t.Fatal("replay executed agent", streams, runs)
	}
	// Exercise server generation fencing independently of cursor syntax.
	db, err := sql.Open("mysql", x.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`UPDATE run_journal_heads SET execution_generation=2 WHERE run_id=?`, rid); err != nil {
		t.Fatal(err)
	}
	if code, _ := get(auth.Token, "1:1"); code != 409 {
		t.Fatal("generation fence", code)
	}
	if _, err = db.Exec(`UPDATE run_journal_heads SET execution_generation=1 WHERE run_id=?`, rid); err != nil {
		t.Fatal(err)
	}

	// Cached content cannot outlive source access, even if answer has no citations.
	x.request(t, "DELETE", documentsPath(doc.Document.WorkspaceID)+"/"+doc.Document.ID, auth.Token, nil, 204)
	if code, data := get(auth.Token, ""); code != 403 || strings.Contains(data, "final answer") {
		t.Fatal("deleted source replay", code, data)
	}

}

func TestHTTPRedisSSELiveSubscriptionOutage(t *testing.T) {
	c := httpRedis(t)
	x := newIndexFixture(t)
	x.server.Close()
	release := make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	t.Cleanup(finish)
	client := &sseIntegrationAgent{stream: func(ctx context.Context, req agent.Request, emit func(agent.Event) error) (agent.Result, error) {
		if err := emitSSETestMessage(req, emit, 1, "live partial"); err != nil {
			return agent.Result{}, err
		}
		select {
		case <-ctx.Done():
			return agent.Result{}, ctx.Err()
		case <-release:
		}
		return agent.Result{RunID: req.RunID, Answer: "completed during outage", Mode: "demo"}, nil
	}}
	x.config.Redis = c
	x.server = httptest.NewServer(httpapi.New(x.store, client, x.config))
	auth := x.register(t, "redis-live")
	session := x.session(t, auth, auth.Workspaces[0].ID, "Live subscription")
	post := openSSETestConnection(t, x.integrationFixture, sessionPath(session)+"/messages", auth.Token, "question")
	started := post.next(t, "run.started")
	post.next(t, "message.start")
	post.next(t, "message.delta")
	path := sessionPath(session) + "/runs/" + started.RunID + "/events"
	req, _ := http.NewRequest("GET", x.server.URL+path, nil)
	req.Header.Set("Authorization", "Bearer "+auth.Token)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != 200 {
		res.Body.Close()
		t.Fatal(res.StatusCode)
	}
	// A second subscriber disconnects; it must not cancel the original owner.
	res.Body.Close()
	c.Close()
	finish()
	terminal := post.next(t, "run.completed")
	if terminal.Answer != "completed during outage" {
		t.Fatal(terminal)
	}
	x.request(t, "GET", path, auth.Token, nil, 503)
	_, streams, runs := client.calls()
	if streams != 1 || runs != 0 {
		t.Fatal("duplicate execution", streams, runs)
	}
}
