package httpapi_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
)

func TestHTTPRecoveryRejectsNativeTerminalJournalWithoutClaim(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "terminaljournal")
	other := x.register(t, "terminalother")
	for _, kind := range []string{"failed", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			session := x.session(t, owner, owner.Workspaces[0].ID, kind)
			var original agent.Request
			x.agent.set(func(ctx context.Context, req agent.Request) (agent.Result, error) {
				original = req
				if err := recoveryAppendFact(ctx, req, 1); err != nil {
					return agent.Result{}, err
				}
				raw, err := json.Marshal(map[string]any{"record": map[string]any{"version": 1, "seq": 2, "kind": kind, "prev_hash": strings.Repeat("a", 64), "digest": strings.Repeat("b", 64), "data": map[string]string{"error": "synthetic terminal failure"}}})
				if err != nil {
					return agent.Result{}, err
				}
				if _, err := req.JournalHandler(ctx, "append", raw); err != nil {
					return agent.Result{}, err
				}
				return agent.Result{}, agent.ErrFailed
			})
			x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "原问题"}, http.StatusBadGateway)
			path := sessionPath(session) + "/runs/" + original.RunID
			before := len(x.agent.calls())
			for range 2 {
				response := x.request(t, "GET", path, owner.Token, nil, 200)
				run := integrationData[domain.Run](t, response)
				raw, err := json.Marshal(response)
				if err != nil {
					t.Fatal(err)
				}
				assertJournalResponsePrivate(t, raw)
				if run.CanRecover || run.Status != "failed" || run.ErrorCode != "agent_failed" {
					t.Fatal(run)
				}
				x.request(t, "POST", path+"/recover", owner.Token, map[string]any{}, http.StatusConflict)
			}
			x.request(t, "GET", path, other.Token, nil, http.StatusNotFound)
			x.request(t, "POST", path+"/recover", other.Token, map[string]any{}, http.StatusNotFound)
			if len(x.agent.calls()) != before {
				t.Fatal("terminal recovery executed Agent")
			}
			db, err := sql.Open("mysql", x.dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var generation uint64
			if err := db.QueryRow("SELECT execution_generation FROM run_journal_heads WHERE run_id=?", original.RunID).Scan(&generation); err != nil {
				t.Fatal(err)
			}
			if generation != original.Generation || len(storedJournalFacts(t, x, original.RunID)) != 2 {
				t.Fatal("denial changed generation or journal")
			}
			messages := integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, 200))
			if len(messages) != 1 || messages[0].Content != "原问题" {
				t.Fatal("denial changed messages")
			}
		})
	}
}
