package httpapi_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"magworkspace/internal/agent"
)

func TestHTTPPromptSnapshotRejectsBeforeAnswerAndRecoveryClaim(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "prompt-snapshot-integrity")
	session := x.session(t, owner, owner.Workspaces[0].ID, "原生快照校验")
	const promptID = "21212121212121212121212121212121"
	x.agent.set(func(ctx context.Context, req agent.Request) (agent.Result, error) {
		if err := recoveryAppendFact(ctx, req, 1); err != nil {
			return agent.Result{}, err
		}
		result := promptTestResult(req, promptID)
		args, err := json.Marshal(map[string]any{"question": result.Prompt.Question, "options": result.Prompt.Options})
		if err != nil {
			return agent.Result{}, err
		}
		var scope []string
		if req.DocumentIDs != nil {
			scope = *req.DocumentIDs
		}
		raw, err := json.Marshal(map[string]any{
			"version": 2, "run_id": req.RunID, "session_id": req.SessionID, "query": req.Query,
			"caller":  map[string]string{"user_id": req.UserID, "workspace_id": req.WorkspaceID},
			"scope":   map[string]any{"workspace_id": req.WorkspaceID, "document_ids": scope},
			"policy":  map[string]string{"access": "read_only"},
			"pending": map[string]any{"blocked_index": 0, "tool_calls": []any{map[string]string{"id": result.Prompt.ToolCallID, "name": "ask_user", "arguments": string(args)}}},
		})
		result.Prompt.Snapshot = string(raw)
		return result, err
	})
	pending := promptSuspendHTTP(t, x, owner, session)
	db, err := sql.Open("mysql", x.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var original []byte
	var generation uint64
	if err = db.QueryRow(`SELECT snapshot FROM run_prompts WHERE id=?`, promptID).Scan(&original); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT execution_generation FROM run_journal_heads WHERE run_id=?`, pending.Run.ID).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	replace := func(raw []byte) {
		t.Helper()
		digest := sha256.Sum256(raw)
		if _, e := db.Exec(`UPDATE run_prompts SET snapshot=?,snapshot_size=?,snapshot_sha256=? WHERE id=?`, raw, len(raw), hex.EncodeToString(digest[:]), promptID); e != nil {
			t.Fatal(e)
		}
	}
	assertClaim := func(runState, promptState, answer string, expectedGeneration uint64) {
		t.Helper()
		var gotRun, gotPrompt, gotAnswer string
		var gotGeneration uint64
		if e := db.QueryRow(`SELECT r.status,p.status,p.answer,h.execution_generation FROM runs r JOIN run_prompts p ON p.run_id=r.id JOIN run_journal_heads h ON h.run_id=r.id WHERE p.id=?`, promptID).Scan(&gotRun, &gotPrompt, &gotAnswer, &gotGeneration); e != nil {
			t.Fatal(e)
		}
		if gotRun != runState || gotPrompt != promptState || gotAnswer != answer || gotGeneration != expectedGeneration {
			t.Fatalf("snapshot rejection changed claim: %s %s %q generation=%d", gotRun, gotPrompt, gotAnswer, gotGeneration)
		}
	}
	answerPath := promptPath(session, pending.Run.ID, promptID) + "/answer"
	for _, bad := range []string{
		`{"opaque":"unsupported adapter format"}`,
		strings.Replace(string(original), `"version":2`, "\"memory_prompt\":\"\xff\",\"version\":2", 1),
		strings.Replace(string(original), `"version":2`, `"version":1`, 1),
		strings.Replace(string(original), `"version":2`, `"version":3`, 1),
		strings.Replace(string(original), `,"version":2`, ``, 1),
		strings.Replace(string(original), `"version":2`, `"version":2,"version":2`, 1),
	} {
		replace([]byte(bad))
		x.request(t, "POST", answerPath, owner.Token, map[string]string{"answer": "国内"}, 400)
		assertClaim("suspended", "pending", "", generation)
		if len(x.agent.calls()) != 1 {
			t.Fatal("invalid snapshot reached Agent before answer claim")
		}
	}
	replace(original)
	x.agent.set(func(context.Context, agent.Request) (agent.Result, error) { return agent.Result{}, agent.ErrFailed })
	x.request(t, "POST", answerPath, owner.Token, map[string]string{"answer": "国内"}, 502)
	assertClaim("failed", "failed", "国内", generation+1)
	replace([]byte(strings.Replace(string(original), `"version":2`, `"version":3`, 1)))
	x.request(t, "POST", recoveryPath(session, pending.Run.ID), owner.Token, map[string]any{}, 400)
	assertClaim("failed", "failed", "国内", generation+1)
	if len(x.agent.calls()) != 2 {
		t.Fatal("invalid persisted snapshot reached Agent during recovery")
	}
	replace(original)
	x.agent.set(func(_ context.Context, req agent.Request) (agent.Result, error) {
		return agent.Result{RunID: req.RunID, Mode: "demo", Answer: "已恢复原运行"}, nil
	})
	x.request(t, "POST", recoveryPath(session, pending.Run.ID), owner.Token, map[string]any{}, 201)
	assertClaim("completed", "done", "国内", generation+2)
}
