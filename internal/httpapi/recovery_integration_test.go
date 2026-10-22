package httpapi_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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
	"magworkspace/internal/indexing"
	"magworkspace/internal/store"
)

const recoveryPrivateFact = "private-journal-fact-not-for-public-api"

type recoveryHTTPResult struct {
	Run    domain.Run `json:"run"`
	Answer string     `json:"answer"`
}

type recoveryStreamingAgent struct{ *integrationAgent }

func (a recoveryStreamingAgent) Stream(ctx context.Context, req agent.Request, _ func(agent.Event) error) (agent.Result, error) {
	return a.integrationAgent.Run(ctx, req)
}

func recoveryPath(session domain.Session, runID string) string {
	return sessionPath(session) + "/runs/" + runID + "/recover"
}

// Synthetic records isolate the HTTP/Store contract. Real Harness record
// replay is separately exercised by the ProcessClient integration tests.
func recoveryAppendFact(ctx context.Context, req agent.Request, seq int) error {
	if req.JournalHandler == nil || req.Generation == 0 {
		return errors.New("authenticated execution omitted journal or generation")
	}
	previous := ""
	if seq > 1 {
		previous = strings.Repeat("a", 64)
	}
	digest := strings.Repeat("a", 64)
	if seq > 1 {
		digest = strings.Repeat("b", 64)
	}
	raw, err := json.Marshal(map[string]any{"record": map[string]any{
		"version": 1, "seq": seq, "kind": "created", "prev_hash": previous, "digest": digest,
		"data": map[string]any{"run_id": req.RunID, "marker": recoveryPrivateFact},
	}})
	if err != nil {
		return err
	}
	_, err = req.JournalHandler(ctx, "append", raw)
	return err
}

func recoveryLoadFacts(ctx context.Context, req agent.Request) error {
	loaded, err := req.JournalHandler(ctx, "load", json.RawMessage(`{}`))
	if err != nil {
		return err
	}
	raw, err := json.Marshal(loaded)
	if err != nil {
		return err
	}
	var facts struct {
		Records []struct {
			Seq  int `json:"seq"`
			Data struct {
				Marker string `json:"marker"`
			} `json:"data"`
		} `json:"records"`
	}
	if json.Unmarshal(raw, &facts) != nil || len(facts.Records) != 1 || facts.Records[0].Seq != 1 || facts.Records[0].Data.Marker != recoveryPrivateFact {
		return errors.New("recovery did not read the durable original journal")
	}
	return nil
}

func recoveryFailedHTTP(t *testing.T, x *indexFixture, auth integrationAuth, session domain.Session, scope []string) (domain.Run, agent.Request) {
	t.Helper()
	before := len(x.agent.calls())
	x.agent.set(func(ctx context.Context, req agent.Request) (agent.Result, error) {
		if req.Recover || req.Resume != nil {
			return agent.Result{}, errors.New("fresh request unexpectedly resumed")
		}
		if err := recoveryAppendFact(ctx, req, 1); err != nil {
			return agent.Result{}, fmt.Errorf("persist initial fact: %w", err)
		}
		return agent.Result{}, agent.ErrFailed
	})
	x.request(t, http.MethodPost, sessionPath(session)+"/messages", auth.Token,
		map[string]any{"content": "恢复原来的读取任务", "document_ids": scope}, http.StatusBadGateway)
	calls := x.agent.calls()
	if len(calls) != before+1 {
		t.Fatalf("initial request did not reach the agent once: %d calls", len(calls)-before)
	}
	request := calls[before]
	run := integrationData[domain.Run](t, x.request(t, http.MethodGet,
		sessionPath(session)+"/runs/"+request.RunID, auth.Token, nil, http.StatusOK))
	if run.Status != "failed" || run.ErrorCode != "agent_failed" || request.Generation == 0 || request.JournalHandler == nil {
		t.Fatalf("failed execution did not retain its recovery identity: run=%+v generation=%d", run, request.Generation)
	}
	return run, request
}

func recoverySQL(t *testing.T, x *indexFixture, query string, args ...any) {
	t.Helper()
	db, err := sql.Open("mysql", x.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		t.Fatalf("recovery failure injection changed %d rows: %v", changed, err)
	}
}

// Read actual SQL bytes without claiming a new execution or exposing them over
// the public API; completed and failed runs deliberately reject journal callbacks.
func storedJournalFacts(t *testing.T, x *indexFixture, runID string) [][]byte {
	t.Helper()
	db, err := sql.Open("mysql", x.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `SELECT content FROM run_journal_records WHERE run_id=? ORDER BY seq`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var records [][]byte
	for rows.Next() {
		var content []byte
		if err = rows.Scan(&content); err != nil {
			t.Fatal(err)
		}
		records = append(records, content)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return records
}

func recoveryRestart(t *testing.T, x *indexFixture) {
	t.Helper()
	x.server.Close()
	x.server = nil
	x.store.Close()
	x.store = nil
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := store.Open(ctx, x.dsn, x.profile)
	if err != nil {
		t.Fatal(err)
	}
	x.store = db
	db.SetObjectStorage(x.objects)
	x.service = indexing.NewService(db, x.parser, x.embedder, x.search, x.objects, x.profile, indexing.Options{JobTimeout: 10 * time.Second})
	x.config.Indexing = x.service
	x.server = httptest.NewServer(httpapi.New(db, x.agent, x.config))
}

func TestHTTPRecoveryDurableSameRunAfterRestartFencesOldExecution(t *testing.T) {
	x := newIndexFixture(t)
	x.store.SetObjectStorage(nil)
	owner := x.register(t, "recoveryrestart")
	session := x.session(t, owner, owner.Workspaces[0].ID, "持久化恢复")
	failed, original := recoveryFailedHTTP(t, x, owner, session, []string{})
	recoveryRestart(t, x)
	x.store.SetObjectStorage(nil)
	x.agent.set(func(ctx context.Context, req agent.Request) (agent.Result, error) {
		if !req.Recover || req.Resume != nil || req.RunID != failed.ID || req.UserID != owner.User.ID || req.WorkspaceID != session.WorkspaceID || req.SessionID != session.ID || req.Role != "owner" || req.Query != original.Query || req.DocumentIDs == nil || len(*req.DocumentIDs) != 0 || req.Generation <= original.Generation {
			return agent.Result{}, errors.New("recovery changed authenticated identity, original query, scope or generation")
		}
		if err := recoveryLoadFacts(ctx, req); err != nil {
			return agent.Result{}, err
		}
		stale := store.WithRunExecution(context.Background(), failed.ID, original.Generation)
		if err := x.store.FinishRun(stale, owner.User.ID, session.WorkspaceID, session.ID, failed.ID, "迟到的旧答案", "demo"); !errors.Is(err, domain.ErrConflict) {
			return agent.Result{}, fmt.Errorf("old generation finished recovered run: %v", err)
		}
		if err := x.store.FailRun(stale, owner.User.ID, session.WorkspaceID, session.ID, failed.ID, "interrupted"); !errors.Is(err, domain.ErrConflict) {
			return agent.Result{}, fmt.Errorf("old generation failed recovered run: %v", err)
		}
		if err := recoveryAppendFact(ctx, req, 2); err != nil {
			return agent.Result{}, err
		}
		return agent.Result{RunID: req.RunID, Answer: "从持久化事实完成读取", Mode: "demo"}, nil
	})
	response := x.call(http.MethodPost, recoveryPath(session, failed.ID), owner.Token, map[string]any{})
	completed := integrationData[recoveryHTTPResult](t, checkIntegrationResponse(t, response, http.StatusCreated))
	if strings.Contains(string(response.body), recoveryPrivateFact) || strings.Contains(string(response.body), "execution_generation") || completed.Run.ID != failed.ID || completed.Run.Status != "completed" || completed.Run.ErrorCode != "" || completed.Answer != "从持久化事实完成读取" || !completed.Run.CreatedAt.Equal(failed.CreatedAt) {
		t.Fatalf("recovery failed to preserve the original run or leaked journal data: %s", response.body)
	}
	messages := integrationData[[]domain.Message](t, x.request(t, http.MethodGet, sessionPath(session)+"/messages", owner.Token, nil, http.StatusOK))
	if len(messages) != 2 || messages[0].RunID != failed.ID || messages[0].Role != "user" || messages[0].Content != original.Query || messages[1].RunID != failed.ID || messages[1].Role != "assistant" || messages[1].Content != completed.Answer {
		t.Fatalf("recovery duplicated the user turn or committed an old result: %+v", messages)
	}
	x.request(t, http.MethodPost, recoveryPath(session, failed.ID), owner.Token, map[string]any{}, http.StatusConflict)
	if len(x.agent.calls()) != 2 {
		t.Fatalf("completed run recovery reached the agent again: %d calls", len(x.agent.calls()))
	}
}

func TestHTTPRecoveryConcurrentClaimExecutesOnce(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "recoveryconcurrent")
	session := x.session(t, owner, owner.Workspaces[0].ID, "只认领一次")
	failed, original := recoveryFailedHTTP(t, x, owner, session, []string{})
	entered, release := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	x.agent.set(func(ctx context.Context, req agent.Request) (agent.Result, error) {
		if !req.Recover {
			return agent.Result{}, errors.New("expected a recovery claim")
		}
		if _, err := original.ToolHandler(context.Background(), "list_documents", json.RawMessage(`{}`)); !errors.Is(err, domain.ErrConflict) {
			return agent.Result{}, fmt.Errorf("old tool capability reached recovered run: %v", err)
		}
		if _, err := original.JournalHandler(context.Background(), "load", json.RawMessage(`{}`)); !errors.Is(err, domain.ErrConflict) {
			return agent.Result{}, fmt.Errorf("old journal capability reached recovered run: %v", err)
		}
		enterOnce.Do(func() { close(entered) })
		select {
		case <-release:
			return agent.Result{RunID: req.RunID, Answer: "唯一恢复结果", Mode: "demo"}, nil
		case <-ctx.Done():
			return agent.Result{}, ctx.Err()
		}
	})
	path := recoveryPath(session, failed.ID)
	first := make(chan integrationResponse, 1)
	go func() { first <- x.call(http.MethodPost, path, owner.Token, map[string]any{}) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first recovery did not reach execution")
	}
	x.request(t, http.MethodPost, path, owner.Token, map[string]any{}, http.StatusConflict)
	x.request(t, http.MethodPost, sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "禁止重叠执行"}, http.StatusConflict)
	unblock()
	select {
	case response := <-first:
		checkIntegrationResponse(t, response, http.StatusCreated)
	case <-time.After(5 * time.Second):
		t.Fatal("claimed recovery did not complete")
	}
	if len(x.agent.calls()) != 2 {
		t.Fatalf("concurrent recovery executed more than once: %d calls", len(x.agent.calls()))
	}
}

func TestHTTPRecoveryLateCitationFailureCannotFailNewGeneration(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprintf("old_stream=%t", streaming), func(t *testing.T) {
			x := newIndexFixture(t)
			owner := x.register(t, "recoverylatecitation")
			session := x.session(t, owner, owner.Workspaces[0].ID, "旧执行不可终止恢复")
			originalEntered := make(chan agent.Request, 1)
			recoveryEntered := make(chan struct{}, 1)
			releaseOriginal, releaseRecovery := make(chan struct{}), make(chan struct{})
			var originalOnce, recoveryOnce sync.Once
			unblockOriginal := func() { originalOnce.Do(func() { close(releaseOriginal) }) }
			unblockRecovery := func() { recoveryOnce.Do(func() { close(releaseRecovery) }) }
			t.Cleanup(func() { unblockOriginal(); unblockRecovery() })
			x.agent.set(func(ctx context.Context, req agent.Request) (agent.Result, error) {
				if !req.Recover {
					if err := recoveryAppendFact(ctx, req, 1); err != nil {
						return agent.Result{}, err
					}
					originalEntered <- req
					select {
					case <-releaseOriginal:
						return agent.Result{RunID: req.RunID, Mode: "demo", Answer: "旧结果 [E:invalid]"}, nil
					case <-ctx.Done():
						return agent.Result{}, ctx.Err()
					}
				}
				recoveryEntered <- struct{}{}
				select {
				case <-releaseRecovery:
					return agent.Result{RunID: req.RunID, Mode: "demo", Answer: "新代唯一完成结果"}, nil
				case <-ctx.Done():
					return agent.Result{}, ctx.Err()
				}
			})
			x.server.Close()
			x.server = httptest.NewServer(httpapi.New(x.store, recoveryStreamingAgent{x.agent}, x.config))
			originalResult := make(chan integrationResponse, 1)
			go func() {
				if !streaming {
					originalResult <- x.call(http.MethodPost, sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "旧任务"})
					return
				}
				req, err := http.NewRequest(http.MethodPost, x.server.URL+sessionPath(session)+"/messages", bytes.NewBufferString(`{"content":"旧任务"}`))
				if err != nil {
					originalResult <- integrationResponse{err: err}
					return
				}
				req.Header.Set("Authorization", "Bearer "+owner.Token)
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Accept", "text/event-stream")
				client := &http.Client{Timeout: 10 * time.Second}
				response, err := client.Do(req)
				if err != nil {
					originalResult <- integrationResponse{err: err}
					return
				}
				defer response.Body.Close()
				raw, err := io.ReadAll(response.Body)
				originalResult <- integrationResponse{status: response.StatusCode, body: raw, err: err}
			}()
			var original agent.Request
			select {
			case original = <-originalEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("initial worker did not persist its journal")
			}
			recoverySQL(t, x, `UPDATE runs SET lease_expires_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE id=?`, original.RunID)
			recoveryResult := make(chan integrationResponse, 1)
			go func() {
				recoveryResult <- x.call(http.MethodPost, recoveryPath(session, original.RunID), owner.Token, map[string]any{})
			}()
			select {
			case <-recoveryEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("new recovery generation did not start")
			}
			unblockOriginal()
			select {
			case response := <-originalResult:
				if streaming {
					if response.err != nil || response.status != http.StatusOK || !strings.Contains(string(response.body), "event: run.failed") {
						t.Fatalf("old stream did not terminate after rejected citation: %+v", response)
					}
				} else {
					checkIntegrationResponse(t, response, http.StatusBadGateway)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("old worker did not return its late citation failure")
			}
			current := integrationData[domain.Run](t, x.request(t, http.MethodGet, sessionPath(session)+"/runs/"+original.RunID, owner.Token, nil, http.StatusOK))
			if current.Status != "running" || current.ErrorCode != "" {
				t.Fatalf("old citation failure terminated the new recovery generation: %+v", current)
			}
			unblockRecovery()
			select {
			case response := <-recoveryResult:
				checkIntegrationResponse(t, response, http.StatusCreated)
			case <-time.After(5 * time.Second):
				t.Fatal("recovered generation did not finish")
			}
		})
	}
}

func TestHTTPRecoveryRejectsIneligibleRunsAndNewerActiveTurn(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "recoveryterminal")
	for _, code := range []string{"agent_canceled", "agent_timeout", "question_canceled", "invalid_citation"} {
		t.Run(code, func(t *testing.T) {
			session := x.session(t, owner, owner.Workspaces[0].ID, code)
			failed, _ := recoveryFailedHTTP(t, x, owner, session, []string{})
			recoverySQL(t, x, `UPDATE runs SET error_code=? WHERE id=?`, code, failed.ID)
			before := len(x.agent.calls())
			x.request(t, http.MethodPost, recoveryPath(session, failed.ID), owner.Token, map[string]any{}, http.StatusConflict)
			got := integrationData[domain.Run](t, x.request(t, http.MethodGet, sessionPath(session)+"/runs/"+failed.ID, owner.Token, nil, http.StatusOK))
			if len(x.agent.calls()) != before || got.Status != "failed" || got.ErrorCode != code {
				t.Fatalf("ineligible run was claimed or mutated: %+v", got)
			}
		})
	}
	t.Run("missing journal", func(t *testing.T) {
		session := x.session(t, owner, owner.Workspaces[0].ID, "旧运行没有日志")
		x.agent.set(func(context.Context, agent.Request) (agent.Result, error) { return agent.Result{}, agent.ErrFailed })
		x.request(t, http.MethodPost, sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "没有可恢复事实"}, http.StatusBadGateway)
		calls := x.agent.calls()
		x.request(t, http.MethodPost, recoveryPath(session, calls[len(calls)-1].RunID), owner.Token, map[string]any{}, http.StatusConflict)
		if len(x.agent.calls()) != len(calls) {
			t.Fatal("run without journal reached recovery execution")
		}
	})
	t.Run("unexpired running", func(t *testing.T) {
		session := x.session(t, owner, owner.Workspaces[0].ID, "租约仍有效")
		failed, _ := recoveryFailedHTTP(t, x, owner, session, []string{})
		recoverySQL(t, x, `UPDATE runs SET status='running',error_code='',finished_at=NULL,lease_expires_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 1 HOUR) WHERE id=?`, failed.ID)
		before := len(x.agent.calls())
		x.request(t, http.MethodPost, recoveryPath(session, failed.ID), owner.Token, map[string]any{}, http.StatusConflict)
		if len(x.agent.calls()) != before {
			t.Fatal("live execution lease was stolen")
		}
	})
	t.Run("newer active turn", func(t *testing.T) {
		session := x.session(t, owner, owner.Workspaces[0].ID, "已有新执行")
		failed, _ := recoveryFailedHTTP(t, x, owner, session, []string{})
		if _, _, _, err := x.store.BeginRun(context.Background(), owner.User.ID, session.WorkspaceID, session.ID, "新任务", time.Minute); err != nil {
			t.Fatal(err)
		}
		before := len(x.agent.calls())
		x.request(t, http.MethodPost, recoveryPath(session, failed.ID), owner.Token, map[string]any{}, http.StatusConflict)
		if len(x.agent.calls()) != before {
			t.Fatal("old recovery overlapped a newer active turn")
		}
	})
}

func TestHTTPRecoveryPrivateIdentityScopeAndRevocation(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "recoverywsowner")
	member := x.register(t, "recoverymember")
	outsider := x.register(t, "recoveryoutsider")
	base := workspacePath(owner.Workspaces[0].ID)
	x.request(t, http.MethodPost, base+"/members", owner.Token, map[string]string{"email": member.User.Email, "role": "contributor"}, http.StatusCreated)
	session := x.session(t, member, owner.Workspaces[0].ID, "成员私有恢复")
	failed, _ := recoveryFailedHTTP(t, x, member, session, []string{})
	path := recoveryPath(session, failed.ID)
	before := len(x.agent.calls())
	x.request(t, http.MethodPost, path, "", map[string]any{}, http.StatusUnauthorized)
	for _, caller := range []integrationAuth{owner, outsider} {
		x.request(t, http.MethodPost, path, caller.Token, map[string]any{}, http.StatusNotFound)
	}
	otherSession := x.session(t, member, owner.Workspaces[0].ID, "同成员另一会话")
	x.request(t, http.MethodPost, recoveryPath(otherSession, failed.ID), member.Token, map[string]any{}, http.StatusNotFound)
	otherWorkspace := x.session(t, member, member.Workspaces[0].ID, "另一空间")
	x.request(t, http.MethodPost, recoveryPath(otherWorkspace, failed.ID), member.Token, map[string]any{}, http.StatusNotFound)
	for _, body := range []any{
		map[string]any{"user_id": owner.User.ID}, map[string]any{"query": "改写原任务"},
		map[string]any{"document_ids": []string{}}, map[string]any{"generation": 999},
	} {
		x.request(t, http.MethodPost, path, member.Token, body, http.StatusBadRequest)
	}
	x.request(t, http.MethodDelete, base+"/members/"+member.User.ID, owner.Token, nil, http.StatusNoContent)
	x.request(t, http.MethodPost, path, member.Token, map[string]any{}, http.StatusNotFound)
	if len(x.agent.calls()) != before {
		t.Fatalf("invalid recovery caller or body reached execution: %d extra calls", len(x.agent.calls())-before)
	}
}

func TestHTTPRecoveryRejectsDeletedDocumentScope(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "recoveryscope")
	document := x.upload(t, owner, "仅在原来的文档范围内读取")
	session := x.session(t, owner, owner.Workspaces[0].ID, "原范围必须仍有效")
	failed, _ := recoveryFailedHTTP(t, x, owner, session, []string{document.Document.ID})
	x.request(t, http.MethodDelete, documentPath(document.Document), owner.Token, nil, http.StatusNoContent)
	before := len(x.agent.calls())
	x.request(t, http.MethodPost, recoveryPath(session, failed.ID), owner.Token, map[string]any{}, http.StatusConflict)
	got := integrationData[domain.Run](t, x.request(t, http.MethodGet, sessionPath(session)+"/runs/"+failed.ID, owner.Token, nil, http.StatusOK))
	if len(x.agent.calls()) != before || got.Status != "failed" || got.ErrorCode != "agent_failed" {
		t.Fatalf("deleted scope was broadened or execution was claimed: %+v", got)
	}
}

func TestHTTPRecoveryExpiredLeaseUsesSSEAndPersistsTerminal(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "recoverysse")
	session := x.session(t, owner, owner.Workspaces[0].ID, "租约到期后流式恢复")
	failed, original := recoveryFailedHTTP(t, x, owner, session, []string{})
	// Reconstruct the durable state left by a process dying before FailRun.
	recoverySQL(t, x, `UPDATE runs SET status='running',error_code='',finished_at=NULL,lease_expires_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE id=?`, failed.ID)
	client := &sseIntegrationAgent{stream: func(ctx context.Context, req agent.Request, emit func(agent.Event) error) (agent.Result, error) {
		if !req.Recover || req.RunID != failed.ID || req.Generation <= original.Generation || req.Query != original.Query || req.UserID != owner.User.ID || req.DocumentIDs == nil || len(*req.DocumentIDs) != 0 {
			return agent.Result{}, errors.New("expired lease recovery lost original execution identity")
		}
		if err := recoveryLoadFacts(ctx, req); err != nil {
			return agent.Result{}, err
		}
		if err := emitSSETestMessage(req, emit, 1, "恢复中的增量"); err != nil {
			return agent.Result{}, err
		}
		return agent.Result{RunID: req.RunID, Answer: "恢复后的最终答案", Mode: "demo"}, nil
	}}
	x.server.Close()
	x.server = httptest.NewServer(httpapi.New(x.store, client, x.config))
	rejected, _, rejectCancel := sseTestRequest(t, x.integrationFixture, recoveryPath(session, failed.ID), owner.Token, map[string]any{}, "previous-event")
	rejectedBody, rejectedErr := io.ReadAll(rejected.Body)
	rejectCancel()
	rejected.Body.Close()
	checkIntegrationResponse(t, integrationResponse{status: rejected.StatusCode, body: rejectedBody, err: rejectedErr}, http.StatusBadRequest)
	if _, streams, runs := client.calls(); streams != 0 || runs != 0 {
		t.Fatal("SSE replay request claimed recovery before rejecting Last-Event-ID")
	}
	response, _, cancel := sseTestRequest(t, x.integrationFixture, recoveryPath(session, failed.ID), owner.Token, map[string]any{}, "")
	defer cancel()
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") || response.Header.Get("X-Run-ID") != failed.ID || strings.Contains(string(raw), recoveryPrivateFact) {
		t.Fatalf("invalid recovery stream: status=%d headers=%v body=%s", response.StatusCode, response.Header, raw)
	}
	var names []string
	for _, block := range strings.Split(string(raw), "\n\n") {
		var name string
		var payload sseTestPayload
		for _, line := range strings.Split(block, "\n") {
			if strings.HasPrefix(line, "event: ") {
				name = strings.TrimPrefix(line, "event: ")
			}
			if strings.HasPrefix(line, "data: ") {
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &payload); err != nil {
					t.Fatal(err)
				}
			}
		}
		if name == "" {
			continue
		}
		names = append(names, name)
		checkSSEIdentity(t, payload, failed.ID, session.ID)
		if name == "run.completed" && (payload.Run.Status != "completed" || payload.Run.FinishedAt == nil || payload.Answer != "恢复后的最终答案") {
			t.Fatalf("invalid recovered terminal event: %+v", payload)
		}
	}
	if strings.Join(names, ",") != "run.started,message.start,message.delta,run.completed" {
		t.Fatalf("unexpected recovery SSE sequence: %v; body=%s", names, raw)
	}
	messages := integrationData[[]domain.Message](t, x.request(t, http.MethodGet, sessionPath(session)+"/messages", owner.Token, nil, http.StatusOK))
	if len(messages) != 2 || messages[0].RunID != failed.ID || messages[1].RunID != failed.ID || messages[1].Content != "恢复后的最终答案" || messages[1].Status != "completed" {
		t.Fatalf("SSE recovery terminal preceded durable messages: %+v", messages)
	}
	_, streams, runs := client.calls()
	if streams != 1 || runs != 0 {
		t.Fatalf("recovery did not use the streaming entry point: streams=%d runs=%d", streams, runs)
	}
}
