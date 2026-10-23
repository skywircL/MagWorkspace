package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
)

func writeProposal(t *testing.T, uid, wid, sid, rid, tool string, args any, scope []string) domain.SuspendedPrompt {
	t.Helper()
	in := promptInput(t)
	in.ToolCallID = "write-" + in.ID
	raw, err := json.Marshal(args)
	must(t, err)
	in.Approval = &domain.DocumentApproval{Tool: tool, Risk: "write", Description: "Write reviewed text", Reason: "always confirm", Arguments: raw}
	snapshot, err := json.Marshal(map[string]any{"run_id": rid, "session_id": sid, "query": "write", "caller": map[string]string{"user_id": uid, "workspace_id": wid}, "scope": map[string]any{"workspace_id": wid, "document_ids": scope}, "policy": map[string]string{"access": "confirm"}, "pending": map[string]any{"blocked_index": 0, "tool_calls": []any{map[string]string{"id": in.ToolCallID, "name": tool, "arguments": string(raw)}}}})
	must(t, err)
	in.Snapshot = string(snapshot)
	return in
}

func TestMySQLDocumentApprovalAtomicReceiptVersionsAndRestart(t *testing.T) {
	s, dsn := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "writeapproval")
	objects := newMemoryObjects()
	s.SetObjectStorage(objects)
	profile := testIndexProfile()
	s.indexProfile = &profile
	ss, err := s.CreateSession(ctx, u.ID, w.ID, "writes")
	must(t, err)
	run, _, _, err := s.BeginRun(ctx, u.ID, w.ID, ss.ID, "write", time.Minute)
	must(t, err)
	gen, err := s.BeginRunJournal(ctx, u.ID, w.ID, ss.ID, run.ID)
	must(t, err)
	oldctx := WithRunExecution(ctx, run.ID, gen)
	in := writeProposal(t, u.ID, w.ID, ss.ID, run.ID, "create_document", domain.DocumentMutation{Title: "审批稿", Filename: "a.md", Content: "原文：不可覆盖"}, nil)
	// The closed schema excludes fields belonging to the other operation.
	_, err = s.SuspendRun(oldctx, u.ID, w.ID, ss.ID, run.ID, "live", in)
	must(t, err)
	_, err = s.ExecuteDocumentWrite(oldctx, u.ID, w.ID, ss.ID, run.ID, in.ToolCallID, in.Approval.Tool, in.Approval.Arguments)
	if err == nil {
		t.Fatal("unapproved write")
	}
	s.Close()
	s, err = Open(ctx, dsn, profile)
	must(t, err)
	defer s.Close()
	// Reading and claiming a persisted approval needs only MySQL after restart.
	p, err := s.RunPrompt(ctx, u.ID, w.ID, ss.ID, run.ID, in.ID)
	must(t, err)
	if p.Kind != "tool_approval" || p.Approval == nil || !strings.Contains(string(p.Approval.Arguments), "不可覆盖") {
		t.Fatalf("proposal lost: %+v", p)
	}
	var metadata []byte
	must(t, s.pool.QueryRow(`SELECT metadata FROM run_document_approvals WHERE prompt_id=?`, in.ID).Scan(&metadata))
	if strings.Contains(string(metadata), "不可覆盖") || strings.Contains(string(metadata), `"arguments":`) {
		t.Fatal("proposal body leaked into public approval metadata")
	}
	var claims atomic.Int32
	var resume domain.ResumedRun
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := s.ResumeApproval(ctx, u.ID, w.ID, ss.ID, run.ID, in.ID, true, time.Minute)
			if e == nil {
				claims.Add(1)
				mu.Lock()
				resume = r
				mu.Unlock()
			} else if !errors.Is(e, domain.ErrConflict) {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if claims.Load() != 1 {
		t.Fatalf("approval claims=%d", claims.Load())
	}
	_, err = s.ExecuteDocumentWrite(WithRunExecution(ctx, run.ID, resume.Generation), u.ID, w.ID, ss.ID, run.ID, in.ToolCallID, in.Approval.Tool, in.Approval.Arguments)
	wantError(t, err, blob.ErrUnavailable)
	s.SetObjectStorage(objects)
	_, err = s.ExecuteDocumentWrite(oldctx, u.ID, w.ID, ss.ID, run.ID, in.ToolCallID, in.Approval.Tool, in.Approval.Arguments)
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("old generation: %v", err)
	}
	ctx = WithRunExecution(ctx, run.ID, resume.Generation)
	receipts := make(chan domain.DocumentWrite, 4)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, e := s.ExecuteDocumentWrite(ctx, u.ID, w.ID, ss.ID, run.ID, in.ToolCallID, in.Approval.Tool, in.Approval.Arguments)
			if e != nil {
				t.Error(e)
				return
			}
			receipts <- out
		}()
	}
	wg.Wait()
	close(receipts)
	var created domain.DocumentWrite
	for out := range receipts {
		if created.Document.ID != "" && out.Version.ID != created.Version.ID {
			t.Fatal("duplicate write")
		}
		created = out
	}
	if created.Version.Number != 1 {
		t.Fatal("missing first version")
	}
	_, total, err := s.Documents(ctx, u.ID, w.ID, 20, 0)
	must(t, err)
	if total != 1 {
		t.Fatalf("created %d documents", total)
	}
	job, err := s.DocumentIndex(ctx, u.ID, w.ID, created.Document.ID, created.Version.ID, profile.ID)
	must(t, err)
	if job.Status != "queued" {
		t.Fatalf("index: %+v", job)
	}
	tampered := json.RawMessage(strings.Replace(string(in.Approval.Arguments), "不可覆盖", "篡改", 1))
	if _, err = s.ExecuteDocumentWrite(ctx, u.ID, w.ID, ss.ID, run.ID, in.ToolCallID, in.Approval.Tool, tampered); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("mutated args: %v", err)
	}
	must(t, s.FinishRun(ctx, u.ID, w.ID, ss.ID, run.ID, "created", "live"))
	p, err = s.RunPrompt(context.Background(), u.ID, w.ID, ss.ID, run.ID, in.ID)
	must(t, err)
	if p.Approval.Result == nil || p.Approval.Result.Version.ID != created.Version.ID {
		t.Fatal("receipt unavailable after completion")
	}
	allowed, e := s.CanHydrateRunHistory(context.Background(), u.ID, w.ID, ss.ID, run.ID, []string{})
	must(t, e)
	if allowed {
		t.Fatal("write receipt lost document provenance")
	}
	query, err := s.ClarifiedQuery(context.Background(), u.ID, w.ID, ss.ID, run.ID, "write")
	must(t, err)
	if query != "write" {
		t.Fatal("approval was inserted as a user clarification")
	}
	// A new run appends a version, retaining old bytes and one index job per head.
	ctx = context.Background()
	r2, _, _, err := s.BeginRun(ctx, u.ID, w.ID, ss.ID, "write", time.Minute, []string{created.Document.ID})
	must(t, err)
	in2 := writeProposal(t, u.ID, w.ID, ss.ID, r2.ID, "update_document", domain.DocumentMutation{DocumentID: created.Document.ID, BaseVersionID: created.Version.ID, Content: "新文：保留历史"}, []string{created.Document.ID})
	_, err = s.SuspendRun(ctx, u.ID, w.ID, ss.ID, r2.ID, "live", in2)
	must(t, err)
	_, err = s.ResumeApproval(ctx, u.ID, w.ID, ss.ID, r2.ID, in2.ID, true, time.Minute)
	must(t, err)
	updated, err := s.ExecuteDocumentWrite(ctx, u.ID, w.ID, ss.ID, r2.ID, in2.ToolCallID, in2.Approval.Tool, in2.Approval.Arguments)
	must(t, err)
	if updated.Version.Number != 2 || updated.Document.ID != created.Document.ID {
		t.Fatalf("bad update: %+v", updated)
	}
	created.Version, err = s.DocumentVersion(ctx, u.ID, w.ID, created.Document.ID, created.Version.ID)
	must(t, err)
	checkDocumentContent(t, s, u.ID, w.ID, created.Version, []byte("原文：不可覆盖"))
	checkDocumentContent(t, s, u.ID, w.ID, updated.Version, []byte("新文：保留历史"))
	must(t, s.Migrate(ctx))
	must(t, s.Migrate(ctx))
}

func TestMySQLDocumentApprovalDenialAndRevalidation(t *testing.T) {
	for _, scenario := range []string{"reject", "cancel", "expire", "stale-before-approve", "stale-after-approve", "revoke-after-approve", "revoke-during-put", "stale-generation-during-put", "object-failure", "wrong-call", "scope-create", "scope-update", "viewer", "snapshot-identity", "snapshot-args", "delete-before-approve", "delete-after-approve", "revoke-before-approve"} {
		t.Run(scenario, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			owner, w := testUser(t, s, "approvalowner")
			u, _ := testUser(t, s, "approvalwriter")
			_, err := s.AddMember(ctx, owner.ID, w.ID, u.Email, "contributor")
			must(t, err)
			objects := newMemoryObjects()
			s.SetObjectStorage(objects)
			doc, err := s.CreateDocument(ctx, u.ID, w.ID, "Original", "a.md", []byte("original"))
			must(t, err)
			ss, err := s.CreateSession(ctx, u.ID, w.ID, "approval")
			must(t, err)
			var scope []string
			if strings.HasPrefix(scenario, "scope-") {
				scope = []string{}
			}
			r, _, _, err := s.BeginRun(ctx, u.ID, w.ID, ss.ID, "write", time.Minute, scope)
			must(t, err)
			gen, err := s.BeginRunJournal(ctx, u.ID, w.ID, ss.ID, r.ID)
			must(t, err)
			ctx = WithRunExecution(ctx, r.ID, gen)
			tool := "update_document"
			args := domain.DocumentMutation{DocumentID: doc.Document.ID, BaseVersionID: doc.Version.ID, Content: "new private text"}
			if scenario == "scope-create" {
				tool = "create_document"
				args = domain.DocumentMutation{Title: "new", Filename: "new.md", Content: "new"}
			}
			in := writeProposal(t, u.ID, w.ID, ss.ID, r.ID, tool, args, scope)
			if scenario == "viewer" {
				_, err = s.pool.Exec(`UPDATE workspace_members SET role='viewer' WHERE workspace_id=? AND user_id=?`, w.ID, u.ID)
				must(t, err)
			}
			if scenario == "snapshot-identity" {
				in.Snapshot = strings.Replace(in.Snapshot, u.ID, owner.ID, 1)
			}
			if scenario == "snapshot-args" {
				in.Approval.Arguments = json.RawMessage(strings.Replace(string(in.Approval.Arguments), "new private text", "tampered", 1))
			}
			_, err = s.SuspendRun(ctx, u.ID, w.ID, ss.ID, r.ID, "live", in)
			if strings.HasPrefix(scenario, "scope-") || scenario == "viewer" || strings.HasPrefix(scenario, "snapshot-") {
				if err == nil {
					t.Fatal("invalid proposal persisted")
				}
				return
			}
			must(t, err)
			if _, err = s.ResumePrompt(ctx, u.ID, w.ID, ss.ID, r.ID, in.ID, "true", time.Minute); !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("answer bypass: %v", err)
			}
			if _, err = s.RunPrompt(context.Background(), owner.ID, w.ID, ss.ID, r.ID, in.ID); err == nil {
				t.Fatal("workspace owner read another user's proposal")
			}
			advance := func() {
				_, e := s.AppendDocumentVersion(context.Background(), owner.ID, w.ID, doc.Document.ID, doc.Version.ID, "a.md", []byte("manual replacement"))
				must(t, e)
			}
			switch scenario {
			case "cancel":
				must(t, s.CancelPrompt(ctx, u.ID, w.ID, ss.ID, r.ID, in.ID))
			case "expire":
				_, err = s.pool.Exec(`UPDATE run_prompts SET expires_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE id=?`, in.ID)
				must(t, err)
			case "stale-before-approve":
				advance()
			case "delete-before-approve":
				must(t, s.DeleteDocument(context.Background(), owner.ID, w.ID, doc.Document.ID))
			case "revoke-before-approve":
				must(t, s.DeleteMember(context.Background(), owner.ID, w.ID, u.ID))
			}
			resumed, err := s.ResumeApproval(context.Background(), u.ID, w.ID, ss.ID, r.ID, in.ID, scenario != "reject", time.Minute)
			if scenario == "cancel" || scenario == "expire" || scenario == "stale-before-approve" || scenario == "delete-before-approve" || scenario == "revoke-before-approve" {
				if err == nil {
					t.Fatal("invalid approval claimed")
				}
				return
			}
			must(t, err)
			ctx = WithRunExecution(context.Background(), r.ID, resumed.Generation)
			switch scenario {
			case "stale-after-approve":
				advance()
			case "delete-after-approve":
				must(t, s.DeleteDocument(context.Background(), owner.ID, w.ID, doc.Document.ID))
			case "revoke-after-approve":
				_, err = s.pool.Exec(`DELETE FROM workspace_members WHERE workspace_id=? AND user_id=?`, w.ID, u.ID)
				must(t, err)
			case "revoke-during-put":
				objects.putHook = func(context.Context, string) error {
					_, e := s.pool.Exec(`DELETE FROM workspace_members WHERE workspace_id=? AND user_id=?`, w.ID, u.ID)
					return e
				}
			case "stale-generation-during-put":
				objects.putHook = func(context.Context, string) error {
					_, e := s.pool.Exec(`UPDATE run_journal_heads SET execution_generation=execution_generation+1 WHERE run_id=?`, r.ID)
					return e
				}
			case "object-failure":
				objects.putHook = func(context.Context, string) error { return blob.ErrUnavailable }
			case "wrong-call":
				in.ToolCallID = "unapproved-call"
			}
			_, err = s.ExecuteDocumentWrite(ctx, u.ID, w.ID, ss.ID, r.ID, in.ToolCallID, tool, in.Approval.Arguments)
			if err == nil {
				t.Fatal("write survived denial or changed authority")
			}
			if scenario == "delete-after-approve" {
				var n int
				must(t, s.pool.QueryRow(`SELECT COUNT(*) FROM document_versions WHERE document_id=?`, doc.Document.ID).Scan(&n))
				if n != 0 {
					t.Fatal("write resurrected deleted document")
				}
				must(t, s.pool.QueryRow(`SELECT COUNT(*) FROM documents WHERE id=?`, doc.Document.ID).Scan(&n))
				if n != 0 {
					t.Fatal("write recreated deleted document metadata")
				}
				must(t, s.pool.QueryRow(`SELECT COUNT(*) FROM run_document_approvals WHERE result IS NOT NULL`).Scan(&n))
				if n != 0 {
					t.Fatal("receipt for deleted document write")
				}
				return
			}
			versions, total, e := s.DocumentVersions(context.Background(), owner.ID, w.ID, doc.Document.ID, 20, 0)
			must(t, e)
			want := 1
			if scenario == "stale-after-approve" {
				want = 2
			}
			if total != want {
				t.Fatalf("unauthorized version count %d", total)
			}
			for _, v := range versions {
				_, body, e := s.DocumentContent(context.Background(), owner.ID, w.ID, doc.Document.ID, v.ID)
				must(t, e)
				if string(body) == "new private text" {
					t.Fatal("unapproved content published")
				}
			}
			var count int
			must(t, s.pool.QueryRow(`SELECT COUNT(*) FROM run_document_approvals WHERE result IS NOT NULL`).Scan(&count))
			if count != 0 {
				t.Fatal("receipt for failed write")
			}
		})
	}
}

func TestMySQLDocumentApprovalOriginalSourcePagesAndReauthorization(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "sourcepages")
	objects := newMemoryObjects()
	s.SetObjectStorage(objects)
	content := strings.Repeat(" \t正文\r\n", 900)
	doc, err := s.CreateDocument(ctx, u.ID, w.ID, "source", "source.md", []byte(content))
	must(t, err)
	ss, err := s.CreateSession(ctx, u.ID, w.ID, "source")
	must(t, err)
	r, _, _, err := s.BeginRun(ctx, u.ID, w.ID, ss.ID, "write", time.Minute, []string{doc.Document.ID})
	must(t, err)
	gen, err := s.BeginRunJournal(ctx, u.ID, w.ID, ss.ID, r.ID)
	must(t, err)
	ctx = WithRunExecution(ctx, r.ID, gen)
	var all strings.Builder
	offset := 0
	vid := ""
	for {
		p, e := s.RunDocumentSource(ctx, u.ID, w.ID, ss.ID, r.ID, doc.Document.ID, vid, offset)
		must(t, e)
		all.WriteString(p.Source.Content)
		vid = p.Version.ID
		if !p.Source.HasMore {
			break
		}
		if p.Source.NextOffset == nil || *p.Source.NextOffset <= offset {
			t.Fatal("invalid source cursor")
		}
		offset = *p.Source.NextOffset
	}
	if all.String() != content {
		t.Fatal("original source whitespace changed or content truncated")
	}
	alien, err := s.CreateDocument(context.Background(), u.ID, w.ID, "other", "other.txt", []byte("outside scope"))
	must(t, err)
	if _, err = s.RunDocumentSource(ctx, u.ID, w.ID, ss.ID, r.ID, alien.Document.ID, "", 0); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("source escaped scope: %v", err)
	}
	objects.readHook = func(context.Context, string) error {
		_, e := s.pool.Exec(`UPDATE run_journal_heads SET execution_generation=execution_generation+1 WHERE run_id=?`, r.ID)
		return e
	}
	if _, err = s.RunDocumentSource(ctx, u.ID, w.ID, ss.ID, r.ID, doc.Document.ID, vid, 0); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("late source release: %v", err)
	}
	objects.readHook = nil
	_, err = s.AppendDocumentVersion(context.Background(), u.ID, w.ID, doc.Document.ID, vid, "source.md", []byte("new head"))
	must(t, err)
	if _, err = s.RunDocumentSource(context.Background(), u.ID, w.ID, ss.ID, r.ID, doc.Document.ID, vid, 2048); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("source pages crossed versions: %v", err)
	}
}
