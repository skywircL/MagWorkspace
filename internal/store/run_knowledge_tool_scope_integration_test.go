package store

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

func beginKnowledgeScopedRun(t *testing.T, f knowledgeSourceFixture, scope domain.RetrievalScope) (context.Context, domain.Session, domain.Run) {
	t.Helper()
	ctx := context.Background()
	ss, err := f.s.CreateSession(ctx, f.u.ID, f.w.ID, "knowledge boundary")
	must(t, err)
	run, _, _, err := f.s.BeginRunWithScope(ctx, f.u.ID, f.w.ID, ss.ID, "write", time.Minute, scope)
	must(t, err)
	generation, err := f.s.BeginRunJournal(ctx, f.u.ID, f.w.ID, ss.ID, run.ID)
	must(t, err)
	return WithRunExecution(ctx, run.ID, generation), ss, run
}

func TestMySQLRunKnowledgeScopeToolsInheritAndRejectExpansion(t *testing.T) {
	f := newKnowledgeSourceFixture(t)
	for _, name := range []string{"all", "selected", "none", "selected-document"} {
		t.Run(name, func(t *testing.T) {
			scope := domain.RetrievalScope{}
			if name == "selected" || name == "selected-document" {
				scope.KnowledgeBaseIDs = []string{f.a.ID}
			} else if name == "none" {
				scope.KnowledgeBaseIDs = []string{}
			}
			if name == "selected-document" {
				scope.DocumentIDs = []string{f.docA.Document.ID}
			}
			ctx, ss, run := beginKnowledgeScopedRun(t, f, scope)
			libraries, err := f.s.RunToolKnowledgeBases(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, "Scoped", 0, 1)
			must(t, err)
			want := 1
			if name == "none" {
				want = 0
			}
			if len(libraries.KnowledgeBases) != want || libraries.HasMore != (name == "all") {
				t.Fatalf("directory leaked outside run scope: %+v", libraries)
			}
			if want == 1 && libraries.KnowledgeBases[0].ID != f.a.ID {
				t.Fatal("unexpected first library")
			}
			var sources int
			must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_knowledge_base_sources WHERE run_id=?`, run.ID).Scan(&sources))
			wantSources := want
			if name == "all" {
				wantSources = 2
			}
			if sources != wantSources {
				t.Fatalf("directory lookahead acquired %d sources, want %d", sources, wantSources)
			}
			// Omitting the per-call filter must inherit the persisted hard bound.
			documents, err := f.s.RunToolDocumentsInKnowledgeBases(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, "", "", 0, 20, nil)
			must(t, err)
			wantDocs := want
			if name == "all" {
				wantDocs = 2
			}
			if len(documents.Documents) != wantDocs || documents.HasMore {
				t.Fatalf("implicit document filter escaped scope: %+v", documents)
			}
			if name != "all" && wantDocs == 1 && documents.Documents[0].Document.ID != f.docA.Document.ID {
				t.Fatal("returned a document from another library")
			}
			empty, err := f.s.RunToolDocumentsInKnowledgeBases(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, "", "", 0, 20, []string{})
			must(t, err)
			if len(empty.Documents) != 0 || empty.HasMore {
				t.Fatal("explicit empty tool selection expanded")
			}
			if name != "all" {
				_, err = f.s.RunToolDocumentsInKnowledgeBases(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, "", "", 0, 20, []string{f.a.ID, f.b.ID})
				wantError(t, err, domain.ErrForbidden)
				_, err = f.s.RunToolDocument(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, f.docB.Document.ID, "", "", "")
				wantError(t, err, domain.ErrForbidden)
				_, err = f.s.RunDocumentSource(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, f.docB.Document.ID, "", 0)
				wantError(t, err, domain.ErrForbidden)
			}
			_, err = f.s.RunToolDocument(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, f.docA.Document.ID, "", "", "")
			if name == "none" {
				wantError(t, err, domain.ErrForbidden)
			} else {
				must(t, err)
				source, err := f.s.RunDocumentSource(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, f.docA.Document.ID, "", 0)
				must(t, err)
				if source.Source.Content != "A content" {
					t.Fatal("allowed original source was not readable")
				}
			}
			f.s.SetObjectStorage(newMemoryObjects())
			capabilities, err := f.s.RunToolCapabilities(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, true)
			f.s.SetObjectStorage(nil)
			must(t, err)
			for _, tool := range []string{"search_knowledge", "search_documents", "read_evidence", "list_documents", "read_document", "update_document"} {
				if slices.Contains(capabilities, tool) != (name != "none") {
					t.Errorf("wrong %s capability for %s: %v", tool, name, capabilities)
				}
			}
			if slices.Contains(capabilities, "create_document") != (name == "all" || name == "selected") {
				t.Fatalf("create capability ignored either scope: %v", capabilities)
			}
		})
	}
}

func TestMySQLRunKnowledgeScopeApprovalTargetsAndReceipts(t *testing.T) {
	for _, scenario := range []string{"create", "create-other", "create-implicit-default", "create-empty", "update", "update-other", "deleted-during-put", "receipt-moved-library"} {
		t.Run(scenario, func(t *testing.T) {
			f := newKnowledgeSourceFixture(t)
			objects := newMemoryObjects()
			f.s.SetObjectStorage(objects)
			scope := domain.RetrievalScope{KnowledgeBaseIDs: []string{f.a.ID}}
			if scenario == "create-empty" {
				scope.KnowledgeBaseIDs = []string{}
			}
			ctx, ss, run := beginKnowledgeScopedRun(t, f, scope)
			tool := "create_document"
			args := domain.DocumentMutation{Title: "Reviewed", Filename: "review.md", Content: "reviewed content", KnowledgeBaseID: f.a.ID}
			switch scenario {
			case "create-other":
				args.KnowledgeBaseID = f.b.ID
			case "create-implicit-default":
				args.KnowledgeBaseID = ""
			case "update", "update-other":
				tool = "update_document"
				document := f.docA
				if scenario == "update-other" {
					document = f.docB
				}
				args = domain.DocumentMutation{DocumentID: document.Document.ID, BaseVersionID: document.Version.ID, Content: "updated content"}
			}
			proposal := writeProposal(t, f.u.ID, f.w.ID, ss.ID, run.ID, tool, args, nil)
			_, err := f.s.SuspendRun(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, "live", proposal)
			if scenario == "create-other" || scenario == "create-implicit-default" || scenario == "create-empty" || scenario == "update-other" {
				wantError(t, err, domain.ErrForbidden)
				var approvals int
				must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_document_approvals WHERE run_id=?`, run.ID).Scan(&approvals))
				if approvals != 0 {
					t.Fatal("out-of-scope approval was persisted")
				}
				return
			}
			must(t, err)
			resumed, err := f.s.ResumeApproval(context.Background(), f.u.ID, f.w.ID, ss.ID, run.ID, proposal.ID, true, time.Minute)
			must(t, err)
			ctx = WithRunExecution(context.Background(), run.ID, resumed.Generation)
			if scenario == "deleted-during-put" {
				objects.putHook = func(context.Context, string) error {
					return f.s.DeleteKnowledgeBase(context.Background(), f.u.ID, f.w.ID, f.a.ID)
				}
			}
			result, err := f.s.ExecuteDocumentWrite(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, proposal.ToolCallID, tool, proposal.Approval.Arguments)
			if scenario == "deleted-during-put" {
				if err == nil {
					t.Fatal("write survived deletion of its run-scoped destination")
				}
				var committed int
				must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_document_approvals WHERE run_id=? AND result IS NOT NULL`, run.ID).Scan(&committed))
				if committed != 0 {
					t.Fatal("rejected write stored a successful receipt")
				}
				return
			}
			must(t, err)
			if result.Document.KnowledgeBaseID != f.a.ID {
				t.Fatal("approved write used another library")
			}
			if scenario == "receipt-moved-library" {
				// Simulate source metadata changing behind an already committed receipt.
				_, err = f.s.pool.ExecContext(ctx, `UPDATE documents SET knowledge_base_id=? WHERE id=?`, f.b.ID, result.Document.ID)
				must(t, err)
				_, err = f.s.ExecuteDocumentWrite(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, proposal.ToolCallID, tool, proposal.Approval.Arguments)
				if !errors.Is(err, domain.ErrForbidden) && !errors.Is(err, domain.ErrConflict) {
					t.Fatalf("receipt escaped its original run scope: %v", err)
				}
				return
			}
			repeated, err := f.s.ExecuteDocumentWrite(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, proposal.ToolCallID, tool, proposal.Approval.Arguments)
			must(t, err)
			if repeated.Document.ID != result.Document.ID || repeated.Version.ID != result.Version.ID {
				t.Fatal("duplicate delivery repeated an approved write")
			}
		})
	}
}
