package store

import (
	"context"
	"slices"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

func beginResourceToolRun(t *testing.T, f knowledgeSourceFixture, selection domain.ResourceSelection) (context.Context, domain.Session, domain.Run) {
	t.Helper()
	ctx := context.Background()
	ss, err := f.s.CreateSession(ctx, f.u.ID, f.w.ID, "mixed resource boundary")
	must(t, err)
	run, _, _, err := f.s.BeginRunWithResources(ctx, f.u.ID, f.w.ID, ss.ID, "write", time.Minute, selection)
	must(t, err)
	generation, err := f.s.BeginRunJournal(ctx, f.u.ID, f.w.ID, ss.ID, run.ID)
	must(t, err)
	return WithRunExecution(ctx, run.ID, generation), ss, run
}

func TestMySQLResourceScopeMixedToolsAndCapabilities(t *testing.T) {
	f := newKnowledgeSourceFixture(t)
	ctx := context.Background()
	a2, err := f.s.CreateKnowledgeBaseDocument(ctx, f.u.ID, f.w.ID, f.a.ID, "A sibling", "a2.md", []byte("other A"))
	must(t, err)
	b2, err := f.s.CreateKnowledgeBaseDocument(ctx, f.u.ID, f.w.ID, f.b.ID, "B sibling", "b2.md", []byte("other B"))
	must(t, err)
	outside, err := f.s.CreateKnowledgeBase(ctx, f.u.ID, f.w.ID, "Outside", "")
	must(t, err)
	ctx, ss, run := beginResourceToolRun(t, f, domain.ResourceSelection{KnowledgeBaseIDs: []string{f.a.ID}, DocumentIDs: []string{f.docB.Document.ID}})
	page, err := f.s.RunToolDocuments(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, "", "", 0, 20)
	must(t, err)
	got := []string{}
	for _, item := range page.Documents {
		got = append(got, item.Document.ID)
	}
	for _, id := range []string{f.docA.Document.ID, a2.Document.ID, f.docB.Document.ID} {
		if !slices.Contains(got, id) {
			t.Fatalf("mixed union lost selected resource %s: %v", id, got)
		}
	}
	if len(got) != 3 || page.HasMore {
		t.Fatalf("mixed union expanded into document-only sibling: %+v", page)
	}
	// Selecting B in a model call retains B's per-document restriction.
	page, err = f.s.RunToolDocumentsInKnowledgeBases(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, "", "", 0, 1, []string{f.b.ID})
	must(t, err)
	if len(page.Documents) != 1 || page.Documents[0].Document.ID != f.docB.Document.ID || page.HasMore {
		t.Fatalf("model KB selection widened document target: %+v", page)
	}
	_, err = f.s.RunToolDocumentsInKnowledgeBases(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, "", "", 0, 20, []string{f.a.ID, outside.ID})
	wantError(t, err, domain.ErrForbidden)
	for _, id := range []string{f.docA.Document.ID, a2.Document.ID, f.docB.Document.ID} {
		_, err = f.s.RunToolDocument(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, id, "", "", "")
		must(t, err)
	}
	_, err = f.s.RunToolDocument(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, b2.Document.ID, "", "", "")
	wantError(t, err, domain.ErrForbidden)
	_, err = f.s.RunDocumentSource(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, b2.Document.ID, "", 0)
	wantError(t, err, domain.ErrForbidden)
	libraries, err := f.s.RunToolKnowledgeBases(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, "", 0, 1)
	must(t, err)
	if len(libraries.KnowledgeBases) != 1 || !libraries.HasMore {
		t.Fatalf("mixed metadata targets or lookahead missing: %+v", libraries)
	}
	var observed int
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_knowledge_base_sources WHERE run_id=?`, run.ID).Scan(&observed))
	if observed != 2 {
		t.Fatalf("lookahead metadata crossed target boundary: %d", observed)
	}
	f.s.SetObjectStorage(newMemoryObjects())
	for _, scenario := range []struct {
		name         string
		scope        domain.ResourceSelection
		create, read bool
	}{
		{"mixed", domain.ResourceSelection{KnowledgeBaseIDs: []string{f.a.ID}, DocumentIDs: []string{f.docB.Document.ID}}, true, true},
		{"document-only", domain.ResourceSelection{DocumentIDs: []string{f.docB.Document.ID}}, false, true},
		{"none", domain.ResourceSelection{KnowledgeBaseIDs: []string{}}, false, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx, ss, run := beginResourceToolRun(t, f, scenario.scope)
			caps, err := f.s.RunToolCapabilities(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, true)
			must(t, err)
			if slices.Contains(caps, "create_document") != scenario.create || slices.Contains(caps, "read_document") != scenario.read {
				t.Fatalf("incorrect mixed-target capabilities: %v", caps)
			}
		})
	}
}

func TestMySQLResourceScopeMixedApprovalsAndReceipts(t *testing.T) {
	for _, scenario := range []string{"create-whole", "create-document-only", "create-default", "update-selected", "update-sibling", "replay-moved"} {
		t.Run(scenario, func(t *testing.T) {
			f := newKnowledgeSourceFixture(t)
			ctx := context.Background()
			sibling, err := f.s.CreateKnowledgeBaseDocument(ctx, f.u.ID, f.w.ID, f.b.ID, "Sibling", "sibling.md", []byte("outside"))
			must(t, err)
			f.s.SetObjectStorage(newMemoryObjects())
			ctx, ss, run := beginResourceToolRun(t, f, domain.ResourceSelection{KnowledgeBaseIDs: []string{f.a.ID}, DocumentIDs: []string{f.docB.Document.ID}})
			tool := "create_document"
			args := domain.DocumentMutation{KnowledgeBaseID: f.a.ID, Title: "New", Filename: "new.md", Content: "new content"}
			switch scenario {
			case "create-document-only":
				args.KnowledgeBaseID = f.b.ID
			case "create-default":
				args.KnowledgeBaseID = ""
			case "update-selected", "update-sibling":
				tool = "update_document"
				doc := f.docB
				if scenario == "update-sibling" {
					doc = sibling
				}
				args = domain.DocumentMutation{DocumentID: doc.Document.ID, BaseVersionID: doc.Version.ID, Content: "updated"}
			}
			proposal := writeProposal(t, f.u.ID, f.w.ID, ss.ID, run.ID, tool, args, nil)
			_, err = f.s.SuspendRun(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, "live", proposal)
			if scenario == "create-document-only" || scenario == "create-default" || scenario == "update-sibling" {
				wantError(t, err, domain.ErrForbidden)
				var count int
				must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_document_approvals WHERE run_id=?`, run.ID).Scan(&count))
				if count != 0 {
					t.Fatal("rejected write persisted an approval")
				}
				return
			}
			must(t, err)
			resumed, err := f.s.ResumeApproval(context.Background(), f.u.ID, f.w.ID, ss.ID, run.ID, proposal.ID, true, time.Minute)
			must(t, err)
			ctx = WithRunExecution(context.Background(), run.ID, resumed.Generation)
			result, err := f.s.ExecuteDocumentWrite(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, proposal.ToolCallID, tool, proposal.Approval.Arguments)
			must(t, err)
			if scenario == "replay-moved" {
				_, err = f.s.pool.ExecContext(ctx, `UPDATE documents SET knowledge_base_id=? WHERE id=?`, f.b.ID, result.Document.ID)
				must(t, err)
				_, err = f.s.ExecuteDocumentWrite(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, proposal.ToolCallID, tool, proposal.Approval.Arguments)
				wantError(t, err, domain.ErrForbidden)
				return
			}
			replayed, err := f.s.ExecuteDocumentWrite(ctx, f.u.ID, f.w.ID, ss.ID, run.ID, proposal.ToolCallID, tool, proposal.Approval.Arguments)
			must(t, err)
			if replayed.Document.ID != result.Document.ID || replayed.Version.ID != result.Version.ID {
				t.Fatal("replayed approval repeated the mutation")
			}
		})
	}
}
