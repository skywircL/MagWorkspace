package store

import (
	"context"
	"encoding/json"
	"errors"
	"magworkspace/internal/domain"
	"slices"
	"testing"
	"time"
)

func TestMySQLKnowledgeBaseToolCapabilitiesAndScope(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "tool-libraries")
	first := defaultKnowledgeBase(t, s, u.ID, w.ID)
	second, err := s.CreateKnowledgeBase(ctx, u.ID, w.ID, "Second", "private metadata")
	must(t, err)
	doc, err := s.CreateKnowledgeBaseDocument(ctx, u.ID, w.ID, second.ID, "source", "source.md", []byte("content"))
	must(t, err)
	for _, scenario := range []string{"all", "selected", "none"} {
		t.Run(scenario, func(t *testing.T) {
			ss, err := s.CreateSession(ctx, u.ID, w.ID, scenario)
			must(t, err)
			var scope []string
			if scenario == "selected" {
				scope = []string{doc.Document.ID}
			} else if scenario == "none" {
				scope = []string{}
			}
			run, _, _, err := s.BeginRun(ctx, u.ID, w.ID, ss.ID, "inspect", time.Minute, scope)
			must(t, err)
			gen, err := s.BeginRunJournal(ctx, u.ID, w.ID, ss.ID, run.ID)
			must(t, err)
			current := WithRunExecution(ctx, run.ID, gen)
			page, err := s.RunToolKnowledgeBases(current, u.ID, w.ID, ss.ID, run.ID, "", 0, 1)
			must(t, err)
			switch scenario {
			case "all":
				if len(page.KnowledgeBases) != 1 || page.KnowledgeBases[0].ID != first.ID || !page.HasMore || page.NextOffset == nil {
					t.Fatalf("bad first page: %+v", page)
				}
				next, err := s.RunToolKnowledgeBases(current, u.ID, w.ID, ss.ID, run.ID, "", *page.NextOffset, 1)
				must(t, err)
				if len(next.KnowledgeBases) != 1 || next.KnowledgeBases[0].ID != second.ID || next.HasMore {
					t.Fatalf("bad second page: %+v", next)
				}
			case "selected":
				if len(page.KnowledgeBases) != 1 || page.KnowledgeBases[0].ID != second.ID || page.HasMore {
					t.Fatal("library metadata exceeded selected documents")
				}
			case "none":
				if len(page.KnowledgeBases) != 0 || page.HasMore {
					t.Fatal("empty scope exposed library metadata")
				}
			}
			names, err := s.RunToolCapabilities(current, u.ID, w.ID, ss.ID, run.ID, true)
			must(t, err)
			if slices.Contains(names, "create_document") || slices.Contains(names, "update_document") {
				t.Fatal("writes exposed without object storage")
			}
			if slices.Contains(names, "search_knowledge") != (scenario != "none") {
				t.Fatal("search ignored empty scope")
			}
			s.SetObjectStorage(newMemoryObjects())
			names, err = s.RunToolCapabilities(current, u.ID, w.ID, ss.ID, run.ID, false)
			must(t, err)
			if slices.Contains(names, "search_knowledge") || slices.Contains(names, "create_document") != (scenario == "all") || slices.Contains(names, "update_document") != (scenario != "none") {
				t.Fatalf("wrong capability filter: %v", names)
			}
			s.SetObjectStorage(nil)
			_, err = s.RunToolCapabilities(WithRunExecution(ctx, run.ID, gen+1), u.ID, w.ID, ss.ID, run.ID, true)
			if !errors.Is(err, domain.ErrConflict) {
				t.Fatalf("stale capability generation: %v", err)
			}
		})
	}
	viewer, _ := testUser(t, s, "tool-library-viewer")
	_, err = s.AddMember(ctx, u.ID, w.ID, viewer.Email, "viewer")
	must(t, err)
	ss, err := s.CreateSession(ctx, viewer.ID, w.ID, "reader")
	must(t, err)
	run, _, _, err := s.BeginRun(ctx, viewer.ID, w.ID, ss.ID, "inspect", time.Minute)
	must(t, err)
	s.SetObjectStorage(newMemoryObjects())
	names, err := s.RunToolCapabilities(ctx, viewer.ID, w.ID, ss.ID, run.ID, true)
	must(t, err)
	if slices.Contains(names, "create_document") || slices.Contains(names, "update_document") {
		t.Fatal("reader received writes")
	}
	_, err = s.pool.Exec(`DELETE FROM workspace_members WHERE workspace_id=? AND user_id=?`, w.ID, viewer.ID)
	must(t, err)
	if _, err = s.RunToolCapabilities(ctx, viewer.ID, w.ID, ss.ID, run.ID, true); err == nil {
		t.Fatal("revoked user received capabilities")
	}
}

func TestMySQLKnowledgeBaseToolCreateApprovalRechecksDestination(t *testing.T) {
	for _, scenario := range []string{"success", "foreign", "deleted-before-approval", "deleted-after-approval", "deleted-during-put", "tampered-target"} {
		t.Run(scenario, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			u, w := testUser(t, s, "kb-write")
			objects := newMemoryObjects()
			s.SetObjectStorage(objects)
			kb, err := s.CreateKnowledgeBase(ctx, u.ID, w.ID, "Destination", "")
			must(t, err)
			ss, err := s.CreateSession(ctx, u.ID, w.ID, "write")
			must(t, err)
			run, _, _, err := s.BeginRun(ctx, u.ID, w.ID, ss.ID, "write", time.Minute)
			must(t, err)
			gen, err := s.BeginRunJournal(ctx, u.ID, w.ID, ss.ID, run.ID)
			must(t, err)
			current := WithRunExecution(ctx, run.ID, gen)
			target := kb.ID
			if scenario == "foreign" {
				other, foreign := testUser(t, s, "kb-other")
				target = defaultKnowledgeBase(t, s, other.ID, foreign.ID).ID
			}
			args := domain.DocumentMutation{Title: "Reviewed", Filename: "a.md", Content: "reviewed bytes", KnowledgeBaseID: target}
			proposal := writeProposal(t, u.ID, w.ID, ss.ID, run.ID, "create_document", args, nil)
			saved, err := s.SuspendRun(current, u.ID, w.ID, ss.ID, run.ID, "live", proposal)
			if scenario == "foreign" {
				if err == nil {
					t.Fatal("foreign target approval persisted")
				}
				return
			}
			must(t, err)
			if saved.Approval.KnowledgeBaseID != kb.ID {
				t.Fatal("approval hid destination")
			}
			if scenario == "deleted-before-approval" {
				must(t, s.DeleteKnowledgeBase(ctx, u.ID, w.ID, kb.ID))
			}
			resumed, err := s.ResumeApproval(ctx, u.ID, w.ID, ss.ID, run.ID, proposal.ID, true, time.Minute)
			if scenario == "deleted-before-approval" {
				if err == nil {
					t.Fatal("deleted target approved")
				}
				return
			}
			must(t, err)
			current = WithRunExecution(ctx, run.ID, resumed.Generation)
			if scenario == "deleted-after-approval" {
				must(t, s.DeleteKnowledgeBase(ctx, u.ID, w.ID, kb.ID))
			}
			if scenario == "deleted-during-put" {
				objects.putHook = func(context.Context, string) error { return s.DeleteKnowledgeBase(ctx, u.ID, w.ID, kb.ID) }
			}
			raw := proposal.Approval.Arguments
			if scenario == "tampered-target" {
				args.KnowledgeBaseID = defaultKnowledgeBase(t, s, u.ID, w.ID).ID
				raw, err = json.Marshal(args)
				must(t, err)
			}
			result, err := s.ExecuteDocumentWrite(current, u.ID, w.ID, ss.ID, run.ID, proposal.ToolCallID, "create_document", raw)
			if scenario != "success" {
				if err == nil {
					t.Fatal("changed destination still committed")
				}
				var count int
				must(t, s.pool.QueryRow(`SELECT COUNT(*) FROM documents WHERE workspace_id=?`, w.ID).Scan(&count))
				if count != 0 {
					t.Fatal("failed write published content")
				}
				return
			}
			must(t, err)
			if result.Document.KnowledgeBaseID != kb.ID {
				t.Fatal("write used wrong library")
			}
			repeated, err := s.ExecuteDocumentWrite(current, u.ID, w.ID, ss.ID, run.ID, proposal.ToolCallID, "create_document", raw)
			must(t, err)
			if result.Document.ID != repeated.Document.ID || result.Version.ID != repeated.Version.ID || result.Document.KnowledgeBaseID != repeated.Document.KnowledgeBaseID {
				t.Fatal("approved duplicate created another document")
			}
		})
	}
}

func TestMySQLKnowledgeBaseToolFinalEvidenceRejectsDeletedEmptySelection(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "kb-final-binding")
	kb := defaultKnowledgeBase(t, s, u.ID, w.ID)
	ss, err := s.CreateSession(ctx, u.ID, w.ID, "read")
	must(t, err)
	run, _, _, err := s.BeginRun(ctx, u.ID, w.ID, ss.ID, "inspect", time.Minute)
	must(t, err)
	_, err = s.RecordRunKnowledgeEvidence(ctx, u.ID, w.ID, ss.ID, run.ID, nil, []string{kb.ID})
	must(t, err)
	must(t, s.DeleteKnowledgeBase(ctx, u.ID, w.ID, kb.ID))
	_, err = s.RecordRunKnowledgeEvidence(ctx, u.ID, w.ID, ss.ID, run.ID, nil, []string{kb.ID})
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("deleted empty selection survived evidence commit: %v", err)
	}
	names, err := s.RunToolCapabilities(ctx, u.ID, w.ID, ss.ID, run.ID, true)
	must(t, err)
	if slices.Contains(names, "read_document") || slices.Contains(names, "search_knowledge") {
		t.Fatal("empty workspace exposed document tools")
	}
}

func TestMySQLKnowledgeBaseToolEvidenceRejectsOutsideSelectionWithoutRows(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	outside, err := f.s.CreateKnowledgeBase(ctx, f.u.ID, f.w.ID, "Other library", "")
	must(t, err)
	for _, selection := range []struct {
		name string
		ids  []string
	}{
		{"same-workspace-other-library", []string{outside.ID}},
		{"explicit-empty-selection", []string{}},
	} {
		t.Run(selection.name, func(t *testing.T) {
			evidence, err := f.s.RecordRunKnowledgeEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, []domain.SearchHit{f.hit}, selection.ids)
			if !errors.Is(err, domain.ErrForbidden) || len(evidence) != 0 {
				t.Fatalf("source outside selected libraries accepted: evidence=%v err=%v", evidence, err)
			}
			var count int
			must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_evidence WHERE run_id=?`, f.run.ID).Scan(&count))
			if count != 0 {
				t.Fatalf("rejected acquisition persisted %d evidence rows", count)
			}
		})
	}
	// The exact same hit must be accepted by the selected source's library.
	// This rules out malformed fixture data as the cause of the denials above.
	accepted, err := f.s.RecordRunKnowledgeEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, []domain.SearchHit{f.hit}, []string{f.write.Document.KnowledgeBaseID})
	must(t, err)
	if len(accepted) != 1 || accepted[0].DocumentID != f.write.Document.ID {
		t.Fatal("valid selected hit did not bind")
	}
}
