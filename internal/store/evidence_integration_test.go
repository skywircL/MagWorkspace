package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

type evidenceFixture struct {
	s       *Store
	u       domain.User
	w       domain.Workspace
	session domain.Session
	run     domain.Run
	write   domain.DocumentWrite
	job     domain.IndexJob
	hit     domain.SearchHit
}

func newEvidenceFixture(t *testing.T) evidenceFixture {
	t.Helper()
	return newEvidenceFixtureWithPassages(t, []string{"住宿费用每晚最高报销 500 元。"})
}

func evidenceTestChunks(job domain.IndexJob, passages []string) []domain.IndexChunk {
	chunks := make([]domain.IndexChunk, 0, 2*len(passages))
	offset, childOrdinal := 0, 0
	for ordinal, content := range passages {
		pair := testIndexChunks(job, content)
		parent := pair[0]
		parent.Ordinal, parent.StartByte, parent.EndByte = ordinal, offset, offset+len(content)
		chunks = append(chunks, parent)
		runes := []rune(content)
		childStart := offset
		for start := 0; start < len(runes); start += domain.ChunkRunes {
			child := pair[1]
			child.Content = string(runes[start:min(start+domain.ChunkRunes, len(runes))])
			child.StartByte, child.EndByte = childStart, childStart+len(child.Content)
			child.Ordinal, child.ParentOrdinal = childOrdinal, ordinal
			identity := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/child/%d", job.ID, job.Generation, childOrdinal)))
			child.ID = hex.EncodeToString(identity[:])[:32]
			digest := sha256.Sum256([]byte(child.Content))
			child.SHA256 = hex.EncodeToString(digest[:])
			chunks = append(chunks, child)
			childStart = child.EndByte
			childOrdinal++
		}
		offset = parent.EndByte
	}
	return chunks
}

func newEvidenceFixtureWithPassages(t *testing.T, passages []string) evidenceFixture {
	t.Helper()
	ctx := context.Background()
	s, _ := testStore(t)
	u, w := testUser(t, s, "evidence")
	content := strings.Join(passages, "")
	write, err := s.CreateDocument(ctx, u.ID, w.ID, "Travel policy", "travel.txt", []byte(content))
	must(t, err)
	profile := testIndexProfile()
	_, _, err = s.EnqueueDocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, profile)
	must(t, err)
	input, err := s.ClaimIndexJob(ctx, profile.ID, time.Minute)
	must(t, err)
	if input == nil {
		t.Fatal("evidence index was not admitted")
	}
	chunks := evidenceTestChunks(input.Job, passages)
	must(t, s.FinishDocumentIndex(ctx, input.Job, chunks))
	job, err := s.DocumentIndex(ctx, u.ID, w.ID, write.Document.ID, write.Version.ID, profile.ID)
	must(t, err)
	session, err := s.CreateSession(ctx, u.ID, w.ID, "Evidence chat")
	must(t, err)
	run, _, _, err := s.BeginRun(ctx, u.ID, w.ID, session.ID, "住宿上限？", time.Minute)
	must(t, err)
	hit := domain.SearchHit{Title: "untrusted search title", Chunk: chunks[0]}
	return evidenceFixture{s, u, w, session, run, write, job, hit}
}

func (f evidenceFixture) record(t *testing.T, hits ...domain.SearchHit) []domain.Evidence {
	t.Helper()
	result, err := f.s.RecordRunEvidence(context.Background(), f.u.ID, f.w.ID, f.session.ID, f.run.ID, hits)
	must(t, err)
	return result
}

func TestMySQLEvidenceStableMetadataAndConcurrentDeduplication(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	first := f.record(t, f.hit, f.hit)
	if len(first) != 1 || first[0].Content != "" || first[0].Title != f.write.Document.Title || first[0].Generation != f.job.Generation || first[0].VersionID != f.write.Version.ID {
		t.Fatalf("evidence must preserve authoritative metadata only: %+v", first)
	}
	start := make(chan struct{})
	results := make(chan []domain.Evidence, 8)
	errors := make(chan error, 8)
	for range 8 {
		go func() {
			<-start
			e, err := f.s.RecordRunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, []domain.SearchHit{f.hit})
			results <- e
			errors <- err
		}()
	}
	close(start)
	for range 8 {
		must(t, <-errors)
		e := <-results
		if len(e) != 1 || e[0].ID != first[0].ID {
			t.Fatalf("concurrent acquisition changed stable evidence: %+v", e)
		}
	}
	var count int
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_evidence WHERE run_id=?`, f.run.ID).Scan(&count))
	if count != 1 {
		t.Fatalf("duplicate acquisition left %d rows", count)
	}
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='run_evidence' AND column_name='content'`).Scan(&count))
	if count != 0 {
		t.Fatal("evidence should reference chunks without duplicating their plaintext")
	}
	loaded, err := f.s.RunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, first[0].ID)
	must(t, err)
	if loaded.ID != first[0].ID || loaded.SHA256 != f.hit.Chunk.SHA256 {
		t.Fatalf("evidence lookup changed identity: %+v", loaded)
	}
}

func TestMySQLEvidenceAcquisitionValidatesMetadataAndCurrentSource(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	for name, change := range map[string]func(*domain.SearchHit){
		"chunk id":      func(h *domain.SearchHit) { h.Chunk.ID = "invalid" },
		"digest":        func(h *domain.SearchHit) { h.Chunk.SHA256 = strings.Repeat("a", 64) },
		"offset":        func(h *domain.SearchHit) { h.Chunk.EndByte++ },
		"negative":      func(h *domain.SearchHit) { h.Chunk.StartByte = -1 },
		"ordinal":       func(h *domain.SearchHit) { h.Chunk.Ordinal = 1 },
		"source hash":   func(h *domain.SearchHit) { h.Chunk.SourceSHA256 = "bad" },
		"unknown chunk": func(h *domain.SearchHit) { h.Chunk.ID = strings.Repeat("e", 32) },
		"generation":    func(h *domain.SearchHit) { h.Chunk.Generation = strings.Repeat("d", 32) },
		"kind":          func(h *domain.SearchHit) { h.Chunk.Kind = "unknown" },
		"parent":        func(h *domain.SearchHit) { h.Chunk.ParentID = strings.Repeat("e", 32) },
		"header":        func(h *domain.SearchHit) { h.Chunk.ContextHeader = "forged section" },
		"plausible forged body": func(h *domain.SearchHit) {
			h.Chunk.Content = strings.Repeat("x", len(h.Chunk.Content))
			digest := sha256.Sum256([]byte(h.Chunk.Content))
			h.Chunk.SHA256 = hex.EncodeToString(digest[:])
		},
	} {
		t.Run(name, func(t *testing.T) {
			hit := f.hit
			change(&hit)
			_, err := f.s.RecordRunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, []domain.SearchHit{hit})
			wantError(t, err, domain.ErrInvalid)
		})
	}
	first := f.record(t, f.hit)[0]
	changed := f.hit
	changed.Chunk.StartByte++
	changed.Chunk.EndByte++
	_, err := f.s.RecordRunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, []domain.SearchHit{changed})
	wantError(t, err, domain.ErrInvalid)
	_, err = f.s.AppendDocumentVersion(ctx, f.u.ID, f.w.ID, f.write.Document.ID, f.write.Version.ID, "updated.txt", []byte("600 元"))
	must(t, err)
	_, err = f.s.RecordRunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, []domain.SearchHit{f.hit})
	wantError(t, err, domain.ErrNotFound)
	// Existing citations keep their original immutable version after a head move.
	old, err := f.s.RunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, first.ID)
	must(t, err)
	if old.VersionID != f.write.Version.ID {
		t.Fatal("historical evidence followed the mutable document head")
	}
	must(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "500 元 [E:"+first.ID+"]", "test"))
	cited, err := f.s.EvidenceForRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, true)
	must(t, err)
	if len(cited) != 1 || !cited[0].Cited {
		t.Fatalf("historical citation not committed: %+v", cited)
	}
}

func TestMySQLEvidenceParentContextAndChildUseDistinctImmutableIdentities(t *testing.T) {
	content := strings.Repeat("此处保留背景和上下文。", 120) + "末尾命中的住宿上限是500元。"
	f := newEvidenceFixtureWithPassages(t, []string{content})
	ctx := context.Background()
	chunks := evidenceTestChunks(f.job, []string{content})
	parent, child := chunks[0], chunks[len(chunks)-1]
	if len([]rune(parent.Content)) <= domain.ChunkRunes || child.ParentID != parent.ID {
		t.Fatal("fixture did not create a large parent and a tail child")
	}
	items := f.record(t, domain.SearchHit{Chunk: parent}, domain.SearchHit{Chunk: child})
	if len(items) != 2 || items[0].ChunkID == items[1].ChunkID || items[0].SHA256 == items[1].SHA256 || items[0].StartByte == items[1].StartByte {
		t.Fatalf("parent and child lost their distinct provenance: %+v", items)
	}
	for i, chunk := range []domain.IndexChunk{parent, child} {
		stored, err := f.s.RunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, items[i].ID)
		must(t, err)
		if !sameEvidenceChunk(stored, chunk, f.job.Generation) {
			t.Fatal("evidence changed immutable parent/child source identity")
		}
	}
	forged := child
	forged.Content, forged.SHA256 = parent.Content, parent.SHA256
	forged.StartByte, forged.EndByte = parent.StartByte, parent.EndByte
	forged.Kind, forged.ParentID = "parent", ""
	_, err := f.s.RecordRunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, []domain.SearchHit{{Chunk: forged}})
	wantError(t, err, domain.ErrInvalid)
}

func TestMySQLEvidenceFinalizationRejectsForeignUnknownAndMalformedCitations(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	first := f.record(t, f.hit)[0]
	unknown, err := newID()
	must(t, err)
	for _, answer := range []string{"Unknown [E:" + unknown + "]", "Malformed [E:bad]", "Partial [E:", "Known [E:" + first.ID + "] then unknown [E:" + unknown + "]"} {
		wantError(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, answer, "test"), domain.ErrInvalid)
		var status string
		var count int
		must(t, f.s.pool.QueryRowContext(ctx, `SELECT status FROM runs WHERE id=?`, f.run.ID).Scan(&status))
		must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE run_id=? AND role='assistant'`, f.run.ID).Scan(&count))
		loaded, err := f.s.RunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, first.ID)
		must(t, err)
		if status != "running" || count != 0 || loaded.Cited {
			t.Fatalf("invalid citation partially finalized: status=%s messages=%d cited=%v", status, count, loaded.Cited)
		}
	}
	otherSession, err := f.s.CreateSession(ctx, f.u.ID, f.w.ID, "Another chat")
	must(t, err)
	otherRun, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, otherSession.ID, "Other query", time.Minute)
	must(t, err)
	wantError(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, otherSession.ID, otherRun.ID, "Foreign [E:"+first.ID+"]", "test"), domain.ErrInvalid)
	_, err = f.s.RunEvidence(ctx, f.u.ID, f.w.ID, otherSession.ID, otherRun.ID, first.ID)
	wantError(t, err, domain.ErrNotFound)
	// Force the assistant write to fail after evidence flags and terminal update.
	_, err = f.s.pool.ExecContext(ctx, `CREATE TRIGGER test_evidence_finish_failure BEFORE INSERT ON messages FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced finalization failure'`)
	must(t, err)
	if err = f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "500 [E:"+first.ID+"]", "test"); err == nil {
		t.Fatal("forced finalization failure accepted")
	}
	loaded, err := f.s.RunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, first.ID)
	must(t, err)
	if loaded.Cited {
		t.Fatal("failed finalization committed citation flag")
	}
	_, err = f.s.pool.ExecContext(ctx, `DROP TRIGGER test_evidence_finish_failure`)
	must(t, err)
	must(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "500 [E:"+first.ID+"] [E:"+first.ID+"]", "test"))
	loaded, err = f.s.RunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, first.ID)
	must(t, err)
	if !loaded.Cited {
		t.Fatal("successful finalization lost citation flag")
	}
}

func TestMySQLEvidenceHonorsPrivateOwnerLiveMembershipAndDeletion(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	first := f.record(t, f.hit)[0]
	other, otherWorkspace := testUser(t, f.s, "other")
	_, err := f.s.AddMember(ctx, f.u.ID, f.w.ID, other.Email, "viewer")
	must(t, err)
	_, err = f.s.RunEvidence(ctx, other.ID, f.w.ID, f.session.ID, f.run.ID, first.ID)
	wantError(t, err, domain.ErrNotFound)
	_, err = f.s.RecordRunEvidence(ctx, other.ID, f.w.ID, f.session.ID, f.run.ID, []domain.SearchHit{f.hit})
	wantError(t, err, domain.ErrNotFound)
	foreign, err := f.s.CreateDocument(ctx, other.ID, otherWorkspace.ID, "Foreign", "foreign.txt", []byte(f.hit.Chunk.Content))
	must(t, err)
	foreignHit := f.hit
	foreignHit.Chunk.DocumentID = foreign.Document.ID
	foreignHit.Chunk.VersionID = foreign.Version.ID
	_, err = f.s.RecordRunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, []domain.SearchHit{foreignHit})
	wantError(t, err, domain.ErrNotFound)
	_, err = f.s.pool.ExecContext(ctx, `DELETE FROM workspace_members WHERE workspace_id=? AND user_id=?`, f.w.ID, f.u.ID)
	must(t, err)
	_, err = f.s.RunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, first.ID)
	wantError(t, err, domain.ErrNotFound)
	_, err = f.s.RecordRunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, []domain.SearchHit{f.hit})
	wantError(t, err, domain.ErrNotFound)
	_, err = f.s.pool.ExecContext(ctx, `INSERT INTO workspace_members(workspace_id,user_id,role) VALUES(?,?,'owner')`, f.w.ID, f.u.ID)
	must(t, err)
	must(t, f.s.DeleteDocument(ctx, f.u.ID, f.w.ID, f.write.Document.ID))
	_, err = f.s.RunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, first.ID)
	wantError(t, err, domain.ErrNotFound)
	all, err := f.s.EvidenceForRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, false)
	must(t, err)
	if len(all) != 0 {
		t.Fatal("deleted document leaked through evidence listing")
	}
	wantError(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "Deleted [E:"+first.ID+"]", "test"), domain.ErrInvalid)
}

func TestMySQLEvidenceScopeDistinguishesAllNoneAndSelected(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	all, err := f.s.RunDocumentScope(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID)
	must(t, err)
	if all != nil {
		t.Fatalf("omitted scope must mean all: %#v", all)
	}
	must(t, f.s.FailRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "test"))
	newRun, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, f.session.ID, "None", time.Minute, []string{})
	must(t, err)
	f.run = newRun
	none, err := f.s.RunDocumentScope(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID)
	must(t, err)
	if none == nil || len(none) != 0 {
		t.Fatalf("explicit empty scope must mean none: %#v", none)
	}
	_, err = f.s.RecordRunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, []domain.SearchHit{f.hit})
	wantError(t, err, domain.ErrForbidden)
	must(t, f.s.FailRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "test"))
	missing, err := newID()
	must(t, err)
	for _, scope := range [][]string{{"bad"}, {f.write.Document.ID, f.write.Document.ID}} {
		_, _, _, err = f.s.BeginRun(ctx, f.u.ID, f.w.ID, f.session.ID, "Invalid", time.Minute, scope)
		wantError(t, err, domain.ErrInvalid)
	}
	_, _, _, err = f.s.BeginRun(ctx, f.u.ID, f.w.ID, f.session.ID, "Missing", time.Minute, []string{missing})
	wantError(t, err, domain.ErrNotFound)
	other, foreignWorkspace := testUser(t, f.s, "foreignscope")
	foreign, err := f.s.CreateDocument(ctx, other.ID, foreignWorkspace.ID, "Foreign", "foreign.txt", []byte("foreign"))
	must(t, err)
	_, _, _, err = f.s.BeginRun(ctx, f.u.ID, f.w.ID, f.session.ID, "Foreign", time.Minute, []string{foreign.Document.ID})
	wantError(t, err, domain.ErrNotFound)
	deleted, err := f.s.CreateDocument(ctx, f.u.ID, f.w.ID, "Deleted", "deleted.txt", []byte("deleted"))
	must(t, err)
	must(t, f.s.DeleteDocument(ctx, f.u.ID, f.w.ID, deleted.Document.ID))
	_, _, _, err = f.s.BeginRun(ctx, f.u.ID, f.w.ID, f.session.ID, "Deleted", time.Minute, []string{deleted.Document.ID})
	wantError(t, err, domain.ErrNotFound)
	_, _, _, err = f.s.BeginRun(ctx, f.u.ID, f.w.ID, f.session.ID, "Too many", time.Minute, make([]string, 101))
	wantError(t, err, domain.ErrInvalid)
	newRun, _, _, err = f.s.BeginRun(ctx, f.u.ID, f.w.ID, f.session.ID, "Selected", time.Minute, []string{f.write.Document.ID})
	must(t, err)
	f.run = newRun
	selected, err := f.s.RunDocumentScope(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID)
	must(t, err)
	if len(selected) != 1 || selected[0] != f.write.Document.ID {
		t.Fatalf("selected scope not persisted: %#v", selected)
	}
	f.record(t, f.hit)
	expireTestRun(t, f.s, f.run.ID)
	_, err = f.s.RecordRunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, []domain.SearchHit{f.hit})
	wantError(t, err, domain.ErrConflict)
	_, err = f.s.RunDocumentScope(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID)
	wantError(t, err, domain.ErrConflict)
}

func TestMySQLEvidenceExpiredFinishDoesNotCommitCitations(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	first := f.record(t, f.hit)[0]
	expireTestRun(t, f.s, f.run.ID)
	wantError(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "Expired [E:"+first.ID+"]", "test"), domain.ErrConflict)
	loaded, err := f.s.RunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, first.ID)
	must(t, err)
	if loaded.Cited {
		t.Fatal("expired finalization committed citation flags")
	}
	var status string
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT status FROM runs WHERE id=?`, f.run.ID).Scan(&status))
	if status != "failed" {
		t.Fatalf("expired run terminal status=%q", status)
	}
	_, err = f.s.RecordRunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, []domain.SearchHit{f.hit})
	wantError(t, err, domain.ErrConflict)
}

func TestMySQLEvidenceCapacityAndGenerationBinding(t *testing.T) {
	passages := make([]string, maxRunEvidence+1)
	for i := range passages {
		passages[i] = fmt.Sprintf("独立条款 %d：住宿费用每晚最高报销 500 元。\n", i)
	}
	f := newEvidenceFixtureWithPassages(t, passages)
	ctx := context.Background()
	chunks := evidenceTestChunks(f.job, passages)
	hits := make([]domain.SearchHit, maxRunEvidence)
	for i := range hits {
		hits[i] = domain.SearchHit{Chunk: chunks[2*i], Title: f.hit.Title}
	}
	first := f.record(t, hits...)
	if len(first) != maxRunEvidence {
		t.Fatalf("expected %d acquired passages, got %d", maxRunEvidence, len(first))
	}
	last := domain.SearchHit{Chunk: chunks[2*maxRunEvidence], Title: f.hit.Title}
	_, err := f.s.RecordRunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, []domain.SearchHit{last})
	wantError(t, err, domain.ErrInvalid)
	if got := f.record(t, hits[0]); len(got) != 1 || got[0].ID != first[0].ID {
		t.Fatal("capacity limit prevented idempotent reacquisition")
	}
	generation, err := newID()
	must(t, err)
	_, err = f.s.pool.ExecContext(ctx, `UPDATE document_indexes SET generation=? WHERE id=?`, generation, f.job.ID)
	must(t, err)
	_, err = f.s.RunEvidence(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, first[0].ID)
	wantError(t, err, domain.ErrNotFound)
	wantError(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "Changed generation [E:"+first[0].ID+"]", "test"), domain.ErrInvalid)
}

func TestMySQLEvidenceHistoryProvenanceSurvivesToolFreeParaphrases(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	f.record(t, f.hit)
	firstRun := f.run.ID
	must(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, firstRun, "住宿费用每晚 500 元，无引用的转述。", "test"))
	second, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, f.session.ID, "再说一遍", time.Minute)
	must(t, err)
	must(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, f.session.ID, second.ID, []string{firstRun}))
	must(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, f.session.ID, second.ID, []string{firstRun}))
	// This turn acquires no evidence of its own and does not cite the source.
	must(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, second.ID, "每晚仍然是 500 元。", "test"))
	var count int
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_evidence WHERE run_id=?`, second.ID).Scan(&count))
	if count != 0 {
		t.Fatal("tool-free paraphrase unexpectedly acquired direct evidence")
	}
	allowed, err := f.s.CanHydrateRunHistory(ctx, f.u.ID, f.w.ID, f.session.ID, second.ID, nil)
	must(t, err)
	if !allowed {
		t.Fatal("live source should allow its derived history")
	}
	allowed, err = f.s.CanHydrateRunHistory(ctx, f.u.ID, f.w.ID, f.session.ID, second.ID, []string{})
	must(t, err)
	if allowed {
		t.Fatal("tool-free paraphrase escaped an empty document scope")
	}
	third, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, f.session.ID, "继续转述", time.Minute)
	must(t, err)
	must(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, f.session.ID, third.ID, []string{second.ID}))
	var sourceRun string
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT source_run_id FROM run_history_sources WHERE run_id=?`, third.ID).Scan(&sourceRun))
	if sourceRun != firstRun {
		t.Fatalf("history dependencies were not flattened to actual source runs: %s", sourceRun)
	}
	// Appending a new head does not invalidate already acquired source versions.
	_, err = f.s.AppendDocumentVersion(ctx, f.u.ID, f.w.ID, f.write.Document.ID, f.write.Version.ID, "new.txt", []byte("600 元"))
	must(t, err)
	allowed, err = f.s.CanHydrateRunHistory(ctx, f.u.ID, f.w.ID, f.session.ID, second.ID, nil)
	must(t, err)
	if !allowed {
		t.Fatal("derived history incorrectly followed a mutable document head")
	}
	must(t, f.s.DeleteDocument(ctx, f.u.ID, f.w.ID, f.write.Document.ID))
	allowed, err = f.s.CanHydrateRunHistory(ctx, f.u.ID, f.w.ID, f.session.ID, second.ID, nil)
	must(t, err)
	if allowed {
		t.Fatal("deleted inherited source remained eligible for model history")
	}
	wantError(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, third.ID, "再次无引用地转述 500 元。", "test"), domain.ErrInvalid)
}

func TestMySQLEvidenceHistoryBindingChecksActualScopeAndPrivateCompletedSources(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	f.record(t, f.hit)
	firstRun := f.run.ID
	otherSession, err := f.s.CreateSession(ctx, f.u.ID, f.w.ID, "Other private session")
	must(t, err)
	otherRun, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, otherSession.ID, "Other run", time.Minute)
	must(t, err)
	wantError(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, f.session.ID, firstRun, []string{otherRun.ID}), domain.ErrNotFound)
	wantError(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, f.session.ID, firstRun, []string{firstRun}), domain.ErrInvalid)
	must(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, firstRun, "500 元。", "test"))
	second, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, f.session.ID, "Narrow scope", time.Minute, []string{})
	must(t, err)
	wantError(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, f.session.ID, second.ID, []string{firstRun}), domain.ErrInvalid)
	var count int
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_history_sources WHERE run_id=?`, second.ID).Scan(&count))
	if count != 0 {
		t.Fatal("failed binding persisted a partial provenance dependency")
	}
	must(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, f.session.ID, second.ID, nil))
	expireTestRun(t, f.s, second.ID)
	wantError(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, f.session.ID, second.ID, nil), domain.ErrConflict)
}

func TestMySQLEvidenceUncitedAnswerRechecksAllSourcesBeforeCommit(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	first := f.record(t, f.hit)[0]
	must(t, f.s.DeleteDocument(ctx, f.u.ID, f.w.ID, f.write.Document.ID))
	wantError(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "每晚报销 500 元，没有引用标记。", "test"), domain.ErrInvalid)
	var status string
	var assistantCount int
	var cited bool
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT status FROM runs WHERE id=?`, f.run.ID).Scan(&status))
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE run_id=? AND role='assistant'`, f.run.ID).Scan(&assistantCount))
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT cited FROM run_evidence WHERE id=?`, first.ID).Scan(&cited))
	if status != "running" || assistantCount != 0 || cited {
		t.Fatal("deleted uncited source caused a partial finalization")
	}
}

func TestMySQLEvidenceHistorySourceLimitFailsClosed(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	first := f.record(t, f.hit)[0]
	parent := f.run.ID
	must(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, parent, "Source-derived parent", "test"))
	// Reconstruct an existing completed turn with 256 flattened, acquired
	// ancestors plus its own acquired evidence. A new run cannot inherit 257.
	for range maxRunHistorySources {
		runID, err := newID()
		must(t, err)
		evidenceID, err := newID()
		must(t, err)
		_, err = f.s.pool.ExecContext(ctx, `INSERT INTO runs(id,session_id,status,lease_expires_at,finished_at) VALUES(?,?,'completed',UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))`, runID, f.session.ID)
		must(t, err)
		_, err = f.s.pool.ExecContext(ctx, `INSERT INTO run_evidence(id,run_id,workspace_id,document_id,version_id,index_id,generation,chunk_id,source_sha256,sha256,title,start_byte,end_byte) SELECT ?,?,workspace_id,document_id,version_id,index_id,generation,chunk_id,source_sha256,sha256,title,start_byte,end_byte FROM run_evidence WHERE id=?`, evidenceID, runID, first.ID)
		must(t, err)
		_, err = f.s.pool.ExecContext(ctx, `INSERT INTO run_history_sources(run_id,source_run_id) VALUES(?,?)`, parent, runID)
		must(t, err)
	}
	target, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, f.session.ID, "New run", time.Minute)
	must(t, err)
	wantError(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, f.session.ID, target.ID, []string{parent}), domain.ErrInvalid)
	var count int
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_history_sources WHERE run_id=?`, target.ID).Scan(&count))
	if count != 0 {
		t.Fatal("over-limit binding persisted partial dependencies")
	}
}

func TestMySQLEvidenceMigrationRetriesPartialDDL(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	for _, table := range []string{"run_evidence", "run_history_sources", "run_retrieval_scopes"} {
		_, err := s.pool.ExecContext(ctx, `DROP TABLE `+table)
		must(t, err)
	}
	_, err := s.pool.ExecContext(ctx, `UPDATE schema_version SET version=4 WHERE singleton=1`)
	must(t, err)
	u, w := testUser(t, s, "evidenceupgrade")
	session, err := s.CreateSession(ctx, u.ID, w.ID, "Existing session")
	must(t, err)
	run, _, _, err := s.BeginRun(ctx, u.ID, w.ID, session.ID, "Existing question", time.Minute)
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `CREATE TABLE migration_blocker (id int PRIMARY KEY, CONSTRAINT run_evidence_offsets CHECK(id>0)) ENGINE=InnoDB`)
	must(t, err)
	if err = s.Migrate(ctx); err == nil {
		t.Fatal("expected interrupted v5 DDL")
	}
	var version int
	must(t, s.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
	if version != 4 {
		t.Fatal("partial evidence DDL advanced schema marker")
	}
	_, err = s.pool.ExecContext(ctx, `DROP TABLE migration_blocker`)
	must(t, err)
	must(t, s.Migrate(ctx))
	loaded, err := s.Run(ctx, u.ID, w.ID, session.ID, run.ID)
	must(t, err)
	if loaded.ID != run.ID {
		t.Fatal("evidence migration changed an existing run")
	}
	must(t, s.FinishRun(ctx, u.ID, w.ID, session.ID, run.ID, "No citation", "test"))
}
