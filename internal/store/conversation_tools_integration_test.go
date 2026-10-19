package store

import (
	"context"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"magworkspace/internal/domain"
)

func conversationTestSession(t *testing.T, s *Store, userID, workspaceID, title string) domain.Session {
	t.Helper()
	session, err := s.CreateSession(context.Background(), userID, workspaceID, title)
	must(t, err)
	return session
}

func conversationTestCompleted(t *testing.T, s *Store, userID, workspaceID, sessionID, question, answer string) domain.Run {
	t.Helper()
	ctx := context.Background()
	run, _, _, err := s.BeginRun(ctx, userID, workspaceID, sessionID, question, time.Minute)
	must(t, err)
	must(t, s.FinishRun(ctx, userID, workspaceID, sessionID, run.ID, answer, "test"))
	return run
}

func TestMySQLConversationSearchPrivateCompletedLiteralAndPagination(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "conversation-owner")
	other, otherWorkspace := testUser(t, s, "conversation-other")
	_, err := s.AddMember(ctx, u.ID, w.ID, other.Email, "viewer")
	must(t, err)
	otherSession := conversationTestSession(t, s, other.ID, w.ID, "他人的配置")
	conversationTestCompleted(t, s, other.ID, w.ID, otherSession.ID, "私人配置", "不可泄露")
	foreign, err := s.CreateWorkspace(ctx, u.ID, "异地配置", "")
	must(t, err)
	foreignSession := conversationTestSession(t, s, u.ID, foreign.ID, "异地配置")
	conversationTestCompleted(t, s, u.ID, foreign.ID, foreignSession.ID, "配置", "不同Workspace")
	conversationTestCompleted(t, s, other.ID, otherWorkspace.ID, conversationTestSession(t, s, other.ID, otherWorkspace.ID, "自己的配置").ID, "配置", "其他用户其他Workspace")

	history := conversationTestSession(t, s, u.ID, w.ID, "开发历史")
	first := conversationTestCompleted(t, s, u.ID, w.ID, history.ID, "配置怎么写？", "使用 alpha=1")
	second := conversationTestCompleted(t, s, u.ID, w.ID, history.ID, "后来的配置？", strings.Repeat("前", 700)+"配置 beta=2"+strings.Repeat("后", 700))
	failed, _, _, err := s.BeginRun(ctx, u.ID, w.ID, history.ID, "失败配置", time.Minute)
	must(t, err)
	must(t, s.FailRun(ctx, u.ID, w.ID, history.ID, failed.ID, "test_failure"))
	_, _, _, err = s.BeginRun(ctx, u.ID, w.ID, history.ID, "仍在运行的配置", time.Minute)
	must(t, err)
	currentSession := conversationTestSession(t, s, u.ID, w.ID, "当前会话")
	current, _, _, err := s.BeginRun(ctx, u.ID, w.ID, currentSession.ID, "当前配置不能被召回", time.Minute)
	must(t, err)

	page, err := s.SearchRunConversations(ctx, u.ID, w.ID, currentSession.ID, current.ID, "配置", 1, 0)
	must(t, err)
	if len(page.Items) != 1 || page.Items[0].RunID != second.ID || !page.HasMore || page.NextOffset == nil || *page.NextOffset != 1 {
		t.Fatalf("first page leaked or lost private completed result: %+v", page)
	}
	hit := page.Items[0]
	if hit.SessionID != history.ID || hit.Title != history.Title || !validHexID(hit.UserMessageID) || !validHexID(hit.AssistantMessageID) || hit.UserMessageID == hit.AssistantMessageID || hit.CreatedAt.IsZero() || hit.FinishedAt.IsZero() || !strings.Contains(hit.Answer, "配置 beta=2") || utf8.RuneCountInString(hit.Answer) > conversationSnippetRunes || !utf8.ValidString(hit.Answer) {
		t.Fatalf("invalid exchange metadata or Unicode excerpt: %+v", hit)
	}
	page, err = s.SearchRunConversations(ctx, u.ID, w.ID, currentSession.ID, current.ID, "配置", 1, 1)
	must(t, err)
	if len(page.Items) != 1 || page.Items[0].RunID != first.ID || page.HasMore || page.NextOffset != nil {
		t.Fatalf("second page: %+v", page)
	}
	for _, literal := range []string{"%", "_", "配置%' OR 1=1 --"} {
		page, err := s.SearchRunConversations(ctx, u.ID, w.ID, currentSession.ID, current.ID, literal, 5, 0)
		must(t, err)
		if len(page.Items) != 0 || page.HasMore {
			t.Fatalf("query treated as wildcard or SQL: %q %+v", literal, page)
		}
	}
	page, err = s.SearchRunConversations(ctx, u.ID, w.ID, currentSession.ID, current.ID, "开发历史", 5, 0)
	must(t, err)
	if len(page.Items) != 2 {
		t.Fatalf("title search: %+v", page)
	}
}

func TestMySQLConversationSearchScopeAndFilteredPagination(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	f.record(t, f.hit)
	must(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "秘密配置：来源文档", "test"))
	plainSession := conversationTestSession(t, f.s, f.u.ID, f.w.ID, "公开配置")
	plain := conversationTestCompleted(t, f.s, f.u.ID, f.w.ID, plainSession.ID, "公开配置", "独立于文档")
	// Force the inaccessible source to sort first; page offsets count only
	// visible exchanges and no hidden match count is returned.
	_, err := f.s.pool.ExecContext(ctx, `UPDATE runs SET finished_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE id=?`, f.run.ID)
	must(t, err)
	targetSession := conversationTestSession(t, f.s, f.u.ID, f.w.ID, "空范围查询")
	target, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, targetSession.ID, "查配置", time.Minute, []string{})
	must(t, err)
	result, err := f.s.SearchRunConversations(ctx, f.u.ID, f.w.ID, targetSession.ID, target.ID, "配置", 1, 0)
	must(t, err)
	if len(result.Items) != 1 || result.Items[0].RunID != plain.ID || result.HasMore {
		t.Fatalf("scope filtering happened after pagination: %+v", result)
	}
	var count int
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_conversation_sources WHERE run_id=? AND source_run_id=?`, target.ID, f.run.ID).Scan(&count))
	if count != 0 {
		t.Fatal("inaccessible result was bound as acquired provenance")
	}
	must(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, targetSession.ID, target.ID, "公开配置被允许", "test"))
	narrow, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, targetSession.ID, "带范围查配置", time.Minute, []string{f.write.Document.ID})
	must(t, err)
	result, err = f.s.SearchRunConversations(ctx, f.u.ID, f.w.ID, targetSession.ID, narrow.ID, "秘密配置", 5, 0)
	must(t, err)
	if len(result.Items) != 1 || result.Items[0].RunID != f.run.ID {
		t.Fatalf("allowed document-derived exchange absent: %+v", result)
	}
	must(t, f.s.DeleteDocument(ctx, f.u.ID, f.w.ID, f.write.Document.ID))
	wantError(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, targetSession.ID, narrow.ID, "没有显式引用的复述", "test"), domain.ErrInvalid)
}

func TestMySQLConversationSearchProvenanceSurvivesHistoryAndSourceDeletion(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	f.record(t, f.hit)
	must(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "原始住宿配置", "test"))
	secondSession := conversationTestSession(t, f.s, f.u.ID, f.w.ID, "回忆会话")
	second, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, secondSession.ID, "检索住宿配置", time.Minute)
	must(t, err)
	result, err := f.s.SearchRunConversations(ctx, f.u.ID, f.w.ID, secondSession.ID, second.ID, "原始住宿配置", 5, 0)
	must(t, err)
	if len(result.Items) != 1 {
		t.Fatalf("expected source recall: %+v", result)
	}
	must(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, secondSession.ID, second.ID, "第一层复述", "test"))
	third, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, secondSession.ID, "继续", time.Minute)
	must(t, err)
	must(t, f.s.BindRunHistory(ctx, f.u.ID, f.w.ID, secondSession.ID, third.ID, []string{second.ID}))
	must(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, secondSession.ID, third.ID, "第二层复述", "test"))
	allowed, err := f.s.CanHydrateRunHistory(ctx, f.u.ID, f.w.ID, secondSession.ID, third.ID, []string{})
	must(t, err)
	if allowed {
		t.Fatal("tool-free history paraphrase lost recalled document scope")
	}
	fourthSession := conversationTestSession(t, f.s, f.u.ID, f.w.ID, "再次召回")
	fourth, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, fourthSession.ID, "寻找复述", time.Minute)
	must(t, err)
	result, err = f.s.SearchRunConversations(ctx, f.u.ID, f.w.ID, fourthSession.ID, fourth.ID, "第二层复述", 5, 0)
	must(t, err)
	if len(result.Items) != 1 {
		t.Fatalf("transitive recall result: %+v", result)
	}
	// Removing the original session cascades its evidence, but must not erase
	// the durable reference that marks the recalled material as unavailable.
	must(t, f.s.DeleteSession(ctx, f.u.ID, f.w.ID, f.session.ID))
	var count int
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_conversation_sources WHERE run_id=? AND source_run_id=?`, fourth.ID, f.run.ID).Scan(&count))
	if count != 1 {
		t.Fatal("source session deletion erased recalled provenance")
	}
	allowed, err = f.s.CanHydrateRunHistory(ctx, f.u.ID, f.w.ID, secondSession.ID, third.ID, nil)
	must(t, err)
	if allowed {
		t.Fatal("deleted original session washed out inherited constraints")
	}
	wantError(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, fourthSession.ID, fourth.ID, "第三层复述", "test"), domain.ErrInvalid)
	var status string
	must(t, f.s.pool.QueryRowContext(ctx, `SELECT status FROM runs WHERE id=?`, fourth.ID).Scan(&status))
	if status != "running" {
		t.Fatal("invalid provenance partially committed a completed run")
	}
}

func TestMySQLConversationSearchValidatesActivePrivateRunAndArguments(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "conversation-checks")
	other, _ := testUser(t, s, "conversation-intruder")
	_, err := s.AddMember(ctx, u.ID, w.ID, other.Email, "admin")
	must(t, err)
	session := conversationTestSession(t, s, u.ID, w.ID, "私有")
	finished := conversationTestCompleted(t, s, u.ID, w.ID, session.ID, "旧配置", "旧答案")
	run, _, _, err := s.BeginRun(ctx, u.ID, w.ID, session.ID, "现在", time.Minute)
	must(t, err)
	for _, input := range []struct {
		query         string
		limit, offset int
	}{{" ", 5, 0}, {string([]byte{0xff}), 5, 0}, {strings.Repeat("字", 171), 5, 0}, {"配置", -1, 0}, {"配置", 9, 0}, {"配置", 5, -1}, {"配置", 5, 201}} {
		_, err := s.SearchRunConversations(ctx, u.ID, w.ID, session.ID, run.ID, input.query, input.limit, input.offset)
		wantError(t, err, domain.ErrInvalid)
	}
	_, err = s.SearchRunConversations(ctx, other.ID, w.ID, session.ID, run.ID, "配置", 5, 0)
	wantError(t, err, domain.ErrNotFound)
	_, err = s.SearchRunConversations(ctx, u.ID, w.ID, session.ID, finished.ID, "配置", 5, 0)
	wantError(t, err, domain.ErrConflict)
	result, err := s.SearchRunConversations(ctx, u.ID, w.ID, session.ID, run.ID, "配置", 0, 0)
	must(t, err)
	if len(result.Items) != 1 || result.Items[0].RunID != finished.ID {
		t.Fatalf("same-session completed history should remain searchable: %+v", result)
	}
	expireTestRun(t, s, run.ID)
	_, err = s.SearchRunConversations(ctx, u.ID, w.ID, session.ID, run.ID, "配置", 5, 0)
	wantError(t, err, domain.ErrConflict)
}

func TestMySQLConversationSearchSourceLimitRollsBack(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "conversation-limit")
	sourceSession := conversationTestSession(t, s, u.ID, w.ID, "历史")
	source := conversationTestCompleted(t, s, u.ID, w.ID, sourceSession.ID, "寻找配置", "实际配置")
	targetSession := conversationTestSession(t, s, u.ID, w.ID, "当前")
	target, _, _, err := s.BeginRun(ctx, u.ID, w.ID, targetSession.ID, "现在", time.Minute)
	must(t, err)
	for range maxRunHistorySources {
		id, err := newID()
		must(t, err)
		_, err = s.pool.ExecContext(ctx, `INSERT INTO runs(id,session_id,status,lease_expires_at,finished_at) VALUES(?,?,'completed',UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))`, id, sourceSession.ID)
		must(t, err)
		_, err = s.pool.ExecContext(ctx, `INSERT INTO run_conversation_sources(run_id,source_run_id) VALUES(?,?)`, target.ID, id)
		must(t, err)
	}
	result, err := s.SearchRunConversations(ctx, u.ID, w.ID, targetSession.ID, target.ID, "实际配置", 5, 0)
	wantError(t, err, domain.ErrInvalid)
	if len(result.Items) != 0 || result.HasMore || result.NextOffset != nil {
		t.Fatalf("failed provenance admission exposed contents: %+v", result)
	}
	var count int
	must(t, s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_conversation_sources WHERE run_id=? AND source_run_id=?`, target.ID, source.ID).Scan(&count))
	if count != 0 {
		t.Fatal("failed provenance admission committed a partial binding")
	}
}

func TestMySQLConversationSearchLookaheadRemainsAnAuthorizedSource(t *testing.T) {
	for _, change := range []string{"delete document", "delete source session", "narrow scope"} {
		t.Run(change, func(t *testing.T) {
			f := newEvidenceFixture(t)
			ctx := context.Background()
			f.record(t, f.hit)
			must(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, f.session.ID, f.run.ID, "较早的文档配置", "test"))
			plainSession := conversationTestSession(t, f.s, f.u.ID, f.w.ID, "独立配置")
			plain := conversationTestCompleted(t, f.s, f.u.ID, f.w.ID, plainSession.ID, "最新配置", "没有文档来源的配置")
			_, err := f.s.pool.ExecContext(ctx, `UPDATE runs SET finished_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 MINUTE) WHERE id=?`, f.run.ID)
			must(t, err)
			targetSession := conversationTestSession(t, f.s, f.u.ID, f.w.ID, "查询配置")
			target, _, _, err := f.s.BeginRun(ctx, f.u.ID, f.w.ID, targetSession.ID, "查配置", time.Minute, []string{f.write.Document.ID})
			must(t, err)
			result, err := f.s.SearchRunConversations(ctx, f.u.ID, f.w.ID, targetSession.ID, target.ID, "配置", 1, 0)
			must(t, err)
			if len(result.Items) != 1 || result.Items[0].RunID != plain.ID || !result.HasMore || result.NextOffset == nil || *result.NextOffset != 1 {
				t.Fatalf("lookahead must affect pagination without exposing its excerpt: %+v", result)
			}
			switch change {
			case "delete document":
				must(t, f.s.DeleteDocument(ctx, f.u.ID, f.w.ID, f.write.Document.ID))
			case "delete source session":
				must(t, f.s.DeleteSession(ctx, f.u.ID, f.w.ID, f.session.ID))
			case "narrow scope":
				_, err := f.s.pool.ExecContext(ctx, `UPDATE run_retrieval_scopes SET document_ids=JSON_ARRAY() WHERE run_id=?`, target.ID)
				must(t, err)
			}
			// HasMore reveals the existence of a matching exchange even though its
			// excerpt was not returned. That source must constrain finalization.
			wantError(t, f.s.FinishRun(ctx, f.u.ID, f.w.ID, targetSession.ID, target.ID, "除了显示的结果，还存在其他配置会话。", "test"), domain.ErrInvalid)
			var count int
			must(t, f.s.pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_conversation_sources WHERE run_id=? AND source_run_id=?`, target.ID, f.run.ID).Scan(&count))
			if count != 1 {
				t.Fatal("lookahead provenance was omitted or erased when its source disappeared")
			}
		})
	}
}

func TestConversationSnippetKeepsUnicodeMatchWithinBudget(t *testing.T) {
	for _, text := range []string{strings.Repeat("前", 1000) + "住宿配置", strings.Repeat("前", 1000) + "住宿配置" + strings.Repeat("后", 1000), "住宿配置" + strings.Repeat("后", 1000), "  住宿配置\r\n"} {
		got := conversationSnippet(text, "住宿配置")
		if !strings.Contains(got, "住宿配置") || !utf8.ValidString(got) || utf8.RuneCountInString(got) > conversationSnippetRunes {
			t.Fatalf("invalid bounded snippet: %q", got)
		}
	}
}
