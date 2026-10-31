package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

func captureRun(t *testing.T, s *Store, uid, wid, query string) (domain.Session, domain.Run) {
	t.Helper()
	ctx := context.Background()
	ss, err := s.CreateSession(ctx, uid, wid, "memory source")
	must(t, err)
	run, _, _, err := s.BeginRun(ctx, uid, wid, ss.ID, query, time.Minute)
	must(t, err)
	return ss, run
}
func completeCaptureRun(t *testing.T, s *Store, uid, wid, query string) (domain.Session, domain.Run, domain.MemoryCaptureJob) {
	t.Helper()
	ctx := context.Background()
	ss, run := captureRun(t, s, uid, wid, query)
	must(t, s.FinishRun(ctx, uid, wid, ss.ID, run.ID, "controlled answer", "test"))
	job, ok, err := s.ClaimMemoryCapture(ctx, run.ID, time.Minute)
	must(t, err)
	if !ok {
		t.Fatal("capture not claimed")
	}
	return ss, run, job
}
func captureCandidate(content, topic string) []domain.MemoryCandidate {
	return []domain.MemoryCandidate{{Kind: "preference", Content: content, Topic: topic, Importance: 3, Evidence: content}}
}
func captureStatus(t *testing.T, s *Store, j domain.MemoryCaptureJob, status string, count int) {
	t.Helper()
	v, err := s.MemoryCaptureStatus(context.Background(), j.UserID, j.WorkspaceID, j.SessionID, j.RunID)
	must(t, err)
	if v.Status != status || v.SavedCount != count {
		t.Fatalf("status %+v; want %s/%d", v, status, count)
	}
}

func TestMemoryCaptureExplicitAndCandidateValidation(t *testing.T) {
	for _, q := range []string{"请记住我喜欢简洁回答", "记住我回答时偏好中文", "请记住：回答优先使用中文", "记住，我喜欢简洁回答", "Remember: I prefer concise answers", "please remember I like tea"} {
		c, recognized := ExplicitMemoryCandidate(q)
		if !recognized || c == nil || !strings.Contains(q, c.Evidence) {
			t.Fatalf("explicit not recognized: %q", q)
		}
	}
	for _, q := range []string{"不要记住：我的偏好", "用户说‘请记住：我的偏好’", "\"remember: quoted\"", "示例：remember: other user"} {
		_, recognized := ExplicitMemoryCandidate(q)
		if recognized {
			t.Fatalf("quoted/negative command accepted: %q", q)
		}
	}
	for _, q := range []string{"请记住：第一行\n第二行", "请记住这句话是什么意思？", "请记住“我喜欢茶”这个例子", "记住：api_key=example-secret", "remember: password is secret", "请记住：身份证 123456789012345678", "remember: " + strings.Repeat("a", 501)} {
		c, recognized := ExplicitMemoryCandidate(q)
		if !recognized || c != nil {
			t.Fatalf("unsafe command accepted: %q", q)
		}
	}
	if err := validateMemoryCandidates("I prefer tea", captureCandidate("I prefer tea", "beverage")); err != nil {
		t.Fatal(err)
	}
	for _, c := range [][]domain.MemoryCandidate{
		captureCandidate("assistant invented this", "beverage"),
		{{Kind: "profile", Content: "api key is secret", Evidence: "I prefer tea", Importance: 3}},
		{{Kind: "profile", Content: "valid", Evidence: "", Importance: 3}},
		{{Kind: "preference", Content: "I prefer tea", Evidence: "I prefer tea", Topic: "api_key=private", Importance: 3}},
	} {
		if !errors.Is(validateMemoryCandidates("I prefer tea", c), domain.ErrInvalid) {
			t.Fatal("unsupported candidate accepted")
		}
	}
}

func TestMySQLMemoryCaptureOnlySuccessfulSourcesAndExplicit(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "capture-lifecycle")
	ss, run := captureRun(t, s, u.ID, w.ID, "请记住：我喜欢简洁回答")
	ids, err := s.PendingMemoryCaptures(ctx, 20)
	must(t, err)
	if len(ids) != 0 {
		t.Fatal("running source scheduled")
	}
	_, ok, err := s.ClaimMemoryCapture(ctx, run.ID, time.Minute)
	must(t, err)
	if ok {
		t.Fatal("running source claimed")
	}
	// Claim before completion must not cancel an eligible future source.
	must(t, s.FinishRun(ctx, u.ID, w.ID, ss.ID, run.ID, "answer", "test"))
	job, ok, err := s.ClaimMemoryCapture(ctx, run.ID, time.Minute)
	must(t, err)
	if !ok || job.Explicit == nil || job.Mode != "explicit" {
		t.Fatal("explicit capture missing")
	}
	must(t, s.CompleteMemoryCapture(ctx, job, nil))
	must(t, s.CompleteMemoryCapture(ctx, job, captureCandidate("ignored payload", "")))
	captureStatus(t, s, job, "done", 1)
	all, err := s.Memories(ctx, u.ID, w.ID, "active")
	must(t, err)
	if len(all) != 1 || all[0].Content != "我喜欢简洁回答" || all[0].Origin != "explicit" || all[0].SourceRunID != run.ID {
		t.Fatalf("explicit mismatch %+v", all)
	}
	failedSession, failedRun := captureRun(t, s, u.ID, w.ID, "must never be saved")
	must(t, s.FailRun(ctx, u.ID, w.ID, failedSession.ID, failedRun.ID, "agent_failed"))
	_, ok, err = s.ClaimMemoryCapture(ctx, failedRun.ID, time.Minute)
	must(t, err)
	if ok {
		t.Fatal("failed answer extracted")
	}
	s.SetObjectStorage(newMemoryObjects())
	suspendedSession, suspendedRun := captureRun(t, s, u.ID, w.ID, "suspended prompt")
	_, err = s.SuspendRun(ctx, u.ID, w.ID, suspendedSession.ID, suspendedRun.ID, "demo", promptInputForRun(t, s, u.ID, w.ID, suspendedSession.ID, suspendedRun.ID))
	must(t, err)
	ids, err = s.PendingMemoryCaptures(ctx, 20)
	must(t, err)
	if len(ids) != 0 {
		t.Fatal("suspended or failed run scheduled")
	}
}

func TestMySQLMemoryCaptureLeaseRestartRetryAndConcurrentClaim(t *testing.T) {
	s, dsn := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "capture-lease")
	ss, run := captureRun(t, s, u.ID, w.ID, "I prefer concise answers")
	must(t, s.FinishRun(ctx, u.ID, w.ID, ss.ID, run.ID, "answer", "test"))
	var wg sync.WaitGroup
	start := make(chan struct{})
	jobs := make(chan domain.MemoryCaptureJob, 2)
	errs := make(chan error, 2)
	for range 2 {
		wg.Go(func() {
			<-start
			j, ok, e := s.ClaimMemoryCapture(ctx, run.ID, time.Minute)
			if ok {
				jobs <- j
			}
			errs <- e
		})
	}
	close(start)
	wg.Wait()
	close(jobs)
	for range 2 {
		must(t, <-errs)
	}
	if len(jobs) != 1 {
		t.Fatalf("claimed %d times", len(jobs))
	}
	old := <-jobs
	_, err := s.pool.ExecContext(ctx, `UPDATE memory_capture_jobs SET lease_expires_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE run_id=?`, run.ID)
	must(t, err)
	fresh, err := Open(ctx, dsn)
	must(t, err)
	defer fresh.Close()
	next, ok, err := fresh.ClaimMemoryCapture(ctx, run.ID, time.Minute)
	must(t, err)
	if !ok || next.Generation != old.Generation+1 || next.Attempt != 2 {
		t.Fatal("restart did not reclaim")
	}
	must(t, s.CompleteMemoryCapture(ctx, old, captureCandidate("I prefer concise answers", "style")))
	captureStatus(t, s, next, "running", 0)
	must(t, fresh.FailMemoryCapture(ctx, next, "provider_unavailable"))
	_, ok, err = s.ClaimMemoryCapture(ctx, run.ID, time.Minute)
	must(t, err)
	if ok {
		t.Fatal("retry ignored backoff")
	}
	_, err = s.pool.ExecContext(ctx, `UPDATE memory_capture_jobs SET next_attempt_at=UTC_TIMESTAMP(6) WHERE run_id=?`, run.ID)
	must(t, err)
	last, ok, err := s.ClaimMemoryCapture(ctx, run.ID, time.Minute)
	must(t, err)
	if !ok || last.Attempt != 3 {
		t.Fatal("retry not claimed")
	}
	must(t, s.FailMemoryCapture(ctx, last, "provider_unavailable"))
	captureStatus(t, s, last, "failed", 0)
	_, ok, err = s.ClaimMemoryCapture(ctx, run.ID, time.Minute)
	must(t, err)
	if ok {
		t.Fatal("exhausted retry claimed")
	}
}

func TestMySQLMemoryCaptureEpochInvalidatesLateJobs(t *testing.T) {
	for _, action := range []string{"disable", "clear", "forget", "edit", "create"} {
		t.Run(action, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			u, w := testUser(t, s, "capture-epoch")
			old, err := s.CreateMemory(ctx, u.ID, w.ID, memoryInput("existing personal preference", ""))
			must(t, err)
			_, _, job := completeCaptureRun(t, s, u.ID, w.ID, "new personal preference")
			ss, running := captureRun(t, s, u.ID, w.ID, "paraphrase of old private information")
			switch action {
			case "disable":
				no, yes := false, true
				_, err = s.MemorySettings(ctx, u.ID, w.ID, &no)
				must(t, err)
				_, err = s.MemorySettings(ctx, u.ID, w.ID, &yes)
				must(t, err)
			case "clear":
				must(t, s.ClearMemories(ctx, u.ID, w.ID))
			case "forget":
				must(t, s.ForgetMemory(ctx, u.ID, w.ID, old.ID))
			case "edit":
				_, err = s.UpdateMemory(ctx, u.ID, w.ID, old.ID, memoryInput("edited preference", ""))
				must(t, err)
			case "create":
				_, err = s.CreateMemory(ctx, u.ID, w.ID, memoryInput("another preference", ""))
				must(t, err)
			}
			valid, err := s.ValidateMemoryCapture(ctx, job)
			must(t, err)
			if valid {
				t.Fatal("late model still eligible")
			}
			must(t, s.CompleteMemoryCapture(ctx, job, captureCandidate("new personal preference", "")))
			captureStatus(t, s, job, "cancelled", 0)
			must(t, s.FinishRun(ctx, u.ID, w.ID, ss.ID, running.ID, "answer", "test"))
			_, ok, err := s.ClaimMemoryCapture(ctx, running.ID, time.Minute)
			must(t, err)
			if ok {
				t.Fatal("begin-time snapshot lost")
			}
			items, err := s.Memories(ctx, u.ID, w.ID, "all")
			must(t, err)
			for _, item := range items {
				if item.Origin == "automatic" {
					t.Fatal("invalidated inference published")
				}
			}
		})
	}
}

func TestMySQLMemoryCaptureAuthorizationDeletionAndSourceBounds(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "capture-owner")
	member, _ := testUser(t, s, "capture-member")
	_, err := s.AddMember(ctx, u.ID, w.ID, member.Email, "viewer")
	must(t, err)
	_, _, job := completeCaptureRun(t, s, member.ID, w.ID, "I prefer concise answers")
	_, err = s.MemoryCaptureStatus(ctx, u.ID, w.ID, job.SessionID, job.RunID)
	wantError(t, err, domain.ErrNotFound)
	must(t, s.DeleteMember(ctx, u.ID, w.ID, member.ID))
	_, err = s.AddMember(ctx, u.ID, w.ID, member.Email, "viewer")
	must(t, err)
	must(t, s.CompleteMemoryCapture(ctx, job, captureCandidate(job.Query, "")))
	var status string
	must(t, s.pool.QueryRowContext(ctx, `SELECT status FROM memory_capture_jobs WHERE run_id=?`, job.ID).Scan(&status))
	if status != "cancelled" {
		t.Fatal("revoked source not cancelled")
	}
	ss, _, deleted := completeCaptureRun(t, s, u.ID, w.ID, "different private statement")
	must(t, s.DeleteSession(ctx, u.ID, w.ID, ss.ID))
	must(t, s.CompleteMemoryCapture(ctx, deleted, captureCandidate(deleted.Query, "")))
	items, err := s.Memories(ctx, u.ID, w.ID, "all")
	must(t, err)
	if len(items) != 0 {
		t.Fatal("deleted source published")
	}
	bigSession, big := captureRun(t, s, u.ID, w.ID, strings.Repeat("x", 16001))
	must(t, s.FinishRun(ctx, u.ID, w.ID, bigSession.ID, big.ID, "answer", "test"))
	_, ok, err := s.ClaimMemoryCapture(ctx, big.ID, time.Minute)
	must(t, err)
	if ok {
		t.Fatal("oversized source accepted")
	}
}

func TestMySQLMemoryCaptureOrderDedupProtectedTopicsAndTombstones(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "capture-order")
	_, _, older := completeCaptureRun(t, s, u.ID, w.ID, "I prefer long answers")
	_, _, newer := completeCaptureRun(t, s, u.ID, w.ID, "I prefer short answers")
	must(t, s.CompleteMemoryCapture(ctx, newer, captureCandidate(newer.Query, "response style")))
	must(t, s.CompleteMemoryCapture(ctx, older, captureCandidate(older.Query, "response style")))
	items, err := s.Memories(ctx, u.ID, w.ID, "active")
	must(t, err)
	if len(items) != 1 || items[0].Content != newer.Query {
		t.Fatal("older inference replaced newer")
	}
	_, _, dup := completeCaptureRun(t, s, u.ID, w.ID, newer.Query)
	must(t, s.CompleteMemoryCapture(ctx, dup, captureCandidate(dup.Query, "different topic")))
	captureStatus(t, s, dup, "done", 0)
	manual, err := s.CreateMemory(ctx, u.ID, w.ID, memoryInput("manual style", "protected"))
	must(t, err)
	_, _, auto := completeCaptureRun(t, s, u.ID, w.ID, "inferred style")
	must(t, s.CompleteMemoryCapture(ctx, auto, captureCandidate(auto.Query, "protected")))
	captureStatus(t, s, auto, "done", 0)
	must(t, s.ForgetMemory(ctx, u.ID, w.ID, manual.ID))
	_, _, forgotten := completeCaptureRun(t, s, u.ID, w.ID, manual.Content)
	must(t, s.CompleteMemoryCapture(ctx, forgotten, captureCandidate(forgotten.Query, "new topic")))
	captureStatus(t, s, forgotten, "done", 0)
	_, _, replacement := completeCaptureRun(t, s, u.ID, w.ID, "I prefer detailed answers")
	must(t, s.CompleteMemoryCapture(ctx, replacement, captureCandidate(replacement.Query, "response style")))
	_, _, restored := completeCaptureRun(t, s, u.ID, w.ID, newer.Query)
	must(t, s.CompleteMemoryCapture(ctx, restored, captureCandidate(restored.Query, "response style")))
	captureStatus(t, s, restored, "done", 1)
	items, err = s.Memories(ctx, u.ID, w.ID, "active")
	must(t, err)
	for _, item := range items {
		if item.Topic == "response style" && item.Content != newer.Query {
			t.Fatal("fresh automatic correction failed")
		}
	}
}

func TestMySQLMemoryCaptureMigrationThirteenResumes(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "capture-migration")
	old, err := s.CreateMemory(ctx, u.ID, w.ID, memoryInput("legacy personal memory", ""))
	must(t, err)
	_, _, job := completeCaptureRun(t, s, u.ID, w.ID, "source before marker reset")
	_, err = s.pool.ExecContext(ctx, `UPDATE schema_version SET version=12`)
	must(t, err)
	must(t, s.Migrate(ctx))
	must(t, s.Migrate(ctx))
	must(t, s.CompleteMemoryCapture(ctx, job, captureCandidate(job.Query, "")))
	all, err := s.Memories(ctx, u.ID, w.ID, "all")
	must(t, err)
	if len(all) != 2 {
		t.Fatal("migration lost history")
	}
	for _, item := range all {
		if item.ID == old.ID && item.Origin != "manual" {
			t.Fatal("legacy origin changed")
		}
	}
}

func TestMySQLMemoryCaptureCapacityAndInvalidOutputPreserveAnswer(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "capture-capacity")
	_, _, invalid := completeCaptureRun(t, s, u.ID, w.ID, "I prefer tea")
	wantError(t, s.CompleteMemoryCapture(ctx, invalid, captureCandidate("model invented this", "")), domain.ErrInvalid)
	must(t, s.FailMemoryCapture(ctx, invalid, "invalid_extraction"))
	captureStatus(t, s, invalid, "failed", 0)
	run, err := s.Run(ctx, u.ID, w.ID, invalid.SessionID, invalid.RunID)
	must(t, err)
	if run.Status != "completed" {
		t.Fatal("capture error rolled back answer")
	}
	_, err = s.pool.ExecContext(ctx, `INSERT INTO user_memories(id,workspace_id,user_id,kind,content,topic,normalized_key,fingerprint,importance,created_at) WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n<1000) SELECT LPAD(HEX(n),32,'0'),?,?,'fact',CONCAT('existing ',n),'','',SHA2(CONCAT('fact',n),256),3,1 FROM numbers`, w.ID, u.ID)
	must(t, err)
	_, _, full := completeCaptureRun(t, s, u.ID, w.ID, "I prefer short answers")
	must(t, s.CompleteMemoryCapture(ctx, full, captureCandidate(full.Query, "")))
	captureStatus(t, s, full, "failed", 0)
	status, err := s.MemoryCaptureStatus(ctx, u.ID, w.ID, full.SessionID, full.RunID)
	must(t, err)
	if status.ErrorCode != "memory_capacity" {
		t.Fatal("capacity error not visible")
	}
	run, err = s.Run(ctx, u.ID, w.ID, full.SessionID, full.RunID)
	must(t, err)
	if run.Status != "completed" {
		t.Fatal("capacity changed completed answer")
	}
}

func TestMySQLMemoryCaptureSourceOnlyExplicitProtectionAndDisabledStart(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "capture-source")
	ss, run := captureRun(t, s, u.ID, w.ID, "I prefer tea")
	must(t, s.FinishRun(ctx, u.ID, w.ID, ss.ID, run.ID, "assistant invented information about user", "test"))
	j, ok, err := s.ClaimMemoryCapture(ctx, run.ID, time.Minute)
	must(t, err)
	if !ok || j.Query != "I prefer tea" {
		t.Fatal("non-user text sent to extraction")
	}
	wantError(t, s.CompleteMemoryCapture(ctx, j, captureCandidate("assistant invented information about user", "")), domain.ErrInvalid)
	must(t, s.CompleteMemoryCapture(ctx, j, captureCandidate(j.Query, "beverage")))
	_, _, explicit := completeCaptureRun(t, s, u.ID, w.ID, "请记住我回答时偏好中文")
	must(t, s.CompleteMemoryCapture(ctx, explicit, nil))
	_, _, automatic := completeCaptureRun(t, s, u.ID, w.ID, "I prefer English responses")
	must(t, s.CompleteMemoryCapture(ctx, automatic, captureCandidate(automatic.Query, "response_language")))
	captureStatus(t, s, automatic, "done", 0)
	_, _, correction := completeCaptureRun(t, s, u.ID, w.ID, "请记住：回答改用英文")
	must(t, s.CompleteMemoryCapture(ctx, correction, nil))
	items, err := s.Memories(ctx, u.ID, w.ID, "active")
	must(t, err)
	count := 0
	for _, item := range items {
		if item.Topic == "response_language" {
			count++
			if item.Content != "回答改用英文" {
				t.Fatal("explicit correction lost")
			}
		}
	}
	if count != 1 {
		t.Fatal("explicit correction duplicated topic")
	}
	no, yes := false, true
	_, err = s.MemorySettings(ctx, u.ID, w.ID, &no)
	must(t, err)
	disabledSession, disabledRun := captureRun(t, s, u.ID, w.ID, "请记住：回答使用中文")
	_, err = s.MemorySettings(ctx, u.ID, w.ID, &yes)
	must(t, err)
	must(t, s.FinishRun(ctx, u.ID, w.ID, disabledSession.ID, disabledRun.ID, "answer", "test"))
	_, ok, err = s.ClaimMemoryCapture(ctx, disabledRun.ID, time.Minute)
	must(t, err)
	if ok {
		t.Fatal("disabled-start run became eligible")
	}
}

func TestMySQLMemoryCaptureOptOutSkipsExtraction(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "capture-opt-out")
	for _, q := range []string{"不要记住我喜欢茶", "请不要保存我的信息", "don't remember I prefer tea", "remember: api_key=secret"} {
		ss, run := captureRun(t, s, u.ID, w.ID, q)
		must(t, s.FinishRun(ctx, u.ID, w.ID, ss.ID, run.ID, "answer", "test"))
		_, ok, err := s.ClaimMemoryCapture(ctx, run.ID, time.Minute)
		must(t, err)
		if ok {
			t.Fatal("opt-out/credential source sent to extraction")
		}
		status, err := s.MemoryCaptureStatus(ctx, u.ID, w.ID, ss.ID, run.ID)
		must(t, err)
		if status.Status != "cancelled" {
			t.Fatal("rejection receipt absent")
		}
	}
}

func TestMySQLMemoryCaptureExplicitCorrectionBackToPriorValue(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "capture-restore")
	for _, q := range []string{"请记住：回答使用中文", "请记住：回答使用英文", "请记住：回答使用中文"} {
		_, _, j := completeCaptureRun(t, s, u.ID, w.ID, q)
		must(t, s.CompleteMemoryCapture(ctx, j, nil))
		captureStatus(t, s, j, "done", 1)
	}
	items, err := s.Memories(ctx, u.ID, w.ID, "active")
	must(t, err)
	if len(items) != 1 || items[0].Content != "回答使用中文" {
		t.Fatal("fresh explicit correction did not restore prior value")
	}
	all, err := s.Memories(ctx, u.ID, w.ID, "all")
	must(t, err)
	if len(all) != 3 {
		t.Fatal("correction mutated immutable history")
	}
	must(t, s.ForgetMemory(ctx, u.ID, w.ID, items[0].ID))
	_, _, forgotten := completeCaptureRun(t, s, u.ID, w.ID, "请记住：回答使用中文")
	must(t, s.CompleteMemoryCapture(ctx, forgotten, nil))
	captureStatus(t, s, forgotten, "done", 0)
}

func TestMySQLMemoryCaptureRecoveredRunSchedulesOnlyOnSuccess(t *testing.T) {
	for _, clear := range []bool{false, true} {
		t.Run(map[bool]string{false: "recover", true: "clear_before_recover"}[clear], func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			u, w := testUser(t, s, "capture-recover")
			s.SetObjectStorage(newMemoryObjects())
			ss, run := captureRun(t, s, u.ID, w.ID, "请记住：回答使用中文")
			gen, err := s.BeginRunJournal(ctx, u.ID, w.ID, ss.ID, run.ID)
			must(t, err)
			appendJournal(t, s, ctx, u, w, ss, run, gen, journalRecord(1, "", strings.Repeat("a", 64)))
			must(t, s.FailRun(WithRunExecution(ctx, run.ID, gen), u.ID, w.ID, ss.ID, run.ID, "agent_failed"))
			ids, err := s.PendingMemoryCaptures(ctx, 20)
			must(t, err)
			if len(ids) != 0 {
				t.Fatal("failed source scheduled")
			}
			if clear {
				must(t, s.ClearMemories(ctx, u.ID, w.ID))
			}
			recovered, err := s.RecoverRun(ctx, u.ID, w.ID, ss.ID, run.ID, time.Minute)
			must(t, err)
			ids, err = s.PendingMemoryCaptures(ctx, 20)
			must(t, err)
			if len(ids) != 0 {
				t.Fatal("recovering source scheduled before success")
			}
			must(t, s.FinishRun(WithRunExecution(ctx, run.ID, recovered.Generation), u.ID, w.ID, ss.ID, run.ID, "answer", "demo"))
			job, ok, err := s.ClaimMemoryCapture(ctx, run.ID, time.Minute)
			must(t, err)
			if ok == clear {
				t.Fatal("recovery eligibility incorrect")
			}
			if ok {
				must(t, s.CompleteMemoryCapture(ctx, job, nil))
				captureStatus(t, s, job, "done", 1)
			}
		})
	}
}

func TestMemoryCaptureExplicitProfileClassification(t *testing.T) {
	for query, topic := range map[string]string{
		"请记住我叫小明":                                 "profile_name",
		"请记住：我的名字是小红。":                            "profile_name",
		"记住我的姓名是张三":                               "profile_name",
		"Remember: my name is Alice Smith":        "profile_name",
		"请记住我的职业是后端工程师":                           "profile_profession",
		"请记住：我的岗位是 Go 开发工程师":                      "profile_profession",
		"remember: I work as a software engineer": "profile_profession",
		"remember: my profession is teacher":      "profile_profession",
	} {
		candidate, recognized := ExplicitMemoryCandidate(query)
		if !recognized || candidate == nil || candidate.Kind != "profile" || candidate.Topic != topic || !strings.Contains(query, candidate.Content) {
			t.Fatalf("profile classification for %q: %+v", query, candidate)
		}
	}
	for _, query := range []string{"请记住我是来问问题的", "请记住：假设我的名字是张三", "请记住：我叫小明，我喜欢咖啡", "remember: my colleague's name is Alice"} {
		candidate, _ := ExplicitMemoryCandidate(query)
		if candidate != nil && candidate.Kind == "profile" {
			t.Fatalf("broad profile inference for %q", query)
		}
	}
}

func TestMySQLMemoryCaptureExplicitProfileNamePersistsAndCorrects(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "capture-profile")
	for _, query := range []string{"请记住我叫小明", "请记住我的名字是小红"} {
		_, _, job := completeCaptureRun(t, s, u.ID, w.ID, query)
		if job.Explicit == nil || job.Explicit.Kind != "profile" || job.Explicit.Topic != "profile_name" {
			t.Fatal("claim lost deterministic profile classification")
		}
		must(t, s.CompleteMemoryCapture(ctx, job, nil))
		captureStatus(t, s, job, "done", 1)
	}
	ss, run := captureRun(t, s, u.ID, w.ID, "我叫什么名字")
	got, err := memoryCall(t, s, u.ID, w.ID, ss.ID, run.ID, 0, "list", map[string]any{})
	must(t, err)
	memories := got.([]domain.Memory)
	if len(memories) != 1 || memories[0].Kind != "profile" || memories[0].Topic != "profile_name" || memories[0].Content != "我的名字是小红" {
		t.Fatalf("new-session profile recall candidates: %+v", memories)
	}
	all, err := s.Memories(ctx, u.ID, w.ID, "all")
	must(t, err)
	if len(all) != 2 {
		t.Fatal("name correction changed immutable history")
	}
}
