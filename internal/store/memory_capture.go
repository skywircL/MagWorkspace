package store

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"magworkspace/internal/domain"
)

const maxMemoryCaptureAttempts = 3

// The workspace lock also serializes epoch changes with BeginRun and publication.
func memoryCaptureEpochTx(ctx context.Context, tx *sql.Tx, uid, wid string) (uint64, error) {
	var epoch uint64
	err := tx.QueryRowContext(ctx, `SELECT COALESCE((SELECT epoch FROM user_memory_capture_state WHERE workspace_id=? AND user_id=?),0)`, wid, uid).Scan(&epoch)
	return epoch, err
}
func invalidateMemoryCapturesTx(ctx context.Context, tx *sql.Tx, uid, wid string) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO user_memory_capture_state(workspace_id,user_id,epoch) VALUES(?,?,1) ON DUPLICATE KEY UPDATE epoch=epoch+1`, wid, uid); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE memory_capture_jobs SET status='cancelled',error_code='memory_changed',updated_at=UTC_TIMESTAMP(6) WHERE workspace_id=? AND user_id=? AND status IN ('waiting','queued','running')`, wid, uid); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `DELETE o FROM memory_capture_outbox o JOIN memory_capture_jobs j ON j.run_id=o.run_id WHERE j.workspace_id=? AND j.user_id=? AND j.status='cancelled'`, wid, uid)
	return err
}
func beginMemoryCaptureTx(ctx context.Context, tx *sql.Tx, uid, wid, sid, rid, mid string) error {
	epoch, err := memoryCaptureEpochTx(ctx, tx, uid, wid)
	if err != nil {
		return err
	}
	enabled, err := memoryEnabledTx(ctx, tx, uid, wid)
	if err != nil {
		return err
	}
	status, code := "waiting", ""
	if !enabled {
		status, code = "cancelled", "memory_disabled"
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO memory_capture_jobs(run_id,workspace_id,user_id,session_id,source_message_id,epoch,status,error_code) VALUES(?,?,?,?,?,?,?,?)`, rid, wid, uid, sid, mid, epoch, status, code)
	return err
}

// PendingMemoryCaptures is the non-Redis polling view of durable intents.
// Dispatch cooldown does not delay local polling, but job retry backoff does.
func (s *Store) PendingMemoryCaptures(ctx context.Context, limit int) ([]string, error) {
	if limit < 1 || limit > 200 {
		return nil, domain.ErrInvalid
	}
	if err := s.cancelFailedWaitingCaptures(ctx); err != nil {
		return nil, err
	}
	rows, err := s.pool.QueryContext(ctx, `SELECT j.run_id FROM memory_capture_jobs j JOIN memory_capture_outbox o ON o.run_id=j.run_id WHERE (j.status='queued' AND j.next_attempt_at<=UTC_TIMESTAMP(6)) OR (j.status='running' AND j.lease_expires_at<=UTC_TIMESTAMP(6)) ORDER BY j.created_at,j.run_id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) cancelFailedWaitingCaptures(ctx context.Context) error {
	// Waiting jobs have no outbox rows. The status predicate is rechecked under
	// the SQL row lock, so concurrent successful FinishRun cannot be cancelled.
	_, err := s.pool.ExecContext(ctx, `UPDATE memory_capture_jobs j JOIN runs r ON r.id=j.run_id SET j.status='cancelled',j.error_code='run_failed',j.updated_at=UTC_TIMESTAMP(6) WHERE j.status='waiting' AND r.status='failed'`)
	return dbError(err)
}

const captureColumns = `run_id,user_id,workspace_id,session_id,epoch,mode,attempt,generation,created_at`

func scanCapture(row rowScanner) (domain.MemoryCaptureJob, error) {
	var j domain.MemoryCaptureJob
	err := row.Scan(&j.RunID, &j.UserID, &j.WorkspaceID, &j.SessionID, &j.Epoch, &j.Mode, &j.Attempt, &j.Generation, &j.CreatedAt)
	j.ID = j.RunID
	return j, dbError(err)
}
func cancelCaptureTx(ctx context.Context, tx *sql.Tx, id, code string) error {
	if _, err := tx.ExecContext(ctx, `UPDATE memory_capture_jobs SET status='cancelled',error_code=?,updated_at=UTC_TIMESTAMP(6) WHERE run_id=? AND status IN ('waiting','queued','running')`, code, id); err != nil {
		return err
	}
	return deleteMemoryCaptureDeliveryTx(ctx, tx, id)
}

// captureAuthorityTx checks live membership and the original USER message. No
// assistant text, tool output, archived conversations or historical scan is input.
func captureAuthorityTx(ctx context.Context, tx *sql.Tx, j *domain.MemoryCaptureJob) (bool, error) {
	var id string
	if err := tx.QueryRowContext(ctx, `SELECT id FROM workspaces WHERE id=? FOR UPDATE`, j.WorkspaceID).Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	var member bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM workspace_members WHERE workspace_id=? AND user_id=?)`, j.WorkspaceID, j.UserID).Scan(&member); err != nil {
		return false, err
	}
	if !member {
		return false, cancelCaptureTx(ctx, tx, j.ID, "access_revoked")
	}
	enabled, err := memoryEnabledTx(ctx, tx, j.UserID, j.WorkspaceID)
	if err != nil {
		return false, err
	}
	epoch, err := memoryCaptureEpochTx(ctx, tx, j.UserID, j.WorkspaceID)
	if err != nil {
		return false, err
	}
	if !enabled || epoch != j.Epoch {
		return false, cancelCaptureTx(ctx, tx, j.ID, "memory_changed")
	}
	err = tx.QueryRowContext(ctx, `SELECT m.content FROM memory_capture_jobs j JOIN sessions s ON s.id=j.session_id JOIN runs r ON r.id=j.run_id AND r.session_id=s.id JOIN messages m ON m.id=j.source_message_id AND m.run_id=r.id AND m.session_id=s.id AND m.role='user' WHERE j.run_id=? AND s.workspace_id=? AND s.user_id=? AND r.status='completed'`, j.ID, j.WorkspaceID, j.UserID).Scan(&j.Query)
	if errors.Is(err, sql.ErrNoRows) {
		return false, cancelCaptureTx(ctx, tx, j.ID, "source_unavailable")
	}
	if err != nil {
		return false, err
	}
	if len(j.Query) > 16000 || !utf8.ValidString(j.Query) {
		return false, cancelCaptureTx(ctx, tx, j.ID, "source_too_large")
	}
	return true, nil
}

var sensitiveMemory = regexp.MustCompile(`(?i)(password|passwd|api[_ -]?key|access[_ -]?token|secret[_ -]?key|bearer\s+[a-z0-9]|private[_ -]?key|-----BEGIN|密码|口令|密钥|身份证|银行卡|信用卡|验证码|\bsk-[a-z0-9]{12,}|\b[0-9]{15,19}\b)`)
var memoryCaptureErrorCode = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
var memoryCaptureOptOut = regexp.MustCompile(`(?i)^(?:(?:请)?(?:不要|别)(?:记住|保存)|(?:please )?(?:do not|don't) (?:remember|save))`)
var explicitMemoryPrefix = regexp.MustCompile(`(?is)^(?:(?:请记住|记住)(?:[：:,，]\s*|\s+)?|(?:please remember|remember)(?:[：:,，]\s*|\s+))(.+)$`)

// Explicit labels classify only a single stated personal field. Generic "我是"
// statements and extra clauses do not become resident identity information.
var explicitProfileName = regexp.MustCompile(`(?i)^(?:(?:我叫|我的名字是|我的姓名是)\s*(?:[\p{Han}·]{1,20}|[a-z][a-z .'-]{0,79})|my name is [\p{L}][\p{L} .'-]{0,79})[。.!！]?$`)
var explicitProfileProfession = regexp.MustCompile(`(?i)^(?:(?:我的职业是|我的岗位是)\s*|(?:my profession is|my occupation is|I work as) )(?:[\p{L}][\p{L}\p{N} /+.#-]{0,59})[。.!！]?$`)

// ExplicitMemoryCandidate recognizes only a direct anchored one-line command.
// Recognized unsafe/oversized commands are not retried as automatic extraction.
func ExplicitMemoryCandidate(query string) (*domain.MemoryCandidate, bool) {
	text := strings.TrimSpace(query)
	match := explicitMemoryPrefix.FindStringSubmatch(text)
	if match == nil {
		return nil, false
	}
	content := strings.TrimSpace(match[1])
	if strings.ContainsAny(content, "\r\n?？") || strings.HasPrefix(content, "如何") || strings.HasPrefix(content, "这个例子") || strings.HasPrefix(content, "这句话") || strings.HasPrefix(content, "以下文本") || strings.HasPrefix(content, "不要保存") || strings.HasPrefix(content, "不要记住") || strings.HasPrefix(content, "不需要保存") || strings.ContainsAny(content, "“”‘’`\"") || strings.ContainsAny(content, "\r\n") || strings.HasPrefix(content, "\"") || strings.HasPrefix(content, "“") || strings.HasPrefix(content, "'") || strings.HasPrefix(content, "`") || utf8.RuneCountInString(content) > 500 || sensitiveMemory.MatchString(content) {
		return nil, true
	}
	content = sanitizeMemory(content)
	if content == "" {
		return nil, true
	}
	kind := "fact"
	for _, marker := range []string{"喜欢", "偏好", "回答", "请用", "使用", "优先", "prefer", "always", "language"} {
		if strings.Contains(strings.ToLower(content), marker) {
			kind = "preference"
			break
		}
	}
	topic := ""
	if kind == "preference" {
		lower := strings.ToLower(content)
		if (strings.Contains(lower, "回答") || strings.Contains(lower, "answer") || strings.Contains(lower, "response")) && (strings.Contains(lower, "中文") || strings.Contains(lower, "英文") || strings.Contains(lower, "chinese") || strings.Contains(lower, "english")) {
			topic = "response_language"
		}
	}
	switch {
	case explicitProfileName.MatchString(content):
		kind, topic = "profile", "profile_name"
	case explicitProfileProfession.MatchString(content):
		kind, topic = "profile", "profile_profession"
	}
	return &domain.MemoryCandidate{Kind: kind, Content: content, Topic: topic, Importance: 4, Evidence: match[1]}, true
}

func (s *Store) ClaimMemoryCapture(ctx context.Context, id string, lease time.Duration) (domain.MemoryCaptureJob, bool, error) {
	return s.claimMemoryCapture(ctx, id, "", lease)
}

func (s *Store) claimMemoryCapture(ctx context.Context, id, deliveryID string, lease time.Duration) (domain.MemoryCaptureJob, bool, error) {
	var job domain.MemoryCaptureJob
	if !validHexID(id) || lease.Microseconds() <= 0 || lease > 10*time.Minute {
		return job, false, domain.ErrInvalid
	}
	initial, err := scanCapture(s.pool.QueryRowContext(ctx, `SELECT `+captureColumns+` FROM memory_capture_jobs WHERE run_id=?`, id))
	if errors.Is(err, domain.ErrNotFound) && deliveryID != "" {
		return job, false, nil
	}
	if err != nil {
		return job, false, err
	}
	claimed := false
	err = s.transaction(ctx, func(tx *sql.Tx) error {
		job = initial
		// Match every mutation's Workspace -> Job -> Outbox lock order. Check
		// the token before authority can cancel anything on a stale delivery.
		var workspaceID string
		if err := tx.QueryRowContext(ctx, `SELECT id FROM workspaces WHERE id=? FOR UPDATE`, job.WorkspaceID).Scan(&workspaceID); errors.Is(err, sql.ErrNoRows) {
			return nil
		} else if err != nil {
			return err
		}
		var status string
		var eligible bool
		if err := tx.QueryRowContext(ctx, `SELECT status,(status='queued' AND next_attempt_at<=UTC_TIMESTAMP(6)) OR (status='running' AND lease_expires_at<=UTC_TIMESTAMP(6)) FROM memory_capture_jobs WHERE run_id=? FOR UPDATE`, id).Scan(&status, &eligible); errors.Is(err, sql.ErrNoRows) {
			return nil
		} else if err != nil {
			return err
		}
		if !eligible {
			return nil
		}
		var currentDelivery string
		if err := tx.QueryRowContext(ctx, `SELECT id FROM memory_capture_outbox WHERE run_id=? FOR UPDATE`, id).Scan(&currentDelivery); errors.Is(err, sql.ErrNoRows) {
			return nil
		} else if err != nil {
			return err
		}
		if deliveryID != "" && currentDelivery != deliveryID {
			return nil
		}
		// Re-read generation after acquiring the lock, never reuse the pre-lock view.
		current, err := scanCapture(tx.QueryRowContext(ctx, `SELECT `+captureColumns+` FROM memory_capture_jobs WHERE run_id=?`, id))
		if err != nil {
			return err
		}
		job = current
		valid, err := captureAuthorityTx(ctx, tx, &job)
		if err != nil || !valid {
			return err
		}
		if job.Attempt >= maxMemoryCaptureAttempts {
			if _, err := tx.ExecContext(ctx, `UPDATE memory_capture_jobs SET status='failed',error_code='attempts_exhausted',updated_at=UTC_TIMESTAMP(6) WHERE run_id=?`, id); err != nil {
				return err
			}
			return deleteMemoryCaptureDeliveryTx(ctx, tx, id)
		}
		if memoryCaptureOptOut.MatchString(strings.TrimSpace(job.Query)) {
			return cancelCaptureTx(ctx, tx, id, "source_opt_out")
		}
		explicit, recognized := ExplicitMemoryCandidate(job.Query)
		if recognized && explicit == nil {
			return cancelCaptureTx(ctx, tx, id, "explicit_rejected")
		}
		job.Explicit = explicit
		if recognized {
			job.Mode = "explicit"
		}
		job.Attempt++
		job.Generation++
		_, err = tx.ExecContext(ctx, `UPDATE memory_capture_jobs SET status='running',mode=?,attempt=?,generation=?,lease_expires_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL ? MICROSECOND),error_code='',updated_at=UTC_TIMESTAMP(6) WHERE run_id=?`, job.Mode, job.Attempt, job.Generation, lease.Microseconds(), id)
		claimed = err == nil
		return err
	})
	return job, claimed && err == nil, err
}

func captureLeaseTx(ctx context.Context, tx *sql.Tx, job domain.MemoryCaptureJob) (bool, error) {
	var valid bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM memory_capture_jobs WHERE run_id=? AND user_id=? AND workspace_id=? AND session_id=? AND epoch=? AND status='running' AND generation=? AND lease_expires_at>UTC_TIMESTAMP(6))`, job.ID, job.UserID, job.WorkspaceID, job.SessionID, job.Epoch, job.Generation).Scan(&valid)
	return valid, err
}
func (s *Store) ValidateMemoryCapture(ctx context.Context, job domain.MemoryCaptureJob) (bool, error) {
	valid := false
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		var err error
		valid, err = captureAuthorityTx(ctx, tx, &job)
		if err != nil || !valid {
			return err
		}
		valid, err = captureLeaseTx(ctx, tx, job)
		return err
	})
	return valid, err
}

func validateMemoryCandidates(query string, candidates []domain.MemoryCandidate) error {
	if len(candidates) > 5 {
		return domain.ErrInvalid
	}
	for _, c := range candidates {
		if !validMemoryKind(c.Kind) || c.Importance < 1 || c.Importance > 5 || !utf8.ValidString(c.Content) || !utf8.ValidString(c.Topic) || !utf8.ValidString(c.Evidence) || strings.TrimSpace(c.Content) == "" || utf8.RuneCountInString(c.Content) > 500 || utf8.RuneCountInString(c.Topic) > 100 || c.Evidence == "" || !strings.Contains(query, c.Evidence) || sensitiveMemory.MatchString(c.Content) || sensitiveMemory.MatchString(c.Evidence) || sensitiveMemory.MatchString(c.Topic) {
			return domain.ErrInvalid
		}
	}
	return nil
}

func (s *Store) CompleteMemoryCapture(ctx context.Context, job domain.MemoryCaptureJob, candidates []domain.MemoryCandidate) error {
	return s.transaction(ctx, func(tx *sql.Tx) error {
		valid, err := captureAuthorityTx(ctx, tx, &job)
		if err != nil {
			return err
		}
		if !valid {
			return nil
		}
		valid, err = captureLeaseTx(ctx, tx, job)
		if err != nil {
			return err
		}
		if !valid {
			return nil
		}
		// Explicit content is derived again from the persisted source, never accepted
		// from a worker/model payload. Automatic evidence must occur in that source.
		if job.Mode == "explicit" {
			candidate, ok := ExplicitMemoryCandidate(job.Query)
			if !ok || candidate == nil {
				return cancelCaptureTx(ctx, tx, job.ID, "explicit_rejected")
			}
			candidates = []domain.MemoryCandidate{*candidate}
		}
		if err := validateMemoryCandidates(job.Query, candidates); err != nil {
			return err
		}
		count := 0
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM user_memories WHERE workspace_id=? AND user_id=?`, job.WorkspaceID, job.UserID).Scan(&count); err != nil {
			return err
		}
		saved := 0
		code := ""
		for _, c := range candidates {
			c.Content = sanitizeMemory(c.Content)
			c.Topic = sanitizeMemory(c.Topic)
			if c.Content == "" {
				continue
			}
			fingerprint := memoryFingerprint(c.Kind, c.Content)
			var exists bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM user_memories WHERE workspace_id=? AND user_id=? AND fingerprint=? AND (status<>'superseded' OR (?='automatic' AND origin<>'automatic') OR source_created_at>? OR (source_created_at=? AND source_run_id>=?))) OR EXISTS(SELECT 1 FROM memory_tombstones WHERE workspace_id=? AND user_id=? AND fingerprint=?)`, job.WorkspaceID, job.UserID, fingerprint, job.Mode, job.CreatedAt, job.CreatedAt, job.RunID, job.WorkspaceID, job.UserID, fingerprint).Scan(&exists); err != nil {
				return err
			}
			if exists {
				continue
			}
			key := strings.ToLower(c.Topic)
			if key != "" {
				// Manual and explicit memories cannot be replaced by a weaker automatic
				// inference. A late older turn can never supersede a newer source turn.
				var protected bool
				if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM user_memories WHERE workspace_id=? AND user_id=? AND status='active' AND BINARY normalized_key=BINARY ? AND ((?='automatic' AND origin<>'automatic') OR source_created_at>? OR (source_created_at=? AND source_run_id>?)))`, job.WorkspaceID, job.UserID, key, job.Mode, job.CreatedAt, job.CreatedAt, job.RunID).Scan(&protected); err != nil {
					return err
				}
				if protected {
					continue
				}
			}
			if count >= maxMemories {
				code = "memory_capacity"
				break
			}
			id, err := newID()
			if err != nil {
				return err
			}
			if key != "" {
				if _, err := tx.ExecContext(ctx, `UPDATE user_memories SET status='superseded',superseded_by=? WHERE workspace_id=? AND user_id=? AND status='active' AND BINARY normalized_key=BINARY ?`, id, job.WorkspaceID, job.UserID, key); err != nil {
					return err
				}
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO user_memories(id,workspace_id,user_id,kind,content,topic,normalized_key,fingerprint,importance,created_at,source_session_id,origin,source_run_id,source_created_at) VALUES(?,?,?,?,?,?,?,?,?,UNIX_TIMESTAMP(UTC_TIMESTAMP(6)),?,?,?,?)`, id, job.WorkspaceID, job.UserID, c.Kind, c.Content, c.Topic, key, fingerprint, c.Importance, job.SessionID, job.Mode, job.RunID, job.CreatedAt); err != nil {
				return err
			}
			saved++
			count++
		}
		status := "done"
		if code != "" && saved == 0 {
			status = "failed"
		}
		updated, err := tx.ExecContext(ctx, `UPDATE memory_capture_jobs SET status=?,saved_count=?,error_code=?,updated_at=UTC_TIMESTAMP(6) WHERE run_id=? AND status='running' AND generation=? AND lease_expires_at>UTC_TIMESTAMP(6)`, status, saved, code, job.ID, job.Generation)
		if err != nil {
			return err
		}
		changed, err := updated.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return domain.ErrConflict
		}
		return deleteMemoryCaptureDeliveryTx(ctx, tx, job.ID)
	})
}

// DeferMemoryCaptureBusy releases a claim only after the authenticated Harness
// explicitly refused admission. Unknown transport outcomes must use failure or
// lease recovery instead: they may already have invoked the model.
func (s *Store) DeferMemoryCaptureBusy(ctx context.Context, job domain.MemoryCaptureJob) error {
	return s.transaction(ctx, func(tx *sql.Tx) error {
		valid, err := captureAuthorityTx(ctx, tx, &job)
		if err != nil || !valid {
			return err
		}
		valid, err = captureLeaseTx(ctx, tx, job)
		if err != nil || !valid {
			return err
		}
		updated, err := tx.ExecContext(ctx, `UPDATE memory_capture_jobs SET status='queued',attempt=attempt-1,error_code='extraction_busy',lease_expires_at=NULL,next_attempt_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 5 SECOND),updated_at=UTC_TIMESTAMP(6) WHERE run_id=? AND status='running' AND generation=? AND attempt>0 AND lease_expires_at>UTC_TIMESTAMP(6)`, job.ID, job.Generation)
		if err != nil {
			return err
		}
		changed, err := updated.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return domain.ErrConflict
		}
		return resetMemoryCaptureDeliveryTx(ctx, tx, job.ID)
	})
}

func (s *Store) FailMemoryCapture(ctx context.Context, job domain.MemoryCaptureJob, code string) error {
	// Keep receipts machine-readable and never persist upstream response text.
	if !memoryCaptureErrorCode.MatchString(code) {
		code = "extraction_failed"
	}
	return s.transaction(ctx, func(tx *sql.Tx) error {
		valid, err := captureAuthorityTx(ctx, tx, &job)
		if err != nil || !valid {
			return err
		}
		valid, err = captureLeaseTx(ctx, tx, job)
		if err != nil || !valid {
			return err
		}
		status := "queued"
		if job.Attempt >= maxMemoryCaptureAttempts || code == "invalid_extraction" {
			status = "failed"
		}
		if _, err = tx.ExecContext(ctx, `UPDATE memory_capture_jobs SET status=?,error_code=?,lease_expires_at=NULL,next_attempt_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL ? SECOND),updated_at=UTC_TIMESTAMP(6) WHERE run_id=? AND status='running' AND generation=?`, status, code, job.Attempt*5, job.ID, job.Generation); err != nil {
			return err
		}
		if status == "queued" {
			return resetMemoryCaptureDeliveryTx(ctx, tx, job.ID)
		}
		return deleteMemoryCaptureDeliveryTx(ctx, tx, job.ID)
	})
}

func (s *Store) MemoryCaptureStatus(ctx context.Context, uid, wid, sid, rid string) (domain.MemoryCaptureStatus, error) {
	var out domain.MemoryCaptureStatus
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, uid, wid); err != nil {
			return err
		}
		if _, err := privateSession(ctx, tx, uid, wid, sid); err != nil {
			return err
		}
		var runStatus string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE id=? AND session_id=?`, rid, sid).Scan(&runStatus); err != nil {
			return err
		}
		err := tx.QueryRowContext(ctx, `SELECT status,saved_count,error_code FROM memory_capture_jobs WHERE run_id=? AND user_id=? AND workspace_id=? AND session_id=?`, rid, uid, wid, sid).Scan(&out.Status, &out.SavedCount, &out.ErrorCode)
		if errors.Is(err, sql.ErrNoRows) {
			out.Status = "not_scheduled"
			return nil
		}
		if err == nil && out.Status == "waiting" && runStatus == "failed" {
			out.Status = "cancelled"
			out.ErrorCode = "run_failed"
		}
		return err
	})
	return out, err
}
