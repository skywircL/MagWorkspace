package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"

	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
)

const promptColumns = `p.id,p.run_id,r.session_id,p.tool_call_id,p.question,p.options,p.status,p.answer,p.created_at,p.expires_at,(SELECT JSON_OBJECT('metadata',a.metadata,'result',a.result) FROM run_document_approvals a WHERE a.prompt_id=p.id)`

func scanPrompt(row rowScanner) (domain.RunPrompt, error) {
	var p domain.RunPrompt
	var options, approval []byte
	err := row.Scan(&p.ID, &p.RunID, &p.SessionID, &p.ToolCallID, &p.Question, &options, &p.Status, &p.Answer, &p.CreatedAt, &p.ExpiresAt, &approval)
	if err == nil {
		err = json.Unmarshal(options, &p.Options)
	}
	p.Kind = "ask_user"
	if err == nil && len(approval) > 0 {
		var envelope struct {
			Metadata domain.DocumentApproval `json:"metadata"`
			Result   *domain.DocumentWrite   `json:"result"`
		}
		err = json.Unmarshal(approval, &envelope)
		p.Kind, p.Approval = "tool_approval", &envelope.Metadata
		p.Approval.Result = envelope.Result
	}
	return p, dbError(err)
}

func validPrompt(in domain.SuspendedPrompt) bool {
	if !validHexID(in.ID) || !validText(in.ToolCallID, 256) || !utf8.ValidString(in.ToolCallID) || !validText(in.Question, 4096) || !utf8.ValidString(in.Question) || len(in.Options) > 10 || len(in.Snapshot) == 0 || len(in.Snapshot) > domain.MaxRunSnapshotBytes || !utf8.ValidString(in.Snapshot) || !json.Valid([]byte(in.Snapshot)) {
		return false
	}
	for _, option := range in.Options {
		if !validText(option, 512) || !utf8.ValidString(option) {
			return false
		}
	}
	now := time.Now()
	return in.ExpiresAt.After(now) && !in.ExpiresAt.After(now.Add(7*24*time.Hour+time.Minute))
}

// SuspendRun atomically commits the private snapshot, prompt and run state.
func (s *Store) SuspendRun(ctx context.Context, uid, wid, sid, rid, mode string, in domain.SuspendedPrompt) (domain.RunPrompt, error) {
	var result domain.RunPrompt
	if !validPrompt(in) || (mode != "demo" && mode != "live") {
		return result, domain.ErrInvalid
	}
	digest := sha256.Sum256([]byte(in.Snapshot))
	options := in.Options
	if options == nil {
		options = []string{}
	}
	rawOptions, _ := json.Marshal(options)
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, uid, wid, sid, rid, true); err != nil {
			return err
		}
		scope, err := runDocumentScopeTx(ctx, tx, rid)
		if err != nil {
			return err
		}
		valid, err := runSourcesValidTx(ctx, tx, wid, rid, scope)
		if err != nil {
			return err
		}
		if !valid {
			return domain.ErrInvalid
		}
		approval, err := prepareApprovalTx(ctx, tx, uid, wid, sid, rid, in)
		if err != nil {
			return err
		}
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_prompts WHERE run_id=?`, rid).Scan(&count); err != nil {
			return err
		}
		if count >= 16 {
			return domain.ErrInvalid
		}
		if _, err = tx.ExecContext(ctx, `UPDATE run_prompts SET status='done' WHERE run_id=? AND status='resuming'`, rid); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO run_prompts(id,run_id,tool_call_id,question,options,answer,snapshot,snapshot_sha256,snapshot_size,expires_at) VALUES(?,?,?,?,?,'',?,?,?,?)`, in.ID, rid, in.ToolCallID, in.Question, rawOptions, []byte(in.Snapshot), hex.EncodeToString(digest[:]), len(in.Snapshot), in.ExpiresAt); err != nil {
			return err
		}
		if approval != nil {
			metadata, _ := json.Marshal(approval)
			if _, err = tx.ExecContext(ctx, `INSERT INTO run_document_approvals(prompt_id,run_id,tool_call_id,metadata) VALUES(?,?,?,?)`, in.ID, rid, in.ToolCallID, metadata); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(ctx, `UPDATE runs SET status='suspended',mode=?,lease_expires_at=? WHERE id=?`, mode, in.ExpiresAt, rid); err != nil {
			return err
		}
		result, err = scanPrompt(tx.QueryRowContext(ctx, `SELECT `+promptColumns+` FROM run_prompts p JOIN runs r ON r.id=p.run_id WHERE p.id=?`, in.ID))
		return err
	})
	if err == nil && result.Approval != nil {
		result.Approval.Arguments, err = approvalArguments(in.Snapshot, in.ToolCallID, result.Approval.Tool)
	}
	return result, err
}

func expirePromptsTx(ctx context.Context, tx *sql.Tx, sessionID string) error {
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET status='failed',error_code='prompt_expired',finished_at=UTC_TIMESTAMP(6) WHERE session_id=? AND status='suspended' AND lease_expires_at<=UTC_TIMESTAMP(6)`, sessionID); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE run_prompts p JOIN runs r ON r.id=p.run_id SET p.status=CASE WHEN r.error_code='prompt_expired' THEN 'expired' ELSE 'failed' END WHERE r.session_id=? AND r.status='failed' AND p.status IN ('pending','resuming')`, sessionID)
	return err
}

func (s *Store) RunPrompt(ctx context.Context, uid, wid, sid, rid, pid string) (domain.RunPrompt, error) {
	var result domain.RunPrompt
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, uid, wid, sid, rid, false); err != nil {
			return err
		}
		if err := expireSessionRuns(ctx, tx, sid); err != nil {
			return err
		}
		query := `SELECT ` + promptColumns + ` FROM run_prompts p JOIN runs r ON r.id=p.run_id WHERE p.run_id=?`
		args := []any{rid}
		if pid != "" {
			query += ` AND p.id=?`
			args = append(args, pid)
		}
		query += ` ORDER BY p.created_at DESC,p.id DESC LIMIT 1`
		var err error
		result, err = scanPrompt(tx.QueryRowContext(ctx, query, args...))
		return err
	})
	if err == nil {
		err = s.hydrateApproval(ctx, uid, wid, sid, rid, &result)
	}
	return result, err
}

// ResumePrompt verifies database snapshot bytes and claims exactly one answer atomically.
// Lease expiry fails a crashed execution; recovery requires a new explicit claim.
func (s *Store) ResumePrompt(ctx context.Context, uid, wid, sid, rid, pid, answer string, lease time.Duration) (domain.ResumedRun, error) {
	return s.resumePrompt(ctx, uid, wid, sid, rid, pid, answer, lease, false)
}

func (s *Store) ResumeApproval(ctx context.Context, uid, wid, sid, rid, pid string, approved bool, lease time.Duration) (domain.ResumedRun, error) {
	answer := "false"
	if approved {
		answer = "true"
	}
	return s.resumePrompt(ctx, uid, wid, sid, rid, pid, answer, lease, true)
}

func (s *Store) resumePrompt(ctx context.Context, uid, wid, sid, rid, pid, answer string, lease time.Duration, approval bool) (domain.ResumedRun, error) {
	var out domain.ResumedRun
	if !validText(answer, 4096) || !utf8.ValidString(answer) || lease <= 0 {
		return out, domain.ErrInvalid
	}
	var prompt domain.RunPrompt
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		var err error
		out.Role, err = lockWorkspace(ctx, tx, uid, wid)
		if err != nil {
			return err
		}
		if _, err = privateSession(ctx, tx, uid, wid, sid); err != nil {
			return err
		}
		var status string
		var active bool
		if err = tx.QueryRowContext(ctx, `SELECT status,lease_expires_at>UTC_TIMESTAMP(6) FROM runs WHERE id=? AND session_id=? FOR UPDATE`, rid, sid).Scan(&status, &active); err != nil {
			return err
		}
		if err = assertRunExecutionTx(ctx, tx, rid); err != nil {
			return err
		}
		if status != "suspended" || !active {
			return domain.ErrConflict
		}
		prompt, err = scanPrompt(tx.QueryRowContext(ctx, `SELECT `+promptColumns+` FROM run_prompts p JOIN runs r ON r.id=p.run_id WHERE p.id=? AND p.run_id=? FOR UPDATE`, pid, rid))
		if err != nil {
			return err
		}
		if (prompt.Approval != nil) != approval {
			return domain.ErrInvalid
		}
		if prompt.Status != "pending" || !prompt.ExpiresAt.After(time.Now()) {
			return domain.ErrConflict
		}
		raw, err := readRunSnapshotTx(ctx, tx, pid, rid)
		if err != nil {
			return err
		}
		out.DocumentIDs, err = runDocumentScopeTx(ctx, tx, rid)
		if err != nil {
			return err
		}
		valid, err := runSourcesValidTx(ctx, tx, wid, rid, out.DocumentIDs)
		if err != nil {
			return err
		}
		if !valid {
			return domain.ErrConflict
		}
		if approval && answer == "true" {
			rawArgs, err := approvalArguments(string(raw), prompt.ToolCallID, prompt.Approval.Tool)
			if err != nil {
				return err
			}
			in, hash, err := domain.ParseDocumentMutation(prompt.Approval.Tool, rawArgs)
			if err != nil || hash != prompt.Approval.ArgumentsSHA256 {
				return domain.ErrInvalid
			}
			if err = checkMutationTargetTx(ctx, tx, uid, wid, rid, prompt.Approval.Tool, in, &domain.DocumentApproval{}); err != nil {
				return err
			}
		}
		if err = tx.QueryRowContext(ctx, `SELECT content FROM messages WHERE run_id=? AND role='user'`, rid).Scan(&out.Query); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE run_prompts SET status='resuming',answer=?,resolved_at=UTC_TIMESTAMP(6) WHERE id=?`, answer, pid); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE runs SET status='running',lease_expires_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL ? MICROSECOND) WHERE id=?`, lease.Microseconds(), rid); err != nil {
			return err
		}
		out.Run, err = scanRun(tx.QueryRowContext(ctx, `SELECT id,session_id,status,error_code,mode,created_at,finished_at FROM runs WHERE id=?`, rid))
		if err != nil {
			return err
		}
		// A new execution claim fences late callbacks from the suspended worker.
		var generation uint64
		err = tx.QueryRowContext(ctx, `SELECT execution_generation FROM run_journal_heads WHERE run_id=? FOR UPDATE`, rid).Scan(&generation)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if err == nil {
			out.Generation = generation + 1
			if _, err = tx.ExecContext(ctx, `UPDATE run_journal_heads SET execution_generation=? WHERE run_id=?`, out.Generation, rid); err != nil {
				return err
			}
		}
		out.Prompt, err = nativeAnsweredPrompt(prompt, uid, wid, sid, rid, raw, answer)
		return err
	})
	return out, err
}

func (s *Store) CancelPrompt(ctx context.Context, uid, wid, sid, rid, pid string) error {
	return s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, uid, wid, sid, rid, false); err != nil {
			return err
		}
		var status string
		if err := tx.QueryRowContext(ctx, `SELECT p.status FROM run_prompts p JOIN runs r ON r.id=p.run_id WHERE p.id=? AND p.run_id=? AND r.status='suspended' FOR UPDATE`, pid, rid).Scan(&status); err != nil {
			return err
		}
		if status != "pending" {
			return domain.ErrConflict
		}
		if _, err := tx.ExecContext(ctx, `UPDATE run_prompts SET status='canceled',resolved_at=UTC_TIMESTAMP(6) WHERE id=?`, pid); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE runs SET status='failed',error_code='question_canceled',finished_at=UTC_TIMESTAMP(6) WHERE id=?`, rid)
		return err
	})
}

// Clarifications are user messages in model history, not instructions promoted
// to a system role. Keep the original public message unchanged.
func promptHistoryTx(ctx context.Context, tx *sql.Tx, runID, query string) (string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT question,answer FROM run_prompts WHERE run_id=? AND status='done' AND NOT EXISTS(SELECT 1 FROM run_document_approvals a WHERE a.prompt_id=run_prompts.id) ORDER BY created_at,id`, runID)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var b strings.Builder
	b.WriteString(query)
	for rows.Next() {
		var question, answer string
		if err = rows.Scan(&question, &answer); err != nil {
			return "", err
		}
		extra := "\n\n澄清问题：" + question + "\n用户答复：" + answer
		if b.Len()+len(extra) > 64000 {
			return "", domain.ErrInvalid
		}
		b.WriteString(extra)
	}
	return b.String(), rows.Err()
}

func (s *Store) ClarifiedQuery(ctx context.Context, uid, wid, sid, rid, query string) (string, error) {
	var result string
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, uid, wid, sid, rid, false); err != nil {
			return err
		}
		var err error
		result, err = promptHistoryTx(ctx, tx, rid, query)
		return err
	})
	return result, err
}

func (s *Store) ValidateRunSources(ctx context.Context, uid, wid, sid, rid string) error {
	return s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, uid, wid, sid, rid, true); err != nil {
			return err
		}
		scope, err := runDocumentScopeTx(ctx, tx, rid)
		if err != nil {
			return err
		}
		valid, err := runSourcesValidTx(ctx, tx, wid, rid, scope)
		if err != nil {
			return err
		}
		if !valid {
			return domain.ErrConflict
		}
		return nil
	})
}

// nativeAnsweredPrompt translates backend CAS state into the native import DTO.
// The authoritative answer remains the already-claimed MySQL prompt row.
func nativeAnsweredPrompt(prompt domain.RunPrompt, uid, wid, sid, rid string, raw []byte, answer string) (json.RawMessage, error) {
	kind := "ask_user"
	var request any = map[string]any{"question": prompt.Question, "options": prompt.Options}
	var response any = map[string]string{"answer": answer}
	if prompt.Approval != nil {
		kind = "tool_approval"
		var err error
		request, err = nativeApprovalRequest(prompt, raw)
		if err != nil {
			return nil, err
		}
		if answer != "true" && answer != "false" {
			return nil, domain.ErrInvalid
		}
		response = map[string]bool{"approved": answer == "true"}
	}
	return json.Marshal(map[string]any{
		"id": prompt.ID, "kind": kind, "workspace_id": wid, "user_id": uid, "session_id": sid, "run_id": rid, "tool_call_id": prompt.ToolCallID,
		"request": request, "status": "answered", "snapshot": string(raw), "response": response,
		"created_at": float64(prompt.CreatedAt.UnixMicro()) / 1e6, "expires_at": float64(prompt.ExpiresAt.UnixMicro()) / 1e6, "resolved_at": float64(time.Now().UnixMicro()) / 1e6, "resolved_by": uid, "close_reason": "",
	})
}

// readRunSnapshotTx is private: callers must authorize and lock the run first.
func readRunSnapshotTx(ctx context.Context, tx *sql.Tx, pid, rid string) ([]byte, error) {
	var raw []byte
	var sha string
	var size int
	if err := tx.QueryRowContext(ctx, `SELECT LEFT(snapshot,?),snapshot_sha256,snapshot_size FROM run_prompts WHERE id=? AND run_id=?`, domain.MaxRunSnapshotBytes+1, pid, rid).Scan(&raw, &sha, &size); err != nil {
		return nil, err
	}
	digest := sha256.Sum256(raw)
	if len(raw) != size || size < 1 || size > domain.MaxRunSnapshotBytes || hex.EncodeToString(digest[:]) != sha {
		return nil, blob.ErrIntegrity
	}
	return raw, nil
}
