package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
)

const maxJournalRecordBytes = 2 << 20
const maxJournalBytes = 16 << 20

type runExecutionKey struct{}
type runExecution struct {
	runID      string
	generation uint64
}

// WithRunExecution binds all writes and tool acquisition to the claimed worker.
// Legacy callers without this value keep their pre-journal behavior.
func WithRunExecution(ctx context.Context, rid string, generation uint64) context.Context {
	return context.WithValue(ctx, runExecutionKey{}, runExecution{rid, generation})
}

func assertRunExecutionTx(ctx context.Context, tx *sql.Tx, rid string) error {
	expected, ok := ctx.Value(runExecutionKey{}).(runExecution)
	if !ok {
		return nil
	}
	if expected.runID != rid || expected.generation == 0 {
		return domain.ErrConflict
	}
	var generation uint64
	if err := tx.QueryRowContext(ctx, `SELECT execution_generation FROM run_journal_heads WHERE run_id=?`, rid).Scan(&generation); err != nil {
		return err
	}
	if generation != expected.generation {
		return domain.ErrConflict
	}
	return nil
}

func (s *Store) BeginRunJournal(ctx context.Context, uid, wid, sid, rid string) (uint64, error) {
	var generation uint64
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, uid, wid, sid, rid, true); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT IGNORE INTO run_journal_heads(run_id) VALUES(?)`, rid); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, `SELECT execution_generation FROM run_journal_heads WHERE run_id=?`, rid).Scan(&generation)
	})
	return generation, err
}

type journalHead struct {
	generation, seq uint64
	digest          string
	total           int
}

func lockJournalHead(ctx context.Context, tx *sql.Tx, rid string, generation uint64) (journalHead, error) {
	var head journalHead
	err := tx.QueryRowContext(ctx, `SELECT execution_generation,seq,digest,total_bytes FROM run_journal_heads WHERE run_id=? FOR UPDATE`, rid).Scan(&head.generation, &head.seq, &head.digest, &head.total)
	if err != nil {
		return head, err
	}
	if generation == 0 || head.generation != generation {
		return head, domain.ErrConflict
	}
	return head, nil
}

// RunJournal is an internal capability callback. Caller identity and generation
// are bound by Go, never chosen by the Python model or tool arguments.
func (s *Store) RunJournal(ctx context.Context, uid, wid, sid, rid string, generation uint64, op string, args json.RawMessage) (any, error) {
	if generation == 0 {
		return nil, domain.ErrConflict
	}
	switch op {
	case "load":
		return s.loadRunJournal(ctx, uid, wid, sid, rid, generation)
	case "append":
		var in struct {
			Record json.RawMessage `json:"record"`
		}
		if decodeJournalJSON(args, &in) != nil || len(in.Record) == 0 {
			return nil, domain.ErrInvalid
		}
		return s.appendRunJournal(ctx, uid, wid, sid, rid, generation, in.Record)
	case "checkpoint_read", "checkpoint_write":
		err := s.transaction(ctx, func(tx *sql.Tx) error {
			if err := authorizeEvidenceRunTx(ctx, tx, uid, wid, sid, rid, true); err != nil {
				return err
			}
			_, err := lockJournalHead(ctx, tx, rid, generation)
			return err
		})
		if op == "checkpoint_read" {
			return nil, err
		}
		return map[string]any{}, err
	default:
		return nil, domain.ErrInvalid
	}
}

func decodeJournalJSON(raw []byte, out any) error {
	if !utf8.Valid(raw) || !uniqueJournalJSON(raw) {
		return domain.ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return domain.ErrInvalid
	}
	if decoder.Decode(new(any)) != io.EOF {
		return domain.ErrInvalid
	}
	return nil
}

// Reject duplicate fields at every nesting level; map decoding alone loses them.
func uniqueJournalJSON(raw []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value func() error
	value = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			keys := map[string]bool{}
			for decoder.More() {
				key, err := decoder.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok || keys[name] {
					return domain.ErrInvalid
				}
				keys[name] = true
				if err = value(); err != nil {
					return err
				}
			}
			token, err = decoder.Token()
			if err != nil || token != json.Delim('}') {
				return domain.ErrInvalid
			}
		case '[':
			for decoder.More() {
				if err = value(); err != nil {
					return err
				}
			}
			token, err = decoder.Token()
			if err != nil || token != json.Delim(']') {
				return domain.ErrInvalid
			}
		default:
			return domain.ErrInvalid
		}
		return nil
	}
	return value() == nil && decoder.Decode(new(any)) == io.EOF
}

type journalEnvelope struct {
	Version  int             `json:"version"`
	Seq      uint64          `json:"seq"`
	Kind     string          `json:"kind"`
	Data     json.RawMessage `json:"data"`
	Previous string          `json:"prev_hash"`
	Digest   string          `json:"digest"`
}

func decodeJournalEnvelope(raw []byte) (journalEnvelope, error) {
	var record journalEnvelope
	if len(raw) > maxJournalRecordBytes || decodeJournalJSON(raw, &record) != nil || record.Version != 1 || record.Seq == 0 || !validText(record.Kind, 100) || len(record.Data) == 0 || record.Data[0] != '{' || !validJournalDigest(record.Digest) || (record.Previous != "" && !validJournalDigest(record.Previous)) {
		return record, domain.ErrInvalid
	}
	return record, nil
}
func validJournalDigest(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
func (s *Store) appendRunJournal(ctx context.Context, uid, wid, sid, rid string, generation uint64, raw json.RawMessage) (any, error) {
	record, err := decodeJournalEnvelope(raw)
	if err != nil {
		return nil, err
	}
	// Preserve Python's canonical number and Unicode spelling, which binds digest.
	content := append(append([]byte{}, raw...), '\n')
	digest := sha256.Sum256(content)
	err = s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, uid, wid, sid, rid, true); err != nil {
			return err
		}
		head, err := lockJournalHead(ctx, tx, rid, generation)
		if err != nil {
			return err
		}
		if record.Seq != head.seq+1 || record.Previous != head.digest || head.total+len(content) > maxJournalBytes {
			return domain.ErrConflict
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO run_journal_records(run_id,seq,digest,content,content_sha256,size_bytes) VALUES(?,?,?,?,?,?)`, rid, record.Seq, record.Digest, content, hex.EncodeToString(digest[:]), len(content)); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE run_journal_heads SET seq=?,digest=?,total_bytes=total_bytes+? WHERE run_id=? AND execution_generation=?`, record.Seq, record.Digest, len(content), rid, generation)
		return err
	})
	if err != nil {
		return nil, err
	}
	return json.RawMessage(append([]byte{}, raw...)), nil
}

func (s *Store) loadRunJournal(ctx context.Context, uid, wid, sid, rid string, generation uint64) (any, error) {
	var head journalHead
	records := []json.RawMessage{}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, uid, wid, sid, rid, true); err != nil {
			return err
		}
		var err error
		head, err = lockJournalHead(ctx, tx, rid, generation)
		if err != nil {
			return err
		}
		// Read one sentinel byte beyond the per-record limit so corrupt SQL
		// values cannot force unbounded materialization before validation.
		rows, err := tx.QueryContext(ctx, `SELECT seq,digest,LEFT(content,?),content_sha256,size_bytes FROM run_journal_records WHERE run_id=? ORDER BY seq`, maxJournalRecordBytes+2, rid)
		if err != nil {
			return err
		}
		defer rows.Close()
		total, previous := 0, ""
		for rows.Next() {
			var seq uint64
			var digest, sha string
			var size int
			var content []byte
			if err = rows.Scan(&seq, &digest, &content, &sha, &size); err != nil {
				return err
			}
			if seq != uint64(len(records)+1) || size < 1 || size > maxJournalRecordBytes+1 || len(content) != size {
				return blob.ErrIntegrity
			}
			sum := sha256.Sum256(content)
			total += len(content)
			if total > maxJournalBytes || hex.EncodeToString(sum[:]) != sha || content[len(content)-1] != '\n' {
				return blob.ErrIntegrity
			}
			raw := content[:len(content)-1]
			record, err := decodeJournalEnvelope(raw)
			if err != nil || record.Seq != seq || record.Digest != digest || record.Previous != previous {
				return blob.ErrIntegrity
			}
			records = append(records, json.RawMessage(raw))
			previous = record.Digest
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if head.seq != uint64(len(records)) || head.digest != previous || head.total != total {
			return blob.ErrIntegrity
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	err = s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, uid, wid, sid, rid, true); err != nil {
			return err
		}
		current, err := lockJournalHead(ctx, tx, rid, generation)
		if err != nil {
			return err
		}
		if current != head {
			return domain.ErrConflict
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"records": records}, nil
}

// RecoverRun claims only interrupted, journaled execution; canceled, timed out,
// expired-question and completed runs never become runnable through this API.
func (s *Store) RecoverRun(ctx context.Context, uid, wid, sid, rid string, lease time.Duration) (domain.RecoveredRun, error) {
	var out domain.RecoveredRun
	if lease <= 0 {
		return out, domain.ErrInvalid
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		var err error
		out.Role, err = lockWorkspace(ctx, tx, uid, wid)
		if err != nil {
			return err
		}
		if _, err = privateSession(ctx, tx, uid, wid, sid); err != nil {
			return err
		}
		if err = expireSessionRuns(ctx, tx, sid); err != nil {
			return err
		}
		var status, code string
		if err = tx.QueryRowContext(ctx, `SELECT status,error_code FROM runs WHERE id=? AND session_id=? FOR UPDATE`, rid, sid).Scan(&status, &code); err != nil {
			return err
		}
		if err = assertRunExecutionTx(ctx, tx, rid); err != nil {
			return err
		}
		if status != "failed" || (code != "lease_expired" && code != "agent_failed" && code != "interrupted") {
			return domain.ErrConflict
		}
		var active bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runs WHERE session_id=? AND id<>? AND status IN ('running','suspended'))`, sid, rid).Scan(&active); err != nil {
			return err
		}
		if active {
			return domain.ErrConflict
		}
		var seq uint64
		if err = tx.QueryRowContext(ctx, `SELECT execution_generation,seq FROM run_journal_heads WHERE run_id=? FOR UPDATE`, rid).Scan(&out.Generation, &seq); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return domain.ErrConflict
			}
			return err
		}
		if seq == 0 {
			return domain.ErrConflict
		}
		out.DocumentIDs, err = runDocumentScopeTx(ctx, tx, rid)
		if err != nil {
			return err
		}
		// An explicit selection is a durable capability boundary even when a
		// document was never read and has no acquired-source row yet.
		for _, documentID := range out.DocumentIDs {
			if _, err = documentInWorkspace(ctx, tx, wid, documentID); err != nil {
				if errors.Is(err, domain.ErrNotFound) {
					return domain.ErrConflict
				}
				return err
			}
		}
		valid, err := runSourcesValidTx(ctx, tx, wid, rid, out.DocumentIDs)
		if err != nil {
			return err
		}
		if !valid {
			return domain.ErrConflict
		}
		if err = tx.QueryRowContext(ctx, `SELECT content FROM messages WHERE run_id=? AND role='user'`, rid).Scan(&out.Query); err != nil {
			return err
		}
		var prompt domain.RunPrompt
		if err = tx.QueryRowContext(ctx, `SELECT id FROM run_prompts WHERE run_id=? AND status IN ('resuming','failed') AND answer<>'' ORDER BY created_at DESC,id DESC LIMIT 1`, rid).Scan(&prompt.ID); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if prompt.ID != "" {
			prompt, err = scanPrompt(tx.QueryRowContext(ctx, `SELECT `+promptColumns+` FROM run_prompts p JOIN runs r ON r.id=p.run_id WHERE p.id=? AND p.run_id=? FOR UPDATE`, prompt.ID, rid))
			if err != nil {
				return err
			}
			if (prompt.Status != "failed" && prompt.Status != "resuming") || prompt.Answer == "" || !prompt.ExpiresAt.After(time.Now()) {
				return domain.ErrConflict
			}
			snapshot, err := readRunSnapshotTx(ctx, tx, prompt.ID, rid)
			if err != nil {
				return err
			}
			out.Prompt, err = nativeAnsweredPrompt(prompt, uid, wid, sid, rid, snapshot, prompt.Answer)
			if err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, `UPDATE run_prompts SET status='resuming' WHERE id=?`, prompt.ID); err != nil {
				return err
			}
		}
		out.Generation++
		if _, err = tx.ExecContext(ctx, `UPDATE run_journal_heads SET execution_generation=? WHERE run_id=?`, out.Generation, rid); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE runs SET status='running',error_code='',finished_at=NULL,lease_expires_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL ? MICROSECOND) WHERE id=?`, lease.Microseconds(), rid); err != nil {
			return err
		}
		out.Run, err = scanRun(tx.QueryRowContext(ctx, `SELECT id,session_id,status,error_code,mode,created_at,finished_at FROM runs WHERE id=?`, rid))
		return err
	})
	return out, err
}
