package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"math"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"magworkspace/internal/domain"
)

const maxMemories = 1000
const memoryColumns = `id,kind,content,topic,normalized_key,importance,status,created_at,expires_at,superseded_by,source_session_id,use_count,last_used_at,origin,source_run_id`

var memoryEnvelope = regexp.MustCompile(`(?i)</?[\s\p{Z}\x{000b}\x{0085}\x{001c}-\x{001f}]*memory[^>]*>`)

func memoryWhitespace(r rune) bool { return unicode.IsSpace(r) || r >= 0x1c && r <= 0x1f }
func sanitizeMemory(s string) string {
	for {
		cleaned := memoryEnvelope.ReplaceAllString(s, "")
		if cleaned == s {
			break
		}
		s = cleaned
	}
	s = strings.Join(strings.FieldsFunc(s, memoryWhitespace), " ")
	runes := []rune(s)
	if len(runes) > 500 {
		s = string(runes[:500]) + "…"
	}
	return s
}
func validMemoryStatus(s string) bool { return s == "active" || s == "superseded" || s == "archived" }
func validMemoryKind(s string) bool {
	return s == "profile" || s == "preference" || s == "fact" || s == "task"
}
func memoryFingerprint(kind, content string) string {
	// Version 1: exact sanitized UTF-8, case-sensitive, portable across runtimes.
	digest := sha256.Sum256([]byte(kind + "\n" + content))
	return hex.EncodeToString(digest[:])
}
func scanMemory(row rowScanner) (domain.Memory, error) {
	var v domain.Memory
	err := row.Scan(&v.ID, &v.Kind, &v.Content, &v.Topic, &v.NormalizedKey, &v.Importance, &v.Status, &v.CreatedAt, &v.ExpiresAt, &v.SupersededBy, &v.SourceSessionID, &v.UseCount, &v.LastUsedAt, &v.Origin, &v.SourceRunID)
	return v, dbError(err)
}
func memoryRows(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]domain.Memory, error) {
	out := []domain.Memory{}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		v, err := scanMemory(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func listMemoryTx(ctx context.Context, tx *sql.Tx, uid, wid, status string) ([]domain.Memory, error) {
	query := `SELECT ` + memoryColumns + ` FROM user_memories m WHERE workspace_id=? AND user_id=? AND status=?`
	if status == "active" {
		query += ` AND (expires_at IS NULL OR expires_at>UNIX_TIMESTAMP(UTC_TIMESTAMP(6))) AND NOT EXISTS(SELECT 1 FROM memory_tombstones t WHERE t.workspace_id=m.workspace_id AND t.user_id=m.user_id AND t.fingerprint=m.fingerprint)`
	}
	return memoryRows(ctx, tx, query+` ORDER BY created_at DESC,id DESC LIMIT ?`, wid, uid, status, maxMemories)
}

func (s *Store) CreateMemory(ctx context.Context, uid, wid string, in domain.CreateMemory) (domain.Memory, error) {
	return s.saveMemory(ctx, uid, wid, "", in)
}

// UpdateMemory appends a replacement; the original immutable ID is the CAS token.
func (s *Store) UpdateMemory(ctx context.Context, uid, wid, id string, in domain.CreateMemory) (domain.Memory, error) {
	if !validHexID(id) {
		return domain.Memory{}, domain.ErrInvalid
	}
	return s.saveMemory(ctx, uid, wid, id, in)
}

func (s *Store) saveMemory(ctx context.Context, uid, wid, previous string, in domain.CreateMemory) (domain.Memory, error) {
	var out domain.Memory
	if !validMemoryKind(in.Kind) || !utf8.ValidString(in.Content) || len(in.Content) > 8000 || !utf8.ValidString(in.Topic) || len(in.Topic) > 8000 || (in.SourceSessionID != "" && !validHexID(in.SourceSessionID)) {
		return out, domain.ErrInvalid
	}
	in.Content, in.Topic = sanitizeMemory(in.Content), sanitizeMemory(in.Topic)
	if in.Content == "" || in.Importance < 1 || in.Importance > 5 || (in.ExpiresAt != nil && (math.IsNaN(*in.ExpiresAt) || math.IsInf(*in.ExpiresAt, 0) || *in.ExpiresAt >= domain.MaxMemoryTimeSeconds || *in.ExpiresAt <= float64(time.Now().UnixNano())/1e9)) {
		return out, domain.ErrInvalid
	}
	id, err := newID()
	if err != nil {
		return out, err
	}
	fingerprint := memoryFingerprint(in.Kind, in.Content)
	err = s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, uid, wid); err != nil {
			return err
		}
		if previous != "" {
			var status, source string
			if err := tx.QueryRowContext(ctx, `SELECT status,source_session_id FROM user_memories WHERE id=? AND workspace_id=? AND user_id=? FOR UPDATE`, previous, wid, uid).Scan(&status, &source); err != nil {
				return err
			}
			if status != "active" {
				return domain.ErrConflict
			}
			if in.SourceSessionID != "" && in.SourceSessionID != source {
				return domain.ErrInvalid
			}
			in.SourceSessionID = source
		}
		if previous == "" && in.SourceSessionID != "" {
			if _, err := privateSession(ctx, tx, uid, wid, in.SourceSessionID); err != nil {
				return err
			}
		}
		var forgotten bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM memory_tombstones WHERE workspace_id=? AND user_id=? AND fingerprint=?)`, wid, uid, fingerprint).Scan(&forgotten); err != nil {
			return err
		}
		if forgotten {
			return domain.ErrConflict
		}
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM user_memories WHERE workspace_id=? AND user_id=?`, wid, uid).Scan(&count); err != nil {
			return err
		}
		if count >= maxMemories {
			return domain.ErrConflict
		}
		if previous != "" {
			if _, err := tx.ExecContext(ctx, `UPDATE user_memories SET status='superseded',superseded_by=? WHERE id=? AND workspace_id=? AND user_id=?`, id, previous, wid, uid); err != nil {
				return err
			}
		}
		key := strings.ToLower(in.Topic)
		// A workspace lock serializes replacements and forget/create races.
		if key != "" {
			if _, err := tx.ExecContext(ctx, `UPDATE user_memories SET status='superseded',superseded_by=? WHERE workspace_id=? AND user_id=? AND status='active' AND BINARY normalized_key=BINARY ?`, id, wid, uid, key); err != nil {
				return err
			}
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO user_memories(id,workspace_id,user_id,kind,content,topic,normalized_key,fingerprint,importance,created_at,expires_at,source_session_id) VALUES(?,?,?,?,?,?,?,?,?,UNIX_TIMESTAMP(UTC_TIMESTAMP(6)),?,?)`, id, wid, uid, in.Kind, in.Content, in.Topic, key, fingerprint, in.Importance, in.ExpiresAt, in.SourceSessionID)
		if err != nil {
			return err
		}
		if err := invalidateMemoryCapturesTx(ctx, tx, uid, wid); err != nil {
			return err
		}
		out, err = scanMemory(tx.QueryRowContext(ctx, `SELECT `+memoryColumns+` FROM user_memories WHERE id=? AND workspace_id=? AND user_id=?`, id, wid, uid))
		return err
	})
	return out, err
}
func (s *Store) Memories(ctx context.Context, uid, wid, status string) ([]domain.Memory, error) {
	if status == "all" {
		return s.ExportMemories(ctx, uid, wid)
	}
	out := []domain.Memory{}
	if status == "" {
		status = "active"
	}
	if !validMemoryStatus(status) {
		return out, domain.ErrInvalid
	}
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, uid, wid); err != nil {
			return err
		}
		var err error
		out, err = listMemoryTx(ctx, tx, uid, wid, status)
		return err
	})
	return out, err
}
func (s *Store) ForgetMemory(ctx context.Context, uid, wid, id string) error {
	if !validHexID(id) {
		return domain.ErrInvalid
	}
	return s.transaction(ctx, func(tx *sql.Tx) error {
		if _, err := lockWorkspace(ctx, tx, uid, wid); err != nil {
			return err
		}
		var fingerprint string
		if err := tx.QueryRowContext(ctx, `SELECT fingerprint FROM user_memories WHERE id=? AND workspace_id=? AND user_id=? FOR UPDATE`, id, wid, uid).Scan(&fingerprint); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT IGNORE INTO memory_tombstones(workspace_id,user_id,fingerprint,forgotten_at) VALUES(?,?,?,UNIX_TIMESTAMP(UTC_TIMESTAMP(6)))`, wid, uid, fingerprint); err != nil {
			return err
		}
		if err := invalidateMemoryCapturesTx(ctx, tx, uid, wid); err != nil {
			return err
		}
		// Forget every duplicate, including replacements and older identical IDs.
		_, err := tx.ExecContext(ctx, `UPDATE user_memories SET status='archived' WHERE workspace_id=? AND user_id=? AND fingerprint=?`, wid, uid, fingerprint)
		return err
	})
}

// RunMemory is a private capability. It accepts no caller-selected identity.
// Every call rechecks membership, private run, lease and execution generation.
func (s *Store) RunMemory(ctx context.Context, uid, wid, sid, rid string, generation uint64, operation string, raw json.RawMessage) (any, error) {
	if generation != 0 {
		ctx = WithRunExecution(ctx, rid, generation)
	}
	var in struct {
		Status  string   `json:"status"`
		ItemIDs []string `json:"item_ids"`
		Now     *float64 `json:"now"`
	}
	if decodeJournalJSON(raw, &in) != nil {
		return nil, domain.ErrInvalid
	}
	if in.Status == "" {
		in.Status = "active"
	}
	if !validMemoryStatus(in.Status) || len(in.ItemIDs) > maxMemories || (in.Now != nil && (math.IsNaN(*in.Now) || math.IsInf(*in.Now, 0))) {
		return nil, domain.ErrInvalid
	}
	for _, id := range in.ItemIDs {
		if !validHexID(id) {
			return nil, domain.ErrInvalid
		}
	}
	var out any
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, uid, wid, sid, rid, true); err != nil {
			return err
		}
		if operation != "list" && operation != "get_items" && operation != "touch" {
			return domain.ErrInvalid
		}
		enabled, err := memoryEnabledTx(ctx, tx, uid, wid)
		if err != nil {
			return err
		}
		if !enabled {
			if operation == "touch" {
				out = map[string]any{}
			} else {
				out = []domain.Memory{}
			}
			return nil
		}
		switch operation {
		case "list":
			items, err := listMemoryTx(ctx, tx, uid, wid, in.Status)
			out = items
			return err
		case "get_items", "touch":
			out = []domain.Memory{}
			if operation == "touch" {
				out = map[string]any{}
			}
			if len(in.ItemIDs) == 0 {
				return nil
			}
			args := []any{wid, uid}
			ids := []string{}
			seen := map[string]bool{}
			for _, id := range in.ItemIDs {
				if !seen[id] {
					ids = append(ids, "?")
					args = append(args, id)
					seen[id] = true
				}
			}
			where := ` WHERE workspace_id=? AND user_id=? AND id IN (` + strings.Join(ids, ",") + `) AND status='active' AND (expires_at IS NULL OR expires_at>UNIX_TIMESTAMP(UTC_TIMESTAMP(6))) AND NOT EXISTS(SELECT 1 FROM memory_tombstones t WHERE t.workspace_id=m.workspace_id AND t.user_id=m.user_id AND t.fingerprint=m.fingerprint)`
			if operation == "touch" {
				_, err := tx.ExecContext(ctx, `UPDATE user_memories m SET use_count=use_count+1,last_used_at=UNIX_TIMESTAMP(UTC_TIMESTAMP(6))`+where, args...)
				return err
			}
			items, err := memoryRows(ctx, tx, `SELECT `+memoryColumns+` FROM user_memories m`+where+` ORDER BY created_at DESC,id DESC`, args...)
			out = items
			return err
		default:
			return domain.ErrInvalid
		}
	})
	return out, err
}
