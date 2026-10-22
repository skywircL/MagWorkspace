package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"slices"
	"strings"

	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
)

// Approval arguments live in the private MySQL snapshot, outside public metadata.
func approvalArguments(snapshot, callID, tool string) (json.RawMessage, error) {
	var v struct {
		Pending struct {
			Blocked int `json:"blocked_index"`
			Calls   []struct {
				ID        string `json:"id"`
				Name      string `json:"name"`
				Arguments string `json:"arguments"`
			} `json:"tool_calls"`
		} `json:"pending"`
	}
	if json.Unmarshal([]byte(snapshot), &v) != nil || v.Pending.Blocked < 0 || v.Pending.Blocked >= len(v.Pending.Calls) {
		return nil, domain.ErrInvalid
	}
	c := v.Pending.Calls[v.Pending.Blocked]
	if c.ID != callID || c.Name != tool {
		return nil, domain.ErrInvalid
	}
	if _, _, err := domain.ParseDocumentMutation(tool, []byte(c.Arguments)); err != nil {
		return nil, err
	}
	return json.RawMessage(c.Arguments), nil
}

func checkMutationTargetTx(ctx context.Context, tx *sql.Tx, uid, wid, rid, tool string, in domain.DocumentMutation, meta *domain.DocumentApproval) error {
	role, err := lockWorkspace(ctx, tx, uid, wid)
	if err != nil {
		return err
	}
	if !canWriteDocument(role) {
		return domain.ErrForbidden
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
	if tool == "create_document" {
		if scope != nil {
			return domain.ErrForbidden
		}
		meta.Title, meta.Filename = in.Title, in.Filename
		return nil
	}
	if scope != nil {
		found := false
		for _, id := range scope {
			if id == in.DocumentID {
				found = true
			}
		}
		if !found {
			return domain.ErrForbidden
		}
	}
	d, err := documentInWorkspace(ctx, tx, wid, in.DocumentID)
	if err != nil {
		return err
	}
	if d.CurrentVersionID != in.BaseVersionID {
		return domain.ErrConflict
	}
	var filename, sha string
	var size int
	if err = tx.QueryRowContext(ctx, `SELECT filename,sha256,size_bytes FROM document_versions WHERE id=? AND document_id=?`, in.BaseVersionID, in.DocumentID).Scan(&filename, &sha, &size); err != nil {
		return err
	}
	if size > domain.MaxAgentDocumentBytes {
		return domain.ErrInvalid
	}
	switch strings.ToLower(filepath.Ext(filename)) {
	case ".txt", ".md", ".markdown":
	default:
		return domain.ErrInvalid
	}
	meta.DocumentID, meta.BaseVersionID, meta.Title, meta.Filename, meta.BaseSHA256 = in.DocumentID, in.BaseVersionID, d.Title, filename, sha
	return nil
}

func prepareApprovalTx(ctx context.Context, tx *sql.Tx, uid, wid, sid, rid string, in domain.SuspendedPrompt) (*domain.DocumentApproval, error) {
	if in.Approval == nil {
		return nil, nil
	}
	var identity struct {
		RunID     string `json:"run_id"`
		SessionID string `json:"session_id"`
		Query     string `json:"query"`
		Caller    struct {
			UserID      string `json:"user_id"`
			WorkspaceID string `json:"workspace_id"`
		} `json:"caller"`
		Scope struct {
			WorkspaceID string   `json:"workspace_id"`
			DocumentIDs []string `json:"document_ids"`
		} `json:"scope"`
		Policy struct {
			Access string `json:"access"`
		} `json:"policy"`
	}
	if json.Unmarshal([]byte(in.Snapshot), &identity) != nil || identity.RunID != rid || identity.SessionID != sid || identity.Caller.UserID != uid || identity.Caller.WorkspaceID != wid || identity.Scope.WorkspaceID != wid || identity.Policy.Access != "confirm" {
		return nil, domain.ErrInvalid
	}
	var query string
	if err := tx.QueryRowContext(ctx, `SELECT content FROM messages WHERE run_id=? AND role='user'`, rid).Scan(&query); err != nil {
		return nil, err
	}
	scope, err := runDocumentScopeTx(ctx, tx, rid)
	if err != nil {
		return nil, err
	}
	slices.Sort(scope)
	slices.Sort(identity.Scope.DocumentIDs)
	if query != identity.Query || (scope == nil) != (identity.Scope.DocumentIDs == nil) || !slices.Equal(scope, identity.Scope.DocumentIDs) {
		return nil, domain.ErrInvalid
	}
	a := domain.DocumentApproval{Tool: in.Approval.Tool, Risk: in.Approval.Risk, Description: in.Approval.Description, Reason: in.Approval.Reason}
	if a.Risk != "write" || len(a.Description) > 2048 || len(a.Reason) > 1024 {
		return nil, domain.ErrInvalid
	}
	raw, err := approvalArguments(in.Snapshot, in.ToolCallID, a.Tool)
	if err != nil {
		return nil, err
	}
	mutation, hash, err := domain.ParseDocumentMutation(a.Tool, raw)
	if err != nil {
		return nil, err
	}
	_, presentedHash, err := domain.ParseDocumentMutation(a.Tool, in.Approval.Arguments)
	if err != nil || presentedHash != hash {
		return nil, domain.ErrInvalid
	}
	if err = checkMutationTargetTx(ctx, tx, uid, wid, rid, a.Tool, mutation, &a); err != nil {
		return nil, err
	}
	digest := sha256.Sum256([]byte(mutation.Content))
	a.ContentSHA256 = hex.EncodeToString(digest[:])
	a.ContentBytes = len(mutation.Content)
	a.ArgumentsSHA256 = hash
	a.Arguments = nil
	a.Result = nil
	return &a, nil
}

func nativeApprovalRequest(p domain.RunPrompt, snapshot []byte) (map[string]any, error) {
	a := p.Approval
	raw, err := approvalArguments(string(snapshot), p.ToolCallID, a.Tool)
	if err != nil {
		return nil, err
	}
	_, hash, err := domain.ParseDocumentMutation(a.Tool, raw)
	if err != nil || hash != a.ArgumentsSHA256 {
		return nil, blob.ErrIntegrity
	}
	return map[string]any{"tool": a.Tool, "risk": a.Risk, "description": a.Description, "reason": a.Reason, "arguments": raw}, nil
}

func (s *Store) hydrateApproval(ctx context.Context, uid, wid, sid, rid string, p *domain.RunPrompt) error {
	if p.Approval == nil || (p.Status != "pending" && p.Status != "resuming") {
		return nil
	}
	var req map[string]any
	err := s.transaction(ctx, func(tx *sql.Tx) error {
		if err := authorizeEvidenceRunTx(ctx, tx, uid, wid, sid, rid, false); err != nil {
			return err
		}
		raw, err := readRunSnapshotTx(ctx, tx, p.ID, rid)
		if err != nil {
			return err
		}
		req, err = nativeApprovalRequest(*p, raw)
		return err
	})
	if err != nil {
		return err
	}

	p.Approval.Arguments = req["arguments"].(json.RawMessage)
	return nil
}

type documentWriteGate struct {
	uid, wid, sid, rid, callID, tool, hash, promptID string
	mutation                                         domain.DocumentMutation
}

func (g *documentWriteGate) check(ctx context.Context, tx *sql.Tx, out *domain.DocumentWrite) (bool, error) {
	if err := authorizeEvidenceRunTx(ctx, tx, g.uid, g.wid, g.sid, g.rid, true); err != nil {
		return false, err
	}
	role, err := lockWorkspace(ctx, tx, g.uid, g.wid)
	if err != nil {
		return false, err
	}
	if !canWriteDocument(role) {
		return false, domain.ErrForbidden
	}
	var metadata, result []byte
	var status, answer string
	var active bool
	err = tx.QueryRowContext(ctx, `SELECT a.prompt_id,a.metadata,a.result,p.status,p.answer,p.expires_at>UTC_TIMESTAMP(6) FROM run_document_approvals a JOIN run_prompts p ON p.id=a.prompt_id WHERE a.run_id=? AND a.tool_call_id=? FOR UPDATE`, g.rid, g.callID).Scan(&g.promptID, &metadata, &result, &status, &answer, &active)
	if err != nil {
		return false, dbError(err)
	}
	var a domain.DocumentApproval
	if json.Unmarshal(metadata, &a) != nil {
		return false, blob.ErrIntegrity
	}
	if a.Tool != g.tool || a.ArgumentsSHA256 != g.hash || answer != "true" {
		return false, domain.ErrForbidden
	}
	if len(result) > 0 {
		if json.Unmarshal(result, out) != nil {
			return false, blob.ErrIntegrity
		}
		// Replayed receipt never leaks a deleted/foreign document.
		if _, err = documentInWorkspace(ctx, tx, g.wid, out.Document.ID); err != nil {
			return false, err
		}
		scope, err := runDocumentScopeTx(ctx, tx, g.rid)
		if err != nil {
			return false, err
		}
		valid, err := runSourcesValidTx(ctx, tx, g.wid, g.rid, scope)
		if err != nil {
			return false, err
		}
		if !valid || (scope != nil && !slices.Contains(scope, out.Document.ID)) {
			return false, domain.ErrConflict
		}
		return true, nil
	}
	if status != "resuming" || !active {
		return false, domain.ErrConflict
	}
	if err := checkMutationTargetTx(ctx, tx, g.uid, g.wid, g.rid, g.tool, g.mutation, &domain.DocumentApproval{}); err != nil {
		return false, err
	}
	return false, nil
}

func (g *documentWriteGate) commit(ctx context.Context, tx *sql.Tx, result domain.DocumentWrite) error {
	// A write receipt is also a model-visible document observation. Bind future
	// history to that source even when the model never called a read tool.
	if err := pinToolDocumentTx(ctx, tx, g.wid, g.rid, ToolDocument{Document: result.Document, Version: result.Version}); err != nil {
		return err
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	r, err := tx.ExecContext(ctx, `UPDATE run_document_approvals SET result=?,executed_at=UTC_TIMESTAMP(6) WHERE prompt_id=? AND result IS NULL`, raw, g.promptID)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err == nil && n != 1 {
		return domain.ErrConflict
	}
	return err
}

// The receipt and immutable version commit together. Private callbacks carry no
// approval authority: the persisted, owned, exact-argument decision is required.
func (s *Store) ExecuteDocumentWrite(ctx context.Context, uid, wid, sid, rid, callID, tool string, raw json.RawMessage) (domain.DocumentWrite, error) {
	var out domain.DocumentWrite
	in, hash, err := domain.ParseDocumentMutation(tool, raw)
	if err != nil || !validText(callID, 256) {
		return out, domain.ErrInvalid
	}
	if s.objects == nil {
		return out, blob.ErrUnavailable
	}
	gate := &documentWriteGate{uid: uid, wid: wid, sid: sid, rid: rid, callID: callID, tool: tool, hash: hash, mutation: in}
	done := false
	meta := domain.DocumentApproval{}
	err = s.transaction(ctx, func(tx *sql.Tx) error {
		var err error
		done, err = gate.check(ctx, tx, &out)
		if err != nil || done {
			return err
		}
		return checkMutationTargetTx(ctx, tx, uid, wid, rid, tool, in, &meta)
	})
	if err != nil || done {
		return out, err
	}
	if tool == "create_document" {
		out, err = s.createDocument(ctx, uid, wid, in.Title, in.Filename, []byte(in.Content), gate)
	} else {
		out, err = s.appendDocumentVersion(ctx, uid, wid, in.DocumentID, in.BaseVersionID, meta.Filename, []byte(in.Content), gate)
	}
	if err != nil {
		// Concurrent delivery or an ambiguous commit may already have succeeded.
		var saved domain.DocumentWrite
		checkErr := s.transaction(ctx, func(tx *sql.Tx) error { var e error; done, e = gate.check(ctx, tx, &saved); return e })
		if checkErr == nil && done {
			return saved, nil
		}
	}
	return out, err
}
