package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"

	"magworkspace/internal/domain"
)

// Source pages preserve original text, including whitespace. They are separate
// from normalized search evidence and must not fabricate citation IDs.
type DocumentSourcePage struct {
	ToolDocument
	ContentStatus string            `json:"content_status"`
	Evidence      []domain.Evidence `json:"evidence"`
	Source        struct {
		Content    string `json:"content"`
		Offset     int    `json:"offset"`
		NextOffset *int   `json:"next_offset,omitempty"`
		HasMore    bool   `json:"has_more"`
	} `json:"source"`
}

func (s *Store) RunDocumentSource(ctx context.Context, uid, wid, sid, rid, did, vid string, offset int) (DocumentSourcePage, error) {
	out := DocumentSourcePage{ContentStatus: "original_text", Evidence: []domain.Evidence{}}
	if !validHexID(did) || (vid != "" && !validHexID(vid)) || offset < 0 || offset > domain.MaxAgentDocumentBytes || (offset > 0 && vid == "") {
		return out, domain.ErrInvalid
	}
	readMeta := func(pin bool) error {
		return s.transaction(ctx, func(tx *sql.Tx) error {
			if err := authorizeEvidenceRunTx(ctx, tx, uid, wid, sid, rid, true); err != nil {
				return err
			}
			if err := runAllowsDocumentTx(ctx, tx, rid, did); err != nil {
				return err
			}
			doc, err := toolDocumentTx(ctx, tx, wid, did, "", "", "")
			if err != nil {
				return err
			}
			if vid != "" && vid != doc.Version.ID {
				return domain.ErrConflict
			}
			if doc.Version.SizeBytes > domain.MaxAgentDocumentBytes {
				return domain.ErrInvalid
			}
			switch strings.ToLower(filepath.Ext(doc.Version.Filename)) {
			case ".txt", ".md", ".markdown":
			default:
				return domain.ErrInvalid
			}
			if pin {
				if err = pinToolDocumentTx(ctx, tx, wid, rid, doc); err != nil {
					return err
				}
			}
			out.ToolDocument = doc
			return nil
		})
	}
	if err := readMeta(true); err != nil {
		return out, err
	}
	vid = out.Version.ID
	_, raw, err := s.DocumentContent(ctx, uid, wid, did, vid)
	if err != nil {
		return out, err
	}
	if !validEditableDocumentContent(string(raw)) || len(raw) > domain.MaxAgentDocumentBytes {
		return out, domain.ErrInvalid
	}
	// Recheck active generation, scope, membership and current head after MinIO.
	if err = readMeta(false); err != nil {
		return out, err
	}
	chars := []rune(string(raw))
	if offset > len(chars) {
		return out, domain.ErrInvalid
	}
	end := min(offset+2048, len(chars))
	out.Source.Content = string(chars[offset:end])
	out.Source.Offset = offset
	if end < len(chars) {
		out.Source.NextOffset = &end
		out.Source.HasMore = true
	}
	return out, nil
}
