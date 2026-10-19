package domain

import "time"

// Upload policy is configured per Store, separately from the storage envelope.
const DefaultMaxFileSizeMB = 50
const MaxStoredDocumentBytes = 512 << 20
const MaxLegacyInlineDocumentBytes = 8 << 20

// Inline editing is bounded by the indexing pipeline's extracted text limit.
const MaxEditableDocumentBytes = MaxExtractedBytes

// Document is the stable identity and mutable head of an append-only history.
type Document struct {
	ID               string    `json:"id"`
	WorkspaceID      string    `json:"workspace_id"`
	Title            string    `json:"title"`
	CurrentVersionID string    `json:"current_version_id"`
	CreatedBy        string    `json:"created_by"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// DocumentVersion contains immutable metadata; content is only served by download.
type DocumentVersion struct {
	ID         string    `json:"id"`
	DocumentID string    `json:"document_id"`
	Number     int       `json:"number"`
	Filename   string    `json:"filename"`
	MediaType  string    `json:"media_type"`
	SizeBytes  int       `json:"size_bytes"`
	SHA256     string    `json:"sha256"`
	CreatedBy  string    `json:"created_by"`
	CreatedAt  time.Time `json:"created_at"`
	ObjectKey  string    `json:"-"`
}

type DocumentWrite struct {
	Document Document        `json:"document"`
	Version  DocumentVersion `json:"version"`
}
