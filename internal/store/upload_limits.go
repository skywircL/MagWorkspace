package store

import "magworkspace/internal/domain"

// SetUploadLimit is deployment configuration; call before serving or workers.
// It restricts new uploads, never access to already stored immutable versions.
func (s *Store) SetUploadLimit(bytes int) error {
	if bytes < 1<<20 || bytes > domain.MaxStoredDocumentBytes || bytes%(1<<20) != 0 {
		return domain.ErrInvalid
	}
	s.maxUploadBytes = bytes
	return nil
}
func (s *Store) UploadLimitBytes() int {
	if s == nil || s.maxUploadBytes == 0 {
		return domain.DefaultMaxFileSizeMB * (1 << 20)
	}
	return s.maxUploadBytes
}

func (s *Store) validUploadSize(size int) bool {
	if size > s.UploadLimitBytes() {
		return false
	}
	// New production originals use MinIO. Preserve bounded inline fixtures and
	// legacy reads without expanding MySQL's role or MEDIUMBLOB storage.
	return s.objects != nil || size <= domain.MaxLegacyInlineDocumentBytes
}
