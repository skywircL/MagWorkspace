package httpapi

import (
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type documentUpload struct {
	title         string
	baseVersionID string
	filename      string
	content       []byte
}

// readDocumentUpload uses bounded in-memory parts, never disk-backed form files.
// Parse the raw disposition: multipart.Part.FileName silently strips path components.
func readDocumentUpload(w http.ResponseWriter, r *http.Request, appendVersion bool, maxFileBytes int) (documentUpload, bool) {
	var in documentUpload
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "multipart/form-data" {
		problem(w, 415, "unsupported_media_type", "Content-Type must be multipart/form-data")
		return in, false
	}
	r.Body = http.MaxBytesReader(w, r.Body, int64(maxFileBytes)+(64<<10))
	reader, err := r.MultipartReader()
	if err != nil {
		problem(w, 400, "invalid_request", "invalid multipart boundary")
		return in, false
	}
	seen := make(map[string]bool)
	for {
		// NextPart decodes quoted-printable automatically; uploaded bytes must
		// remain exact, so transfer encodings are not interpreted here.
		part, err := reader.NextRawPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			uploadError(w, err)
			return in, false
		}
		disposition, params, err := mime.ParseMediaType(part.Header.Get("Content-Disposition"))
		name := params["name"]
		filename, isFile := params["filename"]
		if err != nil || disposition != "form-data" || part.Header.Get("Content-Transfer-Encoding") != "" || seen[name] ||
			(name != "file" && name != "title" && name != "base_version_id") ||
			(name == "title" && appendVersion) || (name == "base_version_id" && !appendVersion) ||
			(name == "file") != isFile {
			problem(w, 400, "invalid_request", "unexpected or duplicate multipart field")
			return in, false
		}
		seen[name] = true
		limit := 255
		if name == "file" {
			if !validUploadFilename(filename) {
				problem(w, 400, "invalid_request", "filename must be a safe basename of at most 255 bytes")
				return in, false
			}
			limit = maxFileBytes
		}
		value, err := io.ReadAll(io.LimitReader(part, int64(limit)+1))
		if err != nil {
			uploadError(w, err)
			return in, false
		}
		if len(value) > limit {
			if name == "file" {
				problem(w, 413, "document_too_large", "file must not exceed "+strconv.Itoa(maxFileBytes/(1<<20))+" MB")
			} else {
				problem(w, 400, "invalid_request", "multipart field too long")
			}
			return in, false
		}
		switch name {
		case "file":
			in.filename, in.content = filename, value
		case "title":
			in.title = strings.TrimSpace(string(value))
		case "base_version_id":
			in.baseVersionID = string(value)
		}
	}
	// Include any multipart epilogue in the request size limit.
	if _, err := io.Copy(io.Discard, r.Body); err != nil {
		uploadError(w, err)
		return in, false
	}
	if !seen["file"] || (appendVersion && in.baseVersionID == "") || (seen["title"] && in.title == "") {
		problem(w, 400, "invalid_request", "file and required nonempty fields must be provided")
		return in, false
	}
	if !appendVersion && !seen["title"] {
		in.title = in.filename
	}
	return in, true
}

func validUploadFilename(name string) bool {
	return len(name) > 0 && len(name) <= 255 && utf8.ValidString(name) &&
		strings.TrimSpace(name) != "" && name != "." && name != ".." &&
		!strings.ContainsAny(name, "/\\") && strings.IndexFunc(name, unicode.IsControl) == -1
}

func uploadError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		problem(w, 413, "document_too_large", "multipart request exceeds size limit")
	} else {
		problem(w, 400, "invalid_request", "invalid or incomplete multipart body")
	}
}

func (s *server) createDocument(w http.ResponseWriter, r *http.Request) {
	in, ok := readDocumentUpload(w, r, false, s.db.UploadLimitBytes())
	if !ok {
		return
	}
	data, err := s.db.CreateDocument(r.Context(), userID(r), r.PathValue("workspaceID"), in.title, in.filename, in.content)
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 201, data)
}

func (s *server) appendDocumentVersion(w http.ResponseWriter, r *http.Request) {
	in, ok := readDocumentUpload(w, r, true, s.db.UploadLimitBytes())
	if !ok {
		return
	}
	data, err := s.db.AppendDocumentVersion(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("documentID"), in.baseVersionID, in.filename, in.content)
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 201, data)
}

func (s *server) documents(w http.ResponseWriter, r *http.Request) {
	p, n, ok := pagination(w, r)
	if !ok {
		return
	}
	data, total, err := s.db.DocumentsMatching(r.Context(), userID(r), r.PathValue("workspaceID"), r.URL.Query().Get("keyword"), n, (p-1)*n)
	if err != nil {
		fail(w, err)
		return
	}
	page(w, data, total, p, n)
}

func (s *server) document(w http.ResponseWriter, r *http.Request) {
	data, err := s.db.Document(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("documentID"))
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 200, data)
}

func (s *server) deleteDocument(w http.ResponseWriter, r *http.Request) {
	if err := s.db.DeleteDocument(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("documentID")); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(204)
}

func (s *server) documentVersions(w http.ResponseWriter, r *http.Request) {
	p, n, ok := pagination(w, r)
	if !ok {
		return
	}
	data, total, err := s.db.DocumentVersions(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("documentID"), n, (p-1)*n)
	if err != nil {
		fail(w, err)
		return
	}
	page(w, data, total, p, n)
}

func (s *server) documentVersion(w http.ResponseWriter, r *http.Request) {
	data, err := s.db.DocumentVersion(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("documentID"), r.PathValue("versionID"))
	if err != nil {
		fail(w, err)
		return
	}
	respond(w, 200, data)
}

func (s *server) documentContent(w http.ResponseWriter, r *http.Request) {
	version, content, err := s.db.DocumentContent(r.Context(), userID(r), r.PathValue("workspaceID"), r.PathValue("documentID"), r.PathValue("versionID"))
	if err != nil {
		fail(w, err)
		return
	}
	w.Header().Set("Content-Type", version.MediaType)
	w.Header().Set("Content-Length", strconv.Itoa(version.SizeBytes))
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": version.Filename}))
	w.Header().Set("ETag", strconv.Quote(version.SHA256))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(content)
}
