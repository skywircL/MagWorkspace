package httpapi_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
	"testing"
	"time"

	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
)

type documentPart struct {
	name     string
	filename string
	content  []byte
	file     bool
	headers  textproto.MIMEHeader
}

func documentFile(filename string, content []byte) documentPart {
	return documentPart{name: "file", filename: filename, content: content, file: true}
}

func documentField(name, value string) documentPart {
	return documentPart{name: name, content: []byte(value)}
}

func documentBody(t *testing.T, parts ...documentPart) ([]byte, string) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for _, part := range parts {
		headers := make(textproto.MIMEHeader)
		params := map[string]string{"name": part.name}
		if part.file {
			// Preserve the caller's raw filename; CreateFormFile/Part.FileName
			// must not conceal path traversal fixtures from the application.
			params["filename"] = part.filename
			headers.Set("Content-Type", "application/x-untrusted-upload")
		}
		headers.Set("Content-Disposition", mime.FormatMediaType("form-data", params))
		for key, values := range part.headers {
			headers[key] = values
		}
		item, err := writer.CreatePart(headers)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := item.Write(part.content); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return body.Bytes(), writer.FormDataContentType()
}

func documentHTTP(f *integrationFixture, method, path, token, contentType string, body []byte) (integrationResponse, http.Header) {
	req, err := http.NewRequest(method, f.server.URL+path, bytes.NewReader(body))
	if err != nil {
		return integrationResponse{err: err}, nil
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	response, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return integrationResponse{err: err}, nil
	}
	defer response.Body.Close()
	content, err := io.ReadAll(response.Body)
	return integrationResponse{status: response.StatusCode, body: content, err: err}, response.Header.Clone()
}

func documentUpload(t *testing.T, f *integrationFixture, path, token string, want int, parts ...documentPart) integrationEnvelope {
	t.Helper()
	body, mediaType := documentBody(t, parts...)
	response, _ := documentHTTP(f, http.MethodPost, path, token, mediaType, body)
	return checkIntegrationResponse(t, response, want)
}

func documentsPath(workspaceID string) string { return workspacePath(workspaceID) + "/documents" }

func documentPath(document domain.Document) string {
	return documentsPath(document.WorkspaceID) + "/" + document.ID
}

func documentVersionPath(document domain.Document, version domain.DocumentVersion) string {
	return documentPath(document) + "/versions/" + version.ID
}

func assertDocumentVersion(t *testing.T, version domain.DocumentVersion, documentID, userID, filename string, content []byte, number int) {
	t.Helper()
	hash := sha256.Sum256(content)
	if version.ID == "" || version.DocumentID != documentID || version.CreatedBy != userID || version.Number != number || version.Filename != filename || version.SizeBytes != len(content) || version.SHA256 != hex.EncodeToString(hash[:]) || version.MediaType != http.DetectContentType(content) || version.CreatedAt.IsZero() {
		t.Fatalf("version does not describe the authenticated immutable upload: %+v", version)
	}
}

func assertDocumentDownload(t *testing.T, f *integrationFixture, token string, document domain.Document, version domain.DocumentVersion, want []byte) {
	t.Helper()
	response, headers := documentHTTP(f, http.MethodGet, documentVersionPath(document, version)+"/content", token, "", nil)
	if response.err != nil || response.status != http.StatusOK {
		t.Fatalf("download failed: status=%d error=%v body=%s", response.status, response.err, response.body)
	}
	if !bytes.Equal(response.body, want) {
		t.Fatalf("immutable content changed: received %d bytes, want %d", len(response.body), len(want))
	}
	disposition, params, err := mime.ParseMediaType(headers.Get("Content-Disposition"))
	if err != nil || disposition != "attachment" || params["filename"] != version.Filename {
		t.Fatalf("unsafe or incorrect download disposition: %q", headers.Get("Content-Disposition"))
	}
	if headers.Get("Content-Type") != version.MediaType || headers.Get("X-Content-Type-Options") != "nosniff" || headers.Get("Content-Length") != strconv.Itoa(len(want)) || headers.Get("ETag") != `"`+version.SHA256+`"` {
		t.Fatalf("download metadata headers do not match the stored version: %v", headers)
	}
}

func TestHTTPDocumentsVersionsDownloadsAndPersistence(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	owner := f.register(t, "document-owner")
	base := documentsPath(owner.Workspaces[0].ID)
	firstBytes := []byte{0, 255, 1, 128, '\r', '\n', 0, 'A', 254}
	first := integrationData[domain.DocumentWrite](t, documentUpload(t, f, base, owner.Token, http.StatusCreated,
		documentFile("研究档案.bin", firstBytes), documentField("title", "  Research archive  ")))
	if first.Document.ID == "" || first.Document.WorkspaceID != owner.Workspaces[0].ID || first.Document.CreatedBy != owner.User.ID || first.Document.CurrentVersionID != first.Version.ID || first.Document.Title != "Research archive" || first.Document.CreatedAt.IsZero() || first.Document.UpdatedAt.IsZero() {
		t.Fatalf("invalid document identity or head: %+v", first.Document)
	}
	assertDocumentVersion(t, first.Version, first.Document.ID, owner.User.ID, "研究档案.bin", firstBytes, 1)
	assertDocumentDownload(t, f, owner.Token, first.Document, first.Version, firstBytes)
	metadata := integrationData[domain.Document](t, f.request(t, http.MethodGet, documentPath(first.Document), owner.Token, nil, http.StatusOK))
	if metadata.ID != first.Document.ID || metadata.CurrentVersionID != first.Version.ID || metadata.Title != first.Document.Title || metadata.CreatedBy != owner.User.ID {
		t.Fatalf("document metadata changed after create: %+v", metadata)
	}
	versionEnvelope := f.request(t, http.MethodGet, documentVersionPath(first.Document, first.Version), owner.Token, nil, http.StatusOK)
	assertDocumentVersion(t, integrationData[domain.DocumentVersion](t, versionEnvelope), first.Document.ID, owner.User.ID, first.Version.Filename, firstBytes, 1)
	if bytes.Contains(versionEnvelope.Data, []byte(`"content"`)) {
		t.Fatal("version metadata must not embed raw content")
	}

	secondBytes := []byte("second immutable revision\n")
	second := integrationData[domain.DocumentWrite](t, documentUpload(t, f, documentPath(first.Document)+"/versions", owner.Token, http.StatusCreated,
		documentFile("revision.txt", secondBytes), documentField("base_version_id", first.Version.ID)))
	if second.Document.ID != first.Document.ID || second.Document.Title != first.Document.Title || second.Document.CurrentVersionID != second.Version.ID || second.Version.ID == first.Version.ID || second.Document.CreatedBy != owner.User.ID {
		t.Fatalf("new version replaced document identity or failed to advance head: %+v", second)
	}
	assertDocumentVersion(t, second.Version, first.Document.ID, owner.User.ID, "revision.txt", secondBytes, 2)
	documentUpload(t, f, documentPath(first.Document)+"/versions", owner.Token, http.StatusConflict,
		documentFile("stale.txt", []byte("must not be saved")), documentField("base_version_id", first.Version.ID))
	assertDocumentDownload(t, f, owner.Token, first.Document, first.Version, firstBytes)
	assertDocumentDownload(t, f, owner.Token, second.Document, second.Version, secondBytes)

	other := integrationData[domain.DocumentWrite](t, documentUpload(t, f, base, owner.Token, http.StatusCreated, documentFile("default-title.txt", []byte("another document"))))
	if other.Document.Title != "default-title.txt" {
		t.Fatalf("omitted title did not default to filename: %q", other.Document.Title)
	}
	docPage := f.request(t, http.MethodGet, base+"?page=1&page_size=1", owner.Token, nil, http.StatusOK)
	if docPage.Total != 2 || docPage.Page != 1 || docPage.PageSize != 1 || len(integrationData[[]domain.Document](t, docPage)) != 1 {
		t.Fatalf("document pagination is incorrect: %+v", docPage)
	}
	versionIDs := make(map[string]bool)
	for page := 1; page <= 2; page++ {
		envelope := f.request(t, http.MethodGet, documentPath(first.Document)+fmt.Sprintf("/versions?page=%d&page_size=1", page), owner.Token, nil, http.StatusOK)
		versions := integrationData[[]domain.DocumentVersion](t, envelope)
		if envelope.Total != 2 || envelope.Page != page || envelope.PageSize != 1 || len(versions) != 1 || versionIDs[versions[0].ID] {
			t.Fatalf("version pagination skipped or repeated an immutable version: %+v", envelope)
		}
		versionIDs[versions[0].ID] = true
	}
	if !versionIDs[first.Version.ID] || !versionIDs[second.Version.ID] {
		t.Fatalf("version list does not contain both uploaded versions: %v", versionIDs)
	}
	for _, path := range []string{base, documentPath(first.Document) + "/versions"} {
		for _, query := range []string{"page=0", "page_size=101", "page_size=invalid"} {
			f.request(t, http.MethodGet, path+"?"+query, owner.Token, nil, http.StatusBadRequest)
		}
	}
	f.restart(t)
	after := integrationData[domain.Document](t, f.request(t, http.MethodGet, documentPath(first.Document), owner.Token, nil, http.StatusOK))
	if after.CurrentVersionID != second.Version.ID || after.Title != first.Document.Title || after.CreatedBy != first.Document.CreatedBy {
		t.Fatalf("document head did not survive reconnect: %+v", after)
	}
	assertDocumentDownload(t, f, owner.Token, after, first.Version, firstBytes)
	assertDocumentDownload(t, f, owner.Token, after, second.Version, secondBytes)
	if len(f.agent.calls()) != 0 {
		t.Fatal("document CRUD unexpectedly invoked the agent")
	}
}

func TestHTTPDocumentsConcurrentVersionsCompareAndSwap(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	owner := f.register(t, "document-cas")
	initial := integrationData[domain.DocumentWrite](t, documentUpload(t, f, documentsPath(owner.Workspaces[0].ID), owner.Token, http.StatusCreated, documentFile("original.txt", []byte("original"))))
	start := make(chan struct{})
	results := make(chan integrationResponse, 2)
	for _, text := range []string{"candidate one", "candidate two"} {
		body, mediaType := documentBody(t, documentFile("candidate.txt", []byte(text)), documentField("base_version_id", initial.Version.ID))
		go func() {
			<-start
			response, _ := documentHTTP(f, http.MethodPost, documentPath(initial.Document)+"/versions", owner.Token, mediaType, body)
			results <- response
		}()
	}
	close(start)
	statuses := make(map[int]int)
	var winner domain.DocumentWrite
	for range 2 {
		select {
		case response := <-results:
			envelope := checkIntegrationResponse(t, response, http.StatusCreated, http.StatusConflict)
			statuses[response.status]++
			if response.status == http.StatusCreated {
				winner = integrationData[domain.DocumentWrite](t, envelope)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("concurrent version requests did not finish")
		}
	}
	if statuses[http.StatusCreated] != 1 || statuses[http.StatusConflict] != 1 || winner.Version.Number != 2 {
		t.Fatalf("expected one committed version and one stale conflict: statuses=%v winner=%+v", statuses, winner)
	}
	current := integrationData[domain.Document](t, f.request(t, http.MethodGet, documentPath(initial.Document), owner.Token, nil, http.StatusOK))
	versions := integrationData[[]domain.DocumentVersion](t, f.request(t, http.MethodGet, documentPath(initial.Document)+"/versions", owner.Token, nil, http.StatusOK))
	if current.CurrentVersionID != winner.Version.ID || len(versions) != 2 {
		t.Fatalf("CAS conflict left an orphan version or wrong head: head=%s versions=%+v", current.CurrentVersionID, versions)
	}
	assertDocumentDownload(t, f, owner.Token, initial.Document, initial.Version, []byte("original"))
	response, _ := documentHTTP(f, http.MethodGet, documentVersionPath(winner.Document, winner.Version)+"/content", owner.Token, "", nil)
	if response.err != nil || response.status != http.StatusOK || (string(response.body) != "candidate one" && string(response.body) != "candidate two") {
		t.Fatalf("winning version was not persisted intact: status=%d err=%v body=%s", response.status, response.err, response.body)
	}
}

func TestHTTPDocumentsPermissionsScopeAndRevocation(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	owner, admin, contributor, viewer, outsider := f.register(t, "doc-owner"), f.register(t, "doc-admin"), f.register(t, "doc-contributor"), f.register(t, "doc-viewer"), f.register(t, "doc-outsider")
	wid := owner.Workspaces[0].ID
	base := documentsPath(wid)
	for _, member := range []struct {
		auth integrationAuth
		role string
	}{{admin, "admin"}, {contributor, "contributor"}, {viewer, "viewer"}} {
		f.request(t, http.MethodPost, workspacePath(wid)+"/members", owner.Token, map[string]string{"email": member.auth.User.Email, "role": member.role}, http.StatusCreated)
	}
	document := integrationData[domain.DocumentWrite](t, documentUpload(t, f, base, owner.Token, http.StatusCreated, documentFile("shared.txt", []byte("workspace members may read"))))
	path := documentPath(document.Document)
	versionPath := documentVersionPath(document.Document, document.Version)
	for _, reader := range []integrationAuth{owner, admin, contributor, viewer} {
		for _, route := range []string{base, path, path + "/versions", versionPath} {
			f.request(t, http.MethodGet, route, reader.Token, nil, http.StatusOK)
		}
		assertDocumentDownload(t, f, reader.Token, document.Document, document.Version, []byte("workspace members may read"))
	}
	for _, writer := range []integrationAuth{admin, contributor} {
		created := integrationData[domain.DocumentWrite](t, documentUpload(t, f, base, writer.Token, http.StatusCreated, documentFile("member.txt", []byte("member upload"))))
		assertDocumentVersion(t, created.Version, created.Document.ID, writer.User.ID, "member.txt", []byte("member upload"), 1)
		if created.Document.CreatedBy != writer.User.ID {
			t.Fatal("document author did not come from the authenticated token")
		}
		updated := integrationData[domain.DocumentWrite](t, documentUpload(t, f, path+"/versions", writer.Token, http.StatusCreated,
			documentFile("member-version.txt", []byte(writer.User.ID)), documentField("base_version_id", document.Document.CurrentVersionID)))
		assertDocumentVersion(t, updated.Version, document.Document.ID, writer.User.ID, "member-version.txt", []byte(writer.User.ID), document.Version.Number+1)
		if updated.Document.CreatedBy != owner.User.ID {
			t.Fatal("appending a version changed the original document author")
		}
		document = updated
		f.request(t, http.MethodDelete, documentPath(created.Document), writer.Token, nil, http.StatusNoContent)
	}
	for _, denied := range []struct {
		token  string
		status int
	}{{"", http.StatusUnauthorized}, {viewer.Token, http.StatusForbidden}, {outsider.Token, http.StatusNotFound}} {
		documentUpload(t, f, base, denied.token, denied.status, documentFile("denied.txt", []byte("denied")))
		documentUpload(t, f, path+"/versions", denied.token, denied.status, documentFile("denied.txt", []byte("denied")), documentField("base_version_id", document.Version.ID))
		f.request(t, http.MethodDelete, path, denied.token, nil, denied.status)
	}
	for _, denied := range []struct {
		token  string
		status int
	}{{"", http.StatusUnauthorized}, {outsider.Token, http.StatusNotFound}} {
		for _, route := range []string{base, path, path + "/versions", versionPath, versionPath + "/content"} {
			f.request(t, http.MethodGet, route, denied.token, nil, denied.status)
		}
	}
	otherWorkspace := integrationData[domain.Workspace](t, f.request(t, http.MethodPost, "/api/v1/workspaces", owner.Token, map[string]string{"name": "another workspace"}, http.StatusCreated))
	foreignPath := documentsPath(otherWorkspace.ID) + "/" + document.Document.ID
	for _, route := range []string{foreignPath, foreignPath + "/versions", foreignPath + "/versions/" + document.Version.ID, foreignPath + "/versions/" + document.Version.ID + "/content"} {
		f.request(t, http.MethodGet, route, owner.Token, nil, http.StatusNotFound)
	}
	documentUpload(t, f, foreignPath+"/versions", owner.Token, http.StatusNotFound, documentFile("cross.txt", []byte("cross")), documentField("base_version_id", document.Version.ID))
	f.request(t, http.MethodDelete, foreignPath, owner.Token, nil, http.StatusNotFound)
	other := integrationData[domain.DocumentWrite](t, documentUpload(t, f, base, owner.Token, http.StatusCreated, documentFile("other.txt", []byte("other"))))
	for _, suffix := range []string{"", "/content"} {
		f.request(t, http.MethodGet, documentPath(other.Document)+"/versions/"+document.Version.ID+suffix, owner.Token, nil, http.StatusNotFound)
	}
	documentUpload(t, f, path+"/versions", owner.Token, http.StatusConflict, documentFile("foreign-base.txt", []byte("foreign")), documentField("base_version_id", other.Version.ID))

	f.request(t, http.MethodPatch, workspacePath(wid)+"/members/"+admin.User.ID, owner.Token, map[string]string{"role": "viewer"}, http.StatusOK)
	documentUpload(t, f, path+"/versions", admin.Token, http.StatusForbidden, documentFile("demoted.txt", []byte("demoted")), documentField("base_version_id", document.Version.ID))
	f.request(t, http.MethodGet, path, admin.Token, nil, http.StatusOK)
	f.request(t, http.MethodDelete, workspacePath(wid)+"/members/"+contributor.User.ID, owner.Token, nil, http.StatusNoContent)
	for _, route := range []string{base, path, path + "/versions", versionPath, versionPath + "/content"} {
		f.request(t, http.MethodGet, route, contributor.Token, nil, http.StatusNotFound)
	}
	documentUpload(t, f, base, contributor.Token, http.StatusNotFound, documentFile("revoked.txt", []byte("revoked")))
	documentUpload(t, f, path+"/versions", contributor.Token, http.StatusNotFound, documentFile("revoked.txt", []byte("revoked")), documentField("base_version_id", document.Version.ID))
	f.request(t, http.MethodDelete, path, contributor.Token, nil, http.StatusNotFound)
	current := integrationData[domain.Document](t, f.request(t, http.MethodGet, path, owner.Token, nil, http.StatusOK))
	if current.CurrentVersionID != document.Version.ID {
		t.Fatal("denied operation changed the document head")
	}
	if len(f.agent.calls()) != 0 {
		t.Fatal("document access reached the agent")
	}
}

func TestHTTPDocumentsStrictMultipartAndUploadLimits(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	owner := f.register(t, "doc-validation")
	base := documentsPath(owner.Workspaces[0].ID)
	valid := documentFile("valid.txt", []byte("valid"))
	encoded := valid
	encoded.headers = textproto.MIMEHeader{"Content-Transfer-Encoding": {"quoted-printable"}}
	encoded.content = []byte("private=0Acontent")
	for _, test := range []struct {
		name  string
		parts []documentPart
	}{
		{"missing file", []documentPart{documentField("title", "missing")}},
		{"file is text field", []documentPart{documentField("file", "not a file")}},
		{"two files", []documentPart{valid, valid}},
		{"unknown file field", []documentPart{{name: "attachment", filename: "x.txt", file: true, content: []byte("x")}}},
		{"unknown author", []documentPart{valid, documentField("created_by", "forged-author")}},
		{"unknown workspace", []documentPart{valid, documentField("workspace_id", "forged-workspace")}},
		{"unknown base on create", []documentPart{valid, documentField("base_version_id", "forged-version")}},
		{"duplicate title", []documentPart{valid, documentField("title", "one"), documentField("title", "two")}},
		{"empty title", []documentPart{valid, documentField("title", " \t ")}},
		{"long title", []documentPart{valid, documentField("title", strings.Repeat("x", 256))}},
		{"title byte bound", []documentPart{valid, documentField("title", strings.Repeat("中", 86))}},
		{"slash filename", []documentPart{documentFile("../escape.txt", []byte("x"))}},
		{"backslash filename", []documentPart{documentFile(`..\escape.txt`, []byte("x"))}},
		{"control filename", []documentPart{documentFile("bad\x01.txt", []byte("x"))}},
		{"empty filename", []documentPart{documentFile("", []byte("x"))}},
		{"transfer encoding", []documentPart{encoded}},
	} {
		t.Run(test.name, func(t *testing.T) {
			documentUpload(t, f, base, owner.Token, http.StatusBadRequest, test.parts...)
		})
	}
	f.request(t, http.MethodPost, base, owner.Token, map[string]string{"file": "not multipart"}, http.StatusUnsupportedMediaType)
	body, contentType := documentBody(t, valid)
	response, _ := documentHTTP(f, http.MethodPost, base, owner.Token, contentType, body[:len(body)-10])
	checkIntegrationResponse(t, response, http.StatusBadRequest)
	if err := f.store.SetUploadLimit(8 << 20); err != nil {
		t.Fatal(err)
	}
	documentUpload(t, f, base, owner.Token, http.StatusRequestEntityTooLarge, documentFile("too-big.bin", make([]byte, (8<<20)+1)))
	oversizedHeader := documentFile("overhead.bin", make([]byte, (8<<20)))
	oversizedHeader.headers = textproto.MIMEHeader{"X-Padding": {strings.Repeat("x", 70<<10)}}
	documentUpload(t, f, base, owner.Token, http.StatusRequestEntityTooLarge, oversizedHeader)
	body, contentType = documentBody(t, valid)
	body = append(body, bytes.Repeat([]byte("e"), (8<<20)+(64<<10))...)
	response, _ = documentHTTP(f, http.MethodPost, base, owner.Token, contentType, body)
	checkIntegrationResponse(t, response, http.StatusRequestEntityTooLarge)
	list := f.request(t, http.MethodGet, base, owner.Token, nil, http.StatusOK)
	if list.Total != 0 || len(integrationData[[]domain.Document](t, list)) != 0 {
		t.Fatalf("invalid uploads created documents: %+v", list)
	}

	empty := integrationData[domain.DocumentWrite](t, documentUpload(t, f, base, owner.Token, http.StatusCreated, documentFile("empty.txt", nil)))
	assertDocumentVersion(t, empty.Version, empty.Document.ID, owner.User.ID, "empty.txt", nil, 1)
	assertDocumentDownload(t, f, owner.Token, empty.Document, empty.Version, nil)
	fullBytes := bytes.Repeat([]byte{0, 255}, (8<<20)/2)
	full := integrationData[domain.DocumentWrite](t, documentUpload(t, f, base, owner.Token, http.StatusCreated, documentFile("exact-limit.bin", fullBytes), documentField("title", strings.Repeat("x", 255))))
	assertDocumentVersion(t, full.Version, full.Document.ID, owner.User.ID, "exact-limit.bin", fullBytes, 1)
	assertDocumentDownload(t, f, owner.Token, full.Document, full.Version, fullBytes)
	versionsPath := documentPath(empty.Document) + "/versions"
	for _, test := range []struct {
		name  string
		parts []documentPart
	}{
		{"missing base", []documentPart{valid}},
		{"empty base", []documentPart{valid, documentField("base_version_id", "")}},
		{"duplicate base", []documentPart{valid, documentField("base_version_id", empty.Version.ID), documentField("base_version_id", empty.Version.ID)}},
		{"unknown title on version", []documentPart{valid, documentField("base_version_id", empty.Version.ID), documentField("title", "rename")}},
		{"unknown version author", []documentPart{valid, documentField("base_version_id", empty.Version.ID), documentField("created_by", "forged")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			documentUpload(t, f, versionsPath, owner.Token, http.StatusBadRequest, test.parts...)
		})
	}
	documentUpload(t, f, versionsPath, owner.Token, http.StatusRequestEntityTooLarge, documentFile("oversized-version.bin", make([]byte, (8<<20)+1)), documentField("base_version_id", empty.Version.ID))
	versions := f.request(t, http.MethodGet, versionsPath, owner.Token, nil, http.StatusOK)
	current := integrationData[domain.Document](t, f.request(t, http.MethodGet, documentPath(empty.Document), owner.Token, nil, http.StatusOK))
	if versions.Total != 1 || len(integrationData[[]domain.DocumentVersion](t, versions)) != 1 || current.CurrentVersionID != empty.Version.ID {
		t.Fatalf("rejected version upload changed history or head: versions=%+v head=%s", versions, current.CurrentVersionID)
	}
	assertDocumentDownload(t, f, owner.Token, empty.Document, empty.Version, nil)
}

func TestHTTPDocumentsSoftDeleteHidesAllVersions(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	owner := f.register(t, "doc-delete")
	base := documentsPath(owner.Workspaces[0].ID)
	first := integrationData[domain.DocumentWrite](t, documentUpload(t, f, base, owner.Token, http.StatusCreated, documentFile("first.txt", []byte("first"))))
	second := integrationData[domain.DocumentWrite](t, documentUpload(t, f, documentPath(first.Document)+"/versions", owner.Token, http.StatusCreated, documentFile("second.txt", []byte("second")), documentField("base_version_id", first.Version.ID)))
	response, _ := documentHTTP(f, http.MethodDelete, documentVersionPath(first.Document, first.Version), owner.Token, "", nil)
	if response.err != nil || (response.status != http.StatusMethodNotAllowed && response.status != http.StatusNotFound) {
		t.Fatalf("immutable versions expose an independent deletion route: status=%d err=%v", response.status, response.err)
	}
	assertDocumentDownload(t, f, owner.Token, first.Document, first.Version, []byte("first"))
	f.request(t, http.MethodDelete, documentPath(first.Document), owner.Token, nil, http.StatusNoContent)
	for _, version := range []domain.DocumentVersion{first.Version, second.Version} {
		for _, route := range []string{documentVersionPath(first.Document, version), documentVersionPath(first.Document, version) + "/content"} {
			f.request(t, http.MethodGet, route, owner.Token, nil, http.StatusNotFound)
		}
	}
	for _, route := range []string{documentPath(first.Document), documentPath(first.Document) + "/versions"} {
		f.request(t, http.MethodGet, route, owner.Token, nil, http.StatusNotFound)
	}
	documentUpload(t, f, documentPath(first.Document)+"/versions", owner.Token, http.StatusNotFound, documentFile("resurrect.txt", []byte("resurrect")), documentField("base_version_id", second.Version.ID))
	f.request(t, http.MethodDelete, documentPath(first.Document), owner.Token, nil, http.StatusNotFound)
	list := f.request(t, http.MethodGet, base, owner.Token, nil, http.StatusOK)
	if list.Total != 0 || len(integrationData[[]domain.Document](t, list)) != 0 {
		t.Fatalf("deleted document remains visible in listing: %+v", list)
	}
	f.restart(t)
	f.request(t, http.MethodGet, documentPath(first.Document), owner.Token, nil, http.StatusNotFound)
	f.request(t, http.MethodGet, documentVersionPath(first.Document, first.Version)+"/content", owner.Token, nil, http.StatusNotFound)
}
