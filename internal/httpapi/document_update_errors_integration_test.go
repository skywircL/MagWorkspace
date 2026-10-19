package httpapi_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
)

func assertDocumentUpdateState(t *testing.T, x *indexFixture, write domain.DocumentWrite, count int) {
	t.Helper()
	db, err := sql.Open("mysql", x.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var head, title string
	var versions, jobs int
	err = db.QueryRowContext(ctx, `SELECT d.current_version_id,d.title,
		(SELECT COUNT(*) FROM document_versions v WHERE v.document_id=d.id),
		(SELECT COUNT(*) FROM document_indexes i WHERE i.document_id=d.id)
		FROM documents d WHERE d.id=? AND d.workspace_id=?`, write.Document.ID, write.Document.WorkspaceID).Scan(&head, &title, &versions, &jobs)
	if err != nil {
		t.Fatal(err)
	}
	if head != write.Version.ID || title != write.Document.Title || versions != count || jobs != count {
		t.Fatalf("document publication changed unexpectedly: head=%s title=%q versions=%d jobs=%d; want head=%s title=%q count=%d", head, title, versions, jobs, write.Version.ID, write.Document.Title, count)
	}
}

func documentUpdateResult(t *testing.T, done <-chan integrationResponse) integrationResponse {
	t.Helper()
	select {
	case response := <-done:
		return response
	case <-time.After(15 * time.Second):
		t.Fatal("document update did not finish after releasing object I/O")
		return integrationResponse{}
	}
}

func TestHTTPDocumentsTextUpdateStrictJSONAndBytes(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "text-update-json")
	first := x.upload(t, owner, "unchanged original")
	path := documentPath(first.Document) + "/content"
	base := fmt.Sprintf(`"base_version_id":%q`, first.Version.ID)
	valid := []byte("{" + base + `,"content":"valid"}`)
	cases := []struct {
		name string
		body []byte
		want int
	}{
		{"missing base", []byte(`{"content":"x"}`), 400},
		{"empty base", []byte(`{"base_version_id":"","content":"x"}`), 400},
		{"null base", []byte(`{"base_version_id":null,"content":"x"}`), 400},
		{"numeric base", []byte(`{"base_version_id":1,"content":"x"}`), 400},
		{"missing content", []byte("{" + base + "}"), 400},
		{"null content", []byte("{" + base + `,"content":null}`), 400},
		{"numeric content", []byte("{" + base + `,"content":42}`), 400},
		{"boolean content", []byte("{" + base + `,"content":false}`), 400},
		{"object content", []byte("{" + base + `,"content":{}}`), 400},
		{"array content", []byte("{" + base + `,"content":[]}`), 400},
		{"duplicate content", []byte("{" + base + `,"content":"first","content":"last"}`), 400},
		{"duplicate escaped content key", []byte("{" + base + `,"content":"first","\u0063ontent":"last"}`), 400},
		{"duplicate base", []byte("{" + base + "," + base + `,"content":"x"}`), 400},
		{"unknown identity", []byte("{" + base + `,"content":"x","created_by":"forged"}`), 400},
		{"trailing object", append(append([]byte(nil), valid...), []byte(` {}`)...), 400},
		{"trailing scalar", append(append([]byte(nil), valid...), []byte(` true`)...), 400},
		{"root array", []byte(`[]`), 400},
		{"root null", []byte(`null`), 400},
		{"malformed JSON", []byte("{" + base + `,"content":"x",}`), 400},
		{"high surrogate", []byte("{" + base + `,"content":"\ud800"}`), 400},
		{"low surrogate", []byte("{" + base + `,"content":"\udfff"}`), 400},
		{"high then high", []byte("{" + base + `,"content":"\ud800\udbff"}`), 400},
		{"high then non surrogate", []byte("{" + base + `,"content":"\ud800\u0041"}`), 400},
		{"reversed pair", []byte("{" + base + `,"content":"\udc00\ud800"}`), 400},
		{"surrogate in base", []byte(`{"base_version_id":"\ud800","content":"x"}`), 400},
		{"NUL", []byte("{" + base + `,"content":"\u0000"}`), 400},
		{"vertical tab", []byte("{" + base + `,"content":"\u000b"}`), 400},
		{"DEL", []byte("{" + base + `,"content":"\u007f"}`), 400},
		{"C1 control", []byte("{" + base + `,"content":"\u0085"}`), 400},
		{"decoded limit", []byte("{" + base + `,"content":"` + strings.Repeat("x", domain.MaxEditableDocumentBytes+1) + `"}`), 413},
		{"raw limit", append(append([]byte(nil), valid...), []byte(strings.Repeat(" ", 6*domain.MaxEditableDocumentBytes+(64<<10)+1-len(valid)))...), 413},
	}
	invalidUTF8 := append([]byte("{"+base+`,"content":"`), 0xff)
	cases = append(cases, struct {
		name string
		body []byte
		want int
	}{"invalid UTF-8", append(invalidUTF8, []byte(`"}`)...), 400})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response, _ := documentHTTP(x.integrationFixture, http.MethodPut, path, owner.Token, "application/json", tc.body)
			checkIntegrationResponse(t, response, tc.want)
		})
	}
	for _, media := range []string{"", "text/plain", "multipart/form-data"} {
		response, _ := documentHTTP(x.integrationFixture, http.MethodPut, path, owner.Token, media, valid)
		checkIntegrationResponse(t, response, 415)
	}
	assertDocumentUpdateState(t, x, first, 1)
	assertDocumentDownload(t, x.integrationFixture, owner.Token, first.Document, first.Version, []byte("unchanged original"))

	current := first
	maximum := strings.Repeat("中", domain.MaxEditableDocumentBytes/3) + strings.Repeat("x", domain.MaxEditableDocumentBytes%3)
	maximumJSON, err := json.Marshal(maximum)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, literal, text string }{
		{"emoji pair", `"\ud83d\ude00"`, "😀"},
		{"minimum pair", `"\ud800\udc00"`, "\U00010000"},
		{"maximum pair", `"\udbff\udfff"`, "\U0010ffff"},
		{"literal escaped surrogate", `"\\ud800"`, `\ud800`},
		{"intentional replacement characters", `"�\ufffd"`, "��"},
		{"allowed whitespace", `"line\r\n\t\fend"`, "line\r\n\t\fend"},
		{"empty content", `""`, ""},
		{"exact UTF-8 byte limit", string(maximumJSON), maximum},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(fmt.Sprintf(`{"base_version_id":%q,"content":%s}`, current.Version.ID, tc.literal))
			response, _ := documentHTTP(x.integrationFixture, http.MethodPut, path, owner.Token, "application/json", body)
			updated := integrationData[domain.DocumentWrite](t, checkIntegrationResponse(t, response, 200))
			assertDocumentVersion(t, updated.Version, first.Document.ID, owner.User.ID, first.Version.Filename, []byte(tc.text), current.Version.Number+1)
			assertDocumentDownload(t, x.integrationFixture, owner.Token, updated.Document, updated.Version, []byte(tc.text))
			current = updated
		})
	}
	assertDocumentUpdateState(t, x, current, current.Version.Number)
	assertDocumentDownload(t, x.integrationFixture, owner.Token, first.Document, first.Version, []byte("unchanged original"))
}

func TestHTTPDocumentsUpdateAuthorizationAndRejectedBases(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "update-permissions-owner")
	writer := x.register(t, "update-permissions-writer")
	viewer := x.register(t, "update-permissions-viewer")
	outsider := x.register(t, "update-permissions-outsider")
	wid := owner.Workspaces[0].ID
	for _, member := range []struct {
		auth integrationAuth
		role string
	}{{writer, "contributor"}, {viewer, "viewer"}} {
		x.request(t, "POST", workspacePath(wid)+"/members", owner.Token, map[string]string{"email": member.auth.User.Email, "role": member.role}, 201)
	}
	first := x.upload(t, owner, "stable")
	path := documentPath(first.Document)
	for _, tc := range []struct {
		token string
		want  int
	}{{"", 401}, {viewer.Token, 403}, {outsider.Token, 404}} {
		x.request(t, "PATCH", path, tc.token, map[string]string{"title": "denied"}, tc.want)
		x.request(t, "PUT", path+"/content", tc.token, map[string]string{"base_version_id": first.Version.ID, "content": "denied"}, tc.want)
	}
	otherWorkspace := integrationData[domain.Workspace](t, x.request(t, "POST", "/api/v1/workspaces", owner.Token, map[string]string{"name": "another space"}, 201))
	crossPath := documentsPath(otherWorkspace.ID) + "/" + first.Document.ID
	x.request(t, "PATCH", crossPath, owner.Token, map[string]string{"title": "cross-space"}, 404)
	x.request(t, "PUT", crossPath+"/content", owner.Token, map[string]string{"base_version_id": first.Version.ID, "content": "cross-space"}, 404)
	other := x.upload(t, owner, "another document")
	for _, base := range []string{other.Version.ID, strings.Repeat("f", 32)} {
		x.request(t, "PUT", path+"/content", owner.Token, map[string]string{"base_version_id": base, "content": "wrong base"}, 404)
	}
	for _, raw := range []string{
		`{}`, `{"title":null}`, `{"title":1}`, `{"title":[]}`, `{"title":""}`, `{"title":"   "}`,
		`{"title":"x","created_by":"forged"}`, `{"title":"x"} {}`, `{"title":"\u0000"}`,
		`{"title":"\ud800"}`, `{"title":"\udfff"}`, `{"title":"first","title":"last"}`,
		`{"title":"first","\u0074itle":"last"}`, "{\"title\":\"" + string([]byte{0xff}) + "\"}",
	} {
		response, _ := documentHTTP(x.integrationFixture, "PATCH", path, owner.Token, "application/json", []byte(raw))
		checkIntegrationResponse(t, response, 400)
	}
	x.request(t, "PATCH", path, owner.Token, map[string]string{"title": strings.Repeat("中", 86)}, 400)
	assertDocumentUpdateState(t, x, first, 1)
	response, _ := documentHTTP(x.integrationFixture, "PATCH", path, owner.Token, "application/json", []byte(`{"title":"�\ufffd"}`))
	first.Document = integrationData[domain.Document](t, checkIntegrationResponse(t, response, 200))
	if first.Document.Title != "��" {
		t.Fatalf("intentional replacement characters were changed: title=%q", first.Document.Title)
	}
	assertDocumentUpdateState(t, x, first, 1)
	for _, filename := range []string{"source.pdf", "source.docx", "source.bin", "source"} {
		write := integrationData[domain.DocumentWrite](t, documentUpload(t, x.integrationFixture, documentsPath(wid), owner.Token, 201, documentFile(filename, []byte("uploaded bytes"))))
		x.request(t, "PUT", documentPath(write.Document)+"/content", owner.Token, map[string]string{"base_version_id": write.Version.ID, "content": "cannot replace binary format"}, 400)
		assertDocumentUpdateState(t, x, write, 1)
	}
	// Each allowed role reaches the real routes; the author is always the token's
	// user and the original filename is retained across all accepted extensions.
	for i, role := range []string{"contributor", "admin", "owner"} {
		auth := writer
		if role == "admin" {
			x.request(t, "PATCH", workspacePath(wid)+"/members/"+writer.User.ID, owner.Token, map[string]string{"role": role}, 200)
		} else if role == "owner" {
			auth = owner
		}
		filename := []string{"editable.txt", "editable.md", "editable.markdown"}[i]
		write := integrationData[domain.DocumentWrite](t, documentUpload(t, x.integrationFixture, documentsPath(wid), owner.Token, 201, documentFile(filename, []byte("before"))))
		document := integrationData[domain.Document](t, x.request(t, "PATCH", documentPath(write.Document), auth.Token, map[string]string{"title": " renamed "}, 200))
		if document.Title != "renamed" || document.CurrentVersionID != write.Version.ID {
			t.Fatalf("title update changed the immutable head: %+v", document)
		}
		updated := integrationData[domain.DocumentWrite](t, x.request(t, "PUT", documentPath(write.Document)+"/content", auth.Token, map[string]string{"base_version_id": write.Version.ID, "content": role}, 200))
		assertDocumentVersion(t, updated.Version, write.Document.ID, auth.User.ID, filename, []byte(role), 2)
		x.request(t, "PUT", documentPath(write.Document)+"/content", auth.Token, map[string]string{"base_version_id": write.Version.ID, "content": "stale"}, 409)
		assertDocumentUpdateState(t, x, updated, 2)
	}
	x.request(t, "DELETE", workspacePath(wid)+"/members/"+writer.User.ID, owner.Token, nil, 204)
	x.request(t, "PATCH", path, writer.Token, map[string]string{"title": "revoked"}, 404)
	x.request(t, "PUT", path+"/content", writer.Token, map[string]string{"base_version_id": first.Version.ID, "content": "revoked"}, 404)
	x.request(t, "DELETE", path, owner.Token, nil, 204)
	x.request(t, "PATCH", path, owner.Token, map[string]string{"title": "deleted"}, 404)
	x.request(t, "PUT", path+"/content", owner.Token, map[string]string{"base_version_id": first.Version.ID, "content": "deleted"}, 404)
	assertDocumentUpdateState(t, x, first, 1)
}

func TestHTTPDocumentsTextUpdateObjectFailureDoesNotPublish(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "text-update-object-failure")
	first := x.upload(t, owner, "retained original")
	attempted := make(chan string, 1)
	x.objects.putHook(func(_ context.Context, key string) error {
		attempted <- key
		return blob.ErrUnavailable
	})
	response := x.request(t, "PUT", documentPath(first.Document)+"/content", owner.Token, map[string]string{"base_version_id": first.Version.ID, "content": "unpublished"}, 503)
	if response.Error.Code != "object_storage_unavailable" {
		t.Fatalf("object failure escaped its safe error mapping: %+v", response.Error)
	}
	select {
	case <-attempted:
	default:
		t.Fatal("PUT never reached the failing object store")
	}
	assertDocumentUpdateState(t, x, first, 1)
	assertDocumentDownload(t, x.integrationFixture, owner.Token, first.Document, first.Version, []byte("retained original"))
	x.objects.mu.Lock()
	count := len(x.objects.data)
	x.objects.mu.Unlock()
	if count != 1 {
		t.Fatalf("failed PUT created a readable object: count=%d", count)
	}
}

func TestHTTPDocumentsTextUpdateRevokedDuringObjectWrite(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "text-update-revoke-owner")
	writer := x.register(t, "text-update-revoke-writer")
	wid := owner.Workspaces[0].ID
	x.request(t, "POST", workspacePath(wid)+"/members", owner.Token, map[string]string{"email": writer.User.Email, "role": "contributor"}, 201)
	first := x.upload(t, owner, "original remains authorized")
	entered, release := make(chan struct{}, 1), make(chan struct{})
	keys := make(chan string, 1)
	var once sync.Once
	defer once.Do(func() { close(release) })
	x.objects.putHook(func(ctx context.Context, key string) error {
		keys <- key
		return indexGate(ctx, entered, release)
	})
	done := make(chan integrationResponse, 1)
	go func() {
		done <- x.call("PUT", documentPath(first.Document)+"/content", writer.Token, map[string]string{"base_version_id": first.Version.ID, "content": "revoked while writing"})
	}()
	indexAwait(t, entered)
	x.request(t, "DELETE", workspacePath(wid)+"/members/"+writer.User.ID, owner.Token, nil, 204)
	once.Do(func() { close(release) })
	checkIntegrationResponse(t, documentUpdateResult(t, done), 404)
	key := <-keys
	assertDocumentUpdateState(t, x, first, 1)
	x.objects.mu.Lock()
	_, staged := x.objects.data[key]
	x.objects.mu.Unlock()
	if !staged {
		t.Fatal("fixture did not complete the remote Put before the final permission check")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := x.store.CleanupObjects(ctx); err != nil {
		t.Fatal(err)
	}
	x.objects.mu.Lock()
	_, retained := x.objects.data[key]
	count := len(x.objects.data)
	deleted := append([]string(nil), x.objects.deleted...)
	x.objects.mu.Unlock()
	if retained || count != 1 || len(deleted) != 1 || deleted[0] != key {
		t.Fatalf("revoked write was not compensated without harming original: retained=%v count=%d deleted=%v", retained, count, deleted)
	}
	assertDocumentDownload(t, x.integrationFixture, owner.Token, first.Document, first.Version, []byte("original remains authorized"))
}

func TestHTTPDocumentsTextUpdateConcurrentCAS(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "text-update-concurrent")
	first := x.upload(t, owner, "immutable before race")
	entered, release := make(chan struct{}, 2), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	x.objects.putHook(func(ctx context.Context, _ string) error { return indexGate(ctx, entered, release) })
	done := make(chan integrationResponse, 2)
	for _, content := range []string{"concurrent alpha", "concurrent beta"} {
		go func(text string) {
			done <- x.call("PUT", documentPath(first.Document)+"/content", owner.Token, map[string]string{"base_version_id": first.Version.ID, "content": text})
		}(content)
	}
	// Both requests passed preflight and are performing external I/O before
	// either may commit, forcing the publication transaction to repeat CAS.
	indexAwait(t, entered)
	indexAwait(t, entered)
	once.Do(func() { close(release) })
	var winner domain.DocumentWrite
	successes, conflicts := 0, 0
	for range 2 {
		response := documentUpdateResult(t, done)
		envelope := checkIntegrationResponse(t, response, 200, 409)
		if response.status == 200 {
			successes++
			winner = integrationData[domain.DocumentWrite](t, envelope)
		} else {
			conflicts++
		}
	}
	if successes != 1 || conflicts != 1 || winner.Version.Number != 2 {
		t.Fatalf("CAS results successes=%d conflicts=%d winner=%+v", successes, conflicts, winner)
	}
	assertDocumentUpdateState(t, x, winner, 2)
	assertDocumentDownload(t, x.integrationFixture, owner.Token, first.Document, first.Version, []byte("immutable before race"))
	response, _ := documentHTTP(x.integrationFixture, "GET", documentVersionPath(winner.Document, winner.Version)+"/content", owner.Token, "", nil)
	if response.err != nil || response.status != 200 || (string(response.body) != "concurrent alpha" && string(response.body) != "concurrent beta") {
		t.Fatalf("winning version did not preserve one complete request: status=%d err=%v", response.status, response.err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := x.store.CleanupObjects(ctx); err != nil {
		t.Fatal(err)
	}
	x.objects.mu.Lock()
	objects, deleted := len(x.objects.data), len(x.objects.deleted)
	x.objects.mu.Unlock()
	if objects != 2 || deleted != 1 {
		t.Fatalf("CAS loser object leaked or winner was removed: objects=%d deleted=%d", objects, deleted)
	}
}
