package httpapi_test

import (
	"bytes"
	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
	"net/http"
	"testing"
)

func TestHTTPDocumentUploadDeploymentLimitAndHistoricalReads(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	objects := &indexTestObjects{data: map[string][]byte{}}
	f.store.SetObjectStorage(objects)
	owner := f.register(t, "large-upload")
	base := documentsPath(owner.Workspaces[0].ID)
	limits := integrationData[map[string]int](t, f.request(t, http.MethodGet, "/api/v1/config", "", nil, 200))
	if limits["max_file_size_bytes"] != 50<<20 {
		t.Fatal(limits)
	}
	payload := bytes.Repeat([]byte("a"), 50<<20)
	first := integrationData[domain.DocumentWrite](t, documentUpload(t, f, base, owner.Token, 201, documentFile("large.bin", payload)))
	assertDocumentVersion(t, first.Version, first.Document.ID, owner.User.ID, "large.bin", payload, 1)
	assertDocumentDownload(t, f, owner.Token, first.Document, first.Version, payload)
	documentUpload(t, f, base, owner.Token, 413, documentFile("oversize.bin", append(payload, 1)))
	// File replacement follows the same policy and preserves the original.
	second := integrationData[domain.DocumentWrite](t, documentUpload(t, f, documentPath(first.Document)+"/versions", owner.Token, 201,
		documentFile("replacement.bin", payload[:9<<20]), documentField("base_version_id", first.Version.ID)))
	if err := f.store.SetUploadLimit(1 << 20); err != nil {
		t.Fatal(err)
	}
	documentUpload(t, f, base, owner.Token, 413, documentFile("now-too-large.bin", payload[:(1<<20)+1]))
	documentUpload(t, f, documentPath(first.Document)+"/versions", owner.Token, 413,
		documentFile("now-too-large.bin", payload[:(1<<20)+1]), documentField("base_version_id", second.Version.ID))
	assertDocumentDownload(t, f, owner.Token, first.Document, first.Version, payload)
	assertDocumentDownload(t, f, owner.Token, second.Document, second.Version, payload[:9<<20])
	objects.mu.Lock()
	count := len(objects.data)
	objects.mu.Unlock()
	if count != 2 {
		t.Fatalf("rejected uploads left objects: %d", count)
	}
	if err := f.store.SetUploadLimit(51 << 20); err != nil {
		t.Fatal(err)
	}
	limits = integrationData[map[string]int](t, f.request(t, http.MethodGet, "/api/v1/config", "", nil, 200))
	if limits["max_file_size_mb"] != 51 {
		t.Fatal(limits)
	}
	documentUpload(t, f, base, owner.Token, 201, documentFile("custom.bin", append(payload, 1)))
}
