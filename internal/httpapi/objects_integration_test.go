package httpapi_test

import (
	"context"
	"net/http"
	"testing"

	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
)

func TestHTTPObjectsFailureDoesNotPublishAndCorruptionDoesNotDownload(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "object-errors")
	path := documentsPath(owner.Workspaces[0].ID)
	x.objects.putHook(func(context.Context, string) error { return blob.ErrUnavailable })
	response := documentUpload(t, x.integrationFixture, path, owner.Token, 503, documentFile("note.txt", []byte("original")))
	if response.Error.Code != "object_storage_unavailable" {
		t.Fatalf("unsafe error mapping: %+v", response.Error)
	}
	listed := integrationData[[]domain.Document](t, x.request(t, "GET", path, owner.Token, nil, 200))
	if len(listed) != 0 {
		t.Fatal("failed object upload published a document")
	}
	x.objects.putHook(nil)
	write := x.upload(t, owner, "original")
	x.objects.mu.Lock()
	for key := range x.objects.data {
		x.objects.data[key] = []byte("corrupted")
	}
	x.objects.mu.Unlock()
	responseHTTP, _ := documentHTTP(x.integrationFixture, http.MethodGet, documentVersionPath(write.Document, write.Version)+"/content", owner.Token, "", nil)
	envelope := checkIntegrationResponse(t, responseHTTP, 503)
	if envelope.Error.Code != "object_integrity_failed" {
		t.Fatalf("corrupt document error: %+v", envelope.Error)
	}
}
