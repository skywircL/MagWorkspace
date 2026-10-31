package main

import (
	"strings"
	"testing"

	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
)

func TestConfigureCleanupIndependentOfIndexing(t *testing.T) {
	t.Setenv("DOCUMENT_INDEXING_ENABLED", "false")
	t.Setenv("TIKA_URL", ":invalid")
	t.Setenv("EMBEDDING_DIMENSIONS", "invalid")
	t.Setenv("QDRANT_URL", "http://vectors-current:6333")
	t.Setenv("CLEANUP_QDRANT_STORES", `[{"url":"http://vectors-old:6333","api_key_env":"TEST_OLD_VECTOR_KEY"}]`)
	t.Setenv("TEST_OLD_VECTOR_KEY", "test-value")
	t.Setenv("CLEANUP_MINIO_STORES", `[{"endpoint":"http://objects-old:9000","bucket":"old-bucket","access_key_env":"TEST_OLD_OBJECT_ACCESS","secret_key_env":"TEST_OLD_OBJECT_SECRET"}]`)
	t.Setenv("TEST_OLD_OBJECT_ACCESS", "test-access")
	t.Setenv("TEST_OLD_OBJECT_SECRET", "test-secret")
	setup, err := configureIndexing()
	if err != nil || setup != nil {
		t.Fatal("disabled indexing tried to initialize model/parser", err)
	}
	current, err := blob.NewMinIO("http://objects-current:9000", "current-bucket", "test-access", "test-secret")
	if err != nil {
		t.Fatal(err)
	}
	objects, vectors, err := configureCleanupTargets(current, nil)
	if err != nil || len(objects) != 2 || len(vectors) != 2 {
		t.Fatal("cleanup destinations not configured independently", err)
	}
	if vectors[1].StorageID() != domain.StorageBindingID("qdrant", "http://vectors-old:6333", "mw_") || objects[1].StorageID() != domain.StorageBindingID("minio", "http://objects-old:9000", "old-bucket") {
		t.Fatal("old destinations lost their identities")
	}
	t.Setenv("TEST_OLD_VECTOR_KEY", "rotated-value")
	_, rotated, err := configureCleanupTargets(current, nil)
	if err != nil || rotated[1].StorageID() != vectors[1].StorageID() {
		t.Fatal("credential rotation changed destination identity", err)
	}
}

func TestConfigureCleanupRejectsAmbiguousTargets(t *testing.T) {
	current, err := blob.NewMinIO("http://objects:9000", "documents", "test-access", "test-secret")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("QDRANT_URL", "http://vectors:6333")
	t.Setenv("CLEANUP_MINIO_STORES", "[]")
	for _, raw := range []string{
		`null`, `{}`, `[] []`, `[{"url":"http://vectors:6333/"}]`,
		`[{"url":"http://other:6333","api_key":"secret"}]`,
		`[{"url":"http://other:6333","api_key_env":"MISSING_CLEANUP_CREDENTIAL"}]`,
		`[{"url":"http://user:secret@other:6333"}]`,
		`[` + strings.Repeat(`{"url":"http://other:6333"},`, 17) + `{"url":"http://last:6333"}]`,
	} {
		t.Setenv("CLEANUP_QDRANT_STORES", raw)
		if _, _, err := configureCleanupTargets(current, nil); err == nil {
			t.Fatalf("accepted ambiguous/invalid cleanup targets %q", raw)
		}
	}
	t.Setenv("CLEANUP_QDRANT_STORES", "[]")
	t.Setenv("TEST_OLD_OBJECT_ACCESS", "test-access")
	t.Setenv("TEST_OLD_OBJECT_SECRET", "test-secret")
	t.Setenv("CLEANUP_MINIO_STORES", `[{"endpoint":"http://objects:9000/","bucket":"documents","access_key_env":"TEST_OLD_OBJECT_ACCESS","secret_key_env":"TEST_OLD_OBJECT_SECRET"}]`)
	if _, _, err := configureCleanupTargets(current, nil); err == nil {
		t.Fatal("accepted duplicate object destination")
	}
}
