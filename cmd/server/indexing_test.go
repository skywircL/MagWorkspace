package main

import "testing"

func TestConfigureQdrantDefaultAndProfile(t *testing.T) {
	t.Setenv("DOCUMENT_INDEXING_ENABLED", "true")
	t.Setenv("EMBEDDING_MODEL", "bge-m3:567m")
	t.Setenv("EMBEDDING_DIMENSIONS", "1024")
	t.Setenv("QDRANT_URL", "http://qdrant:6333")
	t.Setenv("BM25_AVG_LEN", "256")
	a, err := configureIndexing()
	if err != nil {
		t.Fatal(err)
	}
	native, ok := a.search.(interface{ NativeFusion() bool })
	if !ok || !native.NativeFusion() {
		t.Fatal("default is not native Qdrant")
	}
	t.Setenv("BM25_AVG_LEN", "128")
	b, err := configureIndexing()
	if err != nil || a.profile.ID == b.profile.ID {
		t.Fatal("statistics configuration does not change profile", err)
	}
	for _, v := range []string{"0", "NaN", "+Inf", "65537", "invalid"} {
		t.Setenv("BM25_AVG_LEN", v)
		if _, err := configureIndexing(); err == nil {
			t.Fatalf("accepted avg_len %s", v)
		}
	}
}

func TestConfigureEmbeddingTimeoutBoundsAndStableProfile(t *testing.T) {
	t.Setenv("DOCUMENT_INDEXING_ENABLED", "true")
	t.Setenv("BM25_AVG_LEN", "256")
	t.Setenv("EMBEDDING_REQUEST_TIMEOUT", "120s")
	before, err := configureIndexing()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("EMBEDDING_REQUEST_TIMEOUT", "5m")
	after, err := configureIndexing()
	if err != nil || before.profile.ID != after.profile.ID {
		t.Fatal("operational timeout changed index profile", err)
	}
	for _, value := range []string{"bad", "0", "-1s", "999ms", "11m"} {
		t.Setenv("EMBEDDING_REQUEST_TIMEOUT", value)
		if _, err := configureIndexing(); err == nil {
			t.Fatalf("accepted timeout %s", value)
		}
	}
}
