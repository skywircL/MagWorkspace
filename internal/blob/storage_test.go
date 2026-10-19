package blob

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestMinIOConfiguration(t *testing.T) {
	for _, endpoint := range []string{"file:///tmp/a", "http://user:pass@localhost:9000", "http://localhost:9000/path", "http://localhost:9000?secret=x"} {
		if _, err := NewMinIO(endpoint, "documents", "test", "test-secret"); err == nil {
			t.Fatalf("accepted endpoint %s", endpoint)
		}
	}
	for _, key := range []string{"", "/absolute", "a/../b", "a\\b", "a\x00b"} {
		if validKey(key) {
			t.Fatalf("accepted key %q", key)
		}
	}
}

// This contract runs against the Docker MinIO service, never user documents.
func TestMinIOLive(t *testing.T) {
	endpoint := os.Getenv("MAGWORKSPACE_TEST_MINIO_URL")
	if endpoint == "" {
		t.Skip("MAGWORKSPACE_TEST_MINIO_URL is not configured")
	}
	s, err := NewMinIO(endpoint, "mw-test-"+fmt.Sprint(time.Now().UnixNano()), os.Getenv("MAGWORKSPACE_TEST_MINIO_ACCESS_KEY"), os.Getenv("MAGWORKSPACE_TEST_MINIO_SECRET_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err = s.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.client.RemoveBucket(context.Background(), s.bucket); err != nil {
			t.Error(err)
		}
	}()
	for i, data := range [][]byte{nil, []byte("不可变对象\x00\xff")} {
		key := fmt.Sprintf("test/%d", i)
		if err = s.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		got, err := s.Read(ctx, key, 1024)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("read mismatch: %v", err)
		}
		if err = s.Put(ctx, key, []byte("overwrite")); err == nil {
			t.Fatal("immutable object was overwritten")
		}
		got, err = s.Read(ctx, key, 1024)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("content changed: %v", err)
		}
		if len(data) > 0 {
			if _, err = s.Read(ctx, key, 1); err == nil {
				t.Fatal("bounded read accepted oversize")
			}
		}
		if err = s.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}
		if err = s.Delete(ctx, key); err != nil {
			t.Fatalf("delete not idempotent: %v", err)
		}
		if _, err = s.Read(ctx, key, 1024); err == nil {
			t.Fatal("deleted object readable")
		}
	}
}
