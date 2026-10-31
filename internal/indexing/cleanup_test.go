package indexing

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

type cleanerLedger struct {
	items         []domain.IndexCleanup
	completed     []string
	deferred      map[string]string
	preferred     []string
	requeued      bool
	retired       []domain.IndexCleanup
	completeError error
}

func (l *cleanerLedger) RequeueRetiredCleanup(_ context.Context, kind string, _ ...string) error {
	if kind != "index" {
		return errors.New("unexpected retirement kind")
	}
	l.requeued = true
	l.items = append(l.items, l.retired...)
	l.retired = nil
	return nil
}
func (l *cleanerLedger) ClaimIndexCleanup(_ context.Context, ids []string, limit int, lease time.Duration) ([]domain.IndexCleanup, error) {
	l.preferred = ids
	if !l.requeued || limit != 1 || lease <= 15*time.Second {
		return nil, errors.New("invalid claim order or lease")
	}
	if len(l.items) == 0 {
		return nil, nil
	}
	item := l.items[0]
	l.items = l.items[1:]
	return []domain.IndexCleanup{item}, nil
}
func (l *cleanerLedger) CompleteIndexCleanup(_ context.Context, id, token string) error {
	if token != strings.Repeat("c", 32) {
		return errors.New("claim token lost")
	}
	if l.completeError != nil {
		return l.completeError
	}
	l.completed = append(l.completed, id)
	return nil
}
func (l *cleanerLedger) DeferIndexCleanup(_ context.Context, id, token, code string, _ time.Duration) error {
	if token != strings.Repeat("c", 32) {
		return errors.New("claim token lost")
	}
	if l.deferred == nil {
		l.deferred = map[string]string{}
	}
	l.deferred[id] = code
	return nil
}
func cleanupItem(storageID, generation string) domain.IndexCleanup {
	return domain.IndexCleanup{ID: generation, StorageID: storageID, ProfileID: testProfile, Generation: generation, ClaimToken: strings.Repeat("c", 32)}
}

func TestCleanerRoutesOriginalBindingAndRetainsUnknown(t *testing.T) {
	var callsA, callsB []string
	server := func(calls *[]string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" || r.URL.Path != collectionPath(testProfile)+"/points/delete" {
				t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			}
			var body struct {
				Filter struct {
					Must []struct{ Match struct{ Value string } }
				}
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			*calls = append(*calls, body.Filter.Must[0].Match.Value)
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"status":"ok","result":{"status":"completed"}}`))
		}))
	}
	a, b := server(&callsA), server(&callsB)
	defer a.Close()
	defer b.Close()
	clientA, err := NewQdrant(a.URL, "", 256)
	if err != nil {
		t.Fatal(err)
	}
	clientB, err := NewQdrant(b.URL, "", 256)
	if err != nil {
		t.Fatal(err)
	}
	old := cleanupItem(clientA.StorageID(), testGeneration)
	unknown := cleanupItem("", strings.Repeat("d", 32))
	l := &cleanerLedger{items: []domain.IndexCleanup{old, unknown}}
	if err = NewCleaner(l, []SearchStore{clientB}).CleanupOnce(context.Background()); ErrorCode(err) != "storage_binding_unavailable" {
		t.Fatalf("unknown binding: %v", err)
	}
	if len(callsA)+len(callsB)+len(l.completed) != 0 || l.deferred[old.ID] != "storage_binding_unavailable" || l.deferred[unknown.ID] != "storage_binding_unavailable" {
		t.Fatal("unknown/old binding was sent to the active store or acknowledged")
	}
	current := cleanupItem(clientB.StorageID(), strings.Repeat("e", 32))
	l = &cleanerLedger{items: []domain.IndexCleanup{old, current}}
	if err = NewCleaner(l, []SearchStore{clientA, clientB}).CleanupOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(callsA, []string{old.Generation}) || !reflect.DeepEqual(callsB, []string{current.Generation}) || len(l.completed) != 2 || len(l.preferred) != 2 {
		t.Fatal("cleanup did not use its original configured storage")
	}
}

func TestCleanerPersistenceFailureRetainsSafeRetry(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNotFound) }))
	defer server.Close()
	client, err := NewQdrant(server.URL, "", 256)
	if err != nil {
		t.Fatal(err)
	}
	item := cleanupItem(client.StorageID(), testGeneration)
	l := &cleanerLedger{items: []domain.IndexCleanup{item}, completeError: errors.New("private database response")}
	if err = NewCleaner(l, []SearchStore{client}).CleanupOnce(context.Background()); err == nil {
		t.Fatal("persistence failure was ignored")
	}
	if len(l.completed) != 0 || l.deferred[item.ID] != "cleanup_failed" {
		t.Fatal("failed completion lost retry or leaked diagnostic text")
	}
}

func TestCleanerRetirementSweepDeletesLateRemoteWrite(t *testing.T) {
	// The writer's request has returned, but a later remote commit can still
	// recreate the generation after the first successful delete.
	var present atomic.Bool
	present.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		present.Store(false)
		w.Write([]byte(`{"status":"ok","result":{"status":"completed"}}`))
	}))
	defer server.Close()
	client, err := NewQdrant(server.URL, "", 256)
	if err != nil {
		t.Fatal(err)
	}
	item := cleanupItem(client.StorageID(), testGeneration)
	l := &cleanerLedger{items: []domain.IndexCleanup{item}}
	cleaner := NewCleaner(l, []SearchStore{client})
	if err = cleaner.CleanupOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if present.Load() || len(l.completed) != 1 {
		t.Fatal("initial deletion did not complete")
	}
	present.Store(true)
	l.retired = []domain.IndexCleanup{item}
	if err = cleaner.CleanupOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if present.Load() || len(l.completed) != 2 {
		t.Fatal("scheduled retirement sweep missed a late remote write")
	}
}

func TestStorageBindingMustMatchBeforeIndexingAndRetrieval(t *testing.T) {
	client, err := NewQdrant("http://storage-b.example.test", "", 256)
	if err != nil {
		t.Fatal(err)
	}
	profile := Profile("model", 2, "http://parser", "http://embed", "http://storage-a.example.test", 256)
	s := NewService(nil, nil, nil, client, nil, profile, Options{})
	err = s.process(context.Background(), &domain.IndexInput{Job: domain.IndexJob{ProfileID: profile.ID, StorageID: profile.StorageID}})
	requireClientError(t, err, "storage_binding_unavailable")
	_, err = s.Search(context.Background(), "user", "workspace", "query", "lexical", 10)
	requireClientError(t, err, "storage_binding_unavailable")
}

func TestQdrantStorageBindingIsStableAcrossCredentialsAndProfiles(t *testing.T) {
	a, err := NewQdrant("http://qdrant.example.test/base/", "first", 256)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewQdrant("http://qdrant.example.test/base", "rotated", 512)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewQdrant("http://other.example.test/base", "first", 256)
	if err != nil {
		t.Fatal(err)
	}
	profile := Profile("model", 2, "http://parser", "http://embed", "http://qdrant.example.test/base/", 256)
	if a.StorageID() != b.StorageID() || a.StorageID() == c.StorageID() || profile.StorageID != a.StorageID() {
		t.Fatal("storage identity depends on credential/model policy or loses endpoint identity")
	}
}

func TestQdrantStorageBindingMatchesEscapedEndpointRequest(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/proxy/api/collections/mw_") || !strings.HasSuffix(r.URL.Path, "/points/delete") {
			t.Errorf("unexpected normalized request path %s", r.URL.Path)
		}
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok","result":{"status":"completed"}}`))
	}))
	defer server.Close()
	want := domain.StorageBindingID("qdrant", server.URL+"/proxy/api", "mw_")
	for _, suffix := range []string{"/proxy/api", "/proxy%2Fapi/", "/%70roxy/api"} {
		client, err := NewQdrant(server.URL+suffix, "", 256)
		if err != nil {
			t.Fatal(err)
		}
		profile := Profile("model", 2, "http://parser", "http://embed", server.URL+suffix, 256)
		if client.StorageID() != want || profile.StorageID != want {
			t.Fatal("profile and actual client disagree about the storage destination")
		}
		if err := client.DeleteGeneration(context.Background(), profile.ID, testGeneration); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 3 {
		t.Fatal("endpoint variants did not all reach the same provider route")
	}
}
