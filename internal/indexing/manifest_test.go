package indexing

import (
	"context"
	"strings"
	"testing"

	"magworkspace/internal/domain"
)

type manifestMemory struct{ data []byte }

func (m *manifestMemory) Put(_ context.Context, _ string, b []byte) error {
	m.data = append([]byte(nil), b...)
	return nil
}
func (m *manifestMemory) Read(context.Context, string, int) ([]byte, error) { return m.data, nil }
func (m *manifestMemory) Delete(context.Context, string) error              { return nil }

func TestManifestCanonicalTextAndIntegrity(t *testing.T) {
	job := domain.IndexJob{ID: strings.Repeat("1", 32), ProfileID: strings.Repeat("a", 64), Generation: strings.Repeat("2", 32), DocumentID: strings.Repeat("3", 32), VersionID: strings.Repeat("4", 32), SourceSHA256: strings.Repeat("5", 64), Status: "ready"}
	text := strings.Repeat("中文知识库不可变文档。", 150)
	chunks, err := Split(text)
	if err != nil {
		t.Fatal(err)
	}
	for i := range chunks {
		chunks[i].ID = strings.Repeat(string(rune('a'+i)), 32)
	}
	data, err := encodeManifest(job, text, chunks)
	if err != nil {
		t.Fatal(err)
	}
	job.ArtifactKey = artifactKey(job.ProfileID, job.Generation)
	job.ArtifactSHA256 = digestBytes(data)
	job.ChunkCount = len(chunks)
	memory := &manifestMemory{data: data}
	s := &Service{objects: memory}
	got, err := s.loadManifest(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range got {
		if c.Content != chunks[i].Content || c.ID != chunks[i].ID || c.VersionID != job.VersionID || c.IndexID != job.ID {
			t.Fatalf("wrong canonical chunk %+v", c)
		}
	}
	memory.data = append([]byte(nil), data...)
	memory.data[len(data)/2] ^= 1
	if _, err = s.loadManifest(context.Background(), job); err == nil {
		t.Fatal("corruption was accepted")
	}
	memory.data = data
	job.VersionID = strings.Repeat("9", 32)
	if _, err = s.loadManifest(context.Background(), job); err == nil {
		t.Fatal("wrong version binding was accepted")
	}
}
