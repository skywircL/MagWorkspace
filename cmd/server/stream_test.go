package main

import (
	"magworkspace/internal/redisstate"
	"magworkspace/internal/stream"
	"testing"
)

func TestConfigureStreams(t *testing.T) {
	for _, kind := range []string{"", "memory", "redis", "typo"} {
		t.Run(kind, func(t *testing.T) {
			t.Setenv("STREAM_MANAGER_TYPE", kind)
			m, err := configureStreams(nil)
			if kind == "redis" || kind == "typo" {
				if err == nil {
					t.Fatal("accepted missing or invalid config")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := m.(*stream.Memory); !ok {
				t.Fatalf("default %T", m)
			}
		})
	}
	t.Setenv("STREAM_MANAGER_TYPE", "redis")
	c := redisstate.New("127.0.0.1:1", "", "test:")
	defer c.Close()
	m, err := configureStreams(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := m.(*stream.Redis); !ok {
		t.Fatalf("Redis %T", m)
	}
}
