package main

import (
	"strings"
	"testing"
	"time"
)

func TestConfigureAgentHTTPOnly(t *testing.T) {
	t.Setenv("MAGENT_MODE", "")
	client, stop, err := configureAgent(time.Second)
	if err != nil || client != nil {
		t.Fatal("disabled mode", err)
	}
	stop()
	t.Setenv("MAGENT_MODE", "demo")
	t.Setenv("MAGENT_SERVICE_URL", "http://127.0.0.1:8090")
	t.Setenv("MAGENT_SERVICE_API_KEY", strings.Repeat("a", 64))
	t.Setenv("MAGENT_CALLBACK_ADDR", "127.0.0.1:0")
	t.Setenv("MAGENT_CALLBACK_BASE_URL", "http://127.0.0.1:8081")
	// Neither a Python runtime nor provider credentials are backend inputs.
	t.Setenv("MAGENT_PYTHON", "/nonexistent")
	t.Setenv("MAGENT_API_KEY", "")
	client, stop, err = configureAgent(time.Second)
	if err != nil || client == nil {
		t.Fatal("HTTP mode", err)
	}
	stop()
	for _, key := range []string{"MAGENT_SERVICE_URL", "MAGENT_SERVICE_API_KEY"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "")
			if _, _, err := configureAgent(time.Second); err == nil {
				t.Fatal("accepted missing HTTP configuration")
			}
		})
	}
}
