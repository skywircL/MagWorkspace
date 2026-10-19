package main

import (
	"os"
	"time"

	"magworkspace/internal/agent"
)

// The application only owns the HTTP transport. Model credentials, Python and
// context budgets belong to the independently deployed Harness service.
func configureAgent(timeout time.Duration) (agent.Client, func(), error) {
	if os.Getenv("MAGENT_MODE") == "" {
		return nil, func() {}, nil
	}
	client, err := agent.NewHTTPClient(agent.HTTPConfig{
		URL: os.Getenv("MAGENT_SERVICE_URL"), APIKey: os.Getenv("MAGENT_SERVICE_API_KEY"),
		Mode: os.Getenv("MAGENT_MODE"), Timeout: timeout,
		CallbackListenAddr: env("MAGENT_CALLBACK_ADDR", "127.0.0.1:8081"),
		CallbackBaseURL:    env("MAGENT_CALLBACK_BASE_URL", "http://127.0.0.1:8081"),
	})
	if err != nil {
		return nil, func() {}, err
	}
	return client, func() { _ = client.Close() }, nil
}
