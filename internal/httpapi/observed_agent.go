package httpapi

import (
	"context"

	"magworkspace/internal/agent"
	"magworkspace/internal/observability"
)

type observedAgent struct {
	inner     agent.Client
	telemetry *observability.Telemetry
}

func (a observedAgent) Run(ctx context.Context, request agent.Request) (result agent.Result, err error) {
	ctx, finish := a.telemetry.Start(ctx, "agent.json")
	defer func() { finish(err) }()
	return a.inner.Run(ctx, request)
}

type observedStreamingAgent struct {
	observedAgent
	stream agent.StreamingClient
}

func (a observedStreamingAgent) Stream(ctx context.Context, request agent.Request, emit func(agent.Event) error) (result agent.Result, err error) {
	ctx, finish := a.telemetry.Start(ctx, "agent.sse")
	defer func() { finish(err) }()
	return a.stream.Stream(ctx, request, emit)
}

func observeAgent(client agent.Client, telemetry *observability.Telemetry) agent.Client {
	if client == nil || telemetry == nil {
		return client
	}
	wrapped := observedAgent{inner: client, telemetry: telemetry}
	if stream, ok := client.(agent.StreamingClient); ok {
		return observedStreamingAgent{observedAgent: wrapped, stream: stream}
	}
	return wrapped
}
