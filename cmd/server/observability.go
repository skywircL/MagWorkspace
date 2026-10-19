package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"magworkspace/internal/observability"
)

func configureObservability(ctx context.Context) (*observability.Telemetry, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(env("LOG_LEVEL", "INFO"))); err != nil {
		return nil, errors.New("LOG_LEVEL must be DEBUG, INFO, WARN or ERROR")
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)
	enabled, err := strconv.ParseBool(env("TRACING_ENABLED", "false"))
	if err != nil {
		return nil, errors.New("TRACING_ENABLED must be true or false")
	}
	ratio, err := strconv.ParseFloat(env("TRACING_SAMPLE_RATIO", "0.1"), 64)
	if err != nil {
		return nil, errors.New("TRACING_SAMPLE_RATIO must be 0..1")
	}
	return observability.Init(ctx, observability.Config{
		ServiceName: "magworkspace", Environment: env("APP_ENV", "development"),
		TracesEnabled: enabled, SampleRatio: ratio, ExportTimeout: 3 * time.Second,
		OTLPHTTPEndpoint: env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://127.0.0.1:4318/v1/traces"), Logger: logger,
	})
}

// Metrics has a separate listener; it never becomes an unauthenticated API route.
func serveMetrics(telemetry *observability.Telemetry) (func(), error) {
	address := env("METRICS_ADDR", "127.0.0.1:9091")
	if address == "disabled" {
		return func() {}, nil
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, errors.New("metrics listener failed; check METRICS_ADDR")
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", telemetry.MetricsHandler())
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8 << 10}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics listener stopped")
		}
	}()
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if server.Shutdown(ctx) != nil {
			_ = server.Close()
		}
	}, nil
}
