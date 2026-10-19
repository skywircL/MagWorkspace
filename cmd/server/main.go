package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"magworkspace/internal/blob"
	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
	"magworkspace/internal/indexing"
	"magworkspace/internal/redisstate"
	"magworkspace/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func run() error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is required (MySQL driver DSN)")
	}
	uploadLimit, err := configureUploadLimit()
	if err != nil {
		return err
	}
	telemetry, err := configureObservability(context.Background())
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if telemetry.Shutdown(ctx) != nil {
			slog.Warn("trace shutdown deadline exceeded")
		}
	}()
	timeout, err := time.ParseDuration(env("MAGENT_TIMEOUT", "120s"))
	if err != nil || timeout < time.Second || timeout > 10*time.Minute {
		return errors.New("MAGENT_TIMEOUT must be between 1s and 10m")
	}
	registration, err := strconv.ParseBool(env("ALLOW_REGISTRATION", "true"))
	if err != nil {
		return errors.New("ALLOW_REGISTRATION must be true or false")
	}
	client, closeAgent, err := configureAgent(timeout)
	if err != nil {
		return err
	}
	defer closeAgent()
	var redisClient *redisstate.Client
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		redisClient = redisstate.New(addr, os.Getenv("REDIS_PASSWORD"), "magworkspace:v1:")
		defer redisClient.Close()
		ctx, stop := context.WithTimeout(context.Background(), time.Second)
		err := redisClient.Ping(ctx)
		stop()
		if err != nil {
			slog.Warn("Redis unavailable; using durable database dispatch; event replay disabled until recovery")
		}
	}
	indexConfig, err := configureIndexing()
	if err != nil {
		return err
	}
	var profiles []domain.IndexProfile
	if indexConfig != nil {
		profiles = append(profiles, indexConfig.profile)
	}
	objects, err := blob.NewMinIO(env("MINIO_ENDPOINT", "http://127.0.0.1:19000"), env("MINIO_BUCKET", "magworkspace"), os.Getenv("MINIO_ACCESS_KEY"), os.Getenv("MINIO_SECRET_KEY"))
	if err != nil {
		return err
	}
	startup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err = objects.Ensure(startup); err != nil {
		return errors.New("MinIO unavailable; check configuration and bucket permissions")
	}
	db, err := store.Open(startup, dsn, profiles...)
	if err != nil {
		cancel()
		return errors.New("database connection failed; check DATABASE_URL and database health")
	}
	defer db.Close()
	db.SetObjectStorage(objects)
	if err = db.SetUploadLimit(uploadLimit); err != nil {
		return err
	}
	if err = db.Migrate(startup); err != nil {
		cancel()
		return errors.New("database migration failed; inspect schema version and database privileges")
	}
	cancel()
	shutdown, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	workerCtx, cancelWorkers := context.WithCancel(shutdown)
	workersDone := make(chan struct{})
	var indexes *indexing.Service
	if indexConfig != nil {
		indexConfig.opts.Telemetry = telemetry
		indexConfig.opts.Redis = redisClient
		indexes = indexing.NewService(db, indexConfig.parser, indexConfig.embedder, indexConfig.search, objects, indexConfig.profile, indexConfig.opts)
		go func() { defer close(workersDone); indexes.Run(workerCtx) }()
	} else {
		close(workersDone)
	}
	objectsDone := make(chan struct{})
	go func() {
		defer close(objectsDone)
		for workerCtx.Err() == nil {
			cleanup, stop := context.WithTimeout(workerCtx, 30*time.Second)
			cleanup, finishCleanup := telemetry.Start(cleanup, "object.cleanup")
			err := db.CleanupObjects(cleanup)
			finishCleanup(err)
			if err != nil && workerCtx.Err() == nil {
				slog.Warn("object cleanup pending")
			}
			stop()
			select {
			case <-workerCtx.Done():
				return
			case <-time.After(30 * time.Second):
			}
		}
	}()
	defer func() {
		cancelWorkers()
		select {
		case <-objectsDone:
		case <-time.After(5 * time.Second):
			slog.Warn("object cleanup shutdown deadline exceeded")
		}
		select {
		case <-workersDone:
		case <-time.After(10 * time.Second):
			slog.Warn("index worker shutdown deadline exceeded")
		}
	}()
	stopMetrics, err := serveMetrics(telemetry)
	if err != nil {
		return err
	}
	defer stopMetrics()
	srv := &http.Server{Addr: env("HTTP_ADDR", "127.0.0.1:8080"), Handler: httpapi.New(db, client, httpapi.Config{Redis: redisClient, AllowRegistration: registration, RunTimeout: timeout, Indexing: indexes, Telemetry: telemetry}), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: max(timeout+15*time.Second, 45*time.Second), IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10, BaseContext: func(net.Listener) context.Context { return shutdown }}
	serveErr := make(chan error, 1)
	go func() {
		slog.Info("MagWorkspace listening", "addr", srv.Addr, "agent_mode", env("MAGENT_MODE", "disabled"))
		serveErr <- srv.ListenAndServe()
	}()
	select {
	case err = <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("HTTP server stopped: %w", err)
		}
	case <-shutdown.Done():
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err = srv.Shutdown(ctx); err != nil {
			_ = srv.Close()
			return errors.New("HTTP shutdown deadline exceeded")
		}
	}
	return nil
}
