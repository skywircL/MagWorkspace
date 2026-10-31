package main

import (
	"errors"
	"magworkspace/internal/redisstate"
	"magworkspace/internal/stream"
	"os"
)

func configureStreams(redis *redisstate.Client) (stream.Manager, error) {
	switch os.Getenv("STREAM_MANAGER_TYPE") {
	case "", "memory":
		return stream.NewMemory(stream.DefaultTTL), nil
	case "redis":
		if redis == nil {
			return nil, errors.New("STREAM_MANAGER_TYPE=redis requires REDIS_ADDR")
		}
		return redis.StreamManager(), nil
	default:
		return nil, errors.New("STREAM_MANAGER_TYPE must be memory or redis")
	}
}
