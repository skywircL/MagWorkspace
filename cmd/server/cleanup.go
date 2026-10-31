package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"regexp"
	"strings"

	"magworkspace/internal/blob"
	"magworkspace/internal/indexing"
)

type cleanupVectorTarget struct {
	URL       string `json:"url"`
	APIKeyEnv string `json:"api_key_env"`
}

type cleanupObjectTarget struct {
	Endpoint     string `json:"endpoint"`
	Bucket       string `json:"bucket"`
	AccessKeyEnv string `json:"access_key_env"`
	SecretKeyEnv string `json:"secret_key_env"`
}

var credentialEnvName = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

func cleanupCredential(name string, optional bool) (string, error) {
	if name == "" && optional {
		return "", nil
	}
	if !credentialEnvName.MatchString(name) || os.Getenv(name) == "" {
		return "", errors.New("cleanup credential environment variable missing or invalid")
	}
	return os.Getenv(name), nil
}

func decodeCleanupTargets[T any](name string) ([]T, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return nil, nil
	}
	if len(raw) > 32<<10 {
		return nil, errors.New("cleanup target configuration too large")
	}
	var targets []T
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&targets); err != nil || targets == nil || len(targets) > 16 {
		return nil, errors.New("invalid cleanup target configuration")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, errors.New("invalid cleanup target configuration")
	}
	return targets, nil
}

// The deployment explicitly allows old destinations; database rows only carry
// opaque identities and can never direct a worker to an arbitrary URL. Secrets
// remain in environment variables. No old bucket is created during cleanup.
func configureCleanupTargets(objects blob.Storage, search indexing.SearchStore) ([]blob.Storage, []indexing.SearchStore, error) {
	objectTargets := []blob.Storage{objects}
	if search == nil {
		var err error
		search, err = indexing.NewQdrant(env("QDRANT_URL", "http://127.0.0.1:16333"), os.Getenv("QDRANT_API_KEY"), 256)
		if err != nil {
			return nil, nil, errors.New("invalid QDRANT_URL for cleanup")
		}
	}
	vectorTargets := []indexing.SearchStore{search}
	seen := map[string]bool{objects.StorageID(): true, search.StorageID(): true}
	vectors, err := decodeCleanupTargets[cleanupVectorTarget]("CLEANUP_QDRANT_STORES")
	if err != nil {
		return nil, nil, err
	}
	for _, target := range vectors {
		key, err := cleanupCredential(target.APIKeyEnv, true)
		if err != nil {
			return nil, nil, err
		}
		client, err := indexing.NewQdrant(target.URL, key, 256)
		if err != nil || seen[client.StorageID()] {
			return nil, nil, errors.New("invalid or duplicate Qdrant cleanup target")
		}
		seen[client.StorageID()] = true
		vectorTargets = append(vectorTargets, client)
	}
	previousObjects, err := decodeCleanupTargets[cleanupObjectTarget]("CLEANUP_MINIO_STORES")
	if err != nil {
		return nil, nil, err
	}
	for _, target := range previousObjects {
		accessKey, err := cleanupCredential(target.AccessKeyEnv, false)
		if err != nil {
			return nil, nil, err
		}
		secretKey, err := cleanupCredential(target.SecretKeyEnv, false)
		if err != nil {
			return nil, nil, err
		}
		client, err := blob.NewMinIO(target.Endpoint, target.Bucket, accessKey, secretKey)
		if err != nil {
			return nil, nil, errors.New("invalid MinIO cleanup target")
		}
		if seen[client.StorageID()] {
			return nil, nil, errors.New("duplicate MinIO cleanup target")
		}
		seen[client.StorageID()] = true
		objectTargets = append(objectTargets, client)
	}
	return objectTargets, vectorTargets, nil
}
