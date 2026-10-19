// Package blob owns immutable bytes in a private MinIO bucket. Authorization
// remains in store; object keys and credentials are never public download URLs.
package blob

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"magworkspace/internal/domain"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/s3utils"
)

type Storage interface {
	Put(context.Context, string, []byte) error
	Read(context.Context, string, int) ([]byte, error)
	Delete(context.Context, string) error
}

var ErrUnavailable = errors.New("object_storage_unavailable")
var ErrIntegrity = errors.New("object_integrity_failed")

type MinIO struct {
	client *minio.Client
	bucket string
}

func NewMinIO(endpoint, bucket, accessKey, secretKey string) (*MinIO, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || accessKey == "" || secretKey == "" || s3utils.CheckValidBucketNameStrict(bucket) != nil {
		return nil, errors.New("invalid MinIO configuration")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.ResponseHeaderTimeout = 30 * time.Second
	client, err := minio.New(u.Host, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: u.Scheme == "https", Transport: transport, Region: "us-east-1", BucketLookup: minio.BucketLookupPath, MaxRetries: 1})
	if err != nil {
		return nil, errors.New("invalid MinIO configuration")
	}
	return &MinIO{client: client, bucket: bucket}, nil
}

func (s *MinIO) Ensure(ctx context.Context) error {
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return ErrUnavailable
	}
	if exists {
		return nil
	}
	if err = s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		// Another application instance may have created the bucket concurrently.
		exists, checkErr := s.client.BucketExists(ctx, s.bucket)
		if checkErr != nil || !exists {
			return ErrUnavailable
		}
	}
	return nil
}

func validKey(key string) bool {
	return key != "" && len(key) <= 512 && path.Clean(key) == key && !strings.HasPrefix(key, "/") && !strings.Contains(key, "..") && !strings.ContainsAny(key, "\\\r\n\x00")
}

func (s *MinIO) Put(ctx context.Context, key string, data []byte) error {
	if !validKey(key) || len(data) > domain.MaxStoredDocumentBytes {
		return ErrIntegrity
	}
	opts := minio.PutObjectOptions{ContentType: "application/octet-stream", DisableMultipart: true, SendContentMd5: true}
	opts.SetMatchETagExcept("*")
	info, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		return ErrUnavailable
	}
	if info.Size != int64(len(data)) {
		return ErrIntegrity
	}
	return nil
}

func (s *MinIO) Read(ctx context.Context, key string, maximum int) ([]byte, error) {
	if !validKey(key) || maximum < 1 || maximum > domain.MaxStoredDocumentBytes {
		return nil, ErrIntegrity
	}
	object, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, ErrUnavailable
	}
	defer object.Close()
	data, err := io.ReadAll(io.LimitReader(object, int64(maximum)+1))
	if err != nil {
		return nil, ErrUnavailable
	}
	if len(data) > maximum {
		return nil, ErrIntegrity
	}
	return data, nil
}

func (s *MinIO) Delete(ctx context.Context, key string) error {
	if !validKey(key) {
		return ErrIntegrity
	}
	if err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return ErrUnavailable
	}
	return nil
}
