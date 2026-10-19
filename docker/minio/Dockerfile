FROM golang:1.27.2-alpine3.23@sha256:9e45f0eb4a63ed37ad6e604950407558b7c738e34e015ae77a5d4ad369cde079 AS build

RUN apk add --no-cache ca-certificates git
WORKDIR /src

# Build the official source at an immutable commit. Its go.mod requires Go
# 1.24.0 (toolchain 1.24.8); the builder uses the verified Go 1.27.2 image.
RUN git init \
    && git remote add origin https://github.com/minio/minio.git \
    && git fetch --depth 1 origin 7aac2a2c5b7c882e68c1ce017d8256be2feea27f \
    && git checkout --detach FETCH_HEAD \
    && test "$(git rev-parse HEAD)" = '7aac2a2c5b7c882e68c1ce017d8256be2feea27f'

ENV CGO_ENABLED=0 GOTOOLCHAIN=local
RUN --mount=type=cache,id=magworkspace-minio-mod,target=/go/pkg/mod,sharing=locked \
    go mod download
# Match the upstream Makefile's build target, including its version metadata.
RUN --mount=type=cache,id=magworkspace-minio-mod,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,id=magworkspace-minio-build,target=/root/.cache/go-build,sharing=locked \
    go build -tags kqueue -trimpath \
    -ldflags "$(go run buildscripts/gen-ldflags.go)" -o /out/minio .

FROM alpine:3.23.4@sha256:5b10f432ef3da1b8d4c7eb6c487f2f5a8f096bc91145e68878dd4a5019afde11

LABEL org.opencontainers.image.source="https://github.com/minio/minio" \
      org.opencontainers.image.revision="7aac2a2c5b7c882e68c1ce017d8256be2feea27f" \
      org.opencontainers.image.licenses="AGPL-3.0-or-later"

RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 minio \
    && adduser -S -u 10001 -G minio minio \
    && mkdir -p /data /home/minio/certs \
    && chown -R minio:minio /data /home/minio

COPY --from=build /out/minio /usr/local/bin/minio
COPY --from=build /src/LICENSE /src/CREDITS /licenses/

WORKDIR /data
USER 10001:10001
VOLUME ["/data"]
EXPOSE 9000 9001
ENTRYPOINT ["/usr/local/bin/minio", "--certs-dir", "/home/minio/certs"]
CMD ["server", "/data", "--console-address", ":9001"]
