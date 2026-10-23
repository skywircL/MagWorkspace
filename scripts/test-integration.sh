#!/bin/sh
# A disposable MySQL instance; the application's Compose volume is never touched.
set -eu
cd "$(dirname "$0")/.."
command -v docker >/dev/null
command -v go >/dev/null
MYSQL_ROOT_PASSWORD=$(python3 -c 'import secrets; print(secrets.token_hex(24))')
MYSQL_ROOT_HOST=%
export MYSQL_ROOT_PASSWORD MYSQL_ROOT_HOST
container=$(docker run --detach --rm --tmpfs /var/lib/mysql --env MYSQL_ROOT_PASSWORD --env MYSQL_ROOT_HOST --publish 127.0.0.1::3306 mysql:8.4)
redis_container=""
trap 'docker rm --force "$redis_container" >/dev/null 2>&1 || true; docker rm --force "$container" >/dev/null 2>&1 || true' EXIT HUP INT TERM
redis_container=$(docker run --detach --rm --publish 127.0.0.1::6379 redis:8.2-alpine)
redis_port=$(docker port "$redis_container" 6379/tcp | sed 's/.*://')
export MAGWORKSPACE_TEST_REDIS_ADDR="127.0.0.1:$redis_port"
port=$(docker port "$container" 3306/tcp | sed 's/.*://')
tries=0
until docker exec "$container" sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -h 127.0.0.1 -u root -e "SELECT 1" >/dev/null 2>&1'; do
  tries=$((tries+1))
  if [ "$tries" -ge 90 ]; then echo "MySQL did not become ready" >&2; exit 1; fi
  sleep 1
done
export MAGWORKSPACE_TEST_DATABASE_URL="root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:${port})/mysql?parseTime=true"
export GOMODCACHE="${GOMODCACHE:-$PWD/.cache/gomod}"
export GOCACHE="${GOCACHE:-$PWD/.cache/gobuild}"
# The real Harness/HTTP suite exceeds Go's default 10-minute package timeout.
# A caller may still override this test-only budget with a later -timeout flag.
go test -race -count=1 -timeout 20m "$@" ./...
