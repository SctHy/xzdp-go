#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
dc() { docker compose -p "xzdp-pg-tuning-$$" -f script/pgbench/compose.yml "$@"; }
trap 'dc down -v >/dev/null' EXIT
dc up -d --wait pg
BENCH_PRODUCTION_PG="$(dc port pg 5432)" go test -run '^TestPostgresOptimizations$' -count=1 -timeout=6m -v ./script/pgbench
