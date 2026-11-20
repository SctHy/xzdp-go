#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
dc() { docker compose -p "xzdp-pgbench-$$" -f script/pgbench/compose.yml "$@"; }
trap 'dc down -v >/dev/null' EXIT
dc up -d --wait
dc exec -T pg psql -X -v ON_ERROR_STOP=1 -U bench -d bench < script/pgbench/pg.sql
export BENCH_MYSQL="$(dc port mysql 3306)"
export BENCH_PG="$(dc port pg 5432)"
go test -count=1 -v -timeout 12m ./script/pgbench
