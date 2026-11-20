#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
tmpdir=$(mktemp -d)
project="xzdp-bucket-$$"
dc() { docker compose -p "$project" -f script/load/bucket-cluster.yml "$@"; }
cleanup() { dc down -v >/dev/null; rm -f "$tmpdir/bucket.test"; rmdir "$tmpdir"; }
trap cleanup EXIT
dc up -d --wait
arch=$(dc exec -T cluster uname -m)
case "$arch" in x86_64) arch=amd64;; aarch64) arch=arm64;; esac
CGO_ENABLED=0 GOOS=linux GOARCH=$arch go test -c -o "$tmpdir/bucket.test" ./internal/trade/experiment
dc cp "$tmpdir/bucket.test" cluster:/tmp/bucket.test
dc exec -T -e XZDP_EXPERIMENT_CLUSTER=127.0.0.1:7000 cluster /tmp/bucket.test -test.v -test.timeout=90s
