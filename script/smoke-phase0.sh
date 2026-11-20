#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
api="http://127.0.0.1:${API_PORT:-8081}"
dc=(docker compose)
json() { python3 -c 'import json,sys; obj=json.load(sys.stdin); exec(sys.argv[1])' "$1"; }
ready() { curl -fsS --max-time 3 "$api/health/ready" >/dev/null; }
retry() { local i; for i in {1..45}; do if "$@" >/dev/null 2>&1; then return; fi; sleep 1; done; "$@"; }
retry ready
phone=13900000000
curl -fsS -X POST "$api/user/code?phone=$phone" | json 'assert obj["success"]'
code=$("${dc[@]}" exec -T redis redis-cli --raw GET "login:code:$phone" | tr -d '\r')
[[ "$code" =~ ^[0-9]{6}$ ]]
token=$(curl -fsS -H 'Content-Type: application/json' -d "{\"phone\":\"$phone\",\"code\":\"$code\"}" "$api/user/login" | json 'assert obj["success"] and obj["data"]; print(obj["data"])')
curl -fsS -H "Authorization: $token" "$api/user/me" | json 'assert obj["success"] and obj["data"]["id"] > 0'
curl -fsS "$api/shop/1" | json 'assert obj["success"] and obj["data"]["name"]'
curl -fsS "$api/blog/hot?current=1" | json 'assert obj["success"] and obj["data"][0]["title"]'
[[ $(curl -s -o /dev/null -w '%{http_code}' -X POST "$api/voucher-order/seckill/1") == 401 ]]
kafka() { "${dc[@]}" exec -T -e KAFKA_HEAP_OPTS=-Xmx64m kafka "/opt/kafka/bin/$1.sh" "${@:2}"; }
topic="xzdp-smoke-$(date +%s)-$$"
kafka kafka-topics --bootstrap-server kafka:9092 --create --topic "$topic" --partitions 1 --replication-factor 1 >/dev/null
echo phase0 | kafka kafka-console-producer --bootstrap-server kafka:9092 --topic "$topic"
kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic "$topic" --from-beginning --max-messages 1 --timeout-ms 20000 | grep -qx phase0
kafka kafka-topics --bootstrap-server kafka:9092 --delete --topic "$topic"
prom() { curl -fsSG "http://127.0.0.1:${PROMETHEUS_PORT:-9090}/api/v1/query" --data-urlencode 'query=up{job="xzdp-api"}' | json 'assert obj["data"]["result"][0]["value"][1]=="1"'; }
graf() { curl -fsS -u "admin:${GF_SECURITY_ADMIN_PASSWORD:-admin}" "http://127.0.0.1:${GRAFANA_PORT:-3000}/api/datasources/uid/xzdp-prometheus/health" | json 'assert obj["status"]=="OK"'; }
retry prom
retry graf
trap '"${dc[@]}" start redis >/dev/null' EXIT
"${dc[@]}" stop redis
curl -fsS "$api/health/live" | json 'assert obj["status"]=="ok"'
[[ $(curl -sS -o /dev/null -w '%{http_code}' "$api/health/ready") == 503 ]]
"${dc[@]}" start redis
retry ready
trap - EXIT
api_id=$("${dc[@]}" ps -q api)
trap '"${dc[@]}" start api >/dev/null' EXIT
"${dc[@]}" stop api
[[ $(docker inspect -f '{{.State.ExitCode}}' "$api_id") == 0 ]]
"${dc[@]}" start api
retry ready
trap - EXIT
echo "Phase 0 smoke passed"
