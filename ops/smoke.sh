#!/usr/bin/env bash
# End-to-end smoke test — the M10 exit criterion.
#
# Brings the whole stack up with one command, waits for every service to report
# healthy, then proves the observable surface answers: the API's /healthz,
# /status and /metrics, that Prometheus is actually scraping the API, and that
# Grafana is serving. Exits non-zero the moment any check fails, so CI or a
# human gets a single green/red.
#
#   ./ops/smoke.sh            # build, up, check
#   SKIP_BUILD=1 ./ops/smoke.sh   # against an already-built/running stack
set -euo pipefail

cd "$(dirname "$0")/.."

# Ports match docker-compose.yml's defaults; override to match a customised .env.
API_PORT="${API_PORT:-8080}"
PROMETHEUS_PORT="${PROMETHEUS_PORT:-9090}"
GRAFANA_PORT="${GRAFANA_PORT:-3000}"

pass() { printf '  \033[32mPASS\033[0m %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$1"; exit 1; }

# Poll a command until it succeeds or the timeout elapses.
wait_for() {
  local what="$1" timeout="$2"; shift 2
  local deadline=$(( SECONDS + timeout ))
  until "$@" >/dev/null 2>&1; do
    (( SECONDS >= deadline )) && fail "$what (timed out after ${timeout}s)"
    sleep 2
  done
  pass "$what"
}

echo "==> config"
docker compose config -q && pass "docker compose config parses" || fail "compose config invalid"

echo "==> bring up"
if [[ -z "${SKIP_BUILD:-}" ]]; then
  docker compose up -d --build
else
  docker compose up -d
fi

echo "==> health"
# Postgres is the dependency gate for everything else.
wait_for "postgres accepting connections" 60 \
  docker compose exec -T postgres pg_isready -U "${POSTGRES_USER:-chainscope}"

# The API's own readiness probe — 200 only when it can reach the database.
wait_for "api /healthz" 90 \
  curl -fsS "http://localhost:${API_PORT}/healthz"

# Grafana reports its own health, including datasource DB state.
wait_for "grafana /api/health" 90 \
  curl -fsS "http://localhost:${GRAFANA_PORT}/api/health"

# Prometheus readiness.
wait_for "prometheus /-/healthy" 60 \
  curl -fsS "http://localhost:${PROMETHEUS_PORT}/-/healthy"

echo "==> surface"
# /status must return JSON with the ingest heads.
status=$(curl -fsS "http://localhost:${API_PORT}/status")
echo "$status" | grep -q head_height && pass "/status returns ingest heads" \
  || fail "/status missing head_height: $status"

# /metrics must expose the chainscope gauges Prometheus scrapes.
metrics=$(curl -fsS "http://localhost:${API_PORT}/metrics")
echo "$metrics" | grep -q '^chainscope_ingest_lag' && pass "/metrics exposes gauges" \
  || fail "/metrics missing chainscope_ingest_lag"

# Prometheus must have the API target up — proves the scrape works end to end.
# The query carries {}"" so it must be URL-encoded, not pasted into the URL.
scrape_up() {
  curl -fsS --get "http://localhost:${PROMETHEUS_PORT}/api/v1/query" \
    --data-urlencode 'query=up{job="chainscope-api"}' \
    | grep -q '"value":\[[^]]*,"1"\]'
}
wait_for "prometheus scrapes the api (up == 1)" 60 scrape_up

echo
printf '\033[32mSMOKE OK\033[0m — stack healthy, API answering, Prometheus scraping, Grafana up.\n'
echo "Dashboard: http://localhost:${GRAFANA_PORT}  (admin/admin)"
