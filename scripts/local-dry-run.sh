#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${TRAIDAN_LOCAL_DRY_RUN_STATE_DIR:-$ROOT/.agent/state/local-dry-run}"
PID_DIR="$STATE_DIR/pids"
LOG_DIR="$STATE_DIR/logs"
ALPACA_ENV_FILE="${TRAIDAN_ALPACA_ENV_FILE:-$STATE_DIR/alpaca.env}"
ADMIN_TOKEN_FILE="${TRAIDAN_ADMIN_TOKEN_FILE:-$STATE_DIR/admin-token}"
ADMIN_API_URL="${TRAIDAN_ADMIN_API_URL:-http://127.0.0.1:8090}"
WS_URL="${VITE_TRAIDAN_WS_URL:-ws://localhost:8080/stream}"
METRICS_URL="${TRAIDAN_METRICS_URL:-http://127.0.0.1:9100/metrics}"
FRONTEND_PORT="${TRAIDAN_FRONTEND_PORT:-5173}"
FRONTEND_URL="${TRAIDAN_FRONTEND_URL:-http://127.0.0.1:${FRONTEND_PORT}}"
DEFAULT_LLM_EQUITY_FILL_MODE="${TRAIDAN_LLM_EQUITY_FILL_MODE_DEFAULT:-paper}"
DEFAULT_LLM_EQUITY_RISK_MODE="${TRAIDAN_LLM_EQUITY_RISK_MODE_DEFAULT:-fully_invested}"
DEFAULT_ALLOW_LEAST_BAD_FORCED="${TRAIDAN_LLM_EQUITY_ALLOW_LEAST_BAD_FORCED_DEFAULT:-0}"
TRADING_STATE_DIR="${TRAIDAN_LLM_EQUITY_STATE_DIR:-$ROOT/.agent/state/llm-equity-dry-run}"
TRADING_ARCHIVE_DIR="${TRAIDAN_LLM_EQUITY_ARCHIVE_DIR:-$ROOT/.agent/state/llm-equity-archives}"

usage() {
  cat <<USAGE
Usage: scripts/local-dry-run.sh <start|stop|status|smoke|urls|reset-epoch>

Starts the LLM equity dry-run engine, dashboard, and hourly review service.
USAGE
}

ensure_dirs() {
  mkdir -p "$PID_DIR" "$LOG_DIR" "$(dirname "$ADMIN_TOKEN_FILE")"
}

ensure_admin_token() {
  if [ -n "${TRAIDAN_ADMIN_TOKEN:-}" ]; then
    return
  fi
  if [ ! -s "$ADMIN_TOKEN_FILE" ]; then
    python3 - "$ADMIN_TOKEN_FILE" <<'PY'
from pathlib import Path
import secrets
import sys

path = Path(sys.argv[1])
path.write_text(secrets.token_urlsafe(32), encoding="utf-8")
path.chmod(0o600)
PY
  fi
}

admin_token_value() {
  if [ -n "${TRAIDAN_ADMIN_TOKEN:-}" ]; then
    printf '%s\n' "$TRAIDAN_ADMIN_TOKEN"
  else
    cat "$ADMIN_TOKEN_FILE"
  fi
}

load_alpaca_env() {
  if [ ! -r "$ALPACA_ENV_FILE" ]; then
    return
  fi
  while IFS='=' read -r key value; do
    case "$key" in
      ALPACA_API_KEY_ID|ALPACA_API_SECRET_KEY|APCA_API_KEY_ID|APCA_API_SECRET_KEY|ALPACA_DATA_BASE_URL|ALPACA_TRADING_BASE_URL|ALPACA_PAPER_API_BASE_URL|ALPACA_DATA_FEED)
        export "$key=$value"
        ;;
      TRAIDAN_LLM_EQUITY_CONTEXT_AUTO_FETCH|TRAIDAN_LLM_EQUITY_CONTEXT_TIMEOUT_SECONDS|TRAIDAN_LLM_EQUITY_MACRO_FEED_URLS|TRAIDAN_LLM_EQUITY_NEWS_FEED_URLS|TRAIDAN_LLM_EQUITY_MACRO_SUMMARY|TRAIDAN_LLM_EQUITY_NEWS_SUMMARY|TRAIDAN_LLM_EQUITY_MACRO_URL|TRAIDAN_LLM_EQUITY_NEWS_URL)
        export "$key=$value"
        ;;
      TRAIDAN_LLM_EQUITY_TECHNICALS|TRAIDAN_LLM_EQUITY_TECHNICALS_HISTORY_DAYS|TRAIDAN_LLM_EQUITY_TECHNICALS_MAX_SYMBOLS|TRAIDAN_LLM_EQUITY_OUTCOME_MAX_AGE_MINUTES)
        export "$key=$value"
        ;;
      TRAIDAN_LLM_EQUITY_EFFECTIVE_CALL_COST_USD|TRAIDAN_LLM_EQUITY_MODEL_COST_BASIS|TRAIDAN_LLM_SWARM_MONTHLY_MODEL_BUDGET_USD|TRAIDAN_LLM_EQUITY_ESTIMATED_MONTHLY_CYCLES)
        export "$key=$value"
        ;;
    esac
  done <"$ALPACA_ENV_FILE"
}

strategy_epoch_id() {
  PYTHONPATH="$ROOT/intelligence" python3 - "$TRADING_STATE_DIR" <<'PY'
from pathlib import Path
import sys

from llm_equity.hourly_service import ensure_strategy_epoch

print(ensure_strategy_epoch(Path(sys.argv[1]))["epoch_id"])
PY
}

is_running() {
  local name="$1"
  local pid_file="$PID_DIR/$name.pid"
  [ -s "$pid_file" ] && kill -0 "$(cat "$pid_file")" 2>/dev/null
}

start_process() {
  local name="$1"
  shift
  if is_running "$name"; then
    echo "$name=already_running pid=$(cat "$PID_DIR/$name.pid")"
    return
  fi
  (
    cd "$ROOT"
    if command -v setsid >/dev/null 2>&1; then
      setsid "$@" >"$LOG_DIR/$name.log" 2>&1 < /dev/null &
    else
      nohup "$@" >"$LOG_DIR/$name.log" 2>&1 < /dev/null &
    fi
    echo $! >"$PID_DIR/$name.pid"
  )
  echo "$name=started pid=$(cat "$PID_DIR/$name.pid")"
}

start_engine() {
  ensure_dirs
  ensure_admin_token
  export TRAIDAN_RUN_MODE="${TRAIDAN_RUN_MODE:-dry_run}"
  export TRAIDAN_MARKET_DATA_SOURCE="${TRAIDAN_MARKET_DATA_SOURCE:-alpaca_equity}"
  export TRAIDAN_MARKET_SYMBOL="${TRAIDAN_MARKET_SYMBOL:-SPY}"
  export TRAIDAN_ADMIN_TOKEN_FILE="${TRAIDAN_ADMIN_TOKEN_FILE:-$ADMIN_TOKEN_FILE}"
  export TRAIDAN_ADMIN_BIND_ADDR="${TRAIDAN_ADMIN_BIND_ADDR:-127.0.0.1:8090}"
  export TRAIDAN_WS_BIND_ADDR="${TRAIDAN_WS_BIND_ADDR:-127.0.0.1:8080}"
  export TRAIDAN_METRICS_BIND_ADDR="${TRAIDAN_METRICS_BIND_ADDR:-127.0.0.1:9100}"
  export TRAIDAN_LLM_EQUITY_FILL_MODE="${TRAIDAN_LLM_EQUITY_FILL_MODE:-$DEFAULT_LLM_EQUITY_FILL_MODE}"
  export TRAIDAN_LLM_EQUITY_RISK_MODE="${TRAIDAN_LLM_EQUITY_RISK_MODE:-$DEFAULT_LLM_EQUITY_RISK_MODE}"
  export TRAIDAN_LLM_EQUITY_ALLOW_LEAST_BAD_FORCED="${TRAIDAN_LLM_EQUITY_ALLOW_LEAST_BAD_FORCED:-$DEFAULT_ALLOW_LEAST_BAD_FORCED}"
  export TRAIDAN_LLM_EQUITY_MODEL_COST_BASIS="${TRAIDAN_LLM_EQUITY_MODEL_COST_BASIS:-subscription_zero}"
  export TRAIDAN_LLM_EQUITY_STATE_DIR="$TRADING_STATE_DIR"
  local epoch_id
  epoch_id="$(strategy_epoch_id)"
  export TRAIDAN_LLM_EQUITY_AUTHORITY_STATE_FILE="${TRAIDAN_LLM_EQUITY_AUTHORITY_STATE_FILE:-$TRADING_STATE_DIR/epochs/$epoch_id/rust-authority-state.json}"
  export TRAIDAN_LLM_EQUITY_DASHBOARD_STATE_FILE="${TRAIDAN_LLM_EQUITY_DASHBOARD_STATE_FILE:-$TRADING_STATE_DIR/epochs/$epoch_id/rust-dashboard-state.json}"
  (cd "$ROOT" && CI_COMMIT_SHA="$(git rev-parse HEAD)" cargo build --bin traidan)
  start_process rust "$ROOT/target/debug/traidan"
}

start_frontend() {
  start_process frontend env VITE_TRAIDAN_WS_URL="$WS_URL" \
    VITE_TRAIDAN_ADMIN_API_URL="$ADMIN_API_URL" \
    "$ROOT/frontend/node_modules/.bin/vite" "$ROOT/frontend" \
    --host 127.0.0.1 --port "$FRONTEND_PORT" --strictPort
}

start_hourly_service() {
  ensure_admin_token
  load_alpaca_env
  export TRAIDAN_ADMIN_TOKEN_FILE="${TRAIDAN_ADMIN_TOKEN_FILE:-$ADMIN_TOKEN_FILE}"
  export TRAIDAN_ADMIN_API_URL="$ADMIN_API_URL"
  export TRAIDAN_LLM_EQUITY_BOOTSTRAP_ON_START="${TRAIDAN_LLM_EQUITY_BOOTSTRAP_ON_START:-1}"
  export TRAIDAN_LLM_EQUITY_FILL_MODE="${TRAIDAN_LLM_EQUITY_FILL_MODE:-$DEFAULT_LLM_EQUITY_FILL_MODE}"
  export TRAIDAN_LLM_EQUITY_RISK_MODE="${TRAIDAN_LLM_EQUITY_RISK_MODE:-$DEFAULT_LLM_EQUITY_RISK_MODE}"
  export TRAIDAN_LLM_EQUITY_ALLOW_LEAST_BAD_FORCED="${TRAIDAN_LLM_EQUITY_ALLOW_LEAST_BAD_FORCED:-$DEFAULT_ALLOW_LEAST_BAD_FORCED}"
  export TRAIDAN_LLM_EQUITY_MODEL_COST_BASIS="${TRAIDAN_LLM_EQUITY_MODEL_COST_BASIS:-subscription_zero}"
  export TRAIDAN_LLM_EQUITY_STATE_DIR="$TRADING_STATE_DIR"
  export PYTHONPATH="$ROOT/intelligence${PYTHONPATH:+:$PYTHONPATH}"
  start_process llm-equity-service python3 -m llm_equity.hourly_service run
}

start_all() {
  start_engine
  start_frontend
  start_hourly_service
  print_urls
}

stop_all() {
  ensure_dirs
  for name in llm-equity-service frontend rust; do
    local pid_file="$PID_DIR/$name.pid"
    if [ -s "$pid_file" ]; then
      local pid
      pid="$(cat "$pid_file")"
      if kill -0 "$pid" 2>/dev/null; then
        kill -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
        echo "$name=stopped pid=$pid"
        for _ in 1 2 3 4 5 6 7 8 9 10; do
          kill -0 "$pid" 2>/dev/null || break
          sleep 0.1
        done
        if kill -0 "$pid" 2>/dev/null; then
          kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
        fi
      fi
      rm -f "$pid_file"
    fi
  done
}

status_all() {
  ensure_dirs
  for name in rust frontend llm-equity-service; do
    if is_running "$name"; then
      echo "$name=running pid=$(cat "$PID_DIR/$name.pid")"
    else
      echo "$name=stopped"
    fi
  done
  print_hourly_service_mode
  print_urls
}

print_urls() {
  cat <<URLS
frontend=${FRONTEND_URL}
websocket=${WS_URL}
admin_api=${ADMIN_API_URL}
metrics=${METRICS_URL}
URLS
}

process_env_value() {
  local pid="$1"
  local key="$2"
  tr '\0' '\n' <"/proc/$pid/environ" 2>/dev/null | sed -n "s/^${key}=//p" | tail -1
}

present_or_missing() {
  if [ -n "$1" ]; then
    echo "present"
  else
    echo "missing"
  fi
}

enabled_or_disabled() {
  case "${1:-1}" in
    0|false|FALSE|no|NO|off|OFF)
      echo "disabled"
      ;;
    *)
      echo "enabled"
      ;;
  esac
}

readable_file_status() {
  if [ -r "$1" ]; then
    echo "present"
  else
    echo "missing"
  fi
}

credentials_status() {
  if [ -n "$1" ] && [ -n "$2" ]; then
    echo "present"
  else
    echo "missing"
  fi
}

print_hourly_service_mode() {
  local pid_file="$PID_DIR/llm-equity-service.pid"
  if [ ! -s "$pid_file" ] || ! kill -0 "$(cat "$pid_file")" 2>/dev/null; then
    echo "llm_equity_mode=stopped"
    return
  fi
  local pid fixture force_order bootstrap fill_mode risk_mode least_bad_forced alpaca_key alpaca_secret macro_summary news_summary
  local context_auto_fetch
  pid="$(cat "$pid_file")"
  fixture="$(process_env_value "$pid" TRAIDAN_LLM_EQUITY_FIXTURE_MODE)"
  force_order="$(process_env_value "$pid" TRAIDAN_LLM_EQUITY_FIXTURE_FORCE_ORDER)"
  bootstrap="$(process_env_value "$pid" TRAIDAN_LLM_EQUITY_BOOTSTRAP_ON_START)"
  fill_mode="$(process_env_value "$pid" TRAIDAN_LLM_EQUITY_FILL_MODE)"
  risk_mode="$(process_env_value "$pid" TRAIDAN_LLM_EQUITY_RISK_MODE)"
  least_bad_forced="$(process_env_value "$pid" TRAIDAN_LLM_EQUITY_ALLOW_LEAST_BAD_FORCED)"
  alpaca_key="$(process_env_value "$pid" ALPACA_API_KEY_ID)$(process_env_value "$pid" APCA_API_KEY_ID)"
  alpaca_secret="$(process_env_value "$pid" ALPACA_API_SECRET_KEY)$(process_env_value "$pid" APCA_API_SECRET_KEY)"
  macro_summary="$(process_env_value "$pid" TRAIDAN_LLM_EQUITY_MACRO_SUMMARY)"
  news_summary="$(process_env_value "$pid" TRAIDAN_LLM_EQUITY_NEWS_SUMMARY)"
  context_auto_fetch="$(process_env_value "$pid" TRAIDAN_LLM_EQUITY_CONTEXT_AUTO_FETCH)"
  if [ "$fixture" = "1" ]; then
    echo "llm_equity_mode=fixture"
  else
    echo "llm_equity_mode=live"
  fi
  echo "llm_equity_bootstrap_on_start=${bootstrap:-1}"
  echo "llm_equity_fill_mode=${fill_mode:-$DEFAULT_LLM_EQUITY_FILL_MODE}"
  echo "llm_equity_risk_mode=${risk_mode:-$DEFAULT_LLM_EQUITY_RISK_MODE}"
  echo "llm_equity_least_bad_forced=${least_bad_forced:-$DEFAULT_ALLOW_LEAST_BAD_FORCED}"
  echo "llm_equity_fixture_force_order=${force_order:-0}"
  echo "alpaca_env_file=$(readable_file_status "$ALPACA_ENV_FILE")"
  echo "alpaca_credentials=$(credentials_status "$alpaca_key" "$alpaca_secret")"
  echo "context_auto_fetch=${context_auto_fetch:-1}"
  echo "public_context_fetch=$(enabled_or_disabled "$context_auto_fetch")"
  echo "macro_summary_env=$(present_or_missing "$macro_summary")"
  echo "news_summary_env=$(present_or_missing "$news_summary")"
}

wait_http() {
  local label="$1"
  local url="$2"
  local deadline=$((SECONDS + 90))
  until curl --max-time 5 -fsS "$url" >/dev/null 2>&1; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "ERROR: timed out waiting for $label at $url" >&2
      return 1
    fi
    sleep 1
  done
  echo "$label=ok"
}

admin_get_status() {
  local token
  token="$(admin_token_value)"
  curl --max-time 10 -fsS \
    -H "Authorization: Bearer $token" \
    "$ADMIN_API_URL/admin/status"
}

wait_admin_status() {
  local deadline=$((SECONDS + 90))
  until admin_get_status >/tmp/traidan-admin-status.json 2>/dev/null; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "ERROR: timed out waiting for admin API at $ADMIN_API_URL" >&2
      return 1
    fi
    sleep 1
  done
  echo "admin=ok"
}

run_fixture_cycle() {
  local cycle_id_file="$1"
  local result_file="$2"
  export TRAIDAN_ADMIN_TOKEN_FILE="${TRAIDAN_ADMIN_TOKEN_FILE:-$ADMIN_TOKEN_FILE}"
  export TRAIDAN_ADMIN_API_URL="$ADMIN_API_URL"
  export TRAIDAN_LLM_EQUITY_FIXTURE_MODE=1
  export TRAIDAN_LLM_EQUITY_FIXTURE_FORCE_ORDER="${TRAIDAN_LLM_EQUITY_FIXTURE_FORCE_ORDER:-0}"
  export PYTHONPATH="$ROOT/intelligence${PYTHONPATH:+:$PYTHONPATH}"
  python3 -m llm_equity.hourly_service run-once --fixture >"$result_file"
  FIXTURE_CYCLE_ID_FILE="$cycle_id_file" python3 - "$result_file" <<'PY'
import json
import os
from pathlib import Path
import sys

result = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
if result.get("status") != "success":
    raise SystemExit(f"fixture cycle was not posted: {result}")
cycle_id = result.get("cycle_id")
if not isinstance(cycle_id, str) or not cycle_id:
    raise SystemExit(f"fixture cycle returned no cycle_id: {result}")
target = Path(os.environ["FIXTURE_CYCLE_ID_FILE"])
temporary = target.with_name(f".{target.name}.tmp")
temporary.write_text(f"{cycle_id}\n", encoding="utf-8")
temporary.replace(target)
print(f"llm_equity_cycle=posted cycle_id={cycle_id}")
PY
}

wait_for_websocket_ready() {
  local ready_file="$1"
  local websocket_pid="$2"
  local deadline=$((SECONDS + 30))
  until [ -f "$ready_file" ]; do
    if ! kill -0 "$websocket_pid" 2>/dev/null; then
      echo "ERROR: websocket subscriber exited before it connected" >&2
      return 1
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "ERROR: timed out waiting for websocket subscriber connection" >&2
      return 1
    fi
    sleep 1
  done
  echo "websocket=connected"
}

check_websocket() {
  local ready_file="$1"
  local cycle_id_file="$2"
  WS_URL="$WS_URL" WS_READY_FILE="$ready_file" WS_CYCLE_ID_FILE="$cycle_id_file" node --input-type=module <<'NODE'
import { readFileSync, writeFileSync } from 'node:fs';

const expected = new Set([
  'llm_equity_service_status',
  'equity_portfolio_state',
  'market_research_snapshot',
  'llm_equity_cycle',
  'equity_order_simulated',
  'llm_memory_update',
]);
const cycleScopedTypes = new Set([
  'llm_equity_cycle',
  'equity_order_simulated',
  'llm_memory_update',
]);
const seen = new Set();
let completed = false;
let ready = false;
let expectedCycleId = null;
let timeout;
const fixtureCycleId = () => {
  try {
    const value = readFileSync(process.env.WS_CYCLE_ID_FILE, 'utf8').trim();
    return value || null;
  } catch {
    return null;
  }
};
const fail = () => {
  completed = true;
  console.error(
    `ERROR: websocket missing ${[...expected].filter((item) => !seen.has(item)).join(', ')} `
      + `(fixture_cycle_id=${expectedCycleId ?? 'pending'})`,
  );
  ws.close();
  process.exit(1);
};
const armTimeout = (milliseconds) => {
  clearTimeout(timeout);
  timeout = setTimeout(fail, milliseconds);
};
const refreshExpectedCycleId = () => {
  const nextCycleId = fixtureCycleId();
  if (nextCycleId && nextCycleId !== expectedCycleId) {
    expectedCycleId = nextCycleId;
    armTimeout(15_000);
  }
  return expectedCycleId;
};
const ws = new WebSocket(process.env.WS_URL);
const cyclePoll = setInterval(refreshExpectedCycleId, 100);
armTimeout(30_000);
ws.onmessage = (event) => {
  const message = JSON.parse(event.data);
  if (message.type === 'llm_equity_service_status' && !ready) {
    ready = true;
    writeFileSync(process.env.WS_READY_FILE, 'subscribed\n');
  }
  if (!refreshExpectedCycleId()) return;
  if (cycleScopedTypes.has(message.type) && message.data?.cycle_id !== expectedCycleId) return;
  seen.add(message.type);
  if ([...expected].every((item) => seen.has(item))) {
    completed = true;
    clearTimeout(timeout);
    clearInterval(cyclePoll);
    ws.close();
    console.log('websocket=ok');
  }
};
ws.onerror = (error) => {
  if (completed) return;
  clearTimeout(timeout);
  clearInterval(cyclePoll);
  console.error(error);
  process.exit(1);
};
NODE
}

smoke() {
  local restart_after=0 smoke_status=0 smoke_state_root websocket_pid="" websocket_ready_file fixture_cycle_id_file fixture_result_file
  if is_running rust || is_running frontend || is_running llm-equity-service; then
    restart_after=1
  fi
  stop_all
  smoke_state_root="$(mktemp -d "$STATE_DIR/smoke-state.XXXXXX")"
  (
    export TRAIDAN_LLM_EQUITY_AUTHORITY_STATE_FILE="$smoke_state_root/rust-authority-state.json"
    export TRAIDAN_LLM_EQUITY_STATE_DIR="$smoke_state_root/python"
    export TRAIDAN_LLM_EQUITY_FIXTURE_MODE=1
    export TRAIDAN_LLM_EQUITY_BOOTSTRAP_ON_START=0
    export TRAIDAN_LLM_EQUITY_FIXTURE_FORCE_ORDER=1
    export TRAIDAN_LLM_EQUITY_FIXTURE_FORCE_RTH=1
    export TRAIDAN_LLM_EQUITY_FILL_MODE=paper
    export TRAIDAN_LLM_EQUITY_RISK_MODE=forced_aggressive
    export TRAIDAN_LLM_EQUITY_MODEL_COST_BASIS=subscription_zero
    websocket_ready_file="$smoke_state_root/websocket-ready"
    fixture_cycle_id_file="$smoke_state_root/fixture-cycle-id"
    fixture_result_file="$smoke_state_root/llm-equity-cycle.json"
    start_all || smoke_status=$?
    if [ "$smoke_status" -eq 0 ]; then wait_http metrics "$METRICS_URL" || smoke_status=$?; fi
    if [ "$smoke_status" -eq 0 ]; then wait_admin_status || smoke_status=$?; fi
    if [ "$smoke_status" -eq 0 ]; then
      check_websocket "$websocket_ready_file" "$fixture_cycle_id_file" &
      websocket_pid=$!
      wait_for_websocket_ready "$websocket_ready_file" "$websocket_pid" || smoke_status=$?
    fi
    if [ "$smoke_status" -eq 0 ]; then
      run_fixture_cycle "$fixture_cycle_id_file" "$fixture_result_file" || smoke_status=$?
    fi
    if [ -n "$websocket_pid" ]; then
      if [ "$smoke_status" -eq 0 ]; then
        wait "$websocket_pid" || smoke_status=$?
      elif kill -0 "$websocket_pid" 2>/dev/null; then
        kill "$websocket_pid" || true
        wait "$websocket_pid" || true
      fi
    fi
    stop_all
    exit "$smoke_status"
  ) || smoke_status=$?
  rm -rf -- "$smoke_state_root"
  if [ "${TRAIDAN_SMOKE_RESTART_AFTER:-$restart_after}" = "1" ]; then
    export TRAIDAN_LLM_EQUITY_BOOTSTRAP_ON_START=1
    start_all
  fi
  return "$smoke_status"
}

reset_epoch() {
  local archive_stamp archive_root resolved_trading_state allowed_state_root
  resolved_trading_state="$(realpath -m -- "$TRADING_STATE_DIR")"
  allowed_state_root="$(realpath -m -- "$ROOT/.agent/state")"
  case "$resolved_trading_state" in
    "$allowed_state_root"/*) ;;
    *) echo "refusing reset outside $allowed_state_root" >&2; return 1 ;;
  esac
  if is_running rust; then
    python3 - "$ADMIN_API_URL/admin/status" "$(admin_token_value)" <<'PY'
import json
import sys
from urllib import request

req = request.Request(sys.argv[1], headers={"Authorization": f"Bearer {sys.argv[2]}"})
with request.urlopen(req, timeout=10) as response:
    status = json.loads(response.read().decode("utf-8"))
if status.get("live_order_capability_disabled") is not True:
    raise SystemExit("refusing reset: live_order_capability_disabled is not true")
PY
  fi
  stop_all
  archive_stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p "$TRADING_ARCHIVE_DIR"
  archive_root="$(mktemp -d "$TRADING_ARCHIVE_DIR/${archive_stamp}.XXXXXX")"
  if [ -d "$TRADING_STATE_DIR" ]; then
    cp -a -- "$TRADING_STATE_DIR" "$archive_root/llm-equity-dry-run"
  fi
  (
    cd "$archive_root"
    find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum >SHA256SUMS
    if [ -s SHA256SUMS ]; then
      sha256sum -c SHA256SUMS >/dev/null
    fi
  )
  if [ -d "$resolved_trading_state" ]; then
    find "$resolved_trading_state" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
  fi
  mkdir -p "$TRADING_STATE_DIR"
  strategy_epoch_id >/dev/null
  echo "archive=$archive_root"
  echo "initial_cash_usd=10000"
  echo "risk_mode=forced_aggressive"
  echo "model_cost_basis=subscription_zero"
}

case "${1:-}" in
  start) start_all ;;
  stop) stop_all ;;
  status) status_all ;;
  smoke) smoke ;;
  reset-epoch) reset_epoch ;;
  urls) print_urls ;;
  -h|--help|"") usage ;;
  *) usage; exit 2 ;;
esac
