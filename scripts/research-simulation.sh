#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ALPACA_ENV_FILE="${TRAIDAN_ALPACA_ENV_FILE:-$ROOT/.agent/state/local-dry-run/alpaca.env}"

load_research_env() {
  if [ ! -r "$ALPACA_ENV_FILE" ]; then
    return
  fi
  while IFS='=' read -r key value; do
    case "$key" in
      ALPACA_API_KEY_ID|ALPACA_API_SECRET_KEY|APCA_API_KEY_ID|APCA_API_SECRET_KEY|ALPACA_DATA_BASE_URL|ALPACA_TRADING_BASE_URL|ALPACA_PAPER_API_BASE_URL|ALPACA_DATA_FEED)
        export "$key=$value"
        ;;
      TRAIDAN_LLM_EQUITY_EFFECTIVE_CALL_COST_USD|TRAIDAN_LLM_EQUITY_MODEL_COST_BASIS)
        export "$key=$value"
        ;;
    esac
  done <"$ALPACA_ENV_FILE"
}

load_research_env
export PYTHONPATH="$ROOT/intelligence${PYTHONPATH:+:$PYTHONPATH}"
cd "$ROOT"
exec python3 -m llm_equity.research_simulation "$@"
