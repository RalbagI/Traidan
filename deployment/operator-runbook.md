# Operator Runbook

This runbook covers Traidan's resident simulated equity research service. The system uses
live U.S. equity data, a paper-only USD ledger, and autonomous LLM commands that
are validated by Rust before any simulated fill is recorded.

## Operating Boundary

| Surface | Location | Exposure | Purpose |
|---------|----------|----------|---------|
| Dashboard | `https://dashboard.example.com` | Public static Hosting | Read-only LLM Equity Desk. |
| Public WSS | `wss://stream.example.com/stream` | Caddy on `tcp/443` | Streams sanitized dry-run state. |
| Local admin API | `http://127.0.0.1:8090` | Operator workstation only | Authenticated LLM cycle ingestion/status. |
| Rust metrics | `http://127.0.0.1:9100/metrics` | Private Prometheus only | Runtime and dashboard health metrics. |
| Hourly LLM service | `PYTHONPATH=intelligence python3 -m llm_equity.hourly_service run` | Host-local process | Builds hourly context, calls Codex, posts decisions. |

Caddy must proxy only `/stream`. It must not expose `/internal/*`, `/admin/*`,
metrics, raw Rust ports, credentials, prompts, or runtime memory files.

## Dry-Run Startup

Start the local dry-run from the repository root:

```bash
scripts/local-dry-run.sh start
```

Confirm the runtime mode and data readiness:

```bash
scripts/local-dry-run.sh status
```

To begin a clean measured epoch, archive the current mutable paper state and
write its SHA-256 manifest before initializing a new `$10,000` portfolio:

```bash
scripts/local-dry-run.sh reset-epoch
```

The archive is retained under `.agent/state/llm-equity-archives/`; keep it for
rollback and inspection. It contains trading state and its manifest, not the
local admin token, Alpaca env file, or process logs. The command refuses to
proceed when reachable runtime status does not confirm that live-order
capability is disabled.

After market close, publish the 1-day, 30-day, and full-epoch report:

```bash
PYTHONPATH=intelligence python3 -m llm_equity.hourly_service profitability-report \
  --state-dir .agent/state/llm-equity-dry-run --epoch latest --format text
```

Use `--format json` for automation or `--epoch epoch-...` for a preserved
epoch. Subscription inference is zero marginal cost by default; token usage and
`imputed_api_cost_usd` remain diagnostic sensitivity values. The imputed value
is zero unless `TRAIDAN_LLM_EQUITY_EFFECTIVE_CALL_COST_USD` explicitly supplies
the operator's comparison cost; token counts do not imply a provider list rate.
An epoch with model invocations and neither actual nor imputed model cost is
reported as `cost_unknown` and is ineligible for promotion evidence.

The epoch fingerprint includes the configured review-context byte budget.
Changing `TRAIDAN_LLM_EQUITY_REVIEW_CONTEXT_MAX_BYTES` therefore selects a new,
neutral epoch with fresh benchmark marks; preserve the prior epoch for
inspection or rollback before restarting the resident.

The script defaults to:

```bash
TRAIDAN_RUN_MODE=dry_run
TRAIDAN_MARKET_DATA_SOURCE=alpaca_equity
TRAIDAN_MARKET_SYMBOL=SPY
```

The Rust authority seeds the portfolio with exactly `$10,000` cash and no
positions. No simulated order is created until the hourly service completes a
cycle and Rust accepts the decision.

Outside an eligible fresh New York schedule slot, the hourly service publishes
a zero-cost waiting heartbeat with runtime identity. It does not build market
context, call the model, or post a trading cycle merely because the process
started.
If Alpaca market data or the clock is unavailable, the service emits a validated
snapshot with unavailable flags so the desk shows an explicit rejection reason.
Macro and news context is fetched on every executed startup or scheduled
research cycle by default. The service stores only URL, retrieval time,
freshness, and compact summaries; failed public feed retrieval leaves
`macro_calendar` or `news_summary` unavailable and blocks accepted fills.

Use fixture mode for a local smoke without Alpaca credentials:

```bash
TRAIDAN_LLM_EQUITY_FIXTURE_MODE=1 scripts/local-dry-run.sh smoke
```

The smoke command disables startup bootstrap and posts one deterministic
fixture cycle itself, which keeps the WebSocket verification stable. It forces
a fixture command so the WebSocket check covers simulated order/rejection
telemetry even outside regular U.S. market hours. Smoke owns its fixture stack:
it stops the current local processes, verifies the fixture cycle, and restarts
the normal hourly service afterward when a stack had been running.

Use live data only when the Alpaca keys are present in the environment:

```bash
export ALPACA_API_KEY_ID=...
export ALPACA_API_SECRET_KEY=...
export ALPACA_TRADING_BASE_URL=https://paper-api.alpaca.markets
export TRAIDAN_LLM_EQUITY_FIXTURE_MODE=0
PYTHONPATH=intelligence python3 -m llm_equity.hourly_service run
```

For `scripts/local-dry-run.sh`, the same Alpaca settings may be stored in the
ignored runtime file `.agent/state/local-dry-run/alpaca.env`. The loader accepts
either `https://paper-api.alpaca.markets` or `https://paper-api.alpaca.markets/v2`
and normalizes the internal clock request to `/v2/clock`.

The same ignored env file may also contain optional public context settings:

```bash
TRAIDAN_LLM_EQUITY_CONTEXT_AUTO_FETCH=1
TRAIDAN_LLM_EQUITY_CONTEXT_TIMEOUT_SECONDS=8
# Set a documented positive comparison cost before collecting promotion evidence.
# Example only: replace 0.05 with the operator-approved per-call sensitivity.
TRAIDAN_LLM_EQUITY_EFFECTIVE_CALL_COST_USD=0.05
TRAIDAN_LLM_EQUITY_MODEL_COST_BASIS=subscription_zero
TRAIDAN_LLM_EQUITY_MACRO_FEED_URLS='["https://www.federalreserve.gov/feeds/press_monetary.xml","https://www.census.gov/economic-indicators/indicator.xml"]'
TRAIDAN_LLM_EQUITY_NEWS_FEED_URLS='["https://feeds.finance.yahoo.com/rss/2.0/headline?s=SPY,QQQ,IWM&region=US","https://www.cnbc.com/id/100003114/device/rss/rss.html"]'
```

Use a JSON array for multiple feed URLs. Newline-delimited URLs are also
accepted, and a single URL remains intact when its query contains commas.
Legacy comma-separated multi-feed values must be migrated.

Manual `TRAIDAN_LLM_EQUITY_MACRO_SUMMARY` or
`TRAIDAN_LLM_EQUITY_NEWS_SUMMARY` values override fetching for that context
row. Use manual summaries only for controlled outage drills or fixture-style
local tests.

The service never submits broker orders. It posts only to the local Rust
ingestion endpoint.

The local launcher exports `TRAIDAN_LLM_EQUITY_FILL_MODE=paper` and
`TRAIDAN_LLM_EQUITY_RISK_MODE=fully_invested` by default. Direct Rust starts
remain shadow and bounded unless explicitly configured. No mode submits live
broker orders.

## Fully invested paper operation

The default `paper + fully_invested` policy invests all current account NAV,
including gains, with at most $0.01 rounding residue. The model selects 20–40
stocks or approved equity ETFs with weights totaling 100%; each position is at
most 5% NAV at rebalance. The account is long-only, with no borrowing or cash,
Treasury, or cash-equivalent funds. Negative forecasts must be reported honestly
and do not permit abstention. The 3% daily-loss and 10% drawdown measurements
remain visible but do not block investment.

Every sale is part of a complete replacement portfolio. Rust solves quantities
after spread and modeled impact, validates all prices and the current account,
and commits the entire rebalance together. A rejected replacement preserves all
existing positions. The existing 4× NAV daily turnover cap still applies. The
model can keep the same allocation; the service does not require trading merely
to produce activity.

The service checks every 30 seconds for available cash, scheduled reviews,
5% price-stop triggers, and expired theses. Stops and expiry request a replacement
review, never automatic liquidation. Reviews use up to 100 eligible candidates
in a 96 KB packet and allow one model call plus two repairs. Quotes are refreshed
before execution; a move over 25 bps requires fresh model approval of the same
allocation at the new quotes. That short approval includes the original theses,
incoming and outgoing prices, and current evidence, and is bound to their digest.
A rejection triggers a new full selection. Quotes are checked again after approval,
and the complete cycle remains limited to three model invocations. Full selections
have a 600-second cap; price approvals have a 180-second cap, both respecting any
shorter configured timeout. A validated selection awaiting price approval survives
a cycle boundary or service restart for up to 15 minutes. The next cycle retries
approval after 30 seconds instead of discarding that selection; it still needs a
fresh model approval and executable quotes. Expired or newly ineligible selections
return to full selection. Original selection and later approval remain linked in
the audit, with each invocation counted in its own cycle. Unavailable data,
a closed market, or a failed model review leaves existing investments intact
and publishes the actual blocked reason. Any cash awaiting its initial allocation
is shown as incomplete, with retries during fresh, open regular-session trading.
There is no deterministic ETF fallback.

Each accepted allocation has a durable Rust receipt. An interrupted submission
retries the exact payload, including after restart, without executing it twice.
The audit retains every accepted order and the model's review and repair reasons.
Policy migration preserves the actual account and does not assign blanket expiry
times to inherited holdings. Historical `profit_driven` records remain readable.

For existing state, use the stopped-writer `migrate-policy-epoch` command in the
README before starting the new default. Keep `policy-transition.json`, its
source hash, and `account-opening-state.json` with the account. Never use
`reset-epoch` to change a strategy policy. The legacy forced-aggressive envelope
remains available only when explicitly selected.

After activation, read back paper mode, disabled live capability, matching Rust
and Python commit identities, and the same cash/quantities/sequence as the
migration receipt. Generate `profitability-report` for an explicit half-open
window. An absent `economic-cost-policy.json` blocks economic-profit and
promotion claims. Trial comparisons write into a fresh research output
directory and cannot mutate the resident account.

`audit-summary` includes the optional `review_attestation` fields: requirement
and compliance status, model/effort, invocation count, selected action/notional,
Rust-accepted side/notional, and cumulative accepted total. Use
`mandatory_trade_status` for the independent accepted/blocked execution outcome.

Tracked prompts, source files, and docs are not silently edited by the runtime
service. Runtime memory and prompt self-tuning live only under:

```text
.agent/state/llm-equity-dry-run/
```

## Admin Token

Use a high-entropy token stored outside git:

```bash
install -m 0700 -d "/run/user/$UID"
python3 - <<'PY' > "/run/user/$UID/traidan-admin-token"
import secrets
print(secrets.token_urlsafe(48))
PY
chmod 0600 "/run/user/$UID/traidan-admin-token"
export TRAIDAN_ADMIN_TOKEN_FILE="/run/user/$UID/traidan-admin-token"
```

The Rust admin API enforces a token of at least 32 visible ASCII characters and
must stay bound to loopback:

```bash
export TRAIDAN_ADMIN_BIND_ADDR=127.0.0.1:8090
```

Never bind the admin API to a public or LAN address.

## LLM Cycle Ingestion

The hourly service posts:

```text
POST /internal/llm-equity/cycle
```

The body contains both:

```text
market_research_snapshot
llm_trading_decision
```

Rust always validates schema, typed forecast bounds/order, credential/raw-field safety, sequence/replay,
timestamp windows, authoritative open RTH, data freshness, command-symbol
price availability, executable quotes, and positive NAV. Bounded and
`forced_aggressive` modes enforce borrow metadata, exposure, cash, turnover,
loss, and drawdown; legacy `llm_discretion` intentionally does not. Shadow mode returns
`fills_frozen_shadow_only`.

Before each prompt, the hourly service reads `GET /admin/status` and uses the
Rust authority's simulated cash, positions, turnover, and risk accounting as
the portfolio context. The LLM does not reseed a fresh cash-only account after
the first accepted cycle.

## Paper Autonomy

The v1 dry-run is simulated-money only and allows mandatory long/short
directional orders across the dynamic Alpaca U.S. equity research slate:

| Surface | Value |
|-------|-------|
| Market scope | Active/tradable Alpaca U.S. equities |
| Research slate | Max-discovery top `250` plus held/prepared command symbols |
| Static universe gate | none |
| Rust risk gates | `forced_aggressive`: 15% name, 125% gross, 75% absolute net, 60% short gross, 10% high-correlation proxy-group gross, 5% cash reserve, 400% daily turnover, 3% daily loss, 10% drawdown; bounded mode retains its smaller limits |
| Minimum order notional | Every postable decision includes one `$25+` intent; mandatory accepted totals are `$25` (news) or `$100` (release/discovery) |
| Turnover cap | 400% NAV per enforced window in `forced_aggressive` |

Out-of-session orders, options, stale data, unpriced command symbols, invalid
symbol syntax, and schema-invalid commands are rejected.
Every invoked LLM sweep must emit a `buy_notional` or `sell_notional` command
with `risk_limit_verdict: pass`; a no-edge cycle does not invoke it. That verdict
is an intent contract, not execution approval. Rust is the only component that
accepts simulated fills.

## Dashboard Checks

The first screen is the LLM Equity Desk. It should show:

- service status
- NAV, cash, gross exposure, daily PnL, and drawdown
- current positions
- latest thesis, command rationale, evidence freshness, risk assessment, and
  memory conclusion
- simulated orders and rejection reasons

The browser dashboard is Hebrew-first and RTL. Visible dashboard prose,
backend-authored service `detail` text, LLM rationales, evidence summaries, risk
summaries, and memory prose should render naturally in Hebrew. Protocol frame
names, enum/status/reason codes, tickers, URLs, IDs, command values, `USD`, `NAV`,
`P&L`, `Admin API`, `WebSocket`, `Rust`, `LLM`, and `GPT-6 Astra xhigh` remain stable
English/LTR tokens.

Cycle activity and decision traces include each action's date and time in
Israel time (`Asia/Jerusalem`), including retained activity from previous days.

Finite zero or negative paper NAV must remain visible in the Portfolio tile as
an impaired NAV state. It must not be treated as backend unavailability or reset
to the initial `$10,000` cash baseline.

`Unavailable` means the backend stream or service is disconnected. Missing
market, macro, news, or LLM data must show an explicit stale/rejection reason instead
of a generic unavailable state.

Run the local browser smoke after dashboard changes:

```bash
cd frontend
npm run test:dashboard-screenshots
```

Run the hosted smoke for a deployed build:

```bash
cd frontend
npm run smoke:hosted-dashboard -- \
  --url https://dashboard.example.com \
  --wss-url wss://stream.example.com/stream
```

## Public WSS

The hosted dashboard must use:

```bash
VITE_TRAIDAN_WS_URL=wss://stream.example.com/stream
FIREBASE_PREVIEW_WS_URL=wss://stream.example.com/stream
```

Health check:

```bash
node scripts/check-public-wss.js wss://stream.example.com/stream
```

Expected stream frame types are additive and sanitized:

```text
llm_equity_service_status
equity_portfolio_state
market_research_snapshot
llm_equity_cycle
equity_order_simulated
llm_memory_update
operator_event
```

Raw prompts, full article text, credentials, and raw LLM logs must never appear
in dashboard frames, logs, or stored research snapshots.

Operating-cost exclusion is an explicit owner choice in `economic-cost-policy.json`: set
`exclude_operating_costs: true` and all three monthly allocations to zero. The
report labels this `excluded_by_owner`; it still charges spread/impact embedded
in fills, configured commissions, short borrowing, and margin financing. An
epoch inherits the state-directory policy only when it has no local policy.
Session costs follow observed fill and carrying intervals. This metric measures
trading profit excluding operating overhead, rather than total business profit.

Profit-policy recovery uses reduce-only closure when a held position has no
valid matching stop/expiry plan. Migration seeds carried positions with a 5%
stop and a one-calendar-day review deadline, preserving the opening account.
`close` holdings expire five minutes before the authoritative session close;
1d/5d are calendar-day horizons and execute at the next eligible RTH quote if
the deadline falls outside trading hours. Forecasts without a price in their
five-minute settlement window remain unresolved; they never receive a later
price. Dedicated completed minute bars support settlement independently of
optional hourly technical indicators. Closing-session evidence uses verified
pre-close marks; later dashboard telemetry cannot replace it.
