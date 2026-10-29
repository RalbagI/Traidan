# Runtime reference

[← Back to Traidan](../README.md)

Detailed runtime behavior, policies, interfaces, and operational commands.
Run commands from the repository root.

## Runtime

- Market data source: `alpaca_equity`
- Default symbol: `SPY`
- Initial account: `$10,000` cash, no starting position
- Review model command prefix: `codex exec --model gpt-6-astra -c model_reasoning_effort="xhigh"`
- LLM CLI safety: one decision call plus at most two model-only recoveries, strict portfolio-review schema, ephemeral cwd, sanitized environment, read-only sandbox, all agent tools disabled
- Fill mode: the local launcher defaults to `paper` with `TRAIDAN_LLM_EQUITY_RISK_MODE=fully_invested`; direct service/binary starts remain `shadow` and bounded by default. Neither path can submit live capital.
- Allocation policy: `fully_invested_v1`, 20–40 investments, at most 5% NAV each, cash residue at most $0.01.
- Model cost basis: `TRAIDAN_LLM_EQUITY_MODEL_COST_BASIS=subscription_zero`; token usage remains visible, while operator-configured `imputed_api_cost_usd` is a sensitivity metric.
- Local service: `PYTHONPATH=intelligence python3 -m llm_equity.hourly_service run`
- Memory path: ignored `.agent/state/llm-equity-dry-run/`
- Startup behavior: reconcile any pending receipt, then allocate available cash at the next executable regular-session snapshot.

Core research anchors:

`SPY`, `QQQ`, `IWM`, `TLT`, `GLD`, `XLK`, `XLF`, `XLE`, `XLV`,
`AAPL`, `MSFT`, `NVDA`, `AMZN`, `GOOGL`, `META`, `TSLA`.

Every cycle expands beyond those anchors with active/tradable Alpaca U.S.
equities, supported top movers, and most-active stocks, then packages a ranked
research slate of up to 250 symbols. Current positions and prepared intent symbols are
always included in the release-time snapshot when syntactically valid.

## Resident Schedule

All cycle times use `America/New_York`.

| Time | Cycle |
| --- | --- |
| Every hour at `hh:05` | Public event scan; during RTH, review the complete allocation |
| 09:00 and 09:15 | Legacy preparation slots; the current allocation waits for executable RTH data |
| 09:30–09:35 | Revalidate the same opening slot when quotes and a completed bar are ready |
| 10:35, 12:35, and 14:35 | Broad discovery followed by a bounded model review |

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

Legacy `forced_aggressive` and `llm_discretion` remain readable for historical
replay. They are not the launcher default. No mode submits live broker orders.
Cycle-audit V3 records the policy epoch, forecast, source health, and actual
execution; V1/V2 data remains readable.

## Interfaces

Tracked schemas:

- `schemas/llm_portfolio_review_v1.schema.json`
- `schemas/llm_review_result_v3.schema.json` (legacy)
- `schemas/llm_trading_decision.schema.json`
- `schemas/llm_equity_cycle_audit_v3.schema.json`
- `schemas/market_research_snapshot.schema.json`
- `schemas/llm_memory_event.schema.json`

Rust endpoint:

- `POST /internal/llm-equity/cycle`
- `GET /admin/status` for authoritative simulated portfolio context
- localhost-only
- bearer token required
- payload fields: `market_research_snapshot`, `llm_trading_decision`

Dashboard WebSocket frames:

- `llm_equity_service_status`
- `llm_equity_cycle`
- `equity_order_simulated`
- `equity_portfolio_state`
- `market_research_snapshot`
- `llm_memory_update`
- `operator_event`

Research proof is stored in the hash-chained epoch ledger under
`.agent/state/llm-equity-dry-run/epochs/<epoch-id>/research-events.jsonl`. The deterministic
`llm_equity.replay` module isolates horizons and charges spread, impact,
commission, financing, borrow, configured fixed costs, and the selected model
cost basis, including shares assigned to intents that could not execute.

## Paper Autonomy And Operational Gates

Bounded paper authority mode enforces 75% gross exposure, 35% absolute net
exposure, 7.5% per name, 30% short gross, 25% cash reserve, 150% turnover,
1% daily loss, 5% drawdown limits, and the same $700 gross-exposure floor. New shorts require current shortable
metadata. Rust remains authoritative for strict schema
validation, unknown/raw-field rejection, credential/raw-content rejection,
replay protection, timestamp windows, regular-session release, freshness
checks, command-symbol price availability, fills, accounting, portfolio status,
and telemetry.

The LLM decision contract is intentionally directional: each accepted sweep must
emit `buy_notional` or `sell_notional` with `risk_limit_verdict: pass` and at
least one command of `$25` or more. The Python service rejects without paid repair
decisions that use anything outside the five passing review candidates, emit
more than three commands, or ignore learned sizing. The LLM records data/session concerns in
rationale and risk text; Rust can reject the command for malformed payloads,
stale context, closed sessions, replay, timestamp mismatch, missing command
prices, or credential/raw-content violations.

## Source Expansion And Profitability Gate

`config/alt_data/source_catalog.json` remains the 50-source research catalog.
`config/alt_data/telegram_watchlist.json` is additive and currently allows only
public, verified, metadata-summary-only Telegram sources such as
`telegram_tradingview`. All expanded sources start with paper context disabled
and live disabled until fixture tests, legal/storage review, source-health
metrics, and freshness checks pass.

Break-even is tracked as model cost plus data subscriptions, hosting,
estimated spread/slippage/borrow fees, and a safety margin. Live capital remains
disabled until at least 90 trading days of replayable paper results show
positive absolute net P&L after costs and outperformance versus SPY and QQQ.
Equal-weight comparison, PSR, DSR, and confidence intervals are diagnostics,
not promotion thresholds. In the default `subscription_zero` basis, Codex
subscription inference has zero marginal strategy cost; token counts and the
operator-configured call-cost sensitivity remain reported as
`imputed_api_cost_usd`. It is zero unless
`TRAIDAN_LLM_EQUITY_EFFECTIVE_CALL_COST_USD` is set; the service does not infer a
provider list price from token counts. Set
`TRAIDAN_LLM_EQUITY_MODEL_COST_BASIS=configured_api` to charge the configured
effective CLI call cost. Monthly cost overrides use
`TRAIDAN_LLM_EQUITY_MONTHLY_*` names with legacy `TRAIDAN_LLM_MONTHLY_*`
fallbacks so the prompt and Prometheus break-even views stay aligned.
If an epoch contains model invocations but both actual and imputed model cost
remain zero, its profitability report marks `cost_unknown` and fails the
promotion threshold.

## Local Run

Set Alpaca credentials for live market data:

```bash
export ALPACA_API_KEY_ID=...
export ALPACA_API_SECRET_KEY=...
export ALPACA_TRADING_BASE_URL=https://paper-api.alpaca.markets
```

For the local dry-run script, credentials can also live in the ignored runtime
file `.agent/state/local-dry-run/alpaca.env`:

```bash
ALPACA_API_KEY_ID=...
ALPACA_API_SECRET_KEY=...
ALPACA_TRADING_BASE_URL=https://paper-api.alpaca.markets/v2
```

Optional context settings for the same ignored file or process environment:

```bash
TRAIDAN_LLM_EQUITY_CONTEXT_AUTO_FETCH=1
TRAIDAN_LLM_EQUITY_CONTEXT_TIMEOUT_SECONDS=8
TRAIDAN_LLM_EQUITY_MACRO_FEED_URLS='["https://www.federalreserve.gov/feeds/press_monetary.xml","https://www.census.gov/economic-indicators/indicator.xml"]'
TRAIDAN_LLM_EQUITY_NEWS_FEED_URLS='["https://feeds.finance.yahoo.com/rss/2.0/headline?s=SPY,QQQ,IWM&region=US","https://www.cnbc.com/id/100003114/device/rss/rss.html"]'
```

Use a JSON array for multiple feed URLs. Newline-delimited URLs are also
accepted, while a single URL is preserved verbatim, including commas in its
query string. Migrate legacy comma-separated multi-feed values to one of these
two formats.

Manual `TRAIDAN_LLM_EQUITY_MACRO_SUMMARY` and
`TRAIDAN_LLM_EQUITY_NEWS_SUMMARY` values override public fetching for that
row. The runtime stores only source URLs, retrieval time, freshness, and short
summaries in market snapshots.

Admin token setup and loopback binding requirements are in
[`deployment/operator-runbook.md`](../deployment/operator-runbook.md).

Start the local stack:

```bash
scripts/local-dry-run.sh start
```

Check whether the hourly service is running in live or fixture mode:

```bash
scripts/local-dry-run.sh status
```

For an existing account, stop the paper services and migrate the policy epoch
before restarting. This preserves cash, positions, replay IDs, peaks and risk
counters; `reset-epoch` is an explicit legacy account reset and is not a policy
migration.

```bash
scripts/local-dry-run.sh stop
TRAIDAN_LLM_EQUITY_FILL_MODE=paper TRAIDAN_LLM_EQUITY_RISK_MODE=fully_invested \
  PYTHONPATH=intelligence python3 -m llm_equity.hourly_service migrate-policy-epoch \
  --state-dir .agent/state/llm-equity-dry-run
scripts/local-dry-run.sh start
```

Generate the epoch profitability report in text or JSON:

```bash
PYTHONPATH=intelligence python3 -m llm_equity.hourly_service profitability-report \
  --state-dir .agent/state/llm-equity-dry-run --epoch latest \
  --start 2026-09-14 --end 2026-10-10 --format text
```

The report keeps the eligible-fresh `sol_invocation_rate` and also exposes
`observed_rth_cycles`, `hard_gate_blocked_rth_cycles`, and
`rth_sol_invocation_rate`. The JSON `hard_gate_blocks` array lists safely
blocked RTH outages while the text report prints its count;
`hard_gate_violations` remains reserved for stale or closed-session fills that
were incorrectly accepted. A cycle that begins during RTH but receives its
market snapshot after the close is correctly classified as after-hours and
excluded from the RTH denominator. An RTH cycle blocked by a fresh authoritative
closed market clock, such as an exchange holiday or early close, remains in the
observed denominator and is identified as a hard-gate block.

Useful URLs:

| Surface | URL |
| --- | --- |
| Dashboard | `http://127.0.0.1:5173` |
| WebSocket | `ws://127.0.0.1:8080/stream` |
| Admin API | `http://127.0.0.1:8090/admin/status` |
| Metrics | `http://127.0.0.1:9100/metrics` |

Fixture smoke:

```bash
scripts/local-dry-run.sh smoke
```

The smoke path disables the automatic bootstrap, posts one fixture LLM cycle to
Rust, and verifies dashboard frames without broker order submission. It forces
a fixture command so the WebSocket check covers simulated order/rejection
telemetry even when smoke runs outside regular U.S. market hours. Smoke runs
in an isolated fixture stack; if a stack was already running, the script stops
the fixture stack and restarts the normal hourly service afterward.

One direct fixture cycle can also be run with:

```bash
PYTHONPATH=intelligence python3 -m llm_equity.hourly_service run-once --fixture
```

## Verification

```bash
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all --locked
python -m ruff check .
python -m mypy intelligence/
pytest
(cd frontend && npm test && npm run build)
```

Profit-policy recovery uses reduce-only closure when a held position has no
valid matching stop/expiry plan. Migration seeds carried positions with a 5%
stop and a one-calendar-day review deadline, preserving the opening account.
Activation requires the completed transition receipt and matching carried
account; an interrupted migration remains blocked. Accepted fill receipts are
recorded before derivative holding plans, so a plan-write failure retains
accounting evidence.
`close` holdings expire five minutes before the authoritative session close;
their forecasts settle at that close, including shortened sessions.
1d/5d are calendar-day horizons and execute at the next eligible RTH quote if
the deadline falls outside trading hours. Forecasts without a price in their
five-minute settlement window remain unresolved; they never receive a later
price. Dedicated completed minute bars support settlement independently of
optional hourly technical indicators. Closing-session evidence uses verified
pre-close marks; later dashboard telemetry cannot replace it.
Forward promotion always uses the registered start through the assessment
cutoff. Selecting a narrower report window cannot discard forward losses.
Eligibility also requires positive total forward economic P&L and all costs
allocated to completed paired sessions; later or intraday costs keep it pending.
An explicit report cutoff accrues recurring fixed, borrowing and financing
costs through that cutoff, even when no newer valuation mark is available.

