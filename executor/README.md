# Deterministic Strategy Executor

The executor is the Doing layer for reviewed strategist parameters. It runs as
a separate `traidan-executor` systemd user and is not part of the GitLab runner.

This v1 daemon is shadow-gate only: it validates reviewed parameters and live
approval gates, but it does not submit live exchange orders until a later
execution adapter wires `execute_rebalance()` into the process binary.

Live order intent is refused unless every gate passes:

- `TRAIDAN_ALLOW_LIVE_ORDERS=true`
- `/etc/traidan/live-execution-approved.json` exists, is fresh, matches
  `coinbase_fix` and `BTC-USD`, and caps notional at or above the strategy
- Coinbase FIX credentials are present only in the executor service environment
- `strategy_parameters.json` is fresh, valid, and has `execution_enabled=true`
- the kill-switch file is absent

The first production activation is manual only and remains blocked in this
branch. Install `executor/systemd/traidan-strategy-executor.service`, copy
reviewed parameters to the runtime path atomically, and keep
`TRAIDAN_ALLOW_LIVE_ORDERS=false` while validating shadow-gate behavior.
