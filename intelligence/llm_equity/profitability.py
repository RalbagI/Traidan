"""Epoch-scoped profitability reporting for forced-aggressive paper trading."""

from __future__ import annotations

import json
import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .accounting import account_report, deduplicate_audits, timestamp
from .contracts import FORCED_RTH_CYCLE_KINDS, LlmEquityContractError
from .experiments import promotion_gate
from .replay import (
    PointInTimeEventStore,
    ReplayConfig,
    ReplayIntent,
    ReplayObservation,
    bootstrap_mean_confidence_interval,
    deflated_sharpe_ratio,
    probabilistic_sharpe_ratio,
    replay_inputs_from_event_store,
    run_replay,
)

INITIAL_NAV_USD = 10_000.0
EPOCH_ID_PATTERN_PREFIX = "epoch-"


def build_profitability_report(
    state_dir: Path, *, epoch: str = "latest", start: str | None = None, end: str | None = None,
) -> dict[str, Any]:
    requested_start = start
    epoch_id, epoch_dir, descriptor = _resolve_epoch(state_dir, epoch)
    event_path = epoch_dir / "research-events.jsonl"
    inputs = replay_inputs_from_event_store(PointInTimeEventStore(event_path))
    events = PointInTimeEventStore(event_path).read_all()
    ledger_audits = [event["payload"] for event in events if event["event_type"] == "cycle_audit"]
    audit_rows = deduplicate_audits(
        ledger_audits or _read_json_rows(epoch_dir / "cycle-audit-ledger.json")
    )
    account = account_report(epoch_dir, events, start=start, end=end)
    latest_at = _latest_timestamp(inputs.observations, audit_rows)
    first_at = _first_timestamp(inputs.observations, audit_rows, fallback=latest_at)
    full_days = max((latest_at - first_at).total_seconds() / 86_400.0, 1.0)
    fixed_cost_full = _configured_fixed_cost(audit_rows, elapsed_days=full_days)
    windows: dict[str, Any] = {}
    for label, days in (("1d", 1), ("30d", 30), ("full_epoch", None)):
        window_start = first_at if days is None else latest_at - timedelta(days=days)
        observations = [row for row in inputs.observations if row.as_of >= window_start]
        intents = [row for row in inputs.intents if row.as_of >= window_start]
        fixed_cost = fixed_cost_full * min(
            max((latest_at - window_start).total_seconds() / 86_400.0, 1.0),
            full_days,
        ) / full_days
        unattributed = _window_unattributed_cost(
            inputs.unattributed_model_cost_usd,
            events=inputs.unattributed_model_cost_events,
            start=window_start,
            intents=len(intents),
            all_intents=len(inputs.intents),
            full_window=days is None,
        )
        replay = run_replay(
            observations,
            intents,
            config=ReplayConfig(configured_fixed_cost_usd=fixed_cost),
            unattributed_model_cost_usd=unattributed,
        )
        windows[label] = _window_report(
            replay,
            observations=observations,
            intents=intents,
            imputed_api_cost_usd=_window_imputed_cost(
                inputs.total_imputed_api_cost_usd,
                events=inputs.imputed_api_cost_events,
                start=window_start,
                intents=len(intents),
                all_intents=len(inputs.intents),
                full_window=days is None,
            ),
        )
    full = windows["full_epoch"]
    accepted_rows = [row for row in audit_rows if row.get("mandatory_trade_required") is True]
    accepted_count = sum(row.get("mandatory_trade_status") == "accepted" for row in accepted_rows)
    invoked_count = sum(row.get("llm_invoked") is True for row in accepted_rows)
    observed_rth_rows = [row for row in audit_rows if _is_observed_rth_cycle(row)]
    invoked_rth_count = sum(row.get("llm_invoked") is True for row in observed_rth_rows)
    hard_gate_blocks = _hard_gate_blocks(observed_rth_rows)
    hard_gate_violations = _hard_gate_violations(audit_rows)
    risk_envelope_breaches = _risk_envelope_breaches(audit_rows)
    model_cost_integrity = _model_cost_integrity(audit_rows, full)
    if account.get("economic", {}).get("operating_costs") == "excluded_by_owner":
        model_cost_integrity.update(status="excluded_by_owner", promotion_eligible=True)
    economic_pnl = account.get("economic", {}).get("net_pnl_usd")
    economic_return = (
        float(economic_pnl) / float(account["opening_nav_usd"])
        if economic_pnl is not None and account.get("opening_nav_usd", 0) > 0 else None
    )
    benchmarks = account.get("benchmarks", {})
    success = {
        "positive_absolute_net_pnl": economic_pnl is not None and economic_pnl > 0,
        "beats_spy": economic_return is not None and "SPY" in benchmarks
        and benchmarks["SPY"].get("prior_day_gross_matched_return_fraction") is not None
        and economic_return > benchmarks["SPY"]["prior_day_gross_matched_return_fraction"],
        "beats_qqq": economic_return is not None and "QQQ" in benchmarks
        and benchmarks["QQQ"].get("prior_day_gross_matched_return_fraction") is not None
        and economic_return > benchmarks["QQQ"]["prior_day_gross_matched_return_fraction"],
        "zero_hard_gate_violations": not hard_gate_violations,
        "zero_risk_envelope_breaches": not risk_envelope_breaches,
        "known_actual_or_imputed_model_cost": bool(
            model_cost_integrity["promotion_eligible"]
        ),
        "account_reconciled_and_costed": bool(account["promotion_eligible"]),
    }
    registry_path = epoch_dir / "profit-trial-registry.json"
    forward = {"eligible": False, "status": "trials_not_registered"}
    if registry_path.exists():
        registry = json.loads(registry_path.read_text())
        cutoff = min(timestamp(end), datetime.now(UTC)) if end else datetime.now(UTC)
        settled = PointInTimeEventStore(epoch_dir / "forecast-outcomes.jsonl").read_all()
        identities = {(row["payload"].get("cycle_id"), row["payload"].get("command_index"))
            for row in settled if row["event_type"] == "entry_forecast_settled"
            and str(row["payload"].get("opened_at", "")) >= str(registry["forward_start"])
            and timestamp(str(row["payload"]["settled_at"])) < cutoff
            and timestamp(str(row["observed_at"])) < cutoff}
        forward_account = (account_report(epoch_dir, events, start=str(registry["forward_start"]), end=cutoff.isoformat())
            if timestamp(str(registry["forward_start"])) < cutoff
            else {"promotion_eligible": False, "sessions": []})
        forward = promotion_gate(forward_account, registry, matured_forecasts=len(identities), as_of=cutoff)
    return {
        "schema_version": 2,
        "primary_basis": "authority_account",
        "forward_promotion": forward,
        "account": account,
        "requested_window": {"start_inclusive": requested_start, "end_exclusive": end},
        "legacy_windows_basis": "counterfactual_horizon_round_trips_not_account_pnl",
        "strategy_epoch_id": epoch_id,
        "strategy_fingerprint": descriptor.get("fingerprint"),
        "generated_at": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
        "period": {
            "started_at": first_at.isoformat().replace("+00:00", "Z"),
            "ended_at": latest_at.isoformat().replace("+00:00", "Z"),
            "elapsed_days": round(full_days, 6),
        },
        "windows": windows,
        "forced_cycle_accounting": {
            "eligible_cycles": len(accepted_rows),
            "accepted_cycles": accepted_count,
            "accepted_fill_rate": round(accepted_count / len(accepted_rows), 6)
            if accepted_rows
            else 0.0,
            "sol_invocation_rate": round(invoked_count / len(accepted_rows), 6)
            if accepted_rows
            else 0.0,
            "observed_rth_cycles": len(observed_rth_rows),
            "hard_gate_blocked_rth_cycles": len(hard_gate_blocks),
            "rth_sol_invocation_rate": round(invoked_rth_count / len(observed_rth_rows), 6)
            if observed_rth_rows
            else 0.0,
        },
        "hard_gate_blocks": hard_gate_blocks,
        "hard_gate_violations": hard_gate_violations,
        "risk_envelope_breaches": risk_envelope_breaches,
        "model_cost_integrity": model_cost_integrity,
        "success_thresholds": success,
        "promotion_diagnostics_only": [
            "probabilistic_sharpe_ratio",
            "deflated_sharpe_ratio",
            "bootstrap_mean_return_confidence_interval",
            "equal_weight_benchmark",
        ],
    }


def profitability_report_text(report: Mapping[str, Any]) -> list[str]:
    windows = report.get("windows")
    rows = windows if isinstance(windows, Mapping) else {}
    lines = [
        f"strategy_epoch_id={report.get('strategy_epoch_id')}",
        f"strategy_fingerprint={report.get('strategy_fingerprint')}",
    ]
    account = report.get("account")
    if isinstance(account, Mapping):
        lines.append(f"account_status={account.get('status')}")
        if account.get("status") == "available":
            lines.append(
                f"account_pnl_usd={float(account['account_pnl_usd']):.2f} "
                f"economic_pnl_usd={account['economic']['net_pnl_usd']} "
                f"reconciliation={account['reconciliation']['status']}"
            )
    lines.append("legacy_windows_basis=counterfactual_horizon_round_trips_not_account_pnl")
    for label in ("1d", "30d", "full_epoch"):
        window = rows.get(label)
        if not isinstance(window, Mapping):
            continue
        benchmarks = window.get("benchmarks")
        benchmark_rows = benchmarks if isinstance(benchmarks, Mapping) else {}
        lines.append(
            f"forecast_{label}: net_pnl_usd={float(window.get('net_pnl_usd', 0.0)):.2f} "
            f"return_pct={float(window.get('return_pct', 0.0)):.6f} "
            f"spy_pct={_benchmark_return(benchmark_rows, 'SPY'):.6f} "
            f"qqq_pct={_benchmark_return(benchmark_rows, 'QQQ'):.6f} "
            f"turnover_usd={float(window.get('turnover_usd', 0.0)):.2f}"
        )
    accounting = report.get("forced_cycle_accounting")
    if isinstance(accounting, Mapping):
        lines.append(
            "forced_cycles: "
            f"eligible={accounting.get('eligible_cycles', 0)} "
            f"accepted_rate={float(accounting.get('accepted_fill_rate', 0.0)):.6f} "
            f"sol_invocation_rate={float(accounting.get('sol_invocation_rate', 0.0)):.6f} "
            f"observed_rth={accounting.get('observed_rth_cycles', 0)} "
            f"hard_gate_blocked={accounting.get('hard_gate_blocked_rth_cycles', 0)} "
            f"rth_sol_invocation_rate="
            f"{float(accounting.get('rth_sol_invocation_rate', 0.0)):.6f}"
        )
    lines.append(f"hard_gate_blocks={len(report.get('hard_gate_blocks', ()))}")
    lines.append(f"hard_gate_violations={len(report.get('hard_gate_violations', ()))}")
    lines.append(
        f"risk_envelope_breaches={len(report.get('risk_envelope_breaches', ()))}"
    )
    model_cost_integrity = report.get("model_cost_integrity")
    if isinstance(model_cost_integrity, Mapping):
        lines.append(
            "model_cost_integrity: "
            f"status={model_cost_integrity.get('status')} "
            f"invocations={model_cost_integrity.get('invocation_count', 0)} "
            f"promotion_eligible={model_cost_integrity.get('promotion_eligible')}"
        )
    return lines


def _window_report(
    replay: Any,
    *,
    observations: Sequence[ReplayObservation],
    intents: Sequence[ReplayIntent],
    imputed_api_cost_usd: float,
) -> dict[str, Any]:
    trades = [trade for sleeve in replay.sleeves.values() for trade in sleeve.trades]
    trade_returns = [trade.net_pnl_usd / INITIAL_NAV_USD for trade in trades]
    trial_count = max(
        len({(trade.horizon, trade.direction, trade.edge_class) for trade in trades}),
        1,
    )
    confidence_interval = (
        bootstrap_mean_confidence_interval(trade_returns)
        if trade_returns
        else (0.0, 0.0)
    )
    costs = {
        "spread_usd": sum(trade.spread_cost_usd for trade in trades),
        "impact_usd": sum(trade.impact_cost_usd for trade in trades),
        "commission_usd": sum(trade.commission_usd for trade in trades),
        "borrow_usd": sum(trade.borrow_cost_usd for trade in trades),
        "financing_usd": sum(trade.financing_cost_usd for trade in trades),
        "model_usd": replay.total_model_cost_usd,
        "configured_fixed_usd": replay.configured_fixed_cost_usd,
        "imputed_api_cost_usd": imputed_api_cost_usd,
    }
    return_pct = (replay.ending_nav_usd - INITIAL_NAV_USD) / INITIAL_NAV_USD
    return {
        "intent_count": len(intents),
        "trade_count": len(trades),
        "net_pnl_usd": round(replay.total_net_pnl_usd, 6),
        "ending_nav_usd": round(replay.ending_nav_usd, 6),
        "return_pct": round(return_pct, 8),
        "turnover_usd": round(replay.total_turnover_usd, 6),
        "max_drawdown_pct": round(replay.max_drawdown_pct, 8),
        "costs": {key: round(value, 6) for key, value in costs.items()},
        "attribution": _attribution(trades),
        "benchmarks": _benchmark_report(observations),
        "diagnostics": {
            "probabilistic_sharpe_ratio": probabilistic_sharpe_ratio(trade_returns),
            "deflated_sharpe_ratio": deflated_sharpe_ratio(
                trade_returns,
                trial_count=trial_count,
            ),
            "bootstrap_mean_return_confidence_interval": list(confidence_interval),
        },
    }


def _attribution(trades: Sequence[Any]) -> dict[str, Any]:
    buckets: dict[str, dict[str, dict[str, float | int]]] = {
        "horizon": {},
        "side": {},
        "edge_class": {},
    }
    for trade in trades:
        for family, key in (
            ("horizon", str(trade.horizon)),
            ("side", str(trade.direction)),
            ("edge_class", str(trade.edge_class)),
        ):
            row = buckets[family].setdefault(key, {"trade_count": 0, "net_pnl_usd": 0.0})
            row["trade_count"] = int(row["trade_count"]) + 1
            row["net_pnl_usd"] = round(float(row["net_pnl_usd"]) + trade.net_pnl_usd, 6)
    return buckets


def _benchmark_report(observations: Sequence[ReplayObservation]) -> dict[str, Any]:
    by_symbol: dict[str, list[ReplayObservation]] = defaultdict(list)
    for observation in observations:
        by_symbol[observation.symbol].append(observation)
    returns: dict[str, float] = {}
    for symbol, rows in by_symbol.items():
        ordered = sorted(rows, key=lambda row: row.as_of)
        if len(ordered) >= 2 and ordered[0].mid > 0.0:
            returns[symbol] = ordered[-1].mid / ordered[0].mid - 1.0
    equal_values = [value for symbol, value in returns.items() if symbol not in {"SPY", "QQQ"}]
    equal_weight = sum(equal_values) / len(equal_values) if equal_values else 0.0
    return {
        "SPY": _benchmark_row(returns.get("SPY", 0.0)),
        "QQQ": _benchmark_row(returns.get("QQQ", 0.0)),
        "equal_weight": _benchmark_row(equal_weight),
    }


def _benchmark_row(return_pct: float) -> dict[str, float]:
    return {
        "return_pct": round(return_pct, 8),
        "ending_nav_usd": round(INITIAL_NAV_USD * (1.0 + return_pct), 6),
    }


def _configured_fixed_cost(rows: Sequence[Mapping[str, Any]], *, elapsed_days: float) -> float:
    monthly = 0.0
    for row in reversed(rows):
        costs = row.get("costs")
        if not isinstance(costs, Mapping):
            continue
        monthly = sum(
            _number(costs.get(key))
            for key in (
                "monthly_data_subscriptions_usd",
                "monthly_hosting_usd",
                "monthly_safety_margin_usd",
            )
        )
        break
    return monthly * elapsed_days / 30.4375


def _hard_gate_violations(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []
    for row in rows:
        if row.get("mandatory_trade_status") != "accepted":
            continue
        reasons = row.get("source_health_reasons", ())
        stale = [str(reason) for reason in reasons if str(reason).startswith(("stale:", "unavailable:"))]
        if row.get("market_session") != "rth" or stale:
            violations.append(
                {
                    "cycle_id": row.get("cycle_id"),
                    "market_session": row.get("market_session"),
                    "source_health_reasons": stale,
                }
            )
    return violations


def _is_observed_rth_cycle(row: Mapping[str, Any]) -> bool:
    return row.get("market_session") == "rth" and (
        row.get("cycle_kind") in FORCED_RTH_CYCLE_KINDS
        or row.get("mandatory_trade_required") is True
    )


def _hard_gate_blocks(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for row in rows:
        rust_response = row.get("rust_response")
        reason = str(
            row.get("no_trade_reason")
            or (
                rust_response.get("reason")
                if isinstance(rust_response, Mapping)
                else ""
            )
            or ""
        ).strip()
        stale_block = (
            row.get("status") == "skipped_stale"
            or row.get("edge_gate_status") == "stale_context"
        )
        authoritative_closed_block = (
            row.get("edge_gate_status") == "skipped_non_rth"
            and reason == "market_clock_closed"
        )
        if not stale_block and not authoritative_closed_block:
            continue
        reasons = row.get("source_health_reasons", ())
        source_health_reasons = [
            str(reason) for reason in reasons if str(reason).startswith(("stale:", "unavailable:"))
        ]
        block = {
            "cycle_id": row.get("cycle_id"),
            "cycle_kind": row.get("cycle_kind"),
            "status": row.get("status"),
            "source_health_reasons": source_health_reasons,
        }
        if reason:
            block["reason"] = reason
        blocks.append(block)
    return blocks


def _risk_envelope_breaches(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    breaches: list[dict[str, Any]] = []
    limits = {
        "gross_exposure_pct": 1.25,
        "net_exposure_abs_pct": 0.75,
        "short_gross_exposure_pct": 0.60,
        "daily_turnover_pct": 4.0,
        "daily_loss_pct": 0.03,
        "max_drawdown_pct": 0.10,
    }
    for row in rows:
        if (
            row.get("risk_mode") not in {"forced_aggressive", "profit_driven"}
            or (row.get("risk_mode") == "forced_aggressive" and row.get("mandatory_trade_status") != "accepted")
            or (row.get("risk_mode") == "profit_driven" and row.get("status") != "posted")
        ):
            continue
        missing = [key for key in limits if not _is_finite_number(row.get(key))]
        exceeded = {
            key: _number(row.get(key))
            for key, limit in limits.items()
            if _number(row.get(key)) > limit + 1e-9
        }
        if str(row.get("risk_budget_status", "")) == "over_budget":
            exceeded["risk_budget_status"] = 1.0
        if exceeded or missing:
            breach: dict[str, Any] = {"cycle_id": row.get("cycle_id"), "exceeded": exceeded}
            if missing:
                breach["missing_metrics"] = missing
            breaches.append(breach)
    return breaches


def _model_cost_integrity(
    rows: Sequence[Mapping[str, Any]], full_window: Mapping[str, Any]
) -> dict[str, Any]:
    invocation_count = sum(row.get("llm_invoked") is True for row in rows)
    costs = full_window.get("costs")
    cost_rows = costs if isinstance(costs, Mapping) else {}
    actual_cost = _number(cost_rows.get("model_usd"))
    imputed_cost = _number(cost_rows.get("imputed_api_cost_usd"))
    if invocation_count == 0:
        status = "not_applicable"
        promotion_eligible = True
    elif actual_cost > 0.0 or imputed_cost > 0.0:
        status = "complete"
        promotion_eligible = True
    else:
        status = "cost_unknown"
        promotion_eligible = False
    return {
        "status": status,
        "promotion_eligible": promotion_eligible,
        "invocation_count": invocation_count,
        "actual_model_cost_usd": round(actual_cost, 6),
        "imputed_api_cost_usd": round(imputed_cost, 6),
    }


def _is_finite_number(value: object) -> bool:
    return isinstance(value, int | float) and math.isfinite(float(value))


def _resolve_epoch(
    state_dir: Path, epoch: str
) -> tuple[str, Path, Mapping[str, Any]]:
    if epoch == "latest":
        descriptor_path = state_dir / "strategy-epoch.json"
        if not descriptor_path.exists():
            raise ValueError("latest strategy epoch is unavailable")
        descriptor = _read_json_object(descriptor_path)
        epoch_id = str(descriptor.get("epoch_id", ""))
    else:
        epoch_id = epoch
        descriptor = _read_json_object(state_dir / "epochs" / epoch_id / "strategy-epoch.json")
    if not epoch_id.startswith(EPOCH_ID_PATTERN_PREFIX) or len(epoch_id) != 22:
        raise ValueError("strategy epoch id is invalid")
    epoch_dir = state_dir / "epochs" / epoch_id
    if not epoch_dir.is_dir():
        raise ValueError(f"strategy epoch not found: {epoch_id}")
    return epoch_id, epoch_dir, descriptor


def _latest_timestamp(
    observations: Sequence[ReplayObservation], rows: Sequence[Mapping[str, Any]]
) -> datetime:
    timestamps = [row.as_of for row in observations]
    timestamps.extend(
        timestamp
        for row in rows
        if (timestamp := _parse_timestamp(row.get("as_of") or row.get("created_at"))) is not None
    )
    return max(timestamps, default=datetime.now(tz=UTC))


def _first_timestamp(
    observations: Sequence[ReplayObservation],
    rows: Sequence[Mapping[str, Any]],
    *,
    fallback: datetime,
) -> datetime:
    timestamps = [row.as_of for row in observations]
    timestamps.extend(
        timestamp
        for row in rows
        if (timestamp := _parse_timestamp(row.get("as_of") or row.get("created_at"))) is not None
    )
    return min(timestamps, default=fallback)


def _window_unattributed_cost(
    total: float,
    *,
    events: Sequence[tuple[datetime, float]],
    start: datetime,
    intents: int,
    all_intents: int,
    full_window: bool,
) -> float:
    if full_window:
        return total
    if events:
        return sum(cost for as_of, cost in events if as_of >= start)
    if all_intents == 0:
        return total
    return total * intents / all_intents


def _window_imputed_cost(
    total: float,
    *,
    events: Sequence[tuple[datetime, float]],
    start: datetime,
    intents: int,
    all_intents: int,
    full_window: bool,
) -> float:
    return _window_unattributed_cost(
        total,
        events=events,
        start=start,
        intents=intents,
        all_intents=all_intents,
        full_window=full_window,
    )


def _read_json_rows(path: Path) -> list[Mapping[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise LlmEquityContractError(f"{path} must contain a JSON array")
    return [row for row in payload if isinstance(row, Mapping)]


def _read_json_object(path: Path) -> Mapping[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise LlmEquityContractError(f"{path} must contain a JSON object")
    return payload


def _parse_timestamp(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or ""))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo is not None else None


def _number(value: object) -> float:
    return float(value) if isinstance(value, int | float) and math.isfinite(float(value)) else 0.0


def _benchmark_return(rows: Mapping[str, Any], symbol: str) -> float:
    row = rows.get(symbol)
    return float(row.get("return_pct", 0.0)) if isinstance(row, Mapping) else 0.0
