"""Registered paper challengers and forward-only promotion evidence."""
from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .accounting import account_report, number, timestamp
from .epoch import _write
from .research_simulation import paired_block_bootstrap

VARIANTS = (
    {"id": "frozen_execution_reference", "policy": "forced_aggressive", "horizon": None},
    {"id": "quota_free_explicit_exits", "policy": "profit_driven", "horizon": None},
    {"id": "volatility_sized_1d", "policy": "profit_driven", "horizon": "1d"},
    {"id": "volatility_sized_5d", "policy": "profit_driven", "horizon": "5d"},
)


def register_trials(epoch_dir: Path, *, at: datetime, baseline: str) -> dict[str, Any]:
    path = epoch_dir / "profit-trial-registry.json"
    if path.exists():
        existing = json.loads(path.read_text())
        if existing.get("baseline") != baseline:
            raise ValueError("trial baseline is immutable")
        return dict(existing)
    start = at.astimezone(ZoneInfo("America/New_York")).date() + timedelta(days=1)
    while start.weekday() > 4:
        start += timedelta(days=1)
    registry = {"schema_version": 1, "registered_at": at.isoformat(), "baseline": baseline,
        "forward_start": start.isoformat(), "variants": list(VARIANTS),
        "selection_rule": "register before observation; no historical LLM promotion claims",
        "purge_sessions": 5, "embargo_sessions": 5, "stress_execution_multiplier": 2,
        "minimum_forward_sessions": 20, "minimum_matured_entry_forecasts": 100,
        "required_lower_confidence_bound": .95, "max_drawdown_fraction": .10,
        "initial_position_nav_fraction": .05, "planned_loss_nav_fraction": .0025,
        "entry_approval": "LLM approval required at the same horizon",
        "automatic_live_promotion": False}
    epoch_dir.mkdir(parents=True, exist_ok=True)
    _write(path, registry)
    return registry


def purged_partitions(sessions: Sequence[str], *, purge_sessions: int = 5) -> dict[str, list[str]]:
    ordered = sorted(set(sessions))
    if len(ordered) < 2 * purge_sessions + 3:
        return {"train": [], "validation": [], "lockbox": [], "purged": ordered}
    first, second = len(ordered) // 2, len(ordered) * 3 // 4
    return {"train": ordered[:max(first - purge_sessions, 0)],
        "validation": ordered[first:max(second - purge_sessions, first)],
        "lockbox": ordered[second:],
        "purged": ordered[max(first - purge_sessions, 0):first]
                   + ordered[max(second - purge_sessions, first):second]}


def promotion_gate(
    account: Mapping[str, Any], registry: Mapping[str, Any], *, matured_forecasts: int,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    zone = ZoneInfo("America/New_York")
    current = as_of or datetime.now(UTC)
    unique = {str(row["session"]): row for row in account.get("sessions", [])
              if str(row["session"]) >= str(registry["forward_start"])
              and timestamp(row["mark_at"]).astimezone(zone).date().isoformat() == row["session"]
              and timestamp(row["mark_at"]).astimezone(zone).weekday() < 5
              and row.get("session_close_at")
              and timestamp(row["session_close_at"]).astimezone(zone).date().isoformat() == row["session"]
              and timestamp(row["session_close_at"]) <= current
              and 0 <= (timestamp(row["session_close_at"]) - timestamp(row["mark_at"])).total_seconds() <= 300}
    rows = [row for _, row in sorted(unique.items())]
    economic = account.get("economic", {})
    full_profit = economic.get("net_pnl_usd")
    total_cost = sum(number(economic.get(key, 0)) for key in
                     ("fixed_cost_usd", "commission_usd", "borrow_usd", "financing_usd", "model_usd"))
    paired_cost = sum(number(row.get("economic_cost_usd") or 0) for row in rows)
    costs_known = bool(account.get("promotion_eligible"))
    complete = bool(rows) and all(
        row.get(key) is not None and math.isfinite(number(row[key]))
        for row in rows for key in ("economic_return", "SPY_exposure_matched", "QQQ_exposure_matched"))
    lower: dict[str, float] = {}
    if complete:
        for symbol in ("SPY", "QQQ"):
            lower[symbol] = paired_block_bootstrap(
                [number(row["economic_return"]) for row in rows],
                [number(row[symbol + "_exposure_matched"]) for row in rows],
                block_size=5, samples=10000)[0]
    gates = {
        "reconciled_and_costed": costs_known,
        "complete_paired_marks": complete,
        "twenty_future_sessions": len(rows) >= 20,
        "one_hundred_matured_forecasts": matured_forecasts >= 100,
        "all_costs_in_completed_sessions": abs(total_cost - paired_cost) <= 1e-8,
        "positive_economic_profit": complete and full_profit is not None and number(full_profit) > 0
            and sum(number(row["economic_return"]) *
            number(row["opening_nav_usd"]) for row in rows) > 0,
        "within_drawdown_limit": number(account.get("sampled_max_drawdown_fraction", 1)) <= .10,
        "positive_spy_excess_95_lower_bound": lower.get("SPY", -1) > 0,
        "positive_qqq_excess_95_lower_bound": lower.get("QQQ", -1) > 0,
    }
    return {"eligible": all(gates.values()), "gates": gates, "future_sessions": len(rows),
        "matured_entry_forecasts": matured_forecasts, "session_block_bootstrap_95_lower": lower,
        "automatic_live_promotion": False, "live_gate_minimum_future_sessions": 90}


def compare_registered_variants(epoch_dir: Path, output_dir: Path) -> dict[str, Any]:
    """Replay immutable LLM-approved frames through the existing isolated Rust harness.

    Each replay conditions on approvals made for the observed book. It is a
    diagnostic counterfactual, and cannot satisfy forward promotion evidence.
    """
    import hashlib
    from copy import deepcopy

    from .policy import cap_entries, exit_reasons, update_exit_plans
    from .replay import PointInTimeEventStore
    from .research_simulation import RustResearchHarness

    registry = json.loads((epoch_dir / "profit-trial-registry.json").read_text())
    events = PointInTimeEventStore(epoch_dir / "research-events.jsonl").read_all()
    frames = [event for event in events if event["event_type"] == "policy_frame"]
    output_dir.mkdir(parents=True, exist_ok=False)
    seed_path = epoch_dir / "account-opening-state.json"
    seed = json.loads(seed_path.read_text()) if seed_path.exists() else None
    plans_path = epoch_dir / "account-opening-exit-plans.json"
    opening_plans = json.loads(plans_path.read_text()) if plans_path.exists() else {}
    cost_path = epoch_dir / "economic-cost-policy.json"
    cost_policy = json.loads(cost_path.read_text()) if cost_path.exists() else None
    frozen = {"registry": registry, "frames": frames, "economic_cost_policy": cost_policy, "opening_authority": seed,
              "opening_exit_plans": opening_plans}
    encoded = json.dumps(frozen, sort_keys=True).encode()
    (output_dir / "frozen-inputs.json").write_bytes(encoded)
    summary: dict[str, Any] = {"basis": "conditional_approval_counterfactual",
        "forward_promotion_eligible": False, "source_sha256": hashlib.sha256(encoded).hexdigest(),
        "status": "awaiting_frames" if not frames else "running", "variants": []}
    _write(output_dir / "manifest.json", {**summary, "registry": registry})
    if not frames:
        return summary
    for variant in registry["variants"]:
        for stress in (1, 2):
            variant_dir = output_dir / f"{variant['id']}-cost{stress}"
            variant_dir.mkdir()
            if cost_policy is not None:
                _write(variant_dir / "economic-cost-policy.json", cost_policy)
            if seed is not None:
                _write(variant_dir / "account-opening-state.json", seed)
            receipts = []
            ledger = PointInTimeEventStore(variant_dir / "research-events.jsonl")
            plans: dict[str, Any] = deepcopy(opening_plans)
            unpriceable = 0
            applied: set[str] = set()
            with RustResearchHarness(policy=variant["policy"]) as harness:
                if seed is not None:
                    harness.restore_state(seed)
                state = harness.export_state()
                opening = number(state["portfolio"]["cash_usd"]) + sum(
                    number(p["quantity"]) * number(state["last_prices"][symbol])
                    for symbol, p in state["portfolio"]["positions"].items())
                sequence = int(state["highest_sequence"])
                last_nav = opening
                for event in frames:
                    frame = event["payload"]
                    snapshot = deepcopy(frame["snapshot"])
                    at = timestamp(snapshot["as_of"])
                    frame_id = str(snapshot["snapshot_id"])
                    if frame_id in applied:
                        continue
                    applied.add(frame_id)
                    for quote in snapshot["latest_quotes"].values():
                        mid = (number(quote["bid"]) + number(quote["ask"])) / 2
                        half = (number(quote["ask"]) - number(quote["bid"])) / 2
                        # Rust already charges 1 bp impact. Add a second bp under stress.
                        half = half * stress + mid * .0001 * (stress - 1)
                        quote.update(bid=mid - half, ask=mid + half)
                    try:
                        state = harness.mark(snapshot, at)
                    except RuntimeError:
                        unpriceable += 1
                        continue
                    portfolio = state["portfolio"]
                    prices = state["last_prices"]
                    last_nav = number(portfolio["cash_usd"]) + sum(
                        number(p["quantity"]) * number(prices[symbol])
                        for symbol, p in portfolio["positions"].items())
                    context = {"portfolio": {**portfolio, "nav_usd": last_nav,
                        "positions": [{"symbol": symbol, **p, "market_value_usd": p["quantity"] * prices[symbol]}
                                      for symbol, p in portfolio["positions"].items()]},
                        "profitability_scorecard": {"risk": {
                            "daily_pnl_usd": last_nav - portfolio["day_start_nav_usd"],
                            "max_drawdown_pct": max(0, 1 - last_nav / portfolio["peak_nav_usd"])}},
                        "market_research_snapshot": snapshot}
                    raw = frame.get("decision")
                    decision = deepcopy(dict(raw)) if isinstance(raw, Mapping) else None
                    reasons = exit_reasons(context, plans, at) if variant["policy"] == "profit_driven" else {}
                    if reasons:
                        from .policy import risk_exit_payload
                        decision = risk_exit_payload(context, reasons, sequence=sequence + 1)
                    elif decision is not None and (frame.get("entry_approval") == "llm" or
                        decision.get("commands") and all(row.get("reduce_only") is True
                                                       for row in decision["commands"])):
                        if variant["policy"] == "profit_driven":
                            decision["commands"] = [row for row in decision["commands"]
                                if (row.get("reduce_only") or row.get("forecast", {}).get("edge_class") == "positive_edge")
                                and (row.get("reduce_only") or variant["horizon"] is None or row.get("horizon") == variant["horizon"])]
                            decision = cap_entries(decision, context)
                        else:
                            decision.pop("execution_policy", None)
                            decision["commands"] = [row for row in decision["commands"]
                                                    if row["max_notional_usd"] >= 25]
                    else:
                        decision = None
                    if decision and decision.get("commands"):
                        sequence += 1
                        decision.update(sequence=sequence, cycle_id=f"experiment-{sequence}",
                                        as_of=snapshot["as_of"], portfolio_nav_usd=last_nav)
                        for row in decision["memory_updates"]:
                            row["cycle_id"] = decision["cycle_id"]
                        result = harness.apply(decision, snapshot, at)
                        response = {"order_results": result.get("accepted_orders", [])}
                        response["order_results"] = [{**row, "status": "accepted", "quantity": row["qty"]}
                                                     for row in response["order_results"]]
                        plans = update_exit_plans(plans, decision, response, context, at)
                        ledger.append("rust_execution", at, {"decision": decision, "result": result})
                        ledger.append("replay_cycle", at, {"decision_id": decision["cycle_id"],
                            "as_of": at.isoformat(), "order_results": response["order_results"]})
                        state = harness.export_state()
                        last_nav = number(state["portfolio"]["cash_usd"]) + sum(
                            number(p["quantity"]) * number(prices[symbol])
                            for symbol, p in state["portfolio"]["positions"].items())
                    receipts.append({**state, "saved_at": at.isoformat()})
                    ledger.append("portfolio_mark", at, {"nav_usd": last_nav})
                _write(variant_dir / "authority-state.json", state)
            (variant_dir / "rust-authority-state.jsonl").write_text(
                "".join(json.dumps(row) + "\n" for row in receipts))
            account = account_report(variant_dir, ledger.read_all())
            summary["variants"].append({"id": variant["id"], "execution_cost_multiplier": stress,
                "account_pnl_usd": last_nav - opening,
                "economic_pnl_usd": account.get("economic", {}).get("net_pnl_usd"),
                "unpriceable_frames": unpriceable, "cost_status": account.get("economic", {}).get("status", "unavailable"),
                "reconciliation": account.get("reconciliation")})
    summary["status"] = "complete"
    _write(output_dir / "manifest.json", {**summary, "registry": registry})
    _write(output_dir / "summary.json", summary)
    return summary
