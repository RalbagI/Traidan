"""Reconcile executed paper fills; horizon forecasts are never account cashflows."""

from __future__ import annotations

import json
import math
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

COST_KEYS = ("commission_bps", "borrow_apr", "financing_apr", "monthly_model_usd",
             "monthly_data_usd", "monthly_hosting_usd")


def timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        if len(value) != 10:
            raise ValueError("timestamp must include a timezone")
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(value):
        raise ValueError("account number must be finite")
    return float(value)


def position_effect(before: float, after: float) -> str:
    if abs(before) < 1e-10:
        return "open_long" if after > 0 else "open_short"
    if abs(after) < 1e-10:
        return "close_long" if before > 0 else "close_short"
    if before * after < 0:
        return "reverse"
    if abs(after) > abs(before):
        return "increase_long" if before > 0 else "increase_short"
    return "reduce_long" if before > 0 else "cover_short"


def deduplicate_audits(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Merge repeated receipts for a cycle without erasing its richer evidence."""
    result: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(rows):
        key = str(row.get("cycle_id") or f"missing-id:{index}")
        prior = result.get(key, {})
        rich, other = (row, prior) if len(row) >= len(prior) else (prior, row)
        merged = {**other, **rich}
        if prior.get("llm_invoked") or row.get("llm_invoked"):
            merged["llm_invoked"] = True
        result[key] = merged
    return list(result.values())


def _read_object(path: Path) -> dict[str, Any]:
    result = json.loads(path.read_text())
    if not isinstance(result, dict):
        raise TypeError(f"invalid account object: {path.name}")
    return result


def _positions(state: Mapping[str, Any]) -> dict[str, float]:
    return {str(symbol): number(row["quantity"])
            for symbol, row in state["portfolio"]["positions"].items()}


def _nav(state: Mapping[str, Any]) -> float:
    prices = state.get("last_prices", state.get("prices", {}))
    return number(state["portfolio"]["cash_usd"]) + sum(
        quantity * number(prices[symbol]) for symbol, quantity in _positions(state).items()
        if abs(quantity) > 1e-10
    )


def _fills(events: Sequence[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_cycle: dict[str, list[dict[str, Any]]] = {}
    seen: dict[str, dict[str, Any]] = {}
    for event in events:
        if event.get("event_type") != "replay_cycle":
            continue
        payload = event["payload"]
        for order in payload.get("order_results", []):
            if order.get("status") != "accepted":
                continue
            cid = str(order["command_id"])
            if cid in seen:
                if seen[cid] != order:
                    raise ValueError(f"conflicting fill: {cid}")
                continue
            if order.get("side") not in {"buy", "sell"}:
                raise ValueError("invalid executed side")
            for field in ("quantity", "avg_price", "notional_usd"):
                if number(order[field]) <= 0:
                    raise ValueError("executed fill must be positive")
            if abs(number(order["quantity"]) * number(order["avg_price"])
                   - number(order["notional_usd"])) > .01:
                raise ValueError("fill notional does not reconcile")
            seen[cid] = dict(order)
            # Release replay decision IDs identify a slot, while order IDs retain
            # the actual authority cycle. Never join on the synthetic replay ID.
            cycle = str(order.get("cycle_id") or cid.rsplit("-", 1)[0])
            by_cycle.setdefault(cycle, []).append(dict(order))
    return by_cycle


def _adjust(positions: dict[str, float], cash: float, payload: Mapping[str, Any]) -> float:
    symbol = str(payload["symbol"])
    quantity = positions.get(symbol, 0.0)
    action = payload["action_type"]
    if action == "split":
        ratio = number(payload["ratio"])
        if ratio <= 0:
            raise ValueError("split ratio must be positive")
        positions[symbol] = quantity * ratio
    elif action == "dividend":
        cash += quantity * number(payload["cash_amount_usd"])
    elif action == "symbol_change":
        target = str(payload["new_symbol"])
        if target == symbol or target in positions:
            raise ValueError("ambiguous symbol change")
        positions[target] = positions.pop(symbol, 0.0)
    else:
        raise ValueError("unsupported account adjustment")
    return cash


def model_cost_events(events: Sequence[Mapping[str, Any]]) -> list[tuple[datetime, float]]:
    costs: dict[tuple[str, datetime], float] = {}
    for event in events:
        if event.get("event_type") != "replay_cycle":
            continue
        payload = event["payload"]
        if payload.get("cost_disposition", "direct") not in {"direct", "deferred"}:
            continue
        at = payload.get("as_of") or event.get("observed_at")
        if not at:
            continue
        cost = number(payload.get("model_call_cost_usd", 0))
        if cost < 0:
            raise ValueError("model costs must be nonnegative")
        key = (str(payload.get("decision_id", "")), timestamp(at))
        if key in costs and costs[key] != cost:
            raise ValueError("conflicting model invocation cost")
        costs[key] = cost
    return [(key[1], value) for key, value in costs.items()]


def session_returns(
    opening: Mapping[str, Any], marks: Sequence[Mapping[str, Any]], *,
    session_closes: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Use the prior recorded session's gross exposure; today's orders cannot change its weight."""
    zone = ZoneInfo("America/New_York")
    daily: dict[str, Mapping[str, Any]] = {}
    for mark in sorted(marks, key=lambda row: timestamp(row["saved_at"])):
        local = timestamp(mark["saved_at"]).astimezone(zone)
        day = local.date()
        close = (session_closes or {}).get(day.isoformat())
        closing_time = timestamp(close) if close else local.replace(hour=16, minute=0, second=0, microsecond=0)
        if day.weekday() < 5 and local <= closing_time:
            daily[day.isoformat()] = mark
    rows = []
    previous = opening
    for session_day, mark in sorted(daily.items()):
        nav = _nav(previous)
        if timestamp(mark["saved_at"]) <= timestamp(previous["saved_at"]) or nav <= 0:
            continue
        weight = sum(abs(q * number(previous["last_prices"][symbol]))
                     for symbol, q in _positions(previous).items() if abs(q) > 1e-10) / nav
        row: dict[str, Any] = {"session": session_day, "opening_nav_usd": nav,
            "account_return": _nav(mark) / nav - 1, "prior_gross_weight": weight,
            "mark_at": mark["saved_at"]}
        for symbol in ("SPY", "QQQ"):
            start = previous["last_prices"].get(symbol)
            end = mark["last_prices"].get(symbol)
            row[symbol + "_exposure_matched"] = (
                weight * (number(end) / number(start) - 1) if start and end else None)
        rows.append(row)
        previous = mark
    return rows


def account_report(
    epoch_dir: Path, events: Sequence[Mapping[str, Any]], *,
    start: str | None = None, end: str | None = None,
) -> dict[str, Any]:
    """Return a half-open window with a carried opening book and explicit costs.

    Rust receipts are the source of account state. Accepted fill reconstruction
    independently checks every receipt, rather than treating forecasts as fills.
    Missing history or costs prevents promotion, without inventing zero returns.
    Recurring costs run through an explicit finite end; an omitted end values
    the account at its last recorded mark.
    """
    start_at = timestamp(start) if start else datetime.min.replace(tzinfo=UTC)
    end_at = timestamp(end) if end else datetime.max.replace(tzinfo=UTC)
    if start_at >= end_at:
        raise ValueError("account window start must precede end")
    journal = epoch_dir / "rust-authority-state.jsonl"
    if not journal.exists():
        return {"status": "authority_history_unavailable", "promotion_eligible": False}
    states = [json.loads(line) for line in journal.read_text().splitlines() if line.strip()]
    states = sorted((s for s in states if timestamp(s["saved_at"]) < end_at),
                    key=lambda s: (timestamp(s["saved_at"]), s["highest_sequence"]))
    if not states:
        return {"status": "authority_history_unavailable", "promotion_eligible": False}
    seed_path = epoch_dir / "account-opening-state.json"
    seed = _read_object(seed_path) if seed_path.exists() else None
    for state in [*states, *([seed] if seed else [])]:
        missing = [symbol for symbol, quantity in _positions(state).items()
                   if abs(quantity) > 1e-10 and symbol not in state.get("last_prices", {})]
        if missing:
            return {"status": "holding_marks_unavailable", "symbols": missing, "promotion_eligible": False}
    cash = number(seed["portfolio"]["cash_usd"] if seed else
                  states[0]["portfolio"]["initial_cash_usd"])
    quantities = _positions(seed) if seed else {}
    applied = set(seed["seen_cycle_ids"]) if seed else set()
    opening: dict[str, Any] | None = seed
    closing: dict[str, Any] | None = None
    opening_nav = _nav(seed) if seed else cash
    fills = _fills(events)
    effects: Counter[str] = Counter()
    turnover = 0.0
    execution_cost_bases: list[tuple[datetime, float]] = []
    max_cash_error = max_quantity_error = max_nav_error = 0.0
    adjustments = sorted((e for e in events if e.get("event_type") == "account_adjustment"
                          and (seed is None or timestamp(e["observed_at"]) > timestamp(seed["saved_at"]))),
                         key=lambda e: timestamp(e["observed_at"]))
    adjustment_index = 0
    marks: list[dict[str, Any]] = []
    for state in states:
        at = timestamp(state["saved_at"])
        while adjustment_index < len(adjustments) and timestamp(
            adjustments[adjustment_index]["observed_at"]
        ) <= at:
            cash = _adjust(quantities, cash, adjustments[adjustment_index]["payload"])
            adjustment_index += 1
        new_ids = set(state["seen_cycle_ids"]) - applied
        if len(new_ids) > 1:
            # When historical receipts are missing, order order is unknowable.
            # Cash can still reconcile, but attribution cannot be certified.
            max_quantity_error = math.inf
        for cycle in sorted(new_ids):
            for fill in fills.get(cycle, []):
                symbol = str(fill["symbol"])
                signed = number(fill["quantity"]) * (1 if fill["side"] == "buy" else -1)
                before = quantities.get(symbol, 0.0)
                quantities[symbol] = before + signed
                cash -= signed * number(fill["avg_price"])
                if at >= start_at:
                    effects[position_effect(before, quantities[symbol])] += 1
                    turnover += number(fill["notional_usd"])
                    execution_cost_bases.append((at, number(fill["notional_usd"])))
        applied.update(new_ids)
        max_cash_error = max(max_cash_error, abs(cash - number(state["portfolio"]["cash_usd"])))
        actual = _positions(state)
        max_quantity_error = max(max_quantity_error, max(
            (abs(quantities.get(s, 0) - actual.get(s, 0)) for s in quantities.keys() | actual.keys()),
            default=0.0,
        ))
        nav = _nav(state)
        reconstructed_nav = cash + sum(q * number(state["last_prices"][s])
                                      for s, q in quantities.items() if abs(q) > 1e-10)
        max_nav_error = max(max_nav_error, abs(nav - reconstructed_nav))
        if at < start_at:
            opening = state
            opening_nav = nav
        else:
            closing = state
            marks.append(state)
    if closing is None:
        return {"status": "no_marks_in_window", "promotion_eligible": False}
    # Daily telemetry includes real marks even when there was no accepted order.
    sidecar = epoch_dir / "rust-dashboard-state.json"
    if sidecar.exists():
        for row in _read_object(sidecar).get("portfolio_history", []):
            row = {**row, "saved_at": row["updated_at"], "last_prices": row["prices"]}
            at = timestamp(row["saved_at"])
            if at < start_at and (opening is None or at > timestamp(opening["saved_at"])):
                opening, opening_nav = row, _nav(row)
            if start_at <= at < end_at:
                marks.append(row)
                if at > timestamp(closing["saved_at"]):
                    closing = row
    marks.sort(key=lambda row: timestamp(row["saved_at"]))
    nav = _nav(closing)
    pnl = nav - opening_nav
    peak = opening_nav
    drawdown = 0.0
    for mark in marks:
        current = _nav(mark)
        peak = max(peak, current)
        drawdown = max(drawdown, (peak - current) / peak if peak > 0 else 1.0)
    first = opening or {**states[0],
        "saved_at": (timestamp(states[0]["saved_at"]) - timedelta(microseconds=1)).isoformat(),
        "portfolio": {"cash_usd": opening_nav, "positions": {}}}
    benchmarks: dict[str, Any] = {}
    for symbol in ("SPY", "QQQ"):
        p0 = first["last_prices"].get(symbol)
        p1 = closing["last_prices"].get(symbol)
        if p0 and p1:
            benchmarks[symbol] = {"return_fraction": number(p1) / number(p0) - 1}
    clocks = {timestamp(event["payload"]["snapshot"]["as_of"]).astimezone(
                  ZoneInfo("America/New_York")).date().isoformat():
              event["payload"]["snapshot"]["market_calendar_state"].get("next_close")
              for event in events if event.get("event_type") == "policy_frame"
              and event["payload"].get("snapshot", {}).get("market_calendar_state", {}).get("freshness") == "fresh"
              and event["payload"]["snapshot"]["market_calendar_state"].get("is_open") is True}
    sessions = session_returns(first, marks, session_closes=clocks)
    for row in sessions:
        row["session_close_at"] = clocks.get(row["session"])
    for symbol in ("SPY", "QQQ"):
        values = [row[symbol + "_exposure_matched"] for row in sessions]
        benchmarks.setdefault(symbol, {})["prior_day_gross_matched_return_fraction"] = (
            math.prod(1 + value for value in values) - 1
            if values and all(value is not None for value in values) else None)
    matched = max_cash_error <= .01 and max_quantity_error <= 1e-8 and max_nav_error <= .01
    policy_path = epoch_dir / "economic-cost-policy.json"
    policy = _read_object(policy_path) if policy_path.exists() else {}
    known = all(key in policy for key in COST_KEYS)
    if any(number(policy[key]) < 0 for key in COST_KEYS if key in policy):
        raise ValueError("economic costs must be nonnegative")
    excluded = policy.get("exclude_operating_costs", False)
    if not isinstance(excluded, bool):
        raise TypeError("exclude_operating_costs must be boolean")
    if excluded and any(number(policy.get(key, 0)) != 0 for key in COST_KEYS[3:]):
        raise ValueError("excluded operating costs must have zero monthly allocations")
    costs: list[tuple[datetime, str, float]] = [
        (at, "commission", value * number(policy.get("commission_bps", 0)) / 10000)
        for at, value in execution_cost_bases]
    previous = first
    cost_marks = list(marks)
    accrual_end = min(end_at, datetime.now(UTC)) if end else timestamp(closing["saved_at"])
    if accrual_end > timestamp(closing["saved_at"]):
        cost_marks.append({"saved_at": accrual_end.isoformat()})
    for mark in cost_marks:
        at = timestamp(mark["saved_at"])
        days = max(0.0, (at - max(start_at, timestamp(previous["saved_at"]))).total_seconds() / 86400)
        short = sum(max(-q * number(previous["last_prices"][symbol]), 0)
                    for symbol, q in _positions(previous).items() if abs(q) > 1e-10)
        costs.extend([
            (at, "fixed", sum(number(policy.get(key, 0)) for key in COST_KEYS[3:]) * days / 30),
            (at, "borrow", short * number(policy.get("borrow_apr", 0)) * days / 365),
            (at, "financing", max(-number(previous["portfolio"]["cash_usd"]), 0)
             * number(policy.get("financing_apr", 0)) * days / 365),
        ])
        previous = mark
    if not excluded:
        costs.extend((at, "model", cost) for at, cost in model_cost_events(events)
                     if start_at <= at < end_at)
    totals = {kind: sum(cost for _, name, cost in costs if name == kind)
              for kind in ("fixed", "commission", "borrow", "financing", "model")}
    previous_at = min(start_at, timestamp(first["saved_at"]))
    for row in sessions:
        at = timestamp(row["mark_at"])
        session_cost = sum(cost for when, _, cost in costs if previous_at < when <= at)
        row["economic_cost_usd"] = session_cost if known else None
        row["economic_return"] = (row["account_return"] - session_cost / row["opening_nav_usd"]
                                  if known else None)
        previous_at = at
    unallocated_cost = sum(cost for at, _, cost in costs if at > previous_at)
    return {
        "status": "available", "basis": "authority_account", "return_unit": "fraction",
        "opening_mark_at": first["saved_at"], "closing_mark_at": closing["saved_at"],
        "opening_nav_usd": opening_nav, "ending_nav_usd": nav, "account_pnl_usd": pnl,
        "return_fraction": pnl / opening_nav if opening_nav > 0 else None,
        "sampled_max_drawdown_fraction": drawdown, "mark_count": len(marks),
        "mark_coverage": "recorded_marks_only", "benchmarks": benchmarks,
        "sessions": sessions, "session_cost_allocation": "observed_mark_intervals",
        "accepted_fill_count": sum(effects.values()), "fill_counts": dict(effects),
        "turnover_usd": turnover,
        "reconciliation": {"status": "matched" if matched else "mismatch",
                           "max_cash_error_usd": max_cash_error,
                           "max_nav_error_usd": max_nav_error,
                           "max_quantity_error": max_quantity_error if math.isfinite(max_quantity_error) else None},
        "economic": {"status": "configured_estimates" if known else "cost_unknown",
                     "net_pnl_usd": pnl - sum(totals.values()) if known else None,
                     "spread_and_impact": "already_in_authority_fills",
                     "operating_costs": "excluded_by_owner" if excluded else "included",
                     "fixed_cost_usd": totals["fixed"], "commission_usd": totals["commission"],
                     "borrow_usd": totals["borrow"], "financing_usd": totals["financing"],
                     "model_usd": totals["model"], "unallocated_session_cost_usd": unallocated_cost},
        "promotion_eligible": matched and known,
    }
