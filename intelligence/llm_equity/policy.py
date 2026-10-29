"""Profit-driven paper sizing and explicit holding exits; never generates an entry."""
from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any

from .accounting import number, timestamp
from .contracts import LlmEquityContractError

POLICY_VERSION = "profit_driven_v1"


def _positions(context: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {str(p["symbol"]): p for p in context["portfolio"].get("positions", [])}


def stop_fraction(command: Mapping[str, Any]) -> float:
    forecast = command.get("forecast")
    downside = number(forecast.get("return_p10_bps", -500)) if isinstance(forecast, Mapping) else -500
    return max(.005, min(.5, abs(min(downside, -50)) / 10000))


def cap_entries(decision: Mapping[str, Any], context: Mapping[str, Any]) -> dict[str, Any]:
    """Cap the resulting position and planned loss, not just the next ticket."""
    nav = number(context["portfolio"]["nav_usd"])
    positions = _positions(context)
    remaining = max(nav * .10, 0.0)
    commands = []
    seen: set[str] = set()
    for raw in decision.get("commands", []):
        row = dict(raw)
        symbol = str(row["symbol"])
        if symbol in seen:
            raise LlmEquityContractError("profit policy allows one command per symbol")
        seen.add(symbol)
        position = positions.get(symbol, {})
        value = number(position.get("market_value_usd", 0.0))
        sign = 1 if row["action"] == "buy_notional" else -1
        reduction = value * sign < 0
        if row.get("reduce_only") and not reduction:
            continue
        row["reduce_only"] = reduction
        requested = number(row["max_notional_usd"])
        if reduction:
            # Rust clamps to the actual executable quantity using its own quote.
            allowed = (abs(value) * 1.05 if requested >= abs(value) * (1 - 1e-8)
                       else requested)
        else:
            if row.get("horizon") == "close":
                snapshot = context["market_research_snapshot"]
                at = timestamp(str(snapshot["as_of"]))
                if _deadline(at, "close", snapshot.get("market_calendar_state", {})) <= at:
                    raise LlmEquityContractError("too late for close horizon")
            forecast = row.get("forecast")
            if (not isinstance(forecast, Mapping) or forecast.get("edge_class") != "positive_edge"
                    or number(forecast.get("expected_excess_return_bps", 0)) <= 0):
                raise LlmEquityContractError("entry requires a positive after-cost LLM forecast")
            max_position = min(nav * .05, nav * .0025 / stop_fraction(row))
            allowed = min(requested, max(max_position - abs(value), 0), remaining)
            remaining = max(remaining - allowed, 0)
        if allowed > 1e-8:
            row["max_notional_usd"] = allowed
            commands.append(row)
    return {**decision, "execution_policy": POLICY_VERSION, "commands": commands}


def _deadline(at: datetime, horizon: str, calendar: Mapping[str, Any]) -> datetime:
    if horizon == "close":
        close = calendar.get("next_close")
        if not isinstance(close, str):
            raise LlmEquityContractError("close horizon requires an authoritative calendar close")
        return timestamp(close) - timedelta(minutes=5)
    return at + {"30m": timedelta(minutes=30), "60m": timedelta(minutes=60),
                 "1d": timedelta(days=1), "5d": timedelta(days=5)}[horizon]


def update_exit_plans(
    plans: Mapping[str, Any], decision: Mapping[str, Any], response: Mapping[str, Any],
    context: Mapping[str, Any], at: datetime,
) -> dict[str, Any]:
    """Only Rust-accepted entries establish a thesis; increases cannot postpone expiry."""
    result = dict(plans)
    positions = _positions(context)
    commands = {f"{decision['cycle_id']}-{i}": row
                for i, row in enumerate(decision.get("commands", []), start=1)}
    for order in response.get("order_results", []):
        if order.get("status") != "accepted":
            continue
        row = commands.get(str(order.get("command_id")))
        if row is None:
            continue
        symbol = str(order["symbol"])
        sign = 1 if order["side"] == "buy" else -1
        before = number(positions.get(symbol, {}).get("quantity", 0))
        after = before + sign * number(order["quantity"])
        if abs(after) < 1e-8:
            result.pop(symbol, None)
        elif before * sign < 0:
            continue
        else:
            stop = number(order["avg_price"]) * (1 - sign * stop_fraction(row))
            existing = result.get(symbol)
            if isinstance(existing, Mapping) and before * sign > 0:
                stop = (max if sign > 0 else min)(stop, number(existing["stop_price"]))
                result[symbol] = {**existing, "stop_price": stop}
            else:
                result[symbol] = {
                    "policy": POLICY_VERSION, "origin_command_id": order["command_id"],
                    "side": sign, "stop_price": stop,
                    "exit_at": _deadline(at, str(row.get("horizon", "60m")),
                        context.get("market_research_snapshot", {}).get("market_calendar_state", {})).isoformat(),
                    "thesis": row.get("rationale", ""),
                }
    return result


def exit_reasons(
    context: Mapping[str, Any], plans: Mapping[str, Any], at: datetime,
) -> dict[str, str]:
    """Evaluate existing exposure only. A scheduled review is never an exit trigger."""
    nav = number(context["portfolio"]["nav_usd"])
    risk = context.get("profitability_scorecard", {}).get("risk", {})
    daily_pnl = number(risk.get("daily_pnl_usd", 0))
    global_reason = (
        "drawdown_limit" if number(risk.get("max_drawdown_pct", 0)) >= .10 else
        "daily_loss_limit" if daily_pnl <= -.03 * (nav - daily_pnl) else None
    )
    result: dict[str, str] = {}
    for symbol, position in _positions(context).items():
        qty = number(position["quantity"])
        if not qty:
            continue
        price = number(position["market_value_usd"]) / qty
        plan = plans.get(symbol)
        if global_reason:
            result[symbol] = global_reason
        else:
            try:
                if not isinstance(plan, Mapping) or number(plan["side"]) * qty <= 0:
                    raise ValueError("holding plan absent or side mismatch")
                stop, expiry = number(plan["stop_price"]), timestamp(str(plan["exit_at"]))
                if stop <= 0:
                    raise ValueError("holding stop must be positive")
            except (KeyError, TypeError, ValueError):
                result[symbol] = "missing_exit_plan"
                continue
            if (price - stop) * qty <= 0:
                result[symbol] = "position_stop"
            elif at >= expiry:
                result[symbol] = "thesis_horizon"
    return result


def review_signature(context: Mapping[str, Any]) -> str:
    import hashlib
    import json
    snapshot = context.get("market_research_snapshot", {})
    quotes = snapshot.get("latest_quotes", {})
    payload = {
        "scope": context.get("review_scope", "immediate"),
        "portfolio": context.get("portfolio", {}),
        "quotes": {symbol: {side: float(f"{number(row[side]):.4g}") for side in ("bid", "ask")}
                   for symbol, row in quotes.items() if "bid" in row and "ask" in row},
        "news": [{"source": row.get("source"), "summary": row.get("summary"),
                  "freshness": row.get("freshness")}
                 for row in snapshot.get("macro_news_summaries", [])],
        "unavailable": snapshot.get("unavailable_data_flags", []),
        "candidates": [{key: row.get(key) for key in ("symbol", "side", "edge_gate_status")}
                       for row in context.get("ranked_trade_candidates", [])[:5]],
    }
    # Portfolio NAV/market values move with quotes; quantize those too.
    payload["portfolio"] = {"cash_usd": context.get("portfolio", {}).get("cash_usd"),
        "positions": [{"symbol": row["symbol"], "quantity": row["quantity"]}
                      for row in context.get("portfolio", {}).get("positions", [])]}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def failure_kind(error: Exception) -> tuple[str, int]:
    import subprocess
    message = str(error).lower()
    if isinstance(error, (TimeoutError, subprocess.TimeoutExpired)):
        return "model_timeout", 300
    if any(marker in message for marker in ("rate limit", "usage limit", "429", "quota")):
        return "model_rate_limit", 1800
    if any(marker in message for marker in ("unauthorized", "authentication", "401", "403")):
        return "model_authentication", 3600
    if isinstance(error, LlmEquityContractError):
        return "model_contract_invalid", 300
    return "model_transport_failure", 300


def risk_exit_payload(context: Mapping[str, Any], reasons: Mapping[str, str], *, sequence: int) -> dict[str, Any]:
    snapshot = context["market_research_snapshot"]
    commands = []
    for position in context["portfolio"].get("positions", []):
        symbol = str(position["symbol"])
        if symbol not in reasons:
            continue
        commands.append({
            "symbol": symbol, "action": "sell_notional" if position["quantity"] > 0 else "buy_notional",
            "max_notional_usd": abs(float(position["market_value_usd"])) * 1.05,
            "reduce_only": True, "confidence": 1.0, "evidence_refs": ["risk-rule"],
            "rationale": f"יציאת סיכון לפי כלל אחזקה: {reasons[symbol]}", "horizon": "close",
        })
    as_of = str(snapshot["as_of"])
    return {
        "commands": commands,
        "evidence": [{"id": "risk-rule", "source": "deterministic_holding_rule",
            "summary": "כלל סיכון מפורש; הפחתת אחזקה בלבד ללא כניסה חדשה.",
            "retrieved_at": as_of, "freshness": "fresh"}],
        "risk_assessment": {"summary": "הפחתת חשיפה לפי כלל קיים.",
            "max_loss_scenario": "גאפ שוק עלול לחרוג ממחיר העצירה.", "risk_limit_verdict": "pass"},
        "memory_updates": [{"event_id": f"risk-{snapshot['snapshot_id']}",
            "cycle_id": snapshot["snapshot_id"], "created_at": as_of,
            "thesis": "הפחתת סיכון מאושרת מראש.", "outcome": "ממתין לאישור Rust.",
            "conclusion": "אין פתיחת חשיפה ללא אישור LLM.",
            "prompt_adjustment": "מחזור מתוזמן אינו סיבת יציאה.", "next_cycle_watch_items": []}],
        "execution_policy": POLICY_VERSION, "cycle_id": snapshot["snapshot_id"],
        "sequence": sequence, "as_of": as_of, "market_session": snapshot["market_session"],
        "portfolio_nav_usd": context["portfolio"]["nav_usd"],
    }



def revalidate_entry_prices(
    decision: Mapping[str, Any], reviewed: Mapping[str, Any], fresh: Mapping[str, Any],
) -> None:
    for command in decision.get("commands", []):
        if command.get("reduce_only"):
            continue
        symbol = command["symbol"]
        quotes = [context["market_research_snapshot"]["latest_quotes"][symbol]
                  for context in (reviewed, fresh)]
        mids = [(number(quote["bid"]) + number(quote["ask"])) / 2 for quote in quotes]
        edge = number(command.get("forecast", {}).get("expected_excess_return_bps", 0))
        if abs(mids[1] / mids[0] - 1) * 10000 > max(min(edge / 2, 25), 1):
            raise LlmEquityContractError("material price change requires a fresh LLM approval")
