"""Context and fail-closed previews for LLM equity dry-run commands.

Rust is the execution authority for simulated fills, risk checks, replay
protection, and portfolio accounting. Python may validate and package context
for the LLM, but it must not mutate portfolio state as an active trading path.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from .contracts import (
    EquityUniverse,
    LlmEquityContractError,
    LlmTradingCommand,
    load_equity_universe,
    snapshot_requires_hold,
    validate_llm_decision,
    validate_market_research_snapshot,
)


@dataclass
class EquityPosition:
    symbol: str
    quantity: float
    avg_price: float

    def market_value(self, price: float) -> float:
        return self.quantity * price


@dataclass
class EquityPaperPortfolio:
    initial_cash_usd: float = 10_000.0
    cash_usd: float = 10_000.0
    positions: dict[str, EquityPosition] = field(default_factory=dict)
    realized_pnl_usd: float = 0.0
    turnover_this_hour_usd: float = 0.0
    peak_nav_usd: float = 10_000.0
    day_start_nav_usd: float = 10_000.0

    @classmethod
    def seeded(cls, universe: EquityUniverse | None = None) -> EquityPaperPortfolio:
        universe = universe or load_equity_universe()
        return cls(
            initial_cash_usd=universe.initial_cash_usd,
            cash_usd=universe.initial_cash_usd,
            peak_nav_usd=universe.initial_cash_usd,
            day_start_nav_usd=universe.initial_cash_usd,
        )

    def nav_usd(self, prices: Mapping[str, float]) -> float:
        return self.cash_usd + sum(
            position.market_value(prices[position.symbol])
            for position in self.positions.values()
            if position.symbol in prices
        )

    def gross_exposure_usd(self, prices: Mapping[str, float]) -> float:
        return sum(
            abs(position.market_value(prices[position.symbol]))
            for position in self.positions.values()
            if position.symbol in prices
        )


@dataclass(frozen=True)
class SimulatedOrder:
    command_id: str
    symbol: str
    side: str
    status: str
    qty: float
    avg_price: float
    notional_usd: float
    reason: str


@dataclass(frozen=True)
class SimulationResult:
    cycle_id: str
    accepted_orders: tuple[SimulatedOrder, ...]
    rejected_orders: tuple[SimulatedOrder, ...]
    portfolio: EquityPaperPortfolio
    llm_feedback: tuple[str, ...]


def apply_llm_decision(
    decision_payload: Mapping[str, Any],
    snapshot_payload: Mapping[str, Any],
    portfolio: EquityPaperPortfolio | None = None,
    *,
    universe: EquityUniverse | None = None,
) -> SimulationResult:
    """Validate an LLM decision but reject execution until Rust applies it.

    The function remains as a compatibility shim for local Python tests and
    context tooling. It deliberately does not perform risk decisions, fills, or
    accounting; the Rust authority owns those state transitions.
    """

    universe = universe or load_equity_universe()
    decision = validate_llm_decision(decision_payload, universe)
    snapshot = validate_market_research_snapshot(snapshot_payload, universe)
    portfolio = portfolio or EquityPaperPortfolio.seeded(universe)
    prices = _mid_prices(snapshot)
    rejected: list[SimulatedOrder] = []
    feedback: list[str] = []
    for index, command in enumerate(decision.commands, start=1):
        command_id = f"{decision.cycle_id}-{index}"
        rejected.append(_order(command_id, command, "rejected", prices, "rust_authority_required"))
        feedback.append(f"{command.symbol}: rejected rust_authority_required")
    return SimulationResult(
        cycle_id=decision.cycle_id,
        accepted_orders=(),
        rejected_orders=tuple(rejected),
        portfolio=portfolio,
        llm_feedback=tuple(feedback),
    )


def build_llm_context(
    portfolio: EquityPaperPortfolio,
    snapshot_payload: Mapping[str, Any],
    *,
    universe: EquityUniverse | None = None,
) -> Mapping[str, Any]:
    universe = universe or load_equity_universe()
    snapshot = validate_market_research_snapshot(snapshot_payload, universe)
    prices = _mid_prices(snapshot)
    nav = portfolio.nav_usd(prices)
    unpriced_positions = sorted(
        position.symbol for position in portfolio.positions.values() if position.symbol not in prices
    )
    return {
        "system": "llm_equity_dry_run",
        "authority": {
            "trading_intent_source": "llm_only",
            "live_orders_allowed": False,
            "simulated_money_only": True,
            "broker_order_submission": "disabled",
        },
        "market_scope": {
            "mode": "dynamic_us_equities_paper",
            "researched_symbols": list(snapshot.get("symbols", [])),
            "core_symbols": list(universe.core_symbols),
            "research_symbol_limit": universe.research_symbol_limit,
            "selection_policy": (
                "Alpaca active/tradable U.S. equities ranked by required positions, "
                "top movers, core anchors, then active-symbol fallback."
            ),
        },
        "portfolio": {
            "initial_cash_usd": portfolio.initial_cash_usd,
            "cash_usd": portfolio.cash_usd,
            "nav_usd": nav,
            "gross_exposure_usd": portfolio.gross_exposure_usd(prices),
            "valuation_status": "unavailable" if unpriced_positions else "current",
            "unpriced_positions": unpriced_positions,
            "positions": [
                {
                    "symbol": position.symbol,
                    "quantity": position.quantity,
                    "avg_price": position.avg_price,
                    "market_value_usd": position.market_value(prices[position.symbol]),
                }
                for position in portfolio.positions.values()
                if position.symbol in prices
            ],
        },
        "autonomy_policy": {
            "simulated_money_only": True,
            "broker_order_submission": "disabled",
            "llm_may_buy_or_sell_long_or_short": True,
            "deterministic_position_risk_limits": "disabled",
            "operational_gates": [
                "schema",
                "credential_redaction",
                "replay",
                "timestamp_window",
                "regular_session_release",
                "fresh_research_data",
                "command_symbol_price_available",
            ],
        },
        "market_research_snapshot": snapshot,
        "forced_hold": snapshot_requires_hold(snapshot),
    }


def _mid_prices(snapshot: Mapping[str, Any]) -> dict[str, float]:
    quotes = snapshot.get("latest_quotes")
    if not isinstance(quotes, Mapping):
        raise LlmEquityContractError("snapshot latest_quotes must be an object")
    bars = snapshot.get("latest_bars")
    if not isinstance(bars, Mapping):
        raise LlmEquityContractError("snapshot latest_bars must be an object")
    prices: dict[str, float] = {}
    for symbol, value in quotes.items():
        if not isinstance(value, Mapping):
            continue
        source = str(value.get("source", ""))
        if source.endswith("_unavailable") or "unavailable" in source:
            fallback = _bar_close_price(bars.get(symbol))
            if fallback is not None:
                prices[str(symbol)] = fallback
            continue
        bid = float(value.get("bid", 0.0))
        ask = float(value.get("ask", 0.0))
        if bid > 0.0 and ask > 0.0:
            prices[str(symbol)] = (bid + ask) / 2.0
    return prices


def _bar_close_price(value: Any) -> float | None:
    if not isinstance(value, Mapping):
        return None
    source = str(value.get("source", ""))
    if source.endswith("_unavailable") or "unavailable" in source:
        return None
    try:
        close = float(value.get("close", 0.0))
    except (TypeError, ValueError):
        return None
    return close if close > 0.0 else None


def _order(
    command_id: str,
    command: LlmTradingCommand,
    status: str,
    prices: Mapping[str, float],
    reason: str,
    *,
    qty: float | None = None,
    notional: float | None = None,
) -> SimulatedOrder:
    price = prices.get(command.symbol, 0.0)
    order_notional = command.max_notional_usd if notional is None else notional
    return SimulatedOrder(
        command_id=command_id,
        symbol=command.symbol,
        side="buy" if command.action == "buy_notional" else "sell",
        status=status,
        qty=0.0 if price <= 0.0 else (order_notional / price if qty is None else qty),
        avg_price=price,
        notional_usd=order_notional,
        reason=reason,
    )
