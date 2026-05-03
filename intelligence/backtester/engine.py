"""Tick-by-tick latency-adjusted event-driven simulation engine.

Replays historical tick data from QuestDB / Parquet with injected queue-position
modeling and network delays that mirror the Rust execution engine. Models must
pass sim-to-reality parity checks here before any live capital allocation.
See architecture.md §6 — Reinforcement Learning Workflow.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator

import polars as pl


@dataclass
class SimConfig:
    symbol: str
    start: str
    end: str
    latency_us: int = 500          # simulated round-trip latency in microseconds
    fee_rate: float = 0.001        # taker fee
    initial_capital_usd: float = 100_000.0


@dataclass
class SimState:
    position: float = 0.0
    cash_usd: float = 0.0
    realized_pnl: float = 0.0
    fill_log: list[dict] = field(default_factory=list)


class BacktestEngine:
    """Event-driven backtester replaying Parquet tick data."""

    def __init__(self, config: SimConfig) -> None:
        self.config = config

    def load_ticks(self) -> pl.DataFrame:
        # TODO: load from QuestDB or local Parquet via questdb_client
        raise NotImplementedError

    def run(self, agent) -> SimState:
        """Replay ticks, pass state to agent, simulate fills with latency."""
        # TODO: implement event loop with latency injection and Almgren-Chriss slippage
        raise NotImplementedError
