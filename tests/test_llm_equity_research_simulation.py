from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import tarfile
import threading
import time as time_module
from collections.abc import Mapping
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import ClassVar, Self

import pytest
from jsonschema import Draft202012Validator
from llm_equity.alpaca import AlpacaMarketDataClient, AlpacaMarketDataConfig
from llm_equity.contracts import (
    load_equity_universe,
    validate_llm_decision,
    validate_market_research_snapshot,
)
from llm_equity.research_simulation import (
    ARMS,
    SPECIALIST_ROLES,
    CanonicalRunStore,
    DatasetCompleteness,
    InvocationResult,
    QuotePartitionBuilder,
    QuotePartitionIndex,
    ResearchFingerprintMismatch,
    ResearchObservation,
    StoredOutputInvoker,
    SymbolAnonymizer,
    _latest_rust_snapshot,
    analyze_run,
    build_cycle_schedule,
    export_run,
    holm_correct,
    paired_block_bootstrap,
    prepare_run,
    run_prepared_simulation,
    run_research_cycle,
    select_research_universe,
    verify_run,
)

from llm_equity import research_simulation


def _timestamp(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 5, 4, hour, minute, tzinfo=UTC)


def _observation(
    observation_id: str,
    kind: str,
    *,
    event_time: datetime,
    available_at: datetime,
    symbol: str | None = "SPY",
    payload: dict[str, object] | None = None,
) -> ResearchObservation:
    return ResearchObservation.from_mapping(
        {
            "schema_version": 1,
            "observation_id": observation_id,
            "kind": kind,
            "source": "alpaca_historical",
            "event_time": event_time.isoformat().replace("+00:00", "Z"),
            "available_at": available_at.isoformat().replace("+00:00", "Z"),
            "symbol": symbol,
            "payload": payload or {"value": 1.0},
        }
    )


def test_point_in_time_observation_rejects_future_availability_and_unclosed_bar() -> None:
    cutoff = _timestamp(14, 30)
    future = _observation(
        "quote-future",
        "quote",
        event_time=_timestamp(14, 29),
        available_at=_timestamp(14, 31),
        payload={"bid": 99.95, "ask": 100.05},
    )
    unclosed = _observation(
        "bar-unclosed",
        "bar",
        event_time=_timestamp(14, 29),
        available_at=_timestamp(14, 29),
        payload={
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1000.0,
            "adjustment": "raw",
            "bar_end": _timestamp(14, 31).isoformat().replace("+00:00", "Z"),
        },
    )
    future_news = _observation(
        "news-future",
        "news",
        event_time=_timestamp(14, 0),
        available_at=_timestamp(14, 31),
        payload={"summary": "This revision was not available at cutoff."},
    )
    future_action = _observation(
        "split-future",
        "corporate_action",
        event_time=_timestamp(14, 0),
        available_at=_timestamp(14, 0),
        payload={
            "action_type": "split",
            "ratio": 2.0,
            "effective_at": _timestamp(14, 31).isoformat().replace("+00:00", "Z"),
        },
    )

    assert future.eligible_at(cutoff) is False
    assert unclosed.eligible_at(cutoff) is False
    assert future_news.eligible_at(cutoff) is False
    assert future_action.eligible_at(cutoff) is False


def test_point_in_time_pack_bounds_evidence_to_current_state_and_recent_context() -> None:
    cutoff = datetime(2026, 7, 31, 20, 0, tzinfo=UTC)
    start = datetime(2026, 5, 4, 13, 30, tzinfo=UTC)
    observations = [
        _observation(
            f"bar-{index:04d}",
            "bar",
            event_time=start + timedelta(minutes=index),
            available_at=start + timedelta(minutes=index + 1),
            payload={
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1_000.0 + index,
                "session_open": 100.0,
                "adjustment": "raw",
                "bar_end": (start + timedelta(minutes=index + 1))
                .isoformat()
                .replace("+00:00", "Z"),
            },
        )
        for index in range(2_000)
    ]
    observations.append(
        _observation(
            "quote-latest",
            "quote",
            event_time=cutoff - timedelta(seconds=1),
            available_at=cutoff - timedelta(seconds=1),
            payload={"bid": 100.4, "ask": 100.6},
        )
    )
    observations.extend(
        _observation(
            f"news-{index:03d}",
            "news",
            event_time=cutoff - timedelta(minutes=44 - index),
            available_at=cutoff - timedelta(minutes=44 - index),
            payload={"summary": f"Bounded summary {index}"},
        )
        for index in range(45)
    )

    pack, block_reason = research_simulation.build_point_in_time_pack(
        observations,
        cutoff=cutoff,
        semantic=True,
        learning_state={},
    )

    evidence_ids = {row["id"] for row in pack["evidence"]}
    context_ids = {row["id"] for row in pack["event_context"]}
    candidate = pack["ranked_candidates"][0]
    assert block_reason is None
    assert len(pack["evidence"]) == 42
    assert len(pack["event_context"]) == 40
    assert evidence_ids == context_ids | {
        candidate["bar_evidence_id"],
        candidate["quote_evidence_id"],
    }
    assert "bar-0000" not in evidence_ids
    assert "news-000" not in evidence_ids
    assert len(json.dumps(pack, sort_keys=True)) < 100_000


def test_point_in_time_pack_uses_cumulative_rth_volume_and_deduplicates_news() -> None:
    cutoff = datetime(2026, 5, 4, 14, 32, tzinfo=UTC)
    observations = [
        _observation(
            "bar-SPY-0930",
            "bar",
            event_time=datetime(2026, 5, 4, 13, 30, tzinfo=UTC),
            available_at=datetime(2026, 5, 4, 13, 31, tzinfo=UTC),
            payload={
                "open": 100.0,
                "high": 100.2,
                "low": 99.9,
                "close": 100.1,
                "volume": 1_000.0,
                "session_open": 100.0,
                "bar_end": "2026-05-04T13:31:00Z",
                "adjustment": "raw",
            },
        ),
        _observation(
            "bar-SPY-1031",
            "bar",
            event_time=datetime(2026, 5, 4, 14, 31, tzinfo=UTC),
            available_at=cutoff,
            payload={
                "open": 101.0,
                "high": 101.2,
                "low": 100.9,
                "close": 101.1,
                "volume": 2_000.0,
                "session_open": 100.0,
                "bar_end": "2026-05-04T14:32:00Z",
                "adjustment": "raw",
            },
        ),
        _observation(
            "quote-SPY",
            "quote",
            event_time=cutoff - timedelta(seconds=1),
            available_at=cutoff - timedelta(seconds=1),
            payload={"bid": 101.0, "ask": 101.2},
        ),
        _observation(
            "alpaca-news-42-SPY",
            "news",
            event_time=cutoff - timedelta(minutes=10),
            available_at=cutoff - timedelta(minutes=9),
            payload={"article_id": "42", "symbols": ["SPY"], "summary": "first copy"},
        ),
        _observation(
            "alpaca-news-42-QQQ",
            "news",
            event_time=cutoff - timedelta(minutes=10),
            available_at=cutoff - timedelta(minutes=9),
            symbol="QQQ",
            payload={"article_id": "42", "symbols": ["SPY", "QQQ"], "summary": "duplicate"},
        ),
    ]

    pack, block_reason = research_simulation.build_point_in_time_pack(
        observations,
        cutoff=cutoff,
        semantic=False,
        learning_state={},
    )

    assert block_reason is None
    candidate = pack["ranked_candidates"][0]
    assert candidate["cumulative_volume"] == 3_000.0
    assert candidate["volume"] == 3_000.0
    assert candidate["quote_age_seconds"] == 1.0
    news_rows = [row for row in pack["event_context"] if row["kind"] == "news"]
    assert len(news_rows) == 1
    assert news_rows[0]["features"]["symbol_count"] == 2
    assert news_rows[0]["features"]["age_minutes"] == 9.0


def test_cycle_schedule_honors_holiday_and_early_close() -> None:
    schedule = build_cycle_schedule(
        [
            {
                "session_date": "2026-05-04",
                "open": "09:30",
                "close": "16:00",
                "available_at": "2026-05-01T20:00:00Z",
            },
            {
                "session_date": "2026-05-05",
                "open": "09:30",
                "close": "13:00",
                "available_at": "2026-05-01T20:00:00Z",
            },
        ]
    )

    full = [cycle for cycle in schedule if cycle.session_date == date(2026, 5, 4)]
    early = [cycle for cycle in schedule if cycle.session_date == date(2026, 5, 5)]
    assert sum(cycle.is_rth for cycle in full) == 10
    assert sum(cycle.is_rth for cycle in early) == 6
    assert [cycle.local_time for cycle in early if not cycle.is_rth] == [time(9, 0), time(9, 15)]


def test_planned_window_has_62_sessions_and_620_rth_cycles_without_early_close() -> None:
    holidays = {date(2026, 5, 25), date(2026, 6, 19), date(2026, 7, 3)}
    current = date(2026, 5, 3)
    calendar: list[dict[str, str]] = []
    while current < date(2026, 8, 1):
        if current.weekday() < 5 and current not in holidays:
            calendar.append(
                {
                    "session_date": current.isoformat(),
                    "open": "09:30",
                    "close": "16:00",
                    "available_at": "2026-05-01T20:00:00Z",
                }
            )
        current += timedelta(days=1)

    schedule = build_cycle_schedule(calendar)

    assert len({cycle.session_date for cycle in schedule}) == 62
    assert sum(cycle.is_rth for cycle in schedule) == 620
    assert len(schedule) == 744


def test_anonymizer_removes_identity_dates_headlines_and_urls() -> None:
    anonymizer = SymbolAnonymizer(["AAPL", "SPY"])
    pack = anonymizer.sanitize_pack(
        {
            "symbol": "AAPL",
            "company": "Apple Inc.",
            "published_at": "2026-05-04T13:00:00Z",
            "headline": "Apple launches a product",
            "url": "https://example.com/apple",
            "return_bps": 12.5,
            "cutoff": "2026-05-04T13:00:00Z",
            "learning": {"AAPL:long:60m": 3.0},
        }
    )

    assert pack == {
        "symbol": "SYM001",
        "return_bps": 12.5,
        "cutoff": "DAY+001T13:00:00Z",
        "learning": {"SYM001:long:60m": 3.0},
    }
    assert anonymizer.restore_symbol("SYM001") == "AAPL"


def test_anonymized_forecasts_restore_symbols_before_outcome_scoring() -> None:
    anonymizer = SymbolAnonymizer(["AAPL", "SPY"])
    forecast = {
        "symbol": "SYM001",
        "direction": "long",
        "horizon": "60m",
    }

    restored = research_simulation._restore_anonymized_forecast(forecast, anonymizer)

    assert restored == {"symbol": "AAPL", "direction": "long", "horizon": "60m"}


def test_trading_session_horizons_skip_weekends_and_holidays() -> None:
    schedule = [
        {
            "session_date": "2026-05-22",
            "session_close": "2026-05-22T20:00:00Z",
        },
        {
            "session_date": "2026-05-26",
            "session_close": "2026-05-26T20:00:00Z",
        },
        {
            "session_date": "2026-05-27",
            "session_close": "2026-05-27T20:00:00Z",
        },
        {
            "session_date": "2026-05-28",
            "session_close": "2026-05-28T20:00:00Z",
        },
        {
            "session_date": "2026-05-29",
            "session_close": "2026-05-29T20:00:00Z",
        },
        {
            "session_date": "2026-06-01",
            "session_close": "2026-06-01T20:00:00Z",
        },
    ]
    received_at = datetime(2026, 5, 22, 15, 0, tzinfo=UTC)

    assert research_simulation._maturity_for_horizon(
        received_at, "1d", schedule=schedule
    ) == datetime(2026, 5, 26, 20, 0, tzinfo=UTC)
    assert research_simulation._maturity_for_horizon(
        received_at, "5d", schedule=schedule
    ) == datetime(2026, 6, 1, 20, 0, tzinfo=UTC)
    assert research_simulation._maturity_for_horizon(
        received_at, "30m", schedule=schedule
    ) == received_at + timedelta(minutes=30)
    assert research_simulation._maturity_for_horizon(
        received_at, "60m", schedule=schedule
    ) == received_at + timedelta(minutes=60)
    assert research_simulation._maturity_for_horizon(
        received_at, "close", schedule=schedule
    ) == datetime(2026, 5, 22, 20, 0, tzinfo=UTC)
    assert research_simulation._maturity_for_horizon(
        datetime(2026, 6, 1, 15, 0, tzinfo=UTC), "5d", schedule=schedule
    ) == research_simulation.WINDOW_END.astimezone(UTC)
    with pytest.raises(ValueError, match="session close"):
        research_simulation._maturity_for_horizon(
            datetime(2026, 5, 23, 15, 0, tzinfo=UTC), "close", schedule=schedule
        )
    with pytest.raises(ValueError, match="horizon"):
        research_simulation._maturity_for_horizon(
            received_at, "overnight", schedule=schedule
        )


def test_adaptive_learning_uses_net_benchmark_alpha_and_role_bucket() -> None:
    state = research_simulation._initial_learning_state()
    state["pending_outcomes"] = [
        {
            "outcome_id": "outcome-1",
            "symbol": "SPY",
            "side": "long",
            "direction": "long",
            "horizon": "60m",
            "role": "technical_microstructure_analyst",
            "regime": "risk_on",
            "entry_price": 100.0,
            "benchmark_entry_price": 200.0,
            "matures_at": "2026-05-04T15:30:00Z",
            "spread_cost_bps": 3.0,
            "impact_cost_bps": 2.0,
            "commission_cost_bps": 1.0,
        }
    ]
    observations = [
        _observation(
            "quote-SPY-exit",
            "quote",
            event_time=datetime(2026, 5, 4, 15, 30, tzinfo=UTC),
            available_at=datetime(2026, 5, 4, 15, 30, tzinfo=UTC),
            payload={"bid": 100.9, "ask": 101.1},
        ),
        _observation(
            "quote-QQQ-exit",
            "quote",
            event_time=datetime(2026, 5, 4, 15, 30, tzinfo=UTC),
            available_at=datetime(2026, 5, 4, 15, 30, tzinfo=UTC),
            symbol="QQQ",
            payload={"bid": 201.9, "ask": 202.1},
        ),
    ]

    updated, delta = research_simulation._mature_learning(
        state,
        arm="semantic_adaptive",
        cutoff=datetime(2026, 5, 4, 15, 31, tzinfo=UTC),
        phase="learning",
        observations=observations,
        benchmark_symbol="QQQ",
    )

    assert delta is not None
    matured = delta["_matured_outcomes"][0]
    assert matured["outcome_return_bps"] == pytest.approx(100.0)
    assert matured["benchmark_return_bps"] == pytest.approx(100.0)
    assert matured["outcome_net_alpha_bps"] == pytest.approx(-6.0)
    bucket = updated["buckets"][
        "technical_microstructure_analyst:60m:long:risk_on"
    ]
    assert bucket["ewma_net_alpha_bps"] == pytest.approx(-1.2)


def test_forecast_cost_attribution_does_not_charge_avoided_or_embedded_costs() -> None:
    candidate = {"spread_bps": 80.0}
    policy = {"commission_bps": 1.0}

    assert research_simulation._forecast_outcome_costs(
        direction="avoid",
        diagnostic_only=True,
        candidate=candidate,
        cost_policy=policy,
    ) == {
        "spread_cost_bps": 0.0,
        "impact_cost_bps": 0.0,
        "commission_cost_bps": 0.0,
    }
    assert research_simulation._forecast_outcome_costs(
        direction="long",
        diagnostic_only=True,
        candidate=candidate,
        cost_policy=policy,
    ) == {
        "spread_cost_bps": 80.0,
        "impact_cost_bps": 2.0,
        "commission_cost_bps": 1.0,
    }
    assert research_simulation._forecast_outcome_costs(
        direction="long",
        diagnostic_only=False,
        candidate=candidate,
        cost_policy=policy,
    ) == {
        "spread_cost_bps": 0.0,
        "impact_cost_bps": 0.0,
        "commission_cost_bps": 1.0,
    }


def test_mature_learning_waits_for_the_benchmark_quote() -> None:
    state = research_simulation._initial_learning_state()
    state["pending_outcomes"] = [
        {
            "outcome_id": "outcome-missing-benchmark",
            "symbol": "SPY",
            "side": "long",
            "direction": "long",
            "horizon": "60m",
            "role": "catalyst_scout",
            "regime": "mixed",
            "entry_price": 100.0,
            "benchmark_entry_price": 200.0,
            "matures_at": "2026-05-04T15:30:00Z",
        }
    ]
    observations = [
        _observation(
            "quote-SPY-exit-only",
            "quote",
            event_time=datetime(2026, 5, 4, 15, 30, tzinfo=UTC),
            available_at=datetime(2026, 5, 4, 15, 30, tzinfo=UTC),
            payload={"bid": 100.9, "ask": 101.1},
        )
    ]

    updated, delta = research_simulation._mature_learning(
        state,
        arm="semantic_adaptive",
        cutoff=datetime(2026, 5, 4, 15, 31, tzinfo=UTC),
        phase="learning",
        observations=observations,
        benchmark_symbol="QQQ",
    )

    assert delta is None
    assert updated["pending_outcomes"] == state["pending_outcomes"]


def test_historical_execution_snapshot_reuses_existing_rust_contract() -> None:
    cutoff = datetime(2026, 5, 4, 13, 30, tzinfo=UTC)
    observations = [
        _observation(
            "bar-SPY",
            "bar",
            event_time=datetime(2026, 5, 4, 13, 29, tzinfo=UTC),
            available_at=cutoff,
            payload={
                "open": 99.0,
                "high": 101.0,
                "low": 98.5,
                "close": 100.0,
                "volume": 1_000_000.0,
                "session_open": 99.0,
                "bar_end": "2026-05-04T13:30:00Z",
                "adjustment": "raw",
            },
        ),
        _observation(
            "quote-SPY",
            "quote",
            event_time=datetime(2026, 5, 4, 13, 29, 59, tzinfo=UTC),
            available_at=datetime(2026, 5, 4, 13, 29, 59, tzinfo=UTC),
            payload={"bid": 99.95, "ask": 100.05},
        ),
        _observation(
            "macro-1",
            "macro",
            event_time=datetime(2026, 5, 4, 13, 0, tzinfo=UTC),
            available_at=datetime(2026, 5, 4, 13, 0, tzinfo=UTC),
            symbol=None,
            payload={"summary": "Official release was available before cutoff."},
        ),
        _observation(
            "news-1",
            "news",
            event_time=datetime(2026, 5, 4, 13, 0, tzinfo=UTC),
            available_at=datetime(2026, 5, 4, 13, 0, tzinfo=UTC),
            payload={"summary": "Bounded historical news summary."},
        ),
    ]
    cycle = next(
        cycle
        for cycle in build_cycle_schedule(
            [
                {
                    "session_date": "2026-05-04",
                    "open": "09:30",
                    "close": "16:00",
                    "available_at": "2026-05-01T20:00:00Z",
                }
            ]
        )
        if cycle.kind == "release"
    ).to_mapping()

    snapshot = _latest_rust_snapshot(observations, cutoff, cycle)

    assert snapshot is not None
    assert snapshot["unavailable_data_flags"] == []
    validate_market_research_snapshot(snapshot, load_equity_universe())


def test_execution_snapshot_excludes_unrelated_stale_symbols() -> None:
    cutoff = datetime(2026, 5, 4, 13, 31, tzinfo=UTC)
    observations = [
        _observation(
            "bar-SPY",
            "bar",
            event_time=datetime(2026, 5, 4, 13, 30, tzinfo=UTC),
            available_at=cutoff,
            payload={
                "open": 100.0,
                "high": 100.2,
                "low": 99.8,
                "close": 100.0,
                "volume": 1_000_000.0,
                "bar_end": "2026-05-04T13:31:00Z",
                "adjustment": "raw",
            },
        ),
        _observation(
            "quote-SPY",
            "quote",
            event_time=datetime(2026, 5, 4, 13, 30, 59, tzinfo=UTC),
            available_at=datetime(2026, 5, 4, 13, 30, 59, tzinfo=UTC),
            payload={"bid": 99.95, "ask": 100.05},
        ),
        _observation(
            "bar-AAPL",
            "bar",
            event_time=datetime(2026, 5, 4, 13, 0, tzinfo=UTC),
            available_at=datetime(2026, 5, 4, 13, 1, tzinfo=UTC),
            symbol="AAPL",
            payload={
                "open": 200.0,
                "high": 200.2,
                "low": 199.8,
                "close": 200.0,
                "volume": 1_000_000.0,
                "bar_end": "2026-05-04T13:01:00Z",
                "adjustment": "raw",
            },
        ),
        _observation(
            "quote-AAPL",
            "quote",
            event_time=datetime(2026, 5, 4, 13, 0, 59, tzinfo=UTC),
            available_at=datetime(2026, 5, 4, 13, 0, 59, tzinfo=UTC),
            symbol="AAPL",
            payload={"bid": 199.95, "ask": 200.05},
        ),
        _observation(
            "macro-1",
            "macro",
            event_time=datetime(2026, 5, 4, 13, 0, tzinfo=UTC),
            available_at=datetime(2026, 5, 4, 13, 0, tzinfo=UTC),
            symbol=None,
            payload={"summary": "Official release."},
        ),
        _observation(
            "news-1",
            "news",
            event_time=datetime(2026, 5, 4, 13, 0, tzinfo=UTC),
            available_at=datetime(2026, 5, 4, 13, 0, tzinfo=UTC),
            symbol=None,
            payload={"summary": "Historical news."},
        ),
    ]
    cycle = next(
        cycle
        for cycle in build_cycle_schedule(
            [
                {
                    "session_date": "2026-05-04",
                    "open": "09:30",
                    "close": "16:00",
                    "available_at": "2026-05-01T20:00:00Z",
                }
            ]
        )
        if cycle.kind == "release"
    ).to_mapping()

    snapshot = _latest_rust_snapshot(
        observations,
        cutoff,
        cycle,
        required_symbols={"SPY"},
    )

    assert snapshot is not None
    assert snapshot["symbols"] == ["SPY"]
    assert set(snapshot["latest_bars"]) == {"SPY"}
    assert set(snapshot["latest_quotes"]) == {"SPY"}


def test_execution_symbols_include_commands_and_existing_holdings() -> None:
    assert research_simulation._execution_symbols(
        {"commands": [{"symbol": "SPY"}]},
        {"positions": {"AAPL": {"quantity": 1.0}}},
    ) == {"AAPL", "SPY"}


def test_quote_partitions_preserve_exact_cutoffs_and_canonical_hashes(
    tmp_path: Path,
) -> None:
    staging = tmp_path / "quote-staging"
    builder = QuotePartitionBuilder(staging, feed="iex")
    builder.append_page(
        {
            "SPY": [
                {
                    "timestamp": "2026-05-04T13:29:59.100000Z",
                    "bid": 99.9,
                    "ask": 100.1,
                    "source": "alpaca_market_data",
                },
                {
                    "timestamp": "2026-05-04T13:29:59.900000Z",
                    "bid": 100.0,
                    "ask": 100.2,
                    "source": "alpaca_market_data",
                },
                {
                    "timestamp": "2026-05-04T13:30:00.100000Z",
                    "bid": 100.1,
                    "ask": 100.3,
                    "bid_size": 10.0,
                    "ask_size": 20.0,
                    "source": "alpaca_market_data",
                },
            ]
        }
    )
    partitions = builder.finalize()
    assert partitions[0]["row_count"] == 2
    source = tmp_path / "partitioned-source.json"
    source.write_text(
        json.dumps(
            {
                "calendar": [
                    {
                        "session_date": "2026-05-04",
                        "open": "09:30",
                        "close": "16:00",
                        "available_at": "2026-05-01T20:00:00Z",
                    }
                ],
                "observations": [
                    {
                        "schema_version": 1,
                        "observation_id": "bar-SPY",
                        "kind": "bar",
                        "source": "alpaca_historical_bars",
                        "event_time": "2026-05-04T13:29:00Z",
                        "available_at": "2026-05-04T13:30:00Z",
                        "symbol": "SPY",
                        "payload": {
                            "open": 100.0,
                            "high": 100.2,
                            "low": 99.8,
                            "close": 100.0,
                            "volume": 1000000.0,
                            "session_open": 100.0,
                            "bar_end": "2026-05-04T13:30:00Z",
                            "adjustment": "raw",
                        },
                    }
                ],
                "quote_partitions": partitions,
                "coverage": {"quote": "complete"},
            }
        ),
        encoding="utf-8",
    )

    run_dir = prepare_run(
        state_root=tmp_path / "runs",
        run_id="partitioned-run",
        source_path=source,
        code_fingerprint="sha256:test-code",
    )
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    frozen = CanonicalRunStore(run_dir).get_object(manifest["dataset"]["object"])
    index = QuotePartitionIndex(run_dir, frozen["quote_partitions"])

    before = index.latest("SPY", datetime(2026, 5, 4, 13, 29, 59, 500000, tzinfo=UTC))
    after = index.latest("SPY", datetime(2026, 5, 4, 13, 30, 0, 500000, tzinfo=UTC))
    first = index.first_at_or_after(
        "SPY",
        datetime(2026, 5, 4, 13, 29, 59, 500000, tzinfo=UTC),
        datetime(2026, 5, 4, 13, 30, 0, tzinfo=UTC),
    )
    assert before is None
    assert after is not None and after.payload == {
        "bid": 100.1,
        "ask": 100.3,
        "bid_size": 10.0,
        "ask_size": 20.0,
        "feed": "iex",
    }
    assert first is not None and first.payload == {
        "bid": 100.0,
        "ask": 100.2,
        "feed": "iex",
    }
    assert index.latest("SPY", datetime(2026, 5, 4, 12, tzinfo=UTC)) is None
    assert index.first_at_or_after(
        "SPY",
        datetime(2026, 5, 4, 14, tzinfo=UTC),
        datetime(2026, 5, 4, 15, tzinfo=UTC),
    ) is None
    assert index.first_at_or_after(
        "SPY",
        datetime(2026, 5, 4, 12, tzinfo=UTC),
        datetime(2026, 5, 4, 13, tzinfo=UTC),
    ) is None
    assert after.observation_id.startswith("quote:sha256:")
    frozen_observations = [
        ResearchObservation.from_mapping(row) for row in frozen["observations"]
    ]
    pack, block_reason = research_simulation.build_point_in_time_pack(
        frozen_observations,
        cutoff=datetime(2026, 5, 4, 13, 30, 0, 500000, tzinfo=UTC),
        semantic=False,
        learning_state={},
        quote_index=index,
    )
    assert block_reason is None
    assert pack["ranked_candidates"][0]["quote_evidence_id"].startswith("quote:sha256:")

    inline_quote = _observation(
        "inline-later",
        "quote",
        event_time=datetime(2026, 5, 4, 13, 30, 0, 200000, tzinfo=UTC),
        available_at=datetime(2026, 5, 4, 13, 30, 0, 200000, tzinfo=UTC),
        payload={"bid": 100.2, "ask": 100.4},
    )
    latest = research_simulation._latest_quote(
        [inline_quote],
        "SPY",
        datetime(2026, 5, 4, 13, 30, 0, 500000, tzinfo=UTC),
        quote_index=index,
    )
    earliest = research_simulation._first_quote(
        [inline_quote],
        "SPY",
        datetime(2026, 5, 4, 13, 29, 59, 500000, tzinfo=UTC),
        datetime(2026, 5, 4, 13, 30, 0, 500000, tzinfo=UTC),
        quote_index=index,
    )
    inline_only = research_simulation._first_quote(
        [inline_quote],
        "SPY",
        datetime(2026, 5, 4, 13, 30, 0, 150000, tzinfo=UTC),
        datetime(2026, 5, 4, 13, 30, 0, 500000, tzinfo=UTC),
        quote_index=QuotePartitionIndex(run_dir, []),
    )
    assert latest == inline_quote
    assert earliest is not None and earliest.payload == {
        "bid": 100.0,
        "ask": 100.2,
        "feed": "iex",
    }
    assert inline_only == inline_quote
    assert verify_run(run_dir)["valid"] is True

    blob = next((run_dir / "blobs" / "sha256").glob("*.trq"))
    blob.write_bytes(blob.read_bytes() + b"tampered")
    with pytest.raises(ValueError, match="checksum.*mismatch"):
        verify_run(run_dir)


def test_quote_collection_windows_cover_decisions_and_close_without_overlap() -> None:
    calendar = [
        {
            "session_date": "2026-05-04",
            "open": "09:30",
            "close": "16:00",
            "available_at": "2026-05-03T03:59:59Z",
        }
    ]

    windows = research_simulation._quote_collection_windows(
        calendar,
        lookback=timedelta(minutes=3),
        forward=timedelta(minutes=7),
    )

    assert len(windows) == 13
    assert windows[0] == (
        datetime(2026, 5, 4, 12, 57, tzinfo=UTC),
        datetime(2026, 5, 4, 13, 7, tzinfo=UTC),
    )
    assert windows[-1] == (
        datetime(2026, 5, 4, 19, 57, tzinfo=UTC),
        datetime(2026, 5, 4, 20, 0, 1, tzinfo=UTC),
    )
    merged = research_simulation._quote_collection_windows(
        calendar,
        lookback=timedelta(minutes=20),
        forward=timedelta(minutes=20),
    )
    assert len(merged) < len(windows)
    with pytest.raises(ValueError, match="bounded durations"):
        research_simulation._quote_collection_windows(
            calendar,
            lookback=timedelta(minutes=-1),
            forward=timedelta(minutes=7),
        )


def test_quote_window_latency_gate_blocks_only_late_rth_execution() -> None:
    cutoff = datetime(2026, 5, 4, 13, 30, tzinfo=UTC)
    result = research_simulation.ResearchCycleResult(
        cycle_id="cycle-1",
        invocations=(),
        trade_allowed=True,
        block_reason=None,
        arbiter_output={"verdict": "trade"},
        simulated_received_at=cutoff + timedelta(minutes=8),
    )

    gated = research_simulation._enforce_quote_window_latency(
        result, cutoff=cutoff, forward_minutes=7.0, is_rth=True
    )

    assert gated.trade_allowed is False
    assert gated.block_reason == "agent_latency_exceeded_quote_window"
    assert research_simulation._enforce_quote_window_latency(
        result, cutoff=cutoff, forward_minutes=7.0, is_rth=False
    ) is result
    assert research_simulation._enforce_quote_window_latency(
        result, cutoff=cutoff, forward_minutes=0.0, is_rth=True
    ) is result
    on_time = research_simulation.ResearchCycleResult(
        **{
            **result.__dict__,
            "simulated_received_at": cutoff + timedelta(minutes=7),
        }
    )
    assert research_simulation._enforce_quote_window_latency(
        on_time, cutoff=cutoff, forward_minutes=7.0, is_rth=True
    ) is on_time


def _write_quote_partition(path: Path, header: Mapping[str, object]) -> None:
    encoded = json.dumps(header, sort_keys=True, separators=(",", ":")).encode()
    path.write_bytes(
        research_simulation.QUOTE_PARTITION_MAGIC
        + research_simulation.QUOTE_PARTITION_HEADER_LENGTH.pack(len(encoded))
        + encoded
        + research_simulation.QUOTE_PARTITION_RECORD.pack(
            int(datetime(2026, 5, 4, 13, 30, tzinfo=UTC).timestamp() * 1_000_000),
            99.9,
            100.1,
        )
    )


def test_quote_partition_builder_and_index_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="feed is required"):
        QuotePartitionBuilder(tmp_path / "empty-feed", feed=" ")

    invalid = QuotePartitionBuilder(tmp_path / "invalid", feed="iex")
    invalid.append_page(
        {
            "SPY": [
                {"timestamp": "2026-05-04T13:29:58Z", "bid": 0.0, "ask": 100.0},
                {"timestamp": "2026-05-04T13:29:59Z", "bid": 101.0, "ask": 100.0},
                {"timestamp": "2026-05-04T13:30:00Z", "bid": 99.9, "ask": 100.1},
            ]
        }
    )
    invalid_descriptors = invalid.finalize()
    assert invalid.rejected_row_count == 2
    assert invalid_descriptors[0]["row_count"] == 1

    unsorted = QuotePartitionBuilder(tmp_path / "unsorted", feed="iex")
    unsorted.append_page(
        {"SPY": [{"timestamp": "2026-05-04T13:30:01Z", "bid": 99.9, "ask": 100.1}]}
    )
    with pytest.raises(ValueError, match="not time ordered"):
        unsorted.append_page(
            {"SPY": [{"timestamp": "2026-05-04T13:30:00Z", "bid": 99.8, "ask": 100.0}]}
        )

    builder = QuotePartitionBuilder(tmp_path / "valid", feed="iex")
    builder.append_page(
        {"SPY": [{"timestamp": "2026-05-04T13:30:00Z", "bid": 99.9, "ask": 100.1}]}
    )
    descriptors = builder.finalize()
    assert builder.finalize() == descriptors
    with pytest.raises(RuntimeError, match="already finalized"):
        builder.append_page({})

    path = Path(descriptors[0]["path"])
    header, _offset = research_simulation._read_quote_partition_header(path)
    digest = research_simulation._sha256_file(path)
    canonical = tmp_path / "canonical" / "blobs" / "sha256"
    canonical.mkdir(parents=True)
    blob = canonical / f"{digest.removeprefix('sha256:')}.trq"
    blob.write_bytes(path.read_bytes())
    descriptor = {**header, "object": digest}

    with pytest.raises(ValueError, match="object digest"):
        QuotePartitionIndex(tmp_path / "canonical", [{**descriptor, "object": "bad"}])
    with pytest.raises(ValueError, match="descriptor does not match"):
        QuotePartitionIndex(
            tmp_path / "canonical", [{**descriptor, "row_count": 2}]
        )

    index = QuotePartitionIndex(tmp_path / "canonical", [descriptor])
    blob.write_bytes(blob.read_bytes()[:-1])
    with pytest.raises(ValueError, match="record is truncated"):
        index.latest("SPY", datetime(2026, 5, 4, 14, tzinfo=UTC))

    legacy_path = tmp_path / "legacy.trq"
    legacy_header = {
        "schema_version": 1,
        "symbol": "QQQ",
        "session_date": "2026-05-04",
        "source": "alpaca_historical_quotes",
        "feed": "iex",
        "row_count": 1,
        "first_available_at": "2026-05-04T13:30:00Z",
        "last_available_at": "2026-05-04T13:30:00Z",
        "record_size": research_simulation.QUOTE_PARTITION_RECORD.size,
    }
    _write_quote_partition(legacy_path, legacy_header)
    legacy_digest = research_simulation._sha256_file(legacy_path)
    legacy_blob = canonical / f"{legacy_digest.removeprefix('sha256:')}.trq"
    legacy_blob.write_bytes(legacy_path.read_bytes())
    legacy = QuotePartitionIndex(
        tmp_path / "canonical", [{**legacy_header, "object": legacy_digest}]
    ).latest("QQQ", datetime(2026, 5, 4, 14, tzinfo=UTC))
    assert legacy is not None
    assert legacy.payload == {"bid": 99.9, "ask": 100.1, "feed": "iex"}


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("bad_magic", "magic"),
        ("short_length", "header is truncated"),
        ("zero_length", "header length"),
        ("short_header", "header is truncated"),
        ("schema", "schema_version"),
        ("schema_type", "schema_version must be an integer"),
        ("unsupported_schema", "schema_version"),
        ("source", "source is not authorized"),
        ("feed", "feed is missing"),
        ("rows", "row metadata"),
        ("rows_type", "row_count must be an integer"),
        ("bounds", "timestamp bounds"),
        ("size", "size does not match"),
    ],
)
def test_quote_partition_header_rejects_corruption(
    tmp_path: Path, mutation: str, message: str
) -> None:
    path = tmp_path / f"{mutation}.trq"
    header: dict[str, object] = {
        "schema_version": 1,
        "symbol": "SPY",
        "session_date": "2026-05-04",
        "source": "alpaca_historical_quotes",
        "feed": "iex",
        "row_count": 1,
        "first_available_at": "2026-05-04T13:30:00Z",
        "last_available_at": "2026-05-04T13:30:00Z",
        "record_size": research_simulation.QUOTE_PARTITION_RECORD.size,
    }
    if mutation == "bad_magic":
        path.write_bytes(b"INVALID\n")
    elif mutation == "short_length":
        path.write_bytes(research_simulation.QUOTE_PARTITION_MAGIC + b"\x00")
    elif mutation == "zero_length":
        path.write_bytes(
            research_simulation.QUOTE_PARTITION_MAGIC
            + research_simulation.QUOTE_PARTITION_HEADER_LENGTH.pack(0)
        )
    elif mutation == "short_header":
        path.write_bytes(
            research_simulation.QUOTE_PARTITION_MAGIC
            + research_simulation.QUOTE_PARTITION_HEADER_LENGTH.pack(10)
            + b"{}"
        )
    else:
        if mutation == "schema":
            header["schema_version"] = 2
        elif mutation == "schema_type":
            header["schema_version"] = []
        elif mutation == "unsupported_schema":
            header["schema_version"] = 3
        elif mutation == "source":
            header["source"] = "current_snapshot"
        elif mutation == "feed":
            header["feed"] = ""
        elif mutation == "rows":
            header["row_count"] = 0
        elif mutation == "rows_type":
            header["row_count"] = []
        elif mutation == "bounds":
            header["first_available_at"] = "2026-05-04T13:31:00Z"
        _write_quote_partition(path, header)
        if mutation == "size":
            path.write_bytes(path.read_bytes() + b"extra")

    with pytest.raises(ValueError, match=message):
        research_simulation._read_quote_partition_header(path)


def test_prepare_rejects_partition_metadata_or_copy_digest_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    builder = QuotePartitionBuilder(tmp_path / "staging", feed="iex")
    builder.append_page(
        {"SPY": [{"timestamp": "2026-05-04T13:30:00Z", "bid": 99.9, "ask": 100.1}]}
    )
    partition = builder.finalize()[0]
    source = tmp_path / "source.json"

    def write_source(descriptor: Mapping[str, object]) -> None:
        source.write_text(
            json.dumps(
                {
                    "calendar": [],
                    "observations": [],
                    "quote_partitions": [descriptor],
                    "coverage": {"quote": "complete"},
                }
            ),
            encoding="utf-8",
        )

    write_source({**partition, "row_count": 2})
    with pytest.raises(ValueError, match="metadata does not match"):
        prepare_run(
            state_root=tmp_path / "runs",
            run_id="metadata-mismatch",
            source_path=source,
            code_fingerprint="sha256:test-code",
        )

    write_source(partition)
    monkeypatch.setattr(
        CanonicalRunStore,
        "put_blob",
        lambda self, path, suffix: "sha256:" + "0" * 64,
    )
    with pytest.raises(ValueError, match="digest changed"):
        prepare_run(
            state_root=tmp_path / "runs",
            run_id="digest-mismatch",
            source_path=source,
            code_fingerprint="sha256:test-code",
        )


def test_research_universe_uses_only_pre_window_daily_bars(tmp_path: Path) -> None:
    symbol_master = tmp_path / "FINRACATReportableEquitySecurities_SOD.txt"
    symbol_master.write_text(
        "symbol|issueName|listingExchange|testIssueFlag\n"
        "SPY|SPDR S&P 500 ETF|P|N\n"
        "ACTIVE|Active Corp|Q|N\n"
        "VOLATILE|Volatile Corp|N|N\n"
        "NEGLECT|Neglected Corp|A|N\n"
        "FUTURE|Future Corp|Q|N\n"
        "TEST|Test Security|Q|Y\n"
        "OTC|OTC Security|U|N\n"
        "$BAD|Invalid Symbol|Q|N\n"
        "ACTIVE|Duplicate Active Corp|Q|N\n"
        "SHORT|Short History Corp|Q|N\n"
        "PENNY|Penny Corp|Q|N\n"
        "20260501060000|11\n",
        encoding="utf-8",
    )
    daily_bars: dict[str, list[dict[str, object]]] = {}
    for symbol, closes, volume in (
        ("SPY", [500.0 + index * 0.01 for index in range(20)], 10_000_000.0),
        ("ACTIVE", [10.0 + index * 0.01 for index in range(20)], 5_000_000.0),
        ("VOLATILE", [10.0 + (1.0 if index % 2 else -1.0) for index in range(20)], 500_000.0),
        ("NEGLECT", [10.0 + index * 0.001 for index in range(20)], 100_001.0),
        ("FUTURE", [10.0 for _ in range(20)], 5_000_000.0),
    ):
        daily_bars[symbol] = [
            {
                "timestamp": (
                    datetime(2026, 4, 1, tzinfo=UTC) + timedelta(days=index)
                ).isoformat().replace("+00:00", "Z"),
                "close": close,
                "volume": volume,
            }
            for index, close in enumerate(closes)
        ]
    daily_bars["FUTURE"][-1]["timestamp"] = "2026-05-04T00:00:00Z"
    daily_bars["SHORT"] = daily_bars["ACTIVE"][:14]
    daily_bars["PENNY"] = [
        {**row, "close": 0.5} for row in daily_bars["ACTIVE"]
    ]

    selected = select_research_universe(
        symbol_master,
        daily_bars,
        core_symbols=("SPY",),
        as_of=date(2026, 5, 1),
        limit=4,
        minimum_median_dollar_volume=1_000_000.0,
    )

    assert selected["symbols"] == ["SPY", "ACTIVE", "VOLATILE", "NEGLECT"]
    assert selected["lanes"] == {
        "SPY": "core",
        "ACTIVE": "active",
        "VOLATILE": "volatile",
        "NEGLECT": "neglected_liquidity",
    }
    assert selected["symbol_master_created_at"] == "2026-05-01T10:00:00Z"
    assert selected["symbol_master_lag_days"] == 0
    assert selected["symbol_master_freshness"] == "exact_as_of"


def test_research_universe_marks_prior_official_symbol_master_stale(
    tmp_path: Path,
) -> None:
    symbol_master = tmp_path / "FINRACATReportableEquitySecurities_SOD.txt"
    symbol_master.write_text(
        "symbol|issueName|listingExchange|testIssueFlag\n"
        "SPY|SPDR S&P 500 ETF|P|N\n"
        "20260109050741|1\n",
        encoding="utf-8",
    )
    rows = [
        {
            "timestamp": (
                datetime(2026, 4, 1, tzinfo=UTC) + timedelta(days=index)
            ).isoformat(),
            "close": 500.0,
            "volume": 10_000_000.0,
        }
        for index in range(20)
    ]

    selected = select_research_universe(
        symbol_master,
        {"SPY": rows},
        core_symbols=("SPY",),
        as_of=date(2026, 5, 1),
        limit=1,
    )

    assert selected["symbol_master_lag_days"] == 112
    assert selected["symbol_master_freshness"] == "stale_prior_official_snapshot"


@pytest.mark.parametrize(
    ("contents", "message"),
    [
        ("symbol|listingExchange|testIssueFlag\n", "incomplete"),
        (
            "symbol|listingExchange\nSPY|P\n20260501060000|1\n",
            "headers are incomplete",
        ),
        (
            "symbol|listingExchange|testIssueFlag\nSPY|P|N\ninvalid|1\n",
            "footer is invalid",
        ),
        (
            "symbol|listingExchange|testIssueFlag\nSPY|P|N\n20260502060000|1\n",
            "after the requested",
        ),
        (
            "symbol|listingExchange|testIssueFlag\nSPY|P|N\n20260501060000|2\n",
            "record count",
        ),
        (
            "symbol|listingExchange|testIssueFlag\nSPY|P\n20260501060000|1\n",
            "row width",
        ),
    ],
)
def test_cat_symbol_master_rejects_invalid_archives(
    tmp_path: Path, contents: str, message: str
) -> None:
    path = tmp_path / "symbol-master.txt"
    path.write_text(contents, encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        research_simulation._cat_symbol_master(path, as_of=date(2026, 5, 1))


def test_research_universe_rejects_unverified_or_insufficient_symbols(tmp_path: Path) -> None:
    path = tmp_path / "symbol-master.txt"
    path.write_text(
        "symbol|listingExchange|testIssueFlag\n"
        "SPY|P|N\n"
        "AAPL|Q|N\n"
        "20260501060000|2\n",
        encoding="utf-8",
    )
    bars = {
        "SPY": [
            {
                "timestamp": (
                    datetime(2026, 4, 1, tzinfo=UTC) + timedelta(days=index)
                ).isoformat(),
                "close": 500.0,
                "volume": 10_000_000.0,
            }
            for index in range(20)
        ]
    }

    with pytest.raises(ValueError, match="cannot be smaller than the core"):
        select_research_universe(path, bars, core_symbols=("SPY", "AAPL"), limit=1)
    with pytest.raises(ValueError, match="lack point-in-time"):
        select_research_universe(path, bars, core_symbols=("SPY", "AAPL"), limit=2)
    with pytest.raises(ValueError, match="cannot populate"):
        select_research_universe(path, bars, core_symbols=("SPY",), limit=2)


def test_point_in_time_universe_collector_chunks_pre_window_bars(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    core = list(load_equity_universe().core_symbols)
    symbols = [*core, *(f"Z{index:03d}" for index in range(90))]
    master = tmp_path / "symbol-master.txt"
    master.write_text(
        "symbol|listingExchange|testIssueFlag\n"
        + "".join(f"{symbol}|Q|N\n" for symbol in symbols)
        + f"20260501060000|{len(symbols)}\n",
        encoding="utf-8",
    )
    sessions: list[date] = []
    current = date(2026, 4, 1)
    while current <= date(2026, 5, 1):
        if current.weekday() < 5:
            sessions.append(current)
        current += timedelta(days=1)

    class FakeUniverseClient:
        calendar_rows = sessions
        calls: ClassVar[list[tuple[str, ...]]] = []

        def __init__(self, config: AlpacaMarketDataConfig) -> None:
            assert config.feed == "iex"

        def market_calendar(self, *, start: date, end: date) -> list[dict[str, str]]:
            return [
                {"session_date": session.isoformat(), "open": "09:30", "close": "16:00"}
                for session in self.calendar_rows
            ]

        def historical_bars(
            self,
            requested: tuple[str, ...] | list[str],
            *,
            timeframe: str,
            start: datetime,
            end: datetime,
            adjustment: str,
            max_pages: int,
            require_complete: bool,
        ) -> dict[str, list[dict[str, object]]]:
            assert timeframe == "1Day"
            assert adjustment == "raw"
            assert max_pages == 1
            assert require_complete is True
            assert start < end
            self.calls.append(tuple(requested))
            return {
                symbol: [
                    {
                        "timestamp": datetime.combine(session, time(0), UTC).isoformat(),
                        "close": 10.0 + index * 0.01,
                        "volume": 3_000_000.0,
                    }
                    for index, session in enumerate(self.calendar_rows[-20:])
                ]
                for symbol in requested
            }

    monkeypatch.setenv("ALPACA_API_KEY_ID", "key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "secret")
    monkeypatch.setenv("ALPACA_DATA_FEED", "iex")
    monkeypatch.setenv("TRAIDAN_RESEARCH_UNIVERSE_CHUNK_SIZE", "30")
    monkeypatch.setenv("TRAIDAN_RESEARCH_MAX_PAGES", "0")
    monkeypatch.setattr("llm_equity.alpaca.AlpacaMarketDataClient", FakeUniverseClient)

    universe = research_simulation.collect_point_in_time_research_universe(master)

    assert len(universe["symbols"]) == 100
    assert universe["symbols"][:2] == ["SPY", "QQQ"]
    assert universe["minimum_last_close_usd"] == 5.0
    assert universe["minimum_median_dollar_volume"] == 20_000_000.0
    assert len(FakeUniverseClient.calls) == 4
    assert all(len(call) <= 30 for call in FakeUniverseClient.calls)

    FakeUniverseClient.calendar_rows = sessions[:19]
    with pytest.raises(ValueError, match="20 pre-window"):
        research_simulation.collect_point_in_time_research_universe(master)


def test_research_runner_loads_existing_credentials_without_printing_them(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "alpaca.env"
    env_file.write_text(
        "ALPACA_API_KEY_ID=public-test-key\n"
        "ALPACA_API_SECRET_KEY=private-test-secret\n"
        "ALPACA_DATA_FEED=iex\n",
        encoding="utf-8",
    )
    root = Path(__file__).resolve().parents[1]
    completed = subprocess.run(
        ["bash", str(root / "scripts" / "research-simulation.sh"), "--help"],
        cwd=root,
        env={"PATH": os.environ["PATH"], "TRAIDAN_ALPACA_ENV_FILE": str(env_file)},
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    assert "point-in-time equity research simulation" in completed.stdout
    assert "public-test-key" not in completed.stdout + completed.stderr
    assert "private-test-secret" not in completed.stdout + completed.stderr


def test_corporate_actions_use_conservative_process_availability() -> None:
    calendar = [
        {
            "session_date": "2026-05-04",
            "open": "09:30",
            "close": "16:00",
        },
        {
            "session_date": "2026-05-05",
            "open": "09:30",
            "close": "16:00",
        },
    ]
    rows = [
        {
            "family": "forward_splits",
            "id": "split-1",
            "symbol": "SPY",
            "process_date": "2026-05-04",
            "ex_date": "2026-05-04",
            "old_rate": 1.0,
            "new_rate": 2.0,
        },
        {
            "family": "cash_dividends",
            "id": "dividend-1",
            "symbol": "SPY",
            "process_date": "2026-05-04",
            "payable_date": "2026-05-05",
            "rate": 0.25,
        },
        {
            "family": "name_changes",
            "id": "rename-1",
            "old_symbol": "OLD",
            "new_symbol": "NEW",
            "process_date": "2026-05-04",
        },
    ]

    observations = research_simulation._historical_corporate_action_observations(
        rows, calendar
    )

    assert {row["payload"]["action_type"] for row in observations} == {
        "split",
        "cash_dividend",
        "symbol_change",
    }
    assert {row["available_at"] for row in observations} == {"2026-05-05T13:30:00Z"}
    split = next(row for row in observations if row["observation_id"] == "split-1")
    assert split["payload"]["ratio"] == 2.0


def test_corporate_actions_reject_invalid_values_and_fallback_to_weekday() -> None:
    weekend = research_simulation._historical_corporate_action_observations(
        [
            {
                "family": "name_changes",
                "id": "rename-weekend",
                "old_symbol": "OLD",
                "new_symbol": "NEW",
                "process_date": "2026-05-08",
            },
            {
                "family": "unsupported",
                "id": "ignored",
                "process_date": "2026-05-08",
            },
        ],
        [],
    )
    assert weekend[0]["available_at"] == "2026-05-11T13:30:00Z"

    invalid_rows = [
        {
            "family": "forward_splits",
            "id": "",
            "symbol": "SPY",
            "process_date": "2026-05-04",
            "ex_date": "2026-05-04",
            "old_rate": 1.0,
            "new_rate": 2.0,
        },
        {
            "family": "reverse_splits",
            "id": "bad-split",
            "symbol": "SPY",
            "process_date": "2026-05-04",
            "ex_date": "2026-05-04",
            "old_rate": 0.0,
            "new_rate": 1.0,
        },
        {
            "family": "cash_dividends",
            "id": "bad-dividend",
            "symbol": "SPY",
            "process_date": "2026-05-04",
            "payable_date": "2026-05-05",
            "rate": -0.1,
        },
    ]
    for row, message in zip(
        invalid_rows,
        ("id is missing", "rates must be positive", "must be non-negative"),
    ):
        with pytest.raises(ValueError, match=message):
            research_simulation._historical_corporate_action_observations([row], [])


def test_alpaca_source_collection_streams_quotes_and_uses_historical_surfaces(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    macro = tmp_path / "macro.json"
    macro.write_text(
        json.dumps(
            {
                "observations": [
                    {
                        "schema_version": 1,
                        "observation_id": "macro-1",
                        "kind": "macro",
                        "source": "federal_reserve_archive",
                        "event_time": "2026-05-04T12:00:00Z",
                        "available_at": "2026-05-04T12:00:00Z",
                        "symbol": None,
                        "payload": {"value": 1.0},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    class FakeHistoricalClient:
        inline_quote_calls = 0
        streamed_quote_calls = 0

        def __init__(self, config: AlpacaMarketDataConfig) -> None:
            assert config.feed == "iex"

        def market_calendar(self, *, start: date, end: date) -> list[dict[str, str]]:
            return [{"session_date": "2026-05-04", "open": "09:30", "close": "16:00"}]

        def historical_bars(
            self, symbols: tuple[str, ...], **kwargs: object
        ) -> dict[str, list[dict[str, object]]]:
            return {
                "SPY": [
                    {
                        "timestamp": "2026-05-04T13:29:00Z",
                        "open": 100.0,
                        "high": 100.2,
                        "low": 99.8,
                        "close": 100.1,
                        "volume": 1000.0,
                    },
                    {
                        "timestamp": "2026-05-04T13:30:00Z",
                        "open": 100.1,
                        "high": 100.3,
                        "low": 100.0,
                        "close": 100.2,
                        "volume": 1100.0,
                    },
                ]
            }

        def historical_quotes(
            self, symbols: tuple[str, ...], **kwargs: object
        ) -> dict[str, list[dict[str, object]]]:
                type(self).inline_quote_calls += 1
                return {
                    "SPY": [
                        {"timestamp": "2026-05-04T13:29:58Z", "bid": 0.0, "ask": 100.2},
                        {"timestamp": "2026-05-04T13:29:59Z", "bid": 100.0, "ask": 100.2}
                    ]
                }

        def iter_historical_quote_pages(
            self, symbols: tuple[str, ...], **kwargs: object
        ) -> object:
            type(self).streamed_quote_calls += 1
            yield {
                "SPY": [
                    {"timestamp": "2026-05-04T13:29:58Z", "bid": 0.0, "ask": 100.2},
                    {"timestamp": "2026-05-04T13:29:59Z", "bid": 100.0, "ask": 100.2},
                ]
            }

        def historical_news(
            self, symbols: tuple[str, ...], **kwargs: object
        ) -> list[dict[str, object]]:
            return [
                {
                    "id": 1,
                    "created_at": "2026-05-04T13:00:00Z",
                    "updated_at": "2026-05-04T13:01:00Z",
                    "headline": "Historical headline",
                    "summary": "Bounded summary",
                    "url": "https://example.test/1",
                    "source": "wire",
                    "symbols": ["SPY"],
                }
            ]

        def historical_corporate_actions(
            self, symbols: tuple[str, ...], **kwargs: object
        ) -> list[dict[str, object]]:
            return [
                {
                    "family": "forward_splits",
                    "id": "split-1",
                    "symbol": "SPY",
                    "process_date": "2026-05-04",
                    "ex_date": "2026-05-05",
                    "old_rate": 1.0,
                    "new_rate": 2.0,
                }
            ]

    monkeypatch.setenv("ALPACA_API_KEY_ID", "key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "secret")
    monkeypatch.setenv("ALPACA_DATA_FEED", "iex")
    monkeypatch.setenv("TRAIDAN_RESEARCH_MACRO_SOURCE", str(macro))
    monkeypatch.setattr("llm_equity.alpaca.AlpacaMarketDataClient", FakeHistoricalClient)

    inline = research_simulation.collect_alpaca_source_bundle(["spy"])
    streamed = research_simulation.collect_alpaca_source_bundle(
        ["SPY"], quote_partition_root=tmp_path / "quotes"
    )

    assert {row["kind"] for row in inline["observations"]} == {
        "bar",
        "quote",
        "news",
        "macro",
        "corporate_action",
    }
    assert inline["coverage"]["quote"] == "limited_invalid_rows:1"
    bars = [row for row in inline["observations"] if row["kind"] == "bar"]
    assert [row["payload"].get("session_open") for row in bars] == [None, 100.1]
    assert streamed["quote_partitions"][0]["row_count"] == 1
    assert not any(row["kind"] == "quote" for row in streamed["observations"])
    assert streamed["coverage"]["quote"] == "limited_invalid_rows:1"
    assert streamed["coverage"]["borrow"] == "unavailable"
    assert FakeHistoricalClient.inline_quote_calls == 1
    assert FakeHistoricalClient.streamed_quote_calls == 1

    FakeHistoricalClient.streamed_quote_calls = 0
    monkeypatch.setenv("TRAIDAN_RESEARCH_QUOTE_WINDOW_LOOKBACK_MINUTES", "3")
    monkeypatch.setenv("TRAIDAN_RESEARCH_QUOTE_WINDOW_FORWARD_MINUTES", "7")
    windowed = research_simulation.collect_alpaca_source_bundle(
        ["SPY"], quote_partition_root=tmp_path / "windowed-quotes"
    )
    assert FakeHistoricalClient.streamed_quote_calls == 13
    assert windowed["coverage"]["quote"] == (
        "limited_cycle_windows:3m_before:7m_after:invalid_rows:13"
    )
    assert windowed["coverage"]["quote_window_lookback_minutes"] == 3
    assert windowed["coverage"]["quote_window_forward_minutes"] == 7

    monkeypatch.setenv("TRAIDAN_RESEARCH_QUOTE_WINDOW_LOOKBACK_MINUTES", "0")
    with pytest.raises(ValueError, match="both historical quote window"):
        research_simulation.collect_alpaca_source_bundle(
            ["SPY"], quote_partition_root=tmp_path / "invalid-window-quotes"
        )

    with pytest.raises(ValueError, match="historical symbols are required"):
        research_simulation.collect_alpaca_source_bundle([])

    completeness, reasons = research_simulation._dataset_completeness(
        [],
        build_cycle_schedule(streamed["calendar"]),
        streamed["coverage"],
        partition_kinds=frozenset({"quote"}),
    )
    assert completeness is DatasetCompleteness.LIMITED
    assert "quote_limited_invalid_rows:1" in reasons


class _RecordingInvoker:
    def __init__(self, *, invalid_role: str | None = None, sleep_seconds: float = 0.005) -> None:
        self.invalid_role = invalid_role
        self.sleep_seconds = sleep_seconds
        self.calls: list[tuple[str, str]] = []
        self._active = 0
        self.max_active = 0
        self._lock = threading.Lock()

    def invoke(
        self,
        *,
        role: str,
        cycle_id: str,
        evidence_pack: dict[str, object],
        diagnostic_only: bool,
        arbiter_input: list[dict[str, object]] | None = None,
    ) -> InvocationResult:
        with self._lock:
            self.calls.append((role, cycle_id))
            self._active += 1
            self.max_active = max(self.max_active, self._active)
        time_module.sleep(self.sleep_seconds)
        with self._lock:
            self._active -= 1
        if role == self.invalid_role:
            return InvocationResult.invalid(role, "schema_validation_failed", latency_ms=5)
        if role in {"final_arbiter", "single_reviewer"}:
            return InvocationResult.valid(
                role,
                {
                    "schema_version": 1,
                    "verdict": "abstain" if diagnostic_only else "trade",
                    "selected_cards": [
                        "quote-SPY" if role == "single_reviewer" else "card-catalyst_scout"
                    ],
                    "commands": []
                    if diagnostic_only
                    else [
                        {
                            "symbol": "SPY",
                            "side": "long",
                            "notional_usd": 25.0,
                            "horizon": "60m",
                            "edge_class": "positive_edge",
                            "expected_excess_return_bps": 15.0,
                            "probability_positive": 0.6,
                            "return_p10_bps": -20.0,
                            "return_p50_bps": 10.0,
                            "return_p90_bps": 35.0,
                            "strategy_book": "core_ranked",
                            "discovery_lane": "core_ranked",
                        }
                    ],
                    "reason": "bounded research decision",
                },
                latency_ms=5,
            )
        return InvocationResult.valid(
            role,
            {
                "schema_version": 1,
                "abstain": False,
                "cards": [
                    {
                        "card_id": f"card-{role}",
                        "symbol": "SPY",
                        "side": "long",
                        "horizon": "60m",
                        "strategy_book": "core_ranked",
                        "discovery_lane": "core_ranked",
                        "thesis": "structured evidence supports a bounded probe",
                        "evidence_refs": ["quote-SPY"],
                        "confidence": 0.6,
                        "expected_alpha_bps": 15.0,
                        "downside_bps": 20.0,
                        "probability_positive": 0.6,
                        "return_p10_bps": -20.0,
                        "return_p50_bps": 10.0,
                        "return_p90_bps": 35.0,
                    }
                ],
            },
            latency_ms=5,
        )


def test_swarm_runs_eight_isolated_specialists_four_at_a_time_then_arbiter() -> None:
    invoker = _RecordingInvoker()

    result = run_research_cycle(
        cycle_id="cycle-1",
        cutoff=_timestamp(14, 30),
        evidence_pack={"evidence": [{"id": "quote-SPY"}]},
        invoker=invoker,
        hard_gate_reason=None,
    )

    assert tuple(role for role, _cycle in invoker.calls[:8]) == SPECIALIST_ROLES
    assert invoker.calls[8][0] == "final_arbiter"
    assert invoker.max_active == 4
    assert result.trade_allowed is True
    assert len(result.invocations) == 9


def test_swarm_quorum_and_risk_specialist_fail_closed_but_still_charge_arbiter() -> None:
    invoker = _RecordingInvoker(invalid_role="risk_cost_analyst")

    result = run_research_cycle(
        cycle_id="cycle-2",
        cutoff=_timestamp(14, 30),
        evidence_pack={"evidence": [{"id": "quote-SPY"}]},
        invoker=invoker,
        hard_gate_reason=None,
    )

    assert result.trade_allowed is False
    assert result.block_reason == "risk_cost_specialist_failed"
    assert len(result.invocations) == 9
    assert result.invocations[-1].diagnostic_only is True


class _MissingValidOutputInvoker(_RecordingInvoker):
    def invoke(self, **kwargs: object) -> InvocationResult:
        if kwargs.get("role") == "macro_fed_analyst":
            return InvocationResult(
                "macro_fed_analyst", "valid", None, 1, 0, None, {}, 0.0, 0.0
            )
        return super().invoke(**kwargs)


def test_swarm_defensively_ignores_a_valid_result_without_output() -> None:
    result = run_research_cycle(
        cycle_id="missing-valid-output",
        cutoff=_timestamp(14, 30),
        evidence_pack={"evidence": [{"id": "quote-SPY"}]},
        invoker=_MissingValidOutputInvoker(sleep_seconds=0.0),
        hard_gate_reason=None,
    )

    assert result.trade_allowed is True
    assert len(result.invocations) == 9


class _SwarmV2Invoker:
    def __init__(self, *, risk_veto: bool = False) -> None:
        self.risk_veto = risk_veto
        self.arbiter_input: list[dict[str, object]] | None = None

    def invoke(
        self,
        *,
        role: str,
        cycle_id: str,
        evidence_pack: dict[str, object],
        diagnostic_only: bool,
        arbiter_input: list[dict[str, object]] | None = None,
    ) -> InvocationResult:
        del cycle_id
        evidence_rows = evidence_pack.get("evidence", [])
        forecast_symbol = next(
            (
                str(row["symbol"])
                for row in evidence_rows
                if isinstance(row, Mapping) and row.get("symbol")
            ),
            "SPY",
        )
        forecast_evidence_id = next(
            (
                str(row["id"])
                for row in evidence_rows
                if isinstance(row, Mapping) and row.get("id")
            ),
            "quote-SPY",
        )
        if role == "final_arbiter":
            self.arbiter_input = arbiter_input
            forecast_ids = [
                str(forecast["forecast_id"])
                for envelope in arbiter_input or []
                for forecast in envelope["forecasts"]
            ]
            return InvocationResult.valid(
                role,
                {
                    "schema_version": 2,
                    "verdict": "abstain" if diagnostic_only else "trade",
                    "selected_forecasts": [] if diagnostic_only else forecast_ids[:1],
                    "commands": []
                    if diagnostic_only
                    else [
                        {
                            "symbol": forecast_symbol,
                            "side": "long",
                            "notional_usd": 250.0,
                            "horizon": "60m",
                            "edge_class": "positive_edge",
                            "expected_excess_return_bps": 15.0,
                            "probability_positive": 0.6,
                            "return_p10_bps": -20.0,
                            "return_p50_bps": 10.0,
                            "return_p90_bps": 35.0,
                            "strategy_book": "core_ranked",
                            "discovery_lane": "core_ranked",
                        }
                    ],
                    "reason": "bounded v2 decision",
                },
                latency_ms=1,
            )
        abstain = role in {"macro_fed_analyst", "portfolio_learning_analyst"}
        return InvocationResult.valid(
            role,
            {
                "schema_version": 2,
                "abstain": abstain,
                "abstention_reason": "no role-specific edge" if abstain else "",
                "risk_veto": self.risk_veto and role == "risk_cost_analyst",
                "forecasts": []
                if abstain
                else [
                    {
                        "symbol": forecast_symbol,
                        "direction": "avoid"
                        if role == "short_contrarian_analyst"
                        else "long",
                        "horizon": "60m",
                        "strategy_book": "core_ranked",
                        "discovery_lane": "core_ranked",
                        "thesis": "role-specific structured evidence",
                        "evidence_refs": [forecast_evidence_id],
                        "confidence": 0.6,
                        "expected_alpha_bps": 15.0,
                        "downside_bps": 20.0,
                        "probability_positive": 0.6,
                        "return_p10_bps": -20.0,
                        "return_p50_bps": 10.0,
                        "return_p90_bps": 35.0,
                    }
                ],
            },
            latency_ms=1,
        )


def test_swarm_v2_uses_valid_abstentions_for_quorum_and_coordinator_ids() -> None:
    invoker = _SwarmV2Invoker()

    result = run_research_cycle(
        cycle_id="cycle-v2",
        cutoff=_timestamp(14, 30),
        evidence_pack={"evidence": [{"id": "quote-SPY"}]},
        invoker=invoker,
        hard_gate_reason=None,
        protocol="swarm-v2",
    )

    assert result.trade_allowed is True
    assert invoker.arbiter_input is not None
    assert len(invoker.arbiter_input) == len(SPECIALIST_ROLES)
    assert sum(bool(envelope["abstain"]) for envelope in invoker.arbiter_input) == 2
    forecast_ids = [
        str(forecast["forecast_id"])
        for envelope in invoker.arbiter_input
        for forecast in envelope["forecasts"]
    ]
    assert len(forecast_ids) == len(set(forecast_ids))
    assert all(identifier.startswith("cycle-v2-") for identifier in forecast_ids)


def test_swarm_v2_risk_veto_blocks_trade_and_forbids_short_commands() -> None:
    vetoed = run_research_cycle(
        cycle_id="cycle-v2-veto",
        cutoff=_timestamp(14, 30),
        evidence_pack={"evidence": [{"id": "quote-SPY"}]},
        invoker=_SwarmV2Invoker(risk_veto=True),
        hard_gate_reason=None,
        protocol="swarm-v2",
    )
    assert vetoed.trade_allowed is False
    assert vetoed.block_reason == "risk_cost_veto"

    non_risk = _SwarmV2Invoker().invoke(
        role="catalyst_scout",
        cycle_id="invalid-veto",
        evidence_pack={},
        diagnostic_only=False,
    )
    assert non_risk.output is not None
    invalid_veto = dict(non_risk.output)
    invalid_veto["risk_veto"] = True
    with pytest.raises(ValueError, match="only the risk/cost"):
        research_simulation._validate_specialist_output(
            invalid_veto,
            "catalyst_scout",
            {"quote-SPY"},
            protocol="swarm-v2",
        )

    non_risk_instruction = research_simulation._research_instruction(
        "catalyst_scout", diagnostic_only=False, protocol="swarm-v2"
    )
    risk_instruction = research_simulation._research_instruction(
        "risk_cost_analyst", diagnostic_only=False, protocol="swarm-v2"
    )
    assert "risk_veto MUST be false" in non_risk_instruction
    assert "never use risk_veto" in non_risk_instruction
    assert "This role alone may set risk_veto=true" in risk_instruction
    assert "when forecasts is empty, set abstain=true" in non_risk_instruction
    assert "A risk veto does not change this truth table" in risk_instruction

    output = {
        "schema_version": 2,
        "verdict": "trade",
        "selected_forecasts": ["forecast-1"],
        "commands": [
            {
                "symbol": "SPY",
                "side": "short",
                "notional_usd": 250.0,
                "horizon": "60m",
                "edge_class": "positive_edge",
                "expected_excess_return_bps": 15.0,
                "probability_positive": 0.6,
                "return_p10_bps": -20.0,
                "return_p50_bps": 10.0,
                "return_p90_bps": 35.0,
                "strategy_book": "core_ranked",
                "discovery_lane": "core_ranked",
            }
        ],
        "reason": "invalid short",
    }
    with pytest.raises(ValueError, match="long-only"):
        research_simulation._validate_arbiter_output(
            output, {"forecast-1"}, protocol="swarm-v2"
        )


def test_swarm_v2_command_requires_matching_selected_forecast_lineage() -> None:
    command = {
        "symbol": "SPY",
        "horizon": "60m",
        "strategy_book": "core_ranked",
        "discovery_lane": "core_ranked",
    }
    output = {"selected_forecasts": ["forecast-1"], "commands": [command]}
    forecast = {
        "forecast_id": "forecast-1",
        "symbol": "SPY",
        "direction": "long",
        "horizon": "60m",
        "strategy_book": "core_ranked",
        "discovery_lane": "core_ranked",
    }

    research_simulation._validate_v2_command_lineage(output, [forecast])
    with pytest.raises(ValueError, match="selected-forecast lineage"):
        research_simulation._validate_v2_command_lineage(
            {**output, "commands": [{**command, "symbol": "QQQ"}]}, [forecast]
        )
    with pytest.raises(ValueError, match="selected-forecast lineage"):
        research_simulation._validate_v2_command_lineage(
            output, [{**forecast, "direction": "avoid"}]
        )


def test_swarm_v2_reduce_long_requires_matching_avoid_forecast_lineage() -> None:
    command = {
        "symbol": "SPY",
        "side": "reduce_long",
        "horizon": "60m",
        "strategy_book": "core_ranked",
        "discovery_lane": "core_ranked",
    }
    output = {"selected_forecasts": ["forecast-1"], "commands": [command]}
    avoid = {
        "forecast_id": "forecast-1",
        "symbol": "SPY",
        "direction": "avoid",
        "horizon": "60m",
        "strategy_book": "core_ranked",
        "discovery_lane": "core_ranked",
    }

    research_simulation._validate_v2_command_lineage(output, [avoid])

    with pytest.raises(ValueError, match="selected-forecast lineage"):
        research_simulation._validate_v2_command_lineage(
            output, [{**avoid, "direction": "long"}]
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("header", "header"),
        ("abstain_with_forecast", "abstention requires"),
        ("abstain_without_reason", "abstention requires"),
        ("forecast_without_card", "requires forecasts"),
        ("forecast_with_reason", "requires forecasts"),
        ("too_many", "three forecasts"),
    ],
)
def test_swarm_v2_specialist_envelope_fails_closed(
    mutation: str, message: str
) -> None:
    result = _SwarmV2Invoker().invoke(
        role="catalyst_scout",
        cycle_id="validation",
        evidence_pack={"evidence": [{"id": "quote-SPY", "symbol": "SPY"}]},
        diagnostic_only=False,
    )
    assert result.output is not None
    output = json.loads(json.dumps(result.output))
    if mutation == "header":
        output["schema_version"] = 1
    elif mutation == "abstain_with_forecast":
        output["abstain"] = True
        output["abstention_reason"] = "no edge"
    elif mutation == "abstain_without_reason":
        output["abstain"] = True
        output["forecasts"] = []
    elif mutation == "forecast_without_card":
        output["forecasts"] = []
    elif mutation == "forecast_with_reason":
        output["abstention_reason"] = "contradictory"
    else:
        output["forecasts"] *= 4

    with pytest.raises(ValueError, match=message):
        research_simulation._validate_specialist_output(
            output,
            "catalyst_scout",
            {"quote-SPY"},
            protocol="swarm-v2",
        )


def test_specialist_protocol_and_v1_envelope_fail_closed() -> None:
    with pytest.raises(ValueError, match="protocol"):
        research_simulation._validate_specialist_output(
            {}, "catalyst_scout", set(), protocol="swarm-v3"
        )
    with pytest.raises(ValueError, match="header"):
        research_simulation._validate_specialist_output(
            {"schema_version": 2, "abstain": True, "cards": []},
            "catalyst_scout",
            set(),
        )
    with pytest.raises(ValueError, match="cannot contain cards"):
        research_simulation._validate_specialist_output(
            {"schema_version": 1, "abstain": True, "cards": [{}]},
            "catalyst_scout",
            set(),
        )
    with pytest.raises(ValueError, match="card or abstention"):
        research_simulation._validate_specialist_output(
            {"schema_version": 1, "abstain": False, "cards": []},
            "catalyst_scout",
            set(),
        )
    with pytest.raises(ValueError, match="protocol"):
        run_research_cycle(
            cycle_id="invalid-protocol",
            cutoff=_timestamp(14, 30),
            evidence_pack={},
            invoker=_SwarmV2Invoker(),
            hard_gate_reason=None,
            protocol="swarm-v3",
        )

    specialist = _SwarmV2Invoker().invoke(
        role="catalyst_scout",
        cycle_id="mismatched-symbol-evidence",
        evidence_pack={"evidence": [{"id": "quote-SPY", "symbol": "SPY"}]},
        diagnostic_only=False,
    )
    assert specialist.output is not None
    with pytest.raises(ValueError, match="same-symbol evidence"):
        research_simulation._validate_specialist_output(
            specialist.output,
            "catalyst_scout",
            {"quote-SPY"},
            protocol=research_simulation.SWARM_V2,
            evidence_symbols={"quote-SPY": "QQQ"},
        )


def test_swarm_v2_half_kelly_sizing_is_clipped_to_rust_name_limit() -> None:
    assert research_simulation._risk_adjusted_notional(
        {
            "notional_usd": 5_000.0,
            "expected_excess_return_bps": 200.0,
            "probability_positive": 0.8,
            "return_p10_bps": -100.0,
            "return_p90_bps": 300.0,
        },
        portfolio_nav_usd=10_000.0,
    ) == 750.0
    assert research_simulation._risk_adjusted_notional(
        {
            "notional_usd": 5_000.0,
            "expected_excess_return_bps": 1.0,
            "probability_positive": 0.51,
            "return_p10_bps": -500.0,
            "return_p90_bps": 500.0,
        },
        portfolio_nav_usd=10_000.0,
    ) == 25.0


def test_research_arbiter_adapter_emits_contract_valid_memory_updates() -> None:
    arbiter = _RecordingInvoker(sleep_seconds=0.0).invoke(
        role="final_arbiter",
        cycle_id="adapter",
        evidence_pack={},
        diagnostic_only=False,
    )
    assert arbiter.output is not None

    decision = research_simulation._decision_from_arbiter(
        arbiter.output,
        {"as_of": "2026-05-04T14:30:00Z", "market_session": "rth"},
        cycle_id="adapter-rust-1",
        sequence=1,
        portfolio_nav_usd=10_000.0,
    )

    validated = validate_llm_decision(decision)
    assert len(validated.memory_updates) == len(validated.commands)
    assert validated.memory_updates[0]["cycle_id"] == "adapter-rust-1"
    assert validated.memory_updates[0]["created_at"] == "2026-05-04T14:30:00Z"


def test_research_arbiter_adapter_caps_reduce_long_to_held_position() -> None:
    output = {
        "schema_version": 2,
        "verdict": "trade",
        "selected_forecasts": ["forecast-avoid"],
        "commands": [
            {
                "symbol": "SPY",
                "side": "reduce_long",
                "notional_usd": 750.0,
                "horizon": "60m",
                "edge_class": "risk_reduction",
                "expected_excess_return_bps": 25.0,
                "probability_positive": 0.7,
                "return_p10_bps": -10.0,
                "return_p50_bps": 20.0,
                "return_p90_bps": 40.0,
                "strategy_book": "risk_reduction",
                "discovery_lane": "held_position",
            }
        ],
        "reason": "Reduce invalidated exposure.",
    }
    snapshot = {
        "as_of": "2026-05-04T14:30:00Z",
        "market_session": "rth",
        "latest_quotes": {"SPY": {"bid": 100.0, "ask": 100.2}},
    }
    portfolio = {
        "cash_usd": 9_800.0,
        "positions": {
            "SPY": {"symbol": "SPY", "quantity": 2.0, "avg_price": 100.0}
        },
    }
    research_simulation._validate_arbiter_output(
        output, {"forecast-avoid"}, protocol=research_simulation.SWARM_V2
    )
    research_simulation._validate_reduce_long_holdings(
        output,
        {
            "portfolio_context": {
                "positions": [{"symbol": "SPY", "quantity": 2.0}]
            }
        },
    )
    with pytest.raises(ValueError, match="existing long holding"):
        research_simulation._validate_reduce_long_holdings(
            output, {"portfolio_context": {"positions": []}}
        )

    decision = research_simulation._decision_from_arbiter(
        output,
        snapshot,
        cycle_id="reduce-rust-1",
        sequence=1,
        portfolio_nav_usd=10_000.0,
        portfolio=portfolio,
    )

    assert decision["commands"][0]["action"] == "sell_notional"
    assert decision["commands"][0]["max_notional_usd"] == pytest.approx(199.96)


def test_portfolio_context_exposes_marked_holdings_without_pending_learning() -> None:
    context = research_simulation._portfolio_evidence_context(
        {
            "cash_usd": 9_800.0,
            "realized_pnl_usd": 5.0,
            "positions": {
                "SPY": {"symbol": "SPY", "quantity": 2.0, "avg_price": 95.0}
            },
        },
        {
            "nav_usd": 10_000.0,
            "net_pnl_usd": 0.0,
            "unrealized_pnl_usd": -5.0,
            "gross_exposure_usd": 200.0,
            "net_exposure_usd": 200.0,
            "marks": {"SPY": 100.0},
            "unpriced_positions": [],
        },
    )
    learning = research_simulation._learning_evidence_view(
        {
            "schema_version": 1,
            "ewma_alpha": 0.2,
            "buckets": {"risk:60m:avoid:neutral": {"sample_count": 2}},
            "pending_outcomes": [{"outcome_id": "future-outcome"}],
        }
    )

    assert context["position_count"] == 1
    assert context["positions"][0]["symbol"] == "SPY"
    assert context["positions"][0]["market_value_usd"] == 200.0
    assert context["positions"][0]["unrealized_pnl_usd"] == 10.0
    assert learning["pending_outcome_count"] == 1
    assert "pending_outcomes" not in learning


def test_deterministic_output_validation_rejects_duplicate_references() -> None:
    invoker = _RecordingInvoker()
    specialist_result = invoker.invoke(
        role="catalyst_scout",
        cycle_id="duplicate-specialist",
        evidence_pack={},
        diagnostic_only=False,
    )
    assert specialist_result.output is not None
    specialist = dict(specialist_result.output)
    cards = [dict(card) for card in specialist["cards"]]
    cards[0]["evidence_refs"] = ["quote-SPY", "quote-SPY"]
    specialist["cards"] = cards
    with pytest.raises(ValueError, match="duplicate evidence"):
        research_simulation._validate_specialist_output(
            specialist, "catalyst_scout", {"quote-SPY"}
        )

    arbiter_result = invoker.invoke(
        role="final_arbiter",
        cycle_id="duplicate-arbiter",
        evidence_pack={},
        diagnostic_only=False,
    )
    assert arbiter_result.output is not None
    arbiter = dict(arbiter_result.output)
    arbiter["selected_cards"] = ["card-catalyst_scout", "card-catalyst_scout"]
    with pytest.raises(ValueError, match="duplicate selected cards"):
        research_simulation._validate_arbiter_output(
            arbiter, {"card-catalyst_scout"}
        )


class _DuplicateCrossSpecialistCardInvoker(_RecordingInvoker):
    def invoke(self, **kwargs: object) -> InvocationResult:
        result = super().invoke(**kwargs)
        if kwargs.get("role") in SPECIALIST_ROLES and result.output is not None:
            output = dict(result.output)
            cards = [dict(card) for card in output["cards"]]
            cards[0]["card_id"] = "duplicate-global-id"
            output["cards"] = cards
            return InvocationResult.valid(str(kwargs["role"]), output, latency_ms=1)
        return result


def test_v1_duplicate_card_ids_invalidate_every_contributor_before_arbiter() -> None:
    result = run_research_cycle(
        cycle_id="duplicate-cycle",
        cutoff=_timestamp(14, 30),
        evidence_pack={"evidence": [{"id": "quote-SPY"}]},
        invoker=_DuplicateCrossSpecialistCardInvoker(sleep_seconds=0.0),
        hard_gate_reason=None,
    )

    assert result.trade_allowed is False
    assert result.block_reason == "risk_cost_specialist_failed"
    assert all(
        invocation.validation_status == "invalid"
        for invocation in result.invocations[:8]
    )


def test_codex_invoker_rejects_oversized_input_before_starting_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_process(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        raise AssertionError("oversized research input reached Codex")

    monkeypatch.setattr(research_simulation.subprocess, "run", unexpected_process)

    result = research_simulation.CodexResearchInvoker().invoke(
        role="catalyst_scout",
        cycle_id="oversized-input",
        evidence_pack={
            "evidence": ["x" * (research_simulation.RESEARCH_INPUT_MAX_CHARS + 1)]
        },
        diagnostic_only=True,
    )

    assert result.validation_status == "invalid"
    assert result.exit_code == 1
    assert result.error is not None and "safety limit" in result.error
    assert result.failure_kind == "input_rejected"


def test_codex_invoker_retries_infrastructure_failure_and_records_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def flaky_process(
        command: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        assert command[:6] == [
            "codex", "exec", "--model", "gpt-6-astra",
            "-c", 'model_reasoning_effort="xhigh"',
        ]
        calls += 1
        if calls == 1:
            return subprocess.CompletedProcess(
                command,
                1,
                stdout=(
                    '{"type":"turn.failed","error":'
                    '{"message":"WebSocket service unavailable (503)"}}\n'
                ),
                stderr="Reading prompt from stdin...",
            )
        output_path = Path(command[command.index("--output-last-message") + 1])
        output_path.write_text("{}", encoding="utf-8")
        return subprocess.CompletedProcess(
            command,
            0,
            stdout='{"type":"turn.completed","usage":{"input_tokens":12}}\n',
            stderr="",
        )

    monkeypatch.setattr(research_simulation.subprocess, "run", flaky_process)

    result = research_simulation.CodexResearchInvoker(
        max_attempts=2, retry_delay_seconds=0.0
    ).invoke(
        role="catalyst_scout",
        cycle_id="retry-success",
        evidence_pack={"evidence": []},
        diagnostic_only=True,
    )

    assert calls == 2
    assert result.validation_status == "valid"
    assert result.failure_kind is None
    assert result.usage["attempt_count"] == 2
    assert result.imputed_cost_usd == 0.0


def test_codex_invoker_exhausted_infrastructure_failure_uses_jsonl_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def failed_process(
        command: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(
            command,
            1,
            stdout=(
                '{"type":"turn.failed","error":'
                '{"message":"WebSocket service unavailable (503)"}}\n'
            ),
            stderr="Reading prompt from stdin...",
        )

    monkeypatch.setattr(research_simulation.subprocess, "run", failed_process)

    result = research_simulation.CodexResearchInvoker(
        max_attempts=2, retry_delay_seconds=0.0
    ).invoke(
        role="catalyst_scout",
        cycle_id="retry-exhausted",
        evidence_pack={"evidence": []},
        diagnostic_only=True,
    )

    assert calls == 2
    assert result.validation_status == "invalid"
    assert result.failure_kind == "infrastructure_failure"
    assert result.error is not None and "WebSocket service unavailable (503)" in result.error
    assert result.usage["attempt_count"] == 2


def test_codex_invoker_does_not_retry_invalid_structured_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def invalid_output_process(
        command: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        output_path = Path(command[command.index("--output-last-message") + 1])
        output_path.write_text("not-json", encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(research_simulation.subprocess, "run", invalid_output_process)

    result = research_simulation.CodexResearchInvoker(
        max_attempts=3, retry_delay_seconds=0.0
    ).invoke(
        role="catalyst_scout",
        cycle_id="invalid-output",
        evidence_pack={"evidence": []},
        diagnostic_only=True,
    )

    assert calls == 1
    assert result.validation_status == "invalid"
    assert result.failure_kind == "model_output_invalid"


def test_hard_gate_runs_all_nine_calls_in_diagnostic_mode() -> None:
    invoker = _RecordingInvoker()

    result = run_research_cycle(
        cycle_id="cycle-3",
        cutoff=_timestamp(14, 30),
        evidence_pack={"evidence": [{"id": "quote-SPY"}]},
        invoker=invoker,
        hard_gate_reason="market_data_incomplete",
    )

    assert result.trade_allowed is False
    assert result.block_reason == "market_data_incomplete"
    assert all(invocation.diagnostic_only for invocation in result.invocations)


class _UnverifiedShortInvoker(_RecordingInvoker):
    def invoke(self, **kwargs: object) -> InvocationResult:
        result = super().invoke(**kwargs)
        if kwargs.get("role") != "final_arbiter" or result.output is None:
            return result
        output = dict(result.output)
        commands = [dict(command) for command in output["commands"]]
        commands[0]["side"] = "short"
        output["commands"] = commands
        return InvocationResult.valid("final_arbiter", output, latency_ms=result.latency_ms)


def test_unverified_historical_borrow_blocks_new_short() -> None:
    result = run_research_cycle(
        cycle_id="cycle-short",
        cutoff=_timestamp(14, 30),
        evidence_pack={
            "evidence": [{"id": "quote-SPY"}],
            "ranked_candidates": [
                {"symbol": "SPY", "shortable_verified": False}
            ],
        },
        invoker=_UnverifiedShortInvoker(sleep_seconds=0.0),
        hard_gate_reason=None,
    )

    assert result.trade_allowed is False
    assert result.block_reason == "historical_borrow_unverified:SPY"


def test_canonical_store_detects_tampering_and_resume_fingerprint_mismatch(
    tmp_path: Path,
) -> None:
    store = CanonicalRunStore(tmp_path / "run")
    store.initialize(
        {
            "schema_version": 1,
            "run_id": "test-run",
            "fingerprints": {"code": "sha256:aaa", "dataset": "sha256:bbb"},
        }
    )
    store.append_event(
        event_type="cycle_terminal",
        event_time=_timestamp(14, 30),
        available_at=_timestamp(14, 30),
        payload={"cycle_id": "cycle-1", "status": "blocked"},
    )
    assert len(store.read_events()) == 1

    with pytest.raises(ResearchFingerprintMismatch):
        store.assert_resumable({"code": "sha256:changed", "dataset": "sha256:bbb"})

    event_file = next((store.run_dir / "events").glob("*.jsonl"))
    event_file.write_text(event_file.read_text().replace("blocked", "accepted"), encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        store.read_events()


def test_model_catalog_fingerprint_ignores_cache_refresh_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    codex_root = tmp_path / "codex"
    codex_root.mkdir()
    catalog = codex_root / "models_cache.json"
    selected_model = {
        "slug": research_simulation.MODEL,
        "comp_hash": "3000",
        "supported_reasoning_levels": [{"effort": "xhigh"}],
        "context_window": 272_000,
    }
    catalog.write_text(
        json.dumps(
            {
                "fetched_at": "2026-08-24T10:00:00Z",
                "etag": "first",
                "client_version": "1.0.0",
                "models": [selected_model],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("CODEX_HOME", str(codex_root))
    first = research_simulation._model_catalog_fingerprint()

    catalog.write_text(
        json.dumps(
            {
                "fetched_at": "2026-08-24T11:00:00Z",
                "etag": "second",
                "client_version": "1.0.1",
                "models": [selected_model],
            }
        ),
        encoding="utf-8",
    )
    assert research_simulation._model_catalog_fingerprint() == first

    changed = {**selected_model, "context_window": 128_000}
    catalog.write_text(json.dumps({"models": [changed]}), encoding="utf-8")
    assert research_simulation._model_catalog_fingerprint() != first


def test_run_protocol_and_rust_policy_changes_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Rust policy"):
        research_simulation.RustResearchHarness(policy="live_unbounded")
    with pytest.raises(ValueError, match="protocol"):
        run_prepared_simulation(tmp_path / "not-opened", protocol="swarm-v3")

    store = CanonicalRunStore(tmp_path / "protocol-run")
    store.initialize(
        {
            "schema_version": 1,
            "run_id": "protocol-run",
            "status": "running",
            "research_protocol": research_simulation.SWARM_V1,
            "fingerprints": {},
        }
    )
    with pytest.raises(ResearchFingerprintMismatch, match="protocol changed"):
        research_simulation._replace_manifest_status(
            store, "running", protocol=research_simulation.SWARM_V2
        )


def test_canonical_store_rebuilds_chain_across_out_of_order_daily_files(
    tmp_path: Path,
) -> None:
    store = CanonicalRunStore(tmp_path / "cross-day-run")
    store.initialize({"schema_version": 1, "run_id": "cross-day"})
    later = datetime(2026, 5, 5, 14, 30, tzinfo=UTC)
    earlier = datetime(2026, 5, 4, 14, 30, tzinfo=UTC)
    first = store.append_event(
        event_type="portfolio_mark",
        event_time=later,
        available_at=later,
        payload={"order": 1},
    )
    second = store.append_event(
        event_type="benchmark_mark",
        event_time=earlier,
        available_at=earlier,
        payload={"order": 2},
    )

    reloaded = CanonicalRunStore(store.run_dir).read_events()

    assert [event["event_hash"] for event in reloaded] == [
        first["event_hash"],
        second["event_hash"],
    ]


def test_canonical_blob_storage_hashes_streams_and_rejects_collisions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.trq"
    source.write_bytes(b"immutable partition bytes")
    store = CanonicalRunStore(tmp_path / "run")
    original_read_bytes = Path.read_bytes

    def guarded_read_bytes(path: Path) -> bytes:
        if path.suffix == ".trq":
            raise AssertionError("binary quote partitions must not be loaded wholesale")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", guarded_read_bytes)
    digest = store.put_blob(source, suffix=".trq")
    assert digest == research_simulation._sha256(b"immutable partition bytes")
    assert store.put_blob(source, suffix=".trq") == digest
    with pytest.raises(ValueError, match="suffix"):
        store.put_blob(source, suffix=".INVALID")

    target = (
        store.run_dir
        / "blobs"
        / "sha256"
        / f"{digest.removeprefix('sha256:')}.trq"
    )
    target.write_bytes(b"collision")
    with pytest.raises(ValueError, match="blob collision"):
        store.put_blob(source, suffix=".trq")


def test_prepare_direct_collection_and_argument_boundaries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.json"
    source.write_text(json.dumps({"calendar": [], "observations": []}), encoding="utf-8")
    collected_symbols: list[tuple[str, ...]] = []

    def fake_collect(
        symbols: tuple[str, ...], *, quote_partition_root: Path | None = None
    ) -> dict[str, object]:
        assert quote_partition_root is not None
        collected_symbols.append(tuple(symbols))
        return {
            "calendar": [
                {
                    "session_date": "2026-05-04",
                    "open": "09:30",
                    "close": "16:00",
                    "available_at": "2026-05-01T20:00:00Z",
                }
            ],
            "observations": [
                {
                    "schema_version": 1,
                    "observation_id": "quote-SPY",
                    "kind": "quote",
                    "source": "alpaca_historical_quotes",
                    "event_time": "2026-05-04T13:29:59Z",
                    "available_at": "2026-05-04T13:29:59Z",
                    "symbol": "SPY",
                    "payload": {"bid": 99.9, "ask": 100.1},
                }
            ],
            "quote_partitions": [],
            "coverage": {},
        }

    monkeypatch.setattr(research_simulation, "collect_alpaca_source_bundle", fake_collect)
    explicit = prepare_run(
        state_root=tmp_path / "runs",
        run_id="explicit-symbols",
        source_path=None,
        symbols=("spy", "SPY"),
        code_fingerprint="sha256:test-code",
    )
    manifest = json.loads((explicit / "manifest.json").read_text(encoding="utf-8"))
    frozen = CanonicalRunStore(explicit).get_object(manifest["dataset"]["object"])
    assert collected_symbols == [("SPY",)]
    assert frozen["universe"]["symbols"] == ["SPY"]

    master = tmp_path / "master.txt"
    master.write_text("not read by mocked collector", encoding="utf-8")
    monkeypatch.setattr(
        research_simulation,
        "collect_point_in_time_research_universe",
        lambda path: {"schema_version": 1, "symbols": ["SPY"], "lanes": {"SPY": "core"}},
    )
    prepare_run(
        state_root=tmp_path / "runs",
        run_id="symbol-master",
        source_path=None,
        symbol_master_path=master,
        code_fingerprint="sha256:test-code",
    )
    assert collected_symbols[-1] == ("SPY",)

    with pytest.raises(ValueError, match="cannot be combined"):
        prepare_run(
            state_root=tmp_path / "runs",
            source_path=source,
            symbols=("SPY",),
        )
    with pytest.raises(ValueError, match="cannot be combined"):
        prepare_run(
            state_root=tmp_path / "runs",
            source_path=None,
            symbols=("SPY",),
            symbol_master_path=master,
        )
    with pytest.raises(ValueError, match="are required"):
        prepare_run(state_root=tmp_path / "runs", source_path=None)
    with pytest.raises(ValueError, match="cannot exceed 250"):
        prepare_run(
            state_root=tmp_path / "runs",
            source_path=None,
            symbols=tuple(f"X{index}" for index in range(251)),
        )
    with pytest.raises(ValueError, match="run_id is invalid"):
        prepare_run(
            state_root=tmp_path / "runs",
            run_id="bad/run",
            source_path=source,
            code_fingerprint="sha256:test-code",
        )

    parser = research_simulation._build_parser()
    parsed = parser.parse_args(["prepare", "--symbol-master", str(master)])
    assert parsed.symbol_master == master
    prepared_v2 = parser.parse_args(
        [
            "prepare",
            "--symbol-master",
            str(master),
            "--universe-size",
            "100",
            "--side-policy",
            "long-only",
            "--feed",
            "auto-authorized",
        ]
    )
    assert prepared_v2.universe_size == 100
    assert prepared_v2.side_policy == "long-only"
    assert prepared_v2.feed == "auto-authorized"
    reused = parser.parse_args(
        ["prepare", "--reuse-dataset-from", "/tmp/canonical-run"]
    )
    assert reused.reuse_dataset_from == Path("/tmp/canonical-run")
    assert parser.parse_args(["run", "--protocol", "swarm-v2"]).protocol == "swarm-v2"
    assert parser.parse_args(["resume", "--max-cycles", "300"]).max_cycles == 300
    assert parser.parse_args(
        ["run", "--reuse-completed-cycles-from", "/tmp/interrupted-run"]
    ).reuse_completed_cycles_from == Path("/tmp/interrupted-run")
    assert parser.parse_args(["verify", "--strict-economics"]).strict_economics is True
    assert parser.parse_args(["analyze", "--trial-registry", "canonical"]).trial_registry == "canonical"
    assert parser.parse_args(["export", "--receipt"]).receipt is True
    monkeypatch.setenv("ALPACA_DATA_FEED", "iex")
    assert research_simulation._resolve_research_feed("auto-authorized") == "iex"
    assert research_simulation._resolve_research_feed("iex") == "iex"
    with pytest.raises(ValueError, match="already configured authorized"):
        research_simulation._resolve_research_feed("sip")
    with pytest.raises(ValueError, match="policy"):
        research_simulation._resolve_research_feed("consolidated")
    monkeypatch.setenv("ALPACA_DATA_FEED", "invalid")
    with pytest.raises(ValueError, match="configured Alpaca"):
        research_simulation._resolve_research_feed("auto-authorized")
    with pytest.raises(ValueError, match="universe size"):
        prepare_run(source_path=source, state_root=tmp_path / "runs", universe_size=99)
    with pytest.raises(ValueError, match="side policy"):
        prepare_run(
            source_path=source,
            state_root=tmp_path / "runs",
            side_policy="long-short",
        )


def test_prepare_verify_analyze_and_export_fixture_run(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(
            {
                "calendar": [
                    {
                        "session_date": "2026-05-04",
                        "open": "09:30",
                        "close": "16:00",
                        "available_at": "2026-05-01T20:00:00Z",
                    }
                ],
                "observations": [
                    {
                        "schema_version": 1,
                        "observation_id": "quote-SPY",
                        "kind": "quote",
                        "source": "alpaca_historical_quotes",
                        "event_time": "2026-05-04T13:29:59Z",
                        "available_at": "2026-05-04T13:29:59Z",
                        "symbol": "SPY",
                        "payload": {"bid": 99.95, "ask": 100.05},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    run_dir = prepare_run(
        state_root=tmp_path / "runs",
        run_id="fixture-run",
        source_path=source,
        code_fingerprint="sha256:test-code",
    )
    reused_run_dir = prepare_run(
        state_root=tmp_path / "runs",
        run_id="fixture-run-reused",
        source_path=None,
        reuse_dataset_from=run_dir,
        code_fingerprint="sha256:new-code",
    )
    original_manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    reused_manifest = json.loads(
        (reused_run_dir / "manifest.json").read_text(encoding="utf-8")
    )
    assert original_manifest["model"] == {"id": "gpt-6-astra", "effort": "xhigh"}
    assert reused_manifest["model"] == original_manifest["model"]
    assert reused_manifest["dataset"]["object"] == original_manifest["dataset"]["object"]
    assert reused_manifest["fingerprints"]["dataset"] == original_manifest["fingerprints"][
        "dataset"
    ]
    assert reused_manifest["fingerprints"]["code"] == "sha256:new-code"
    assert verify_run(reused_run_dir)["valid"] is True
    verification = verify_run(run_dir)
    store = CanonicalRunStore(run_dir)
    mark_time = datetime(2026, 5, 4, 20, 0, tzinfo=UTC)
    for arm in ARMS:
        store.append_event(
            event_type="portfolio_mark",
            event_time=mark_time,
            available_at=mark_time,
            payload={"arm": arm, "nav_usd": 10_010.0, "phase": "learning"},
        )
        for symbol, benchmark_return in (
            ("SPY", 0.0005),
            ("QQQ", 0.0004),
            ("equal_weight", 0.0003),
        ):
            store.append_event(
                event_type="benchmark_mark",
                event_time=mark_time,
                available_at=mark_time,
                payload={"arm": arm, "symbol": symbol, "return": benchmark_return},
            )
    store.write_checksums()
    report = analyze_run(run_dir)
    archive = export_run(run_dir, tmp_path / "fixture-export")
    schema_dir = Path(__file__).resolve().parents[1] / "schemas"
    for schema_name in (
        "llm_research_run_manifest.schema.json",
        "llm_research_event.schema.json",
        "llm_research_specialist_invocation.schema.json",
        "llm_research_arbiter_provenance.schema.json",
        "llm_research_learning_delta.schema.json",
        "llm_research_rust_execution.schema.json",
        "llm_research_specialist_result.schema.json",
        "llm_research_arbiter_result.schema.json",
        "llm_research_specialist_opinion_v2.schema.json",
        "llm_research_arbiter_decision_v2.schema.json",
        "llm_research_trial_registry.schema.json",
        "llm_research_export_receipt.schema.json",
        "llm_research_forecast_outcome.schema.json",
    ):
        Draft202012Validator.check_schema(
            json.loads((schema_dir / schema_name).read_text(encoding="utf-8"))
        )
    Draft202012Validator(
        json.loads(
            (schema_dir / "llm_research_run_manifest.schema.json").read_text(encoding="utf-8")
        )
    ).validate(json.loads((run_dir / "manifest.json").read_text(encoding="utf-8")))

    assert verification["valid"] is True
    assert verification["scheduled_cycles"] == 12
    assert report["labels"]["evidence_strength"] == DatasetCompleteness.LIMITED.value
    assert report["labels"]["performance_evidence_eligible"] is False
    assert report["verification"]["strict_economics"]["valid"] is False
    assert "100-symbol universe" in report["verification"]["strict_economics"]["error"]
    assert report["conclusions"]["anonymized"] == "operationally_invalid"
    assert tuple(report["arms"]) == ARMS
    assert report["trial_registry"]["trial_count"] == len(ARMS)
    assert len(report["comparisons"]["holm_corrected"]) == len(ARMS) * 3
    assert all(
        "cost_adjusted_pnl_usd" in report["arms"][arm]["periods"]["lockbox"]
        for arm in ARMS
    )
    assert archive.is_file()
    receipt = archive.with_suffix(archive.suffix + ".receipt.json")
    assert receipt.is_file()
    receipt_payload = json.loads(receipt.read_text(encoding="utf-8"))
    assert receipt_payload["schema_version"] == 1
    assert receipt_payload["archive_sha256"] == research_simulation._sha256_file(archive)
    with tarfile.open(archive, "r:gz") as exported:
        names = set(exported.getnames())
    assert "checksums.sha256" in names
    assert "analysis.sqlite" in names
    assert not any(name.startswith("exports/") for name in names)
    with sqlite3.connect(run_dir / "analysis.sqlite") as connection:
        indexed_events = connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    assert indexed_events == len(store.read_events())
    assert verify_run(run_dir)["valid"] is True
    with pytest.raises(ValueError, match="100-symbol universe"):
        verify_run(run_dir, strict_economics=True)
    assert research_simulation.main(
        ["--state-root", str(tmp_path / "runs"), "verify", "--run-id", "fixture-run"]
    ) == 0
    assert '"valid": true' in capsys.readouterr().out


def test_verify_analyze_and_export_reject_missing_checksum_envelope(tmp_path: Path) -> None:
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(
            {
                "calendar": [
                    {
                        "session_date": "2026-05-04",
                        "open": "09:30",
                        "close": "16:00",
                        "available_at": "2026-05-01T20:00:00Z",
                    }
                ],
                "observations": [],
            }
        ),
        encoding="utf-8",
    )
    run_dir = prepare_run(
        state_root=tmp_path / "runs",
        run_id="missing-checksum",
        source_path=source,
        code_fingerprint="sha256:test-code",
    )
    (run_dir / "checksums.sha256").unlink()

    with pytest.raises(ValueError, match="checksum envelope is missing"):
        verify_run(run_dir)
    with pytest.raises(ValueError, match="checksum envelope is missing"):
        analyze_run(run_dir)
    with pytest.raises(ValueError, match="checksum envelope is missing"):
        export_run(run_dir, tmp_path / "invalid-export")


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("universe", "100-symbol universe"),
        ("feed", "historical feed"),
        ("protocol", "Swarm V2"),
        ("policy", "risk-adjusted Rust policy"),
        ("alias", "unrestored anonymized symbol"),
        ("duplicate", "duplicate forecast outcome IDs"),
        ("untracked", "untracked specialist forecasts"),
        ("net_alpha", "net-alpha attribution"),
        ("benchmark", "benchmark entry"),
        ("unmatured", "unmatured eligible forecasts"),
        ("infrastructure", "infrastructure invocation failures"),
    ],
)
def test_strict_economics_proof_requirements_fail_closed(
    mutation: str, message: str
) -> None:
    manifest: dict[str, object] = {
        "dataset": {"universe_size": 100, "historical_feed": "iex"},
        "research_protocol": research_simulation.SWARM_V2,
        "rust_research_policy": research_simulation.DEFAULT_RUST_RESEARCH_POLICY,
    }
    pending = {
        "outcome_id": "forecast-1",
        "symbol": "SPY",
        "matures_at": "2026-05-04T15:30:00Z",
    }
    matured = {
        "outcome_id": "forecast-1",
        "outcome_net_alpha_bps": 10.0,
        "benchmark_entry_price": 100.0,
    }
    events: list[dict[str, object]] = [
        {"event_type": "pending_outcome", "payload": pending},
        {"event_type": "outcome_matured", "payload": matured},
    ]
    if mutation == "universe":
        manifest["dataset"] = {"universe_size": 99, "historical_feed": "iex"}
    elif mutation == "feed":
        manifest["dataset"] = {"universe_size": 100, "historical_feed": "unknown"}
    elif mutation == "protocol":
        manifest["research_protocol"] = research_simulation.SWARM_V1
    elif mutation == "policy":
        manifest["rust_research_policy"] = "paper_default"
    elif mutation == "alias":
        pending["symbol"] = "SYM001"
    elif mutation == "duplicate":
        events.insert(1, {"event_type": "pending_outcome", "payload": dict(pending)})
    elif mutation == "untracked":
        events.append(
            {
                "event_type": "agent_invocation",
                "payload": {
                    "invocation_id": "untracked-specialist",
                    "role": "catalyst_scout",
                    "validation_status": "valid",
                    "output": {"schema_version": 2, "forecasts": [{}]},
                },
            }
        )
    elif mutation == "net_alpha":
        del matured["outcome_net_alpha_bps"]
    elif mutation == "benchmark":
        matured["benchmark_entry_price"] = None
    elif mutation == "infrastructure":
        events.append(
            {
                "event_type": "agent_invocation",
                "payload": {
                    "validation_status": "invalid",
                    "failure_kind": "infrastructure_failure",
                    "error": "WebSocket service unavailable (503)",
                },
            }
        )
    else:
        events.pop()

    with pytest.raises(ValueError, match=message):
        research_simulation._verify_strict_economics(manifest, events)


def test_strict_economics_accepts_complete_lineage_and_future_maturity() -> None:
    manifest = {
        "dataset": {"universe_size": 100, "historical_feed": "sip"},
        "research_protocol": research_simulation.SWARM_V2,
        "rust_research_policy": research_simulation.DEFAULT_RUST_RESEARCH_POLICY,
    }
    events = [
        {
            "event_type": "pending_outcome",
            "payload": {
                "outcome_id": "matured",
                "symbol": "SPY",
                "matures_at": "2026-05-04T15:30:00Z",
            },
        },
        {
            "event_type": "pending_outcome",
            "payload": {
                "outcome_id": "after-window",
                "symbol": "QQQ",
                "matures_at": "2026-08-02T15:30:00Z",
            },
        },
        {
            "event_type": "outcome_matured",
            "payload": {
                "outcome_id": "matured",
                "outcome_net_alpha_bps": 10.0,
                "benchmark_entry_price": 100.0,
            },
        },
    ]

    research_simulation._verify_strict_economics(manifest, events)


def test_legacy_codex_process_error_is_classified_as_infrastructure_failure() -> None:
    assert research_simulation._invocation_failure_kind(
        {
            "validation_status": "invalid",
            "exit_code": 1,
            "error": "Reading prompt from stdin...",
        }
    ) == "infrastructure_failure"
    assert research_simulation._invocation_failure_kind(
        {
            "validation_status": "invalid",
            "exit_code": 1,
            "error": "specialist schema validation failed",
        }
    ) == "model_output_invalid"


def test_strict_economics_allows_unpriceable_in_window_forecast() -> None:
    manifest = {
        "dataset": {"universe_size": 100, "historical_feed": "iex"},
        "research_protocol": research_simulation.SWARM_V2,
        "rust_research_policy": research_simulation.DEFAULT_RUST_RESEARCH_POLICY,
    }
    events = [
        {
            "event_type": "pending_outcome",
            "payload": {
                "outcome_id": "unpriceable",
                "symbol": "CTRA",
                "matures_at": "2026-05-04T15:30:00Z",
            },
        }
    ]

    research_simulation._verify_strict_economics(
        manifest,
        events,
        outcome_is_priceable=lambda _row: False,
    )
    with pytest.raises(ValueError, match="unmatured eligible forecasts"):
        research_simulation._verify_strict_economics(
            manifest,
            events,
            outcome_is_priceable=lambda _row: True,
        )


def test_research_model_output_schemas_use_codex_supported_subset() -> None:
    schema_dir = Path(__file__).resolve().parents[1] / "schemas"
    unsupported = {
        "allOf",
        "anyOf",
        "oneOf",
        "if",
        "then",
        "else",
        "not",
        "uniqueItems",
    }

    def walk(value: object) -> None:
        if isinstance(value, Mapping):
            assert unsupported.isdisjoint(value)
            if "const" in value:
                assert "type" in value
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    for schema_name in (
        "llm_research_specialist_result.schema.json",
        "llm_research_arbiter_result.schema.json",
        "llm_research_specialist_opinion_v2.schema.json",
        "llm_research_arbiter_decision_v2.schema.json",
    ):
        schema = json.loads((schema_dir / schema_name).read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
        walk(schema)

    assert research_simulation._model_output_schema(
        "catalyst_scout", protocol=research_simulation.SWARM_V1
    ).name == "llm_research_specialist_result.schema.json"
    assert research_simulation._model_output_schema(
        "final_arbiter", protocol=research_simulation.SWARM_V1
    ).name == "llm_research_arbiter_result.schema.json"
    assert research_simulation._model_output_schema(
        "catalyst_scout", protocol=research_simulation.SWARM_V2
    ).name == "llm_research_specialist_opinion_v2.schema.json"
    assert research_simulation._model_output_schema(
        "final_arbiter", protocol=research_simulation.SWARM_V2
    ).name == "llm_research_arbiter_decision_v2.schema.json"


def test_paired_block_bootstrap_is_seeded_and_holm_is_monotone() -> None:
    strategy = [0.02, 0.01, -0.005, 0.015, 0.01] * 5
    benchmark = [0.0] * len(strategy)

    first = paired_block_bootstrap(strategy, benchmark, block_size=5, samples=10_000, seed=42)
    second = paired_block_bootstrap(strategy, benchmark, block_size=5, samples=10_000, seed=42)
    corrected = holm_correct({"spy": 0.01, "qqq": 0.03, "cash": 0.2})

    assert first == second
    assert first[0] > 0.0
    assert 0.0 <= corrected["spy"] <= corrected["qqq"] <= corrected["cash"] <= 1.0


class _HistoricalResponse:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> object:
        return self.payload


class _HistoricalHttp:
    def __init__(self) -> None:
        self.requests: list[tuple[str, dict[str, str]]] = []

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        params: Mapping[str, str],
        timeout: float,
    ) -> _HistoricalResponse:
        del headers, timeout
        self.requests.append((url, dict(params)))
        if url.endswith("/v2/stocks/bars"):
            return _HistoricalResponse({"bars": {}, "next_page_token": None})
        if url.endswith("/v2/stocks/quotes"):
            return _HistoricalResponse(
                {
                    "quotes": {
                        "SPY": [
                            {"t": "2026-05-04T13:29:59Z", "bp": 99.95, "ap": 100.05}
                        ]
                    },
                    "next_page_token": None,
                }
            )
        if url.endswith("/v1beta1/news"):
            return _HistoricalResponse(
                {
                    "news": [
                        {
                            "id": 7,
                            "headline": "Bounded title",
                            "summary": "Bounded summary",
                            "content": "must not be returned",
                            "created_at": "2026-05-04T13:00:00Z",
                            "updated_at": "2026-05-04T13:01:00Z",
                            "url": "https://example.com/story",
                            "symbols": ["SPY"],
                            "source": "authorized-wire",
                        }
                    ],
                    "next_page_token": None,
                }
            )
        if url.endswith("/v2/calendar"):
            return _HistoricalResponse(
                [{"date": "2026-05-04", "open": "09:30", "close": "16:00"}]
            )
        raise AssertionError(url)


def test_alpaca_research_reads_raw_bars_quotes_news_and_official_calendar() -> None:
    http = _HistoricalHttp()
    client = AlpacaMarketDataClient(
        AlpacaMarketDataConfig(key_id="key", secret_key="secret"), http_client=http
    )
    start = datetime(2026, 5, 3, tzinfo=UTC)
    end = datetime(2026, 5, 5, tzinfo=UTC)

    client.historical_bars(["SPY"], start=start, end=end, adjustment="raw")
    quotes = client.historical_quotes(["SPY"], start=start, end=end)
    news = client.historical_news(["SPY"], start=start, end=end)
    calendar = client.market_calendar(start=date(2026, 5, 3), end=date(2026, 5, 5))

    bars_request = next(params for url, params in http.requests if url.endswith("/bars"))
    assert bars_request["adjustment"] == "raw"
    assert quotes["SPY"][0]["bid"] == 99.95
    assert news[0]["headline"] == "Bounded title"
    assert "content" not in news[0]
    assert calendar == [{"session_date": "2026-05-04", "open": "09:30", "close": "16:00"}]


def test_alpaca_historical_news_ignores_non_us_symbol_tags() -> None:
    class _CrossListedNewsHttp:
        def get(
            self,
            url: str,
            *,
            headers: dict[str, str],
            params: dict[str, str],
            timeout: float,
        ) -> _HistoricalResponse:
            return _HistoricalResponse(
                {
                    "news": [
                        {
                            "id": 8,
                            "headline": "Cross-listed issuer update",
                            "summary": "Bounded summary",
                            "created_at": "2026-05-04T13:00:00Z",
                            "updated_at": "2026-05-04T13:01:00Z",
                            "url": "https://example.com/story",
                            "symbols": ["CRD", "TSX:CRD", "CRD"],
                            "source": "authorized-wire",
                        }
                    ],
                    "next_page_token": None,
                }
            )

    client = AlpacaMarketDataClient(
        AlpacaMarketDataConfig(key_id="key", secret_key="secret"),
        http_client=_CrossListedNewsHttp(),
    )

    news = client.historical_news(
        ["CRD"], start=datetime(2026, 5, 3, tzinfo=UTC)
    )

    assert news[0]["symbols"] == ["CRD"]


class _UnusedRustHarness:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        return None

    def apply(
        self,
        decision: Mapping[str, object],
        snapshot: Mapping[str, object],
        simulated_received_at: datetime,
    ) -> Mapping[str, object]:
        del decision, snapshot, simulated_received_at
        raise AssertionError("hard-gated fixture must never reach Rust")

    def export_state(self) -> Mapping[str, object]:
        return {"coordinator_checkpoint": True}

    def restore_state(self, state: Mapping[str, object]) -> None:
        assert state == {"coordinator_checkpoint": True}


class _PartialFillRustHarness:
    def __init__(self) -> None:
        self.cash = 10_000.0
        self.quantity = 0.0

    def __enter__(self) -> Self:
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        return None

    def apply(
        self,
        decision: Mapping[str, object],
        snapshot: Mapping[str, object],
        simulated_received_at: datetime,
    ) -> Mapping[str, object]:
        del snapshot, simulated_received_at
        command = decision["commands"][0]  # type: ignore[index]
        assert isinstance(command, Mapping)
        notional = min(float(command["max_notional_usd"]), 40.0)
        quantity = notional / 100.0
        self.cash -= notional
        self.quantity += quantity
        return {
            "accepted_orders": [
                {
                    "command_id": f"{decision['cycle_id']}-1",
                    "symbol": "SPY",
                    "side": "buy",
                    "qty": quantity,
                    "avg_price": 100.0,
                    "notional_usd": notional,
                }
            ],
            "rejected_orders": [],
            "portfolio": {
                "initial_cash_usd": 10_000.0,
                "cash_usd": self.cash,
                "positions": {
                    "SPY": {
                        "symbol": "SPY",
                        "quantity": self.quantity,
                        "avg_price": 100.0,
                    }
                },
                "realized_pnl_usd": 0.0,
            },
            "prices": {"SPY": 100.0},
        }

    def apply_corporate_actions(
        self, actions: list[Mapping[str, object]]
    ) -> Mapping[str, object]:
        del actions
        raise AssertionError("fixture has no corporate actions")

    def export_state(self) -> Mapping[str, object]:
        return {"cash": self.cash, "quantity": self.quantity}

    def restore_state(self, state: Mapping[str, object]) -> None:
        self.cash = float(state["cash"])
        self.quantity = float(state["quantity"])


def test_complete_coordinator_records_terminal_state_and_nine_rth_calls(tmp_path: Path) -> None:
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(
            {
                "calendar": [
                    {
                        "session_date": "2026-05-04",
                        "open": "09:30",
                        "close": "16:00",
                        "available_at": "2026-05-01T20:00:00Z",
                    }
                ],
                "observations": [
                    {
                        "schema_version": 1,
                        "observation_id": "quote-SPY",
                        "kind": "quote",
                        "source": "alpaca_historical_quotes",
                        "event_time": "2026-05-04T13:29:59Z",
                        "available_at": "2026-05-04T13:29:59Z",
                        "symbol": "SPY",
                        "payload": {"bid": 99.95, "ask": 100.05},
                    },
                    {
                        "schema_version": 1,
                        "observation_id": "quote-QQQ",
                        "kind": "quote",
                        "source": "alpaca_historical_quotes",
                        "event_time": "2026-05-04T13:29:59Z",
                        "available_at": "2026-05-04T13:29:59Z",
                        "symbol": "QQQ",
                        "payload": {"bid": 199.95, "ask": 200.05},
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    run_dir = prepare_run(
        state_root=tmp_path / "runs",
        run_id="coordinator-run",
        source_path=source,
        code_fingerprint="sha256:test-code",
    )

    result = run_prepared_simulation(
        run_dir,
        invoker=_RecordingInvoker(),
        rust_factory=_UnusedRustHarness,
    )

    assert result["valid"] is True
    assert result["status"] == "complete"
    assert result["terminal_cycles"] == 60
    schema_dir = Path(__file__).resolve().parents[1] / "schemas"
    event_validator = Draft202012Validator(
        json.loads((schema_dir / "llm_research_event.schema.json").read_text(encoding="utf-8"))
    )
    invocation_validator = Draft202012Validator(
        json.loads(
            (schema_dir / "llm_research_specialist_invocation.schema.json").read_text(
                encoding="utf-8"
            )
        )
    )
    arbiter_validator = Draft202012Validator(
        json.loads(
            (schema_dir / "llm_research_arbiter_provenance.schema.json").read_text(
                encoding="utf-8"
            )
        )
    )
    events = [
        json.loads(line)
        for path in sorted((run_dir / "events").glob("*.jsonl"))
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]
    for event in events:
        event_validator.validate(event)
        if event["event_type"] in {"agent_invocation", "arbiter_recovery_invocation"}:
            invocation_validator.validate(event["payload"])
        if event["event_type"] == "arbiter_provenance":
            arbiter_validator.validate(event["payload"])
    rth_cycles = {
        (event["payload"]["arm"], event["payload"]["cycle_id"])
        for event in events
        if event["event_type"] == "cycle_terminal"
        and event["payload"]["is_rth"]
        and event["payload"]["arm"] != "single_reviewer_reference"
    }
    invocation_counts = {
        key: sum(
            event["event_type"] == "agent_invocation"
            and (event["payload"]["arm"], event["payload"]["cycle_id"]) == key
            for event in events
        )
        for key in rth_cycles
    }
    assert invocation_counts and set(invocation_counts.values()) == {9}
    replay = StoredOutputInvoker(run_dir)
    replay_event = next(
        event
        for event in events
        if event["event_type"] == "agent_invocation"
        and event["payload"]["model_cycle_id"]
        == "anonymized_frozen-2026-05-04-0900-prepare_release"
    )
    replay_input = CanonicalRunStore(run_dir).get_object(
        replay_event["payload"]["input_object"]
    )
    replayed = replay.invoke(
        role="single_reviewer",
        cycle_id="anonymized_frozen-2026-05-04-0900-prepare_release",
        evidence_pack=replay_input["evidence_pack"],
        diagnostic_only=True,
    )
    assert replayed.validation_status == "invalid"
    assert replayed.latency_ms == 5


def test_complete_coordinator_rejects_missing_final_qqq_benchmark(tmp_path: Path) -> None:
    source = tmp_path / "missing-qqq-source.json"
    source.write_text(
        json.dumps(
            {
                "calendar": [
                    {
                        "session_date": "2026-05-04",
                        "open": "09:30",
                        "close": "16:00",
                        "available_at": "2026-05-01T20:00:00Z",
                    }
                ],
                "observations": [
                    {
                        "schema_version": 1,
                        "observation_id": "quote-SPY",
                        "kind": "quote",
                        "source": "alpaca_historical_quotes",
                        "event_time": "2026-05-04T13:29:59Z",
                        "available_at": "2026-05-04T13:29:59Z",
                        "symbol": "SPY",
                        "payload": {"bid": 99.95, "ask": 100.05},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    run_dir = prepare_run(
        state_root=tmp_path / "runs",
        run_id="missing-final-benchmark",
        source_path=source,
        code_fingerprint="sha256:test-code",
    )

    with pytest.raises(ValueError, match="final benchmark marks"):
        run_prepared_simulation(
            run_dir,
            invoker=_RecordingInvoker(sleep_seconds=0.0),
            rust_factory=_UnusedRustHarness,
        )


def test_coordinator_records_rust_lineage_and_two_bounded_recoveries(
    tmp_path: Path,
) -> None:
    source = tmp_path / "execution-source.json"
    source.write_text(
        json.dumps(
            {
                "calendar": [
                    {
                        "session_date": "2026-05-04",
                        "open": "09:30",
                        "close": "16:00",
                        "available_at": "2026-05-01T20:00:00Z",
                    }
                ],
                "observations": [
                    {
                        "schema_version": 1,
                        "observation_id": "bar-SPY",
                        "kind": "bar",
                        "source": "alpaca_historical_bars",
                        "event_time": "2026-05-04T13:29:00Z",
                        "available_at": "2026-05-04T13:30:00Z",
                        "symbol": "SPY",
                        "payload": {
                            "open": 100.0,
                            "high": 100.2,
                            "low": 99.8,
                            "close": 100.0,
                            "volume": 1000000.0,
                            "session_open": 100.0,
                            "bar_end": "2026-05-04T13:30:00Z",
                            "adjustment": "raw",
                        },
                    },
                    {
                        "schema_version": 1,
                        "observation_id": "quote-SPY",
                        "kind": "quote",
                        "source": "alpaca_historical_quotes",
                        "event_time": "2026-05-04T13:29:59Z",
                        "available_at": "2026-05-04T13:29:59Z",
                        "symbol": "SPY",
                        "payload": {"bid": 99.95, "ask": 100.05},
                    },
                    {
                        "schema_version": 1,
                        "observation_id": "quote-SPY-outcome",
                        "kind": "quote",
                        "source": "alpaca_historical_quotes",
                        "event_time": "2026-05-04T15:31:00Z",
                        "available_at": "2026-05-04T15:31:00Z",
                        "symbol": "SPY",
                        "payload": {"bid": 100.95, "ask": 101.05},
                    },
                    {
                        "schema_version": 1,
                        "observation_id": "quote-SPY-final-maturity",
                        "kind": "quote",
                        "source": "alpaca_historical_quotes",
                        "event_time": "2026-05-04T20:06:00Z",
                        "available_at": "2026-05-04T20:06:00Z",
                        "symbol": "SPY",
                        "payload": {"bid": 101.95, "ask": 102.05},
                    },
                    {
                        "schema_version": 1,
                        "observation_id": "quote-QQQ",
                        "kind": "quote",
                        "source": "alpaca_historical_quotes",
                        "event_time": "2026-05-04T20:06:00Z",
                        "available_at": "2026-05-04T20:06:00Z",
                        "symbol": "QQQ",
                        "payload": {"bid": 200.95, "ask": 201.05},
                    },
                    {
                        "schema_version": 1,
                        "observation_id": "borrow-SPY",
                        "kind": "borrow",
                        "source": "alpaca_historical_borrow",
                        "event_time": "2026-05-04T13:29:00Z",
                        "available_at": "2026-05-04T13:29:00Z",
                        "symbol": "SPY",
                        "payload": {"shortable": True},
                    },
                    {
                        "schema_version": 1,
                        "observation_id": "macro-1",
                        "kind": "macro",
                        "source": "federal_reserve_archive",
                        "event_time": "2026-05-04T13:00:00Z",
                        "available_at": "2026-05-04T13:00:00Z",
                        "symbol": None,
                        "payload": {"summary": "Bounded official macro release."},
                    },
                    {
                        "schema_version": 1,
                        "observation_id": "news-SPY",
                        "kind": "news",
                        "source": "alpaca_historical_news",
                        "event_time": "2026-05-04T13:00:00Z",
                        "available_at": "2026-05-04T13:00:00Z",
                        "symbol": "SPY",
                        "payload": {"summary": "Bounded historical summary."},
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    run_dir = prepare_run(
        state_root=tmp_path / "runs",
        run_id="execution-run",
        source_path=source,
        code_fingerprint="sha256:test-code",
    )

    result = run_prepared_simulation(
        run_dir,
        invoker=_SwarmV2Invoker(),
        rust_factory=_PartialFillRustHarness,
        protocol=research_simulation.SWARM_V2,
    )
    events = CanonicalRunStore(run_dir).read_events()
    rust_events = [event for event in events if event["event_type"] == "rust_execution"]
    pending_outcomes = [
        event["payload"]
        for event in events
        if event["event_type"] == "pending_outcome"
    ]

    assert result["status"] == "complete"
    assert verify_run(run_dir)["valid"] is True
    assert {event["payload"]["attempt"] for event in rust_events} == {0, 1, 2}
    pending_ids = [str(outcome["outcome_id"]) for outcome in pending_outcomes]
    assert len(pending_ids) == len(set(pending_ids))
    assert pending_ids and all("-forecast-" in outcome_id for outcome_id in pending_ids)
    assert any(outcome["diagnostic_only"] is False for outcome in pending_outcomes)
    assert any(outcome["diagnostic_only"] is True for outcome in pending_outcomes)
    final_maturity_outcomes = [
        event["payload"]
        for event in events
        if event["event_type"] == "outcome_matured"
        and event["payload"].get("cycle_id") == "final-maturity"
    ]
    assert final_maturity_outcomes
    assert {
        event["payload"]["arm"]
        for event in events
        if event["event_type"] == "learning_delta"
        and event["payload"].get("cycle_id") == "final-maturity"
    } == set(ARMS[:-1])
    for event in rust_events:
        assert event["payload"]["decision_object"].startswith("sha256:")
        assert event["payload"]["snapshot_object"].startswith("sha256:")
        assert event["payload"]["result_object"].startswith("sha256:")
    partial_cycle = next(
        event
        for event in events
        if event["event_type"] == "cycle_terminal"
        and event["payload"]["cycle_kind"] == "release"
        and event["payload"]["arm"] == "semantic_frozen"
    )
    assert partial_cycle["payload"]["invocation_count"] == 11
    recovery_invocations = [
        event
        for event in events
        if event["event_type"] == "arbiter_recovery_invocation"
        and event["payload"]["arm"] == "semantic_frozen"
        and event["payload"]["cycle_id"] == partial_cycle["payload"]["cycle_id"]
    ]
    assert len(recovery_invocations) == 2
    for attempt, event in enumerate(recovery_invocations, 1):
        stored_input = CanonicalRunStore(run_dir).get_object(
            event["payload"]["input_object"]
        )
        assert "instruction" not in stored_input
        assert stored_input["cycle_id"].endswith(f"-recovery-{attempt}")
        assert stored_input["evidence_pack"]["recovery_context"]["attempt"] == attempt
        assert stored_input["specialist_envelopes"]
    final_marks = [
        event
        for event in events
        if event["event_type"] == "portfolio_mark"
        and event["payload"].get("final_eligible_quote_mark") is True
    ]
    assert len(final_marks) == len(ARMS)


def test_coordinator_pauses_cleanly_only_after_a_terminal_checkpoint(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(
            {
                "calendar": [
                    {
                        "session_date": "2026-05-04",
                        "open": "09:30",
                        "close": "16:00",
                        "available_at": "2026-05-01T20:00:00Z",
                    }
                ],
                "observations": [],
            }
        ),
        encoding="utf-8",
    )
    run_dir = prepare_run(
        state_root=tmp_path / "runs",
        run_id="bounded-coordinator-run",
        source_path=source,
        code_fingerprint="sha256:test-code",
    )

    result = run_prepared_simulation(
        run_dir,
        invoker=_RecordingInvoker(),
        rust_factory=_UnusedRustHarness,
        max_cycles=1,
    )

    assert result["status"] == "running"
    assert result["paused"] is True
    assert result["processed_cycles"] == 1
    assert result["terminal_cycles"] == 1
    assert result["remaining_cycles"] == 59
    assert verify_run(run_dir)["valid"] is True
    checkpoints = list((run_dir / "checkpoints").glob("*/*.json"))
    assert len(checkpoints) == 1


def test_completed_cycle_replay_uses_only_compatible_terminal_cycles(
    tmp_path: Path,
) -> None:
    source = CanonicalRunStore(tmp_path / "source-run")
    fingerprints = {
        key: f"sha256:{key}"
        for key in ("code", "policy", "schemas", "prompt_templates", "model", "catalog")
    }
    manifest = {
        "run_id": "source-run",
        "research_protocol": research_simulation.SWARM_V2,
        "dataset": {"object": "sha256:dataset"},
        "fingerprints": fingerprints,
    }
    source.initialize(manifest)
    pack = {
        "schema_version": 1,
        "research_protocol": research_simulation.SWARM_V2,
        "evidence": [{"id": "quote-SPY"}],
    }
    input_object = source.put_object(
        research_simulation._research_model_input(
            role="catalyst_scout",
            cycle_id="anonymized_adaptive-cycle-1",
            evidence_pack=pack,
            diagnostic_only=False,
            arbiter_input=None,
            protocol=research_simulation.SWARM_V2,
        )
    )
    output = {
        "schema_version": 2,
        "abstain": True,
        "abstention_reason": "No edge.",
        "risk_veto": False,
        "forecasts": [],
    }
    invocation_event = source.append_event(
        event_type="agent_invocation",
        event_time=_timestamp(14, 30),
        available_at=_timestamp(14, 30),
        payload={
            "model_cycle_id": "anonymized_adaptive-cycle-1",
            "arm": "anonymized_adaptive",
            "cycle_id": "cycle-1",
            "role": "catalyst_scout",
            "validation_status": "valid",
            "output": output,
            "output_hash": None,
            "input_object": input_object,
            "diagnostic_only": False,
            "latency_ms": 12,
            "exit_code": 0,
            "usage": {"input_tokens": 10},
            "actual_cost_usd": 0.0,
            "imputed_cost_usd": 0.0,
            "error": None,
        },
    )
    source.append_event(
        event_type="cycle_terminal",
        event_time=_timestamp(14, 30),
        available_at=_timestamp(14, 30),
        payload={
            "arm": "anonymized_adaptive",
            "cycle_id": "cycle-1",
            "infrastructure_failure_count": 0,
            "invocation_count": 1,
        },
    )
    recovery_cycle_id = "anonymized_adaptive-cycle-recovery"
    source.append_event(
        event_type="agent_invocation",
        event_time=_timestamp(14, 31),
        available_at=_timestamp(14, 31),
        payload={
            **invocation_event["payload"],
            "model_cycle_id": recovery_cycle_id,
            "cycle_id": "cycle-recovery",
        },
    )
    source.append_event(
        event_type="arbiter_recovery_invocation",
        event_time=_timestamp(14, 31),
        available_at=_timestamp(14, 31),
        payload={
            **invocation_event["payload"],
            "model_cycle_id": f"{recovery_cycle_id}-recovery-1",
            "cycle_id": "cycle-recovery",
            "role": "final_arbiter",
        },
    )
    source.append_event(
        event_type="cycle_terminal",
        event_time=_timestamp(14, 31),
        available_at=_timestamp(14, 31),
        payload={
            "arm": "anonymized_adaptive",
            "cycle_id": "cycle-recovery",
            "infrastructure_failure_count": 0,
            "invocation_count": 2,
        },
    )
    fallback = _RecordingInvoker(sleep_seconds=0.0)
    invoker = research_simulation.CompletedCycleReplayInvoker(
        source.run_dir,
        fallback=fallback,
        target_manifest=manifest,
        protocol=research_simulation.SWARM_V2,
    )

    replayed = invoker.invoke(
        role="catalyst_scout",
        cycle_id="anonymized_adaptive-cycle-1",
        evidence_pack=pack,
        diagnostic_only=False,
    )
    live = invoker.invoke(
        role="catalyst_scout",
        cycle_id="anonymized_adaptive-cycle-2",
        evidence_pack=pack,
        diagnostic_only=False,
    )
    recovery_cycle_live = invoker.invoke(
        role="catalyst_scout",
        cycle_id=recovery_cycle_id,
        evidence_pack=pack,
        diagnostic_only=False,
    )

    assert replayed.output == output
    assert replayed.usage["replay_source"]["run_id"] == "source-run"
    assert replayed.usage["replay_source"]["event_hash"] == invocation_event["event_hash"]
    assert live.validation_status == "valid"
    assert recovery_cycle_live.validation_status == "valid"
    assert fallback.calls == [
        ("catalyst_scout", "anonymized_adaptive-cycle-2"),
        ("catalyst_scout", recovery_cycle_id),
    ]
    with pytest.raises(ValueError, match="input object mismatch"):
        invoker.invoke(
            role="catalyst_scout",
            cycle_id="anonymized_adaptive-cycle-1",
            evidence_pack={"schema_version": 1, "evidence": []},
            diagnostic_only=False,
        )


def test_stored_output_replay_rejects_changed_evidence_and_fingerprints(
    tmp_path: Path,
) -> None:
    source = CanonicalRunStore(tmp_path / "stored-source")
    fingerprints = {
        key: f"sha256:{key}"
        for key in ("code", "policy", "schemas", "prompt_templates", "model", "catalog")
    }
    manifest = {
        "run_id": "stored-source",
        "research_protocol": research_simulation.SWARM_V2,
        "dataset": {"object": "sha256:dataset"},
        "fingerprints": fingerprints,
    }
    source.initialize(manifest)
    pack = {
        "schema_version": 1,
        "research_protocol": research_simulation.SWARM_V2,
        "evidence": [{"id": "quote-SPY"}],
    }
    source.append_event(
        event_type="agent_invocation",
        event_time=_timestamp(14, 30),
        available_at=_timestamp(14, 30),
        payload={
            "model_cycle_id": "anonymized_adaptive-cycle-1",
            "arm": "anonymized_adaptive",
            "cycle_id": "cycle-1",
            "role": "catalyst_scout",
            "validation_status": "valid",
            "output": {"schema_version": 2, "abstain": True, "forecasts": []},
            "output_hash": None,
            "input_object": source.put_object(
                research_simulation._research_model_input(
                    role="catalyst_scout",
                    cycle_id="anonymized_adaptive-cycle-1",
                    evidence_pack=pack,
                    diagnostic_only=False,
                    arbiter_input=None,
                    protocol=research_simulation.SWARM_V2,
                )
            ),
            "diagnostic_only": False,
            "latency_ms": 1,
            "exit_code": 0,
            "usage": {"input_tokens": 10},
            "actual_cost_usd": 0.01,
            "imputed_cost_usd": 0.01,
            "error": None,
        },
    )

    replay = StoredOutputInvoker(
        source.run_dir,
        target_manifest=manifest,
        protocol=research_simulation.SWARM_V2,
    )
    assert replay.invoke(
        role="catalyst_scout",
        cycle_id="anonymized_adaptive-cycle-1",
        evidence_pack=pack,
        diagnostic_only=False,
    ).validation_status == "valid"
    with pytest.raises(ValueError, match="input object mismatch"):
        replay.invoke(
            role="catalyst_scout",
            cycle_id="anonymized_adaptive-cycle-1",
            evidence_pack={"schema_version": 1, "evidence": []},
            diagnostic_only=False,
        )

    mismatched = {**manifest, "fingerprints": {**fingerprints, "policy": "sha256:changed"}}
    with pytest.raises(ResearchFingerprintMismatch, match="fingerprint mismatch: policy"):
        StoredOutputInvoker(
            source.run_dir,
            target_manifest=mismatched,
            protocol=research_simulation.SWARM_V2,
        )


class _FailAfterFirstCycleInvoker(_RecordingInvoker):
    def __init__(self) -> None:
        super().__init__()
        self._calls = 0

    def invoke(self, **kwargs: object) -> InvocationResult:
        self._calls += 1
        if self._calls == 2:
            raise RuntimeError("simulated coordinator crash")
        return super().invoke(**kwargs)


class _InfrastructureFailureInvoker(_RecordingInvoker):
    def invoke(self, **kwargs: object) -> InvocationResult:
        return InvocationResult.invalid(
            str(kwargs.get("role", "single_reviewer")),
            "WebSocket service unavailable (503)",
            latency_ms=1,
            usage={"input_tokens": 100},
            failure_kind="infrastructure_failure",
        )


def test_exhausted_infrastructure_failure_finalizes_hashes_and_lifecycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(
            {
                "calendar": [
                    {
                        "session_date": "2026-05-04",
                        "open": "09:30",
                        "close": "16:00",
                        "available_at": "2026-05-01T20:00:00Z",
                    }
                ],
                "observations": [],
            }
        ),
        encoding="utf-8",
    )
    run_dir = prepare_run(
        state_root=tmp_path / "runs",
        run_id="infrastructure-failure-run",
        source_path=source,
        code_fingerprint="sha256:test-code",
    )

    with pytest.raises(RuntimeError, match="exhausted Codex infrastructure failure"):
        run_prepared_simulation(
            run_dir,
            invoker=_InfrastructureFailureInvoker(),
            rust_factory=_UnusedRustHarness,
        )

    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    events = CanonicalRunStore(run_dir).read_events()
    failure = next(event for event in events if event["event_type"] == "run_failed")
    assert manifest["status"] == "failed"
    assert failure["payload"]["failure_kind"] == "infrastructure_failure"
    assert failure["payload"]["resumable_by_explicit_command"] is True
    assert verify_run(run_dir)["valid"] is True

    CanonicalRunStore(run_dir).append_event(
        event_type="outcome_matured",
        event_time=_timestamp(14, 30),
        available_at=_timestamp(14, 30),
        payload={
            "arm": "anonymized_frozen",
            "outcome_id": "already-recorded-before-resume",
        },
    )

    monkeypatch.setattr(
        research_simulation, "_git_fingerprint", lambda: "sha256:test-code"
    )
    monkeypatch.setattr(
        research_simulation,
        "_model_catalog_fingerprint",
        lambda: manifest["fingerprints"]["catalog"],
    )
    resumed = run_prepared_simulation(
        run_dir,
        invoker=_RecordingInvoker(),
        rust_factory=_UnusedRustHarness,
        resume=True,
        max_cycles=1,
    )
    assert resumed["paused"] is True


def test_failed_run_without_explicit_resume_event_stays_terminal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(
            {
                "calendar": [
                    {
                        "session_date": "2026-05-04",
                        "open": "09:30",
                        "close": "16:00",
                        "available_at": "2026-05-01T20:00:00Z",
                    }
                ],
                "observations": [],
            }
        ),
        encoding="utf-8",
    )
    run_dir = prepare_run(
        state_root=tmp_path / "runs",
        run_id="terminal-failed-run",
        source_path=source,
        code_fingerprint="sha256:test-code",
    )
    store = CanonicalRunStore(run_dir)
    manifest = research_simulation._replace_manifest_status(store, "failed")
    monkeypatch.setattr(
        research_simulation, "_git_fingerprint", lambda: "sha256:test-code"
    )
    monkeypatch.setattr(
        research_simulation,
        "_model_catalog_fingerprint",
        lambda: manifest["fingerprints"]["catalog"],
    )

    with pytest.raises(RuntimeError, match="not explicitly resumable"):
        run_prepared_simulation(run_dir, resume=True)


def test_operational_integrity_rejects_unknown_model_cost_and_invalid_lockbox() -> None:
    events: list[dict[str, object]] = [
        {
            "event_type": "agent_invocation",
            "payload": {
                "arm": "anonymized_adaptive",
                "validation_status": "valid",
                "usage": {
                    "attempts": [{"input_tokens": 100, "output_tokens": 10}]
                },
                "actual_cost_usd": 0.0,
                "imputed_cost_usd": 0.0,
            },
        },
        {
            "event_type": "cycle_terminal",
            "payload": {
                "arm": "anonymized_adaptive",
                "phase": "lockbox",
                "status": "blocked",
                "block_reason": "corporate_action_symbol_collision",
            },
        },
        {
            "event_type": "pending_outcome",
            "payload": {
                "arm": "anonymized_adaptive",
                "outcome_id": "late-forecast",
                "matures_at": "2026-07-31T19:00:00Z",
            },
        },
    ]

    integrity = research_simulation._arm_operational_integrity(
        events, "anonymized_adaptive"
    )

    assert integrity["model_cost_status"] == "cost_unknown"
    assert integrity["outcome_accounting_complete"] is False
    assert integrity["lockbox_evidence_eligible"] is False
    assert integrity["persistent_lockbox_block_reason"] == (
        "corporate_action_symbol_collision"
    )
    assert integrity["performance_evidence_eligible"] is False


def test_operational_integrity_requires_complete_usage_cost_and_final_marks() -> None:
    arm = "anonymized_adaptive"
    events: list[dict[str, object]] = [
        {
            "event_type": "agent_invocation",
            "payload": {
                "arm": arm,
                "validation_status": "valid",
                "usage": {"input_tokens": 100, "output_tokens": 10},
                "actual_cost_usd": 0.01,
                "imputed_cost_usd": 0.01,
            },
        },
        {
            "event_type": "agent_invocation",
            "payload": {
                "arm": arm,
                "validation_status": "valid",
                "usage": {},
                "actual_cost_usd": 0.0,
                "imputed_cost_usd": 0.0,
            },
        },
        {
            "event_type": "cycle_terminal",
            "payload": {
                "arm": arm,
                "phase": "lockbox",
                "status": "accepted",
                "block_reason": None,
            },
        },
        {
            "event_type": "arm_finalized",
            "payload": {
                "arm": arm,
                "final_eligible_quote_mark": True,
                "unpriced_positions": ["SPY"],
            },
        },
    ]

    integrity = research_simulation._arm_operational_integrity(events, arm)

    assert integrity["usage_recorded_count"] == 1
    assert integrity["usage_coverage"] == pytest.approx(0.5)
    assert integrity["cost_recorded_count"] == 1
    assert integrity["model_cost_status"] == "cost_unknown"
    assert integrity["final_valuation_complete"] is False
    assert integrity["performance_evidence_eligible"] is False


def test_lockbox_cost_adjustment_uses_actual_nav_and_one_extra_execution_layer() -> None:
    summary = research_simulation._lockbox_cost_adjusted_pnl(
        starting_nav_usd=12_500.0,
        ending_nav_usd=12_625.0,
        spread_usd=10.0,
        impact_usd=5.0,
        commission_usd=2.0,
        carrying_cost_usd=3.0,
        model_cost_usd=4.0,
    )

    assert summary["gross_pnl_usd"] == pytest.approx(125.0)
    assert summary["cost_adjusted_pnl_usd"] == pytest.approx(116.0)
    assert summary["double_execution_cost_pnl_usd"] == pytest.approx(99.0)


def test_final_maturity_resume_filter_removes_only_recorded_outcomes() -> None:
    learning: dict[str, object] = {
        "pending_outcomes": [
            {"outcome_id": "matured"},
            {"outcome_id": "still-pending"},
        ]
    }

    research_simulation._remove_already_matured_outcomes(
        learning, {"matured"}
    )

    assert learning["pending_outcomes"] == [{"outcome_id": "still-pending"}]


def test_resume_restores_last_coordinator_owned_rust_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(
            {
                "calendar": [
                    {
                        "session_date": "2026-05-04",
                        "open": "09:30",
                        "close": "16:00",
                        "available_at": "2026-05-01T20:00:00Z",
                    }
                ],
                "observations": [
                    {
                        "schema_version": 1,
                        "observation_id": f"quote-{symbol}",
                        "kind": "quote",
                        "source": "alpaca_historical_quotes",
                        "event_time": "2026-05-04T13:29:59Z",
                        "available_at": "2026-05-04T13:29:59Z",
                        "symbol": symbol,
                        "payload": {"bid": price - 0.05, "ask": price + 0.05},
                    }
                    for symbol, price in (("SPY", 100.0), ("QQQ", 200.0))
                ],
            }
        ),
        encoding="utf-8",
    )
    run_dir = prepare_run(
        state_root=tmp_path / "runs",
        run_id="resume-run",
        source_path=source,
        code_fingerprint="sha256:test-code",
    )
    with pytest.raises(RuntimeError, match="simulated coordinator crash"):
        run_prepared_simulation(
            run_dir,
            invoker=_FailAfterFirstCycleInvoker(),
            rust_factory=_UnusedRustHarness,
        )

    monkeypatch.setattr(
        research_simulation, "_git_fingerprint", lambda: "sha256:test-code"
    )
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    monkeypatch.setattr(
        research_simulation,
        "_model_catalog_fingerprint",
        lambda: manifest["fingerprints"]["catalog"],
    )
    result = run_prepared_simulation(
        run_dir,
        invoker=_RecordingInvoker(),
        resume=True,
        rust_factory=_UnusedRustHarness,
    )

    assert result["status"] == "complete"
    completed_events = result["events"]
    assert run_prepared_simulation(run_dir, resume=True)["events"] == completed_events
