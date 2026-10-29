from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar

from llm_equity.technicals import (
    CONFLUENCE_WEIGHT_FVG,
    CONFLUENCE_WEIGHT_GOLDEN_POCKET,
    CONFLUENCE_WEIGHT_MSS,
    CONFLUENCE_WEIGHT_WYCKOFF,
    MIN_TRIGGER_BARS,
    aggregate_4h_bars,
    build_symbol_technicals,
    detect_fvgs,
    detect_mss,
    detect_swings,
    golden_pocket,
    liquidity_sweep_trigger,
    wyckoff_state,
)

_SESSION_OPEN = datetime(2026, 6, 1, 13, 30, tzinfo=UTC)


def _bar(
    open_price: float,
    high: float,
    low: float,
    close: float,
    *,
    timestamp: str = "2026-06-01T13:30:00Z",
    volume: float = 1_000.0,
) -> dict[str, Any]:
    return {
        "timestamp": timestamp,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "source": "test_fixture",
    }


def _hourly(rows: list[tuple[float, float, float, float]], *, start: datetime = _SESSION_OPEN) -> list[dict[str, Any]]:
    bars = []
    for index, (open_price, high, low, close) in enumerate(rows):
        timestamp = (start + timedelta(hours=index)).isoformat().replace("+00:00", "Z")
        bars.append(_bar(open_price, high, low, close, timestamp=timestamp))
    return bars


def _swing(index: int, kind: str, price: float) -> dict[str, Any]:
    return {"index": index, "kind": kind, "price": price, "timestamp": "2026-06-01T13:30:00Z"}


class TestDetectSwings:
    def test_detects_pivot_high_and_low(self) -> None:
        bars = _hourly(
            [
                (100.0, 101.0, 99.0, 100.0),
                (100.0, 102.0, 99.5, 101.0),
                (101.0, 105.0, 100.0, 104.0),
                (104.0, 103.0, 98.0, 99.0),
                (99.0, 100.0, 97.0, 98.0),
                (98.0, 99.5, 96.0, 99.0),
                (99.0, 101.0, 98.5, 100.5),
                (100.5, 102.0, 99.0, 101.5),
            ]
        )
        swings = detect_swings(bars)
        kinds = {(swing["kind"], swing["index"]) for swing in swings}
        assert ("high", 2) in kinds
        assert ("low", 5) in kinds

    def test_plateau_never_pivots(self) -> None:
        bars = _hourly(
            [
                (100.0, 101.0, 99.0, 100.0),
                (100.0, 103.0, 99.0, 100.0),
                (100.0, 103.0, 99.0, 100.0),
                (100.0, 103.0, 99.0, 100.0),
                (100.0, 101.0, 99.0, 100.0),
            ]
        )
        assert detect_swings(bars) == []

    def test_insufficient_bars(self) -> None:
        bars = _hourly([(100.0, 101.0, 99.0, 100.0)] * 4)
        assert detect_swings(bars) == []

    def test_boolean_bar_values_are_rejected(self) -> None:
        bars = _hourly(
            [
                (100.0, 101.0, 99.0, 100.0),
                (100.0, 102.0, 99.5, 101.0),
                (101.0, 105.0, 100.0, 104.0),
                (104.0, 103.0, 98.0, 99.0),
                (99.0, 100.0, 97.0, 98.0),
            ]
        )
        bars[2] = {**bars[2], "high": True}
        assert detect_swings(bars) == []


class TestDetectFvgs:
    def test_bullish_gap(self) -> None:
        bars = _hourly(
            [
                (99.0, 100.0, 98.0, 99.5),
                (100.5, 103.0, 100.2, 102.5),
                (102.0, 104.0, 101.0, 103.0),
            ]
        )
        zones = detect_fvgs(bars, current_price=103.0)
        assert len(zones) == 1
        assert zones[0]["side"] == "bullish"
        assert zones[0]["top"] == 101.0
        assert zones[0]["bottom"] == 100.0
        assert zones[0]["mitigated"] is False

    def test_bearish_gap(self) -> None:
        bars = _hourly(
            [
                (101.0, 102.0, 100.0, 100.5),
                (99.5, 99.8, 97.0, 97.5),
                (97.0, 99.0, 96.0, 96.5),
            ]
        )
        zones = detect_fvgs(bars, current_price=96.5)
        assert len(zones) == 1
        assert zones[0]["side"] == "bearish"
        assert zones[0]["top"] == 100.0
        assert zones[0]["bottom"] == 99.0

    def test_touching_wicks_are_not_a_gap(self) -> None:
        bars = _hourly(
            [
                (99.0, 101.0, 98.0, 99.5),
                (100.5, 103.0, 100.2, 102.5),
                (102.0, 104.0, 101.0, 103.0),
            ]
        )
        assert detect_fvgs(bars, current_price=103.0) == []

    def test_filled_gap_is_dropped(self) -> None:
        bars = _hourly(
            [
                (99.0, 100.0, 98.0, 99.5),
                (100.5, 103.0, 100.2, 102.5),
                (102.0, 104.0, 101.0, 103.0),
                (103.0, 103.5, 99.5, 100.2),
            ]
        )
        assert detect_fvgs(bars, current_price=100.2) == []

    def test_wick_entry_marks_mitigated(self) -> None:
        bars = _hourly(
            [
                (99.0, 100.0, 98.0, 99.5),
                (100.5, 103.0, 100.2, 102.5),
                (102.0, 104.0, 101.0, 103.0),
                (103.0, 103.5, 100.6, 102.8),
            ]
        )
        zones = detect_fvgs(bars, current_price=102.8)
        assert len(zones) == 1
        assert zones[0]["mitigated"] is True


_DOWNTREND_ROWS: list[tuple[float, float, float, float]] = [
    (104.0, 108.0, 100.0, 105.0),
    (105.0, 109.0, 101.0, 106.0),
    (106.0, 110.0, 102.0, 107.0),
    (104.0, 104.5, 97.0, 98.0),
    (98.0, 100.0, 95.0, 96.0),
    (96.0, 103.0, 96.0, 102.0),
    (102.0, 105.0, 98.0, 100.0),
    (100.0, 102.0, 93.0, 94.0),
    (94.0, 99.0, 90.0, 91.0),
    (91.0, 100.0, 94.0, 99.0),
    (99.0, 101.0, 95.0, 100.0),
]


class TestDetectMss:
    def test_body_close_above_lower_high_confirms_bullish_mss(self) -> None:
        bars = _hourly([*_DOWNTREND_ROWS, (100.0, 107.0, 99.0, 106.0)])
        swings = detect_swings(bars)
        mss = detect_mss(bars, swings)
        assert mss["state"] == "bullish_mss"
        assert mss["level"] == 105.0
        assert mss["confirmed_at"] == bars[11]["timestamp"]
        assert mss["fvg_confirmed"] is False

    def test_wick_only_break_is_not_mss(self) -> None:
        bars = _hourly([*_DOWNTREND_ROWS, (100.0, 107.0, 99.0, 104.0)])
        swings = detect_swings(bars)
        assert detect_mss(bars, swings)["state"] == "none"

    def test_fvg_after_break_sets_confirmation(self) -> None:
        bars = _hourly(
            [
                *_DOWNTREND_ROWS,
                (100.0, 107.0, 99.0, 106.0),
                (107.0, 110.0, 106.5, 109.0),
                (109.0, 111.0, 108.0, 110.0),
            ]
        )
        swings = detect_swings(bars)
        mss = detect_mss(bars, swings)
        assert mss["state"] == "bullish_mss"
        assert mss["fvg_confirmed"] is True


class TestGoldenPocket:
    _UPTREND_SWINGS: ClassVar[list[dict[str, Any]]] = [
        _swing(0, "low", 95.0),
        _swing(2, "high", 105.0),
        _swing(4, "low", 100.0),
        _swing(6, "high", 110.0),
    ]
    _DOWNTREND_SWINGS: ClassVar[list[dict[str, Any]]] = [
        _swing(0, "high", 115.0),
        _swing(2, "low", 105.0),
        _swing(4, "high", 110.0),
        _swing(6, "low", 100.0),
    ]

    def test_touch_at_level_is_in_pocket(self) -> None:
        pocket = golden_pocket(self._UPTREND_SWINGS, 103.82)
        assert pocket["direction"] == "long"
        assert pocket["level"] == 103.82
        assert pocket["in_pocket"] is True

    def test_half_percent_below_is_in_pocket(self) -> None:
        pocket = golden_pocket(self._UPTREND_SWINGS, 103.82 * 0.995)
        assert pocket["in_pocket"] is True

    def test_deeper_than_tolerance_is_out(self) -> None:
        pocket = golden_pocket(self._UPTREND_SWINGS, 103.82 * 0.994)
        assert pocket["in_pocket"] is False

    def test_above_level_is_out(self) -> None:
        pocket = golden_pocket(self._UPTREND_SWINGS, 105.0)
        assert pocket["in_pocket"] is False

    def test_short_side_mirror(self) -> None:
        pocket = golden_pocket(self._DOWNTREND_SWINGS, 106.18)
        assert pocket["direction"] == "short"
        assert pocket["level"] == 106.18
        assert pocket["in_pocket"] is True
        assert golden_pocket(self._DOWNTREND_SWINGS, 106.18 * 1.005)["in_pocket"] is True
        assert golden_pocket(self._DOWNTREND_SWINGS, 106.18 * 1.006)["in_pocket"] is False


class TestLiquiditySweepTrigger:
    def test_long_trigger(self) -> None:
        bars = _hourly(
            [
                (100.0, 101.0, 100.0, 100.5),
                (100.5, 101.0, 99.0, 100.8),
            ]
        )
        trigger = liquidity_sweep_trigger(bars)
        assert trigger["side"] == "long"
        assert trigger["stop_level"] == 99.0

    def test_short_trigger(self) -> None:
        bars = _hourly(
            [
                (110.0, 110.5, 109.0, 110.2),
                (110.3, 111.0, 109.5, 109.6),
            ]
        )
        trigger = liquidity_sweep_trigger(bars)
        assert trigger["side"] == "short"
        assert trigger["stop_level"] == 111.0

    def test_doji_fires_neither_side(self) -> None:
        bars = _hourly(
            [
                (100.0, 101.0, 100.0, 100.5),
                (100.0, 101.0, 99.0, 100.0),
            ]
        )
        assert liquidity_sweep_trigger(bars)["side"] == "none"

    def test_no_sweep_no_trigger(self) -> None:
        bars = _hourly(
            [
                (100.0, 101.0, 99.0, 100.5),
                (100.5, 101.5, 99.5, 101.0),
            ]
        )
        assert liquidity_sweep_trigger(bars)["side"] == "none"

    def test_confluence_flags(self) -> None:
        bars = _hourly(
            [
                (100.0, 101.0, 100.0, 100.5),
                (100.5, 101.0, 99.0, 100.8),
            ]
        )
        trigger = liquidity_sweep_trigger(
            bars,
            pocket={"direction": "long", "level": 99.5, "in_pocket": True},
            fvg_zones=[{"side": "bullish", "top": 99.6, "bottom": 99.2}],
        )
        assert trigger["in_golden_pocket"] is True
        assert trigger["in_fvg_zone"] is True

    def test_confluence_requires_matching_direction(self) -> None:
        bars = _hourly(
            [
                (100.0, 101.0, 100.0, 100.5),
                (100.5, 101.0, 99.0, 100.8),
            ]
        )
        trigger = liquidity_sweep_trigger(
            bars,
            pocket={"direction": "short", "level": 99.5, "in_pocket": True},
            fvg_zones=[{"side": "bearish", "top": 99.6, "bottom": 99.2}],
        )
        assert trigger["in_golden_pocket"] is False
        assert trigger["in_fvg_zone"] is False


def _range_rows(count: int) -> list[tuple[float, float, float, float]]:
    return [(100.0, 101.0, 99.0, 100.0)] * count


class TestWyckoffState:
    def test_trading_range_detected(self) -> None:
        bars = _hourly(_range_rows(30))
        state = wyckoff_state(bars)
        assert state["phase"] == "range"
        assert state["range_low"] == 99.0
        assert state["range_high"] == 101.0

    def test_spring_detected(self) -> None:
        rows = _range_rows(29) + [(99.5, 100.0, 97.0, 99.5)]
        state = wyckoff_state(_hourly(rows))
        assert state["phase"] == "spring"

    def test_lps_retest_after_spring(self) -> None:
        rows = _range_rows(25) + [(99.5, 100.0, 97.0, 99.5)] + _range_rows(3) + [
            (100.0, 100.5, 99.0, 100.2)
        ]
        state = wyckoff_state(_hourly(rows))
        assert state["phase"] == "lps"

    def test_trending_series_is_none(self) -> None:
        rows = [
            (100.0 + step, 101.0 + step, 99.0 + step, 100.5 + step)
            for step in range(0, 60, 2)
        ]
        assert wyckoff_state(_hourly(rows))["phase"] == "none"

    def test_insufficient_bars(self) -> None:
        assert wyckoff_state(_hourly(_range_rows(20)))["phase"] == "none"


class TestAggregate4hBars:
    def test_session_anchored_buckets(self) -> None:
        day_one = _hourly([(100.0 + i, 101.0 + i, 99.0 + i, 100.5 + i) for i in range(7)])
        day_two = _hourly(
            [(200.0 + i, 201.0 + i, 199.0 + i, 200.5 + i) for i in range(7)],
            start=_SESSION_OPEN + timedelta(days=1),
        )
        aggregated = aggregate_4h_bars([*day_one, *day_two])
        assert len(aggregated) == 4
        first = aggregated[0]
        assert first["timestamp"] == day_one[0]["timestamp"]
        assert first["open"] == 100.0
        assert first["high"] == 104.0
        assert first["low"] == 99.0
        assert first["close"] == 103.5
        assert first["volume"] == 4_000.0
        partial = aggregated[1]
        assert partial["timestamp"] == day_one[4]["timestamp"]
        assert partial["close"] == 106.5
        assert aggregated[2]["timestamp"] == day_two[0]["timestamp"]

    def test_deterministic(self) -> None:
        bars = _hourly([(100.0, 101.0, 99.0, 100.5)] * 9)
        assert aggregate_4h_bars(bars) == aggregate_4h_bars(bars)


class TestBuildSymbolTechnicals:
    def test_insufficient_history_returns_none(self) -> None:
        bars = _hourly([(100.0, 101.0, 99.0, 100.5)] * (MIN_TRIGGER_BARS - 1))
        assert build_symbol_technicals(bars) is None

    def test_payload_shape_and_budget(self) -> None:
        rows = [
            (
                100.0 + i * 0.1,
                101.0 + i * 0.1,
                99.0 + i * 0.1,
                100.5 + i * 0.1,
            )
            for i in range(60)
        ]
        payload = build_symbol_technicals(_hourly(rows))
        assert payload is not None
        assert set(payload) == {
            "mss_state",
            "mss_level",
            "mss_fvg_confirmed",
            "fvg_zones",
            "golden_pocket",
            "reversal_trigger",
            "wyckoff_phase",
            "confluence_score",
            "confluence_factors",
        }
        assert 0.0 <= payload["confluence_score"] <= 1.0
        assert len(json.dumps(payload).encode("utf-8")) < 900

    def test_confluence_score_matches_reported_factors(self) -> None:
        weights = {
            "mss_aligned": CONFLUENCE_WEIGHT_MSS,
            "trigger_in_golden_pocket": CONFLUENCE_WEIGHT_GOLDEN_POCKET,
            "trigger_in_fvg_zone": CONFLUENCE_WEIGHT_FVG,
            "wyckoff_accumulation": CONFLUENCE_WEIGHT_WYCKOFF,
        }
        for seed in range(5):
            rows = [
                (
                    100.0 + (i % 7) * (0.3 + seed * 0.1),
                    101.0 + (i % 5) * 0.4,
                    99.0 - (i % 3) * 0.4,
                    100.0 + ((i + seed) % 6) * 0.3,
                )
                for i in range(64)
            ]
            payload = build_symbol_technicals(_hourly(rows))
            assert payload is not None
            expected = round(
                min(sum(weights[factor] for factor in payload["confluence_factors"]), 1.0),
                4,
            )
            assert payload["confluence_score"] == expected

    def test_no_trigger_means_zero_confluence(self) -> None:
        rows = [(100.0, 101.0, 99.0, 100.5)] * 60
        payload = build_symbol_technicals(_hourly(rows))
        assert payload is not None
        assert payload["reversal_trigger"]["side"] == "none"
        assert payload["confluence_score"] == 0.0
        assert payload["confluence_factors"] == []
