"""Deterministic ICT/Wyckoff/Fibonacci structure detectors for the LLM equity dry-run.

Advisory-only: the outputs are packed into the LLM context as evidence and
per-candidate hints; they never gate or size trades directly. Structure
(swings, FVG, MSS, golden pocket, Wyckoff) is analyzed on locally aggregated
4-hour bars and the liquidity-sweep entry trigger on 1-hour bars.

4-hour aggregation is session-anchored: bars are grouped per New York trading
date into buckets of four consecutive 1-hour bars starting at the first bar of
the session (09:30 ET), so an RTH day yields a 09:30-13:30 bucket plus a
partial trailing bucket. This differs from midnight-anchored 4h charts but is
deterministic and self-consistent across detectors.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Final
from zoneinfo import ZoneInfo

MIN_TRIGGER_BARS: Final = 40
SWING_PIVOT_LEFT: Final = 2
SWING_PIVOT_RIGHT: Final = 2
MAX_FVG_ZONES: Final = 3
GOLDEN_POCKET_RATIO: Final = 0.618
GOLDEN_POCKET_TOLERANCE: Final = 0.005
WYCKOFF_MIN_RANGE_BARS: Final = 20
WYCKOFF_RANGE_BAND_PCT: Final = 0.03
WYCKOFF_SPRING_LOOKBACK_BARS: Final = 3
WYCKOFF_LPS_LOOKBACK_BARS: Final = 10
WYCKOFF_LPS_SUPPORT_TOLERANCE: Final = 0.01
CONFLUENCE_WEIGHT_MSS: Final = 0.35
CONFLUENCE_WEIGHT_GOLDEN_POCKET: Final = 0.25
CONFLUENCE_WEIGHT_FVG: Final = 0.20
CONFLUENCE_WEIGHT_WYCKOFF: Final = 0.20

_NY_ZONE: Final = ZoneInfo("America/New_York")

Bar = Mapping[str, Any]


def aggregate_4h_bars(bars_1h: Sequence[Bar]) -> list[dict[str, Any]]:
    """Aggregate ascending 1-hour bars into session-anchored 4-hour bars.

    The trailing bucket of a session may be partial; it is included so the
    most recent (still forming) structure candle is visible to detectors.
    """
    aggregated: list[dict[str, Any]] = []
    bucket: list[tuple[datetime, Bar]] = []
    bucket_date = None
    for bar in bars_1h:
        timestamp = _parse_timestamp(bar.get("timestamp"))
        if timestamp is None or not _bar_is_numeric(bar):
            continue
        trading_date = timestamp.astimezone(_NY_ZONE).date()
        if bucket and (trading_date != bucket_date or len(bucket) >= 4):
            aggregated.append(_merge_bucket(bucket))
            bucket = []
        bucket_date = trading_date
        bucket.append((timestamp, bar))
    if bucket:
        aggregated.append(_merge_bucket(bucket))
    return aggregated


def detect_swings(
    bars: Sequence[Bar],
    *,
    left: int = SWING_PIVOT_LEFT,
    right: int = SWING_PIVOT_RIGHT,
) -> list[dict[str, Any]]:
    """Detect pivot swing highs/lows. Strict comparisons: plateaus never pivot."""
    swings: list[dict[str, Any]] = []
    for index in range(left, len(bars) - right):
        high = _bar_float(bars[index], "high")
        low = _bar_float(bars[index], "low")
        if high is None or low is None:
            continue
        neighbors = [*range(index - left, index), *range(index + 1, index + right + 1)]
        neighbor_highs = [_bar_float(bars[i], "high") for i in neighbors]
        neighbor_lows = [_bar_float(bars[i], "low") for i in neighbors]
        if all(value is not None and high > value for value in neighbor_highs):
            swings.append(
                {
                    "index": index,
                    "kind": "high",
                    "price": round(high, 6),
                    "timestamp": str(bars[index].get("timestamp", "")),
                }
            )
        if all(value is not None and low < value for value in neighbor_lows):
            swings.append(
                {
                    "index": index,
                    "kind": "low",
                    "price": round(low, 6),
                    "timestamp": str(bars[index].get("timestamp", "")),
                }
            )
    return swings


def detect_fvgs(
    bars: Sequence[Bar],
    *,
    current_price: float,
    max_zones: int = MAX_FVG_ZONES,
) -> list[dict[str, Any]]:
    """Three-candle fair value gaps: bullish when high[1] < low[3], bearish mirrored.

    Only unfilled zones are returned (a zone is filled when a later bar trades
    fully through it), nearest to ``current_price`` first. ``mitigated`` marks
    zones a later wick has already entered.
    """
    zones: list[dict[str, Any]] = []
    for index in range(len(bars) - 2):
        first = bars[index]
        third = bars[index + 2]
        first_high = _bar_float(first, "high")
        first_low = _bar_float(first, "low")
        third_high = _bar_float(third, "high")
        third_low = _bar_float(third, "low")
        if None in (first_high, first_low, third_high, third_low):
            continue
        assert first_high is not None and first_low is not None
        assert third_high is not None and third_low is not None
        if first_high < third_low:
            side = "bullish"
            top = third_low
            bottom = first_high
        elif first_low > third_high:
            side = "bearish"
            top = first_low
            bottom = third_high
        else:
            continue
        filled = False
        mitigated = False
        for later in bars[index + 3 :]:
            later_high = _bar_float(later, "high")
            later_low = _bar_float(later, "low")
            if later_high is None or later_low is None:
                continue
            if later_low <= top and later_high >= bottom:
                mitigated = True
            if side == "bullish" and later_low <= bottom:
                filled = True
                break
            if side == "bearish" and later_high >= top:
                filled = True
                break
        if filled:
            continue
        midpoint = (top + bottom) / 2.0
        zones.append(
            {
                "side": side,
                "top": round(top, 6),
                "bottom": round(bottom, 6),
                "start_index": index,
                "mitigated": mitigated,
                "distance": abs(current_price - midpoint),
            }
        )
    zones.sort(key=lambda item: (float(item["distance"]), int(item["start_index"])))
    for zone in zones:
        zone.pop("distance", None)
    return zones[: max(max_zones, 0)]


def detect_mss(bars: Sequence[Bar], swings: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Market structure shift: a candle BODY closing beyond the last swing extreme.

    In a downtrend the tracked extreme is the last swing high (bullish MSS on a
    close above it); in an uptrend the last swing low (bearish MSS on a close
    below it). A wick beyond the level without a body close is not an MSS.
    ``fvg_confirmed`` is set when a same-direction FVG forms within the three
    candles following the confirming close.
    """
    result: dict[str, Any] = {
        "state": "none",
        "level": None,
        "confirmed_at": None,
        "fvg_confirmed": False,
    }
    trend = _trend_from_swings(swings)
    if trend == "none":
        return result
    kind = "high" if trend == "down" else "low"
    extremes = [swing for swing in swings if swing.get("kind") == kind]
    if not extremes:
        return result
    extreme = extremes[-1]
    level = float(extreme["price"])
    break_index: int | None = None
    for index in range(int(extreme["index"]) + 1, len(bars)):
        close = _bar_float(bars[index], "close")
        if close is None:
            continue
        if (trend == "down" and close > level) or (trend == "up" and close < level):
            break_index = index
            break
    if break_index is None:
        return result
    state = "bullish_mss" if trend == "down" else "bearish_mss"
    fvg_side = "bullish" if state == "bullish_mss" else "bearish"
    fvg_confirmed = False
    close_price = _bar_float(bars[-1], "close") or level
    for zone in detect_fvgs(bars, current_price=close_price, max_zones=len(bars)):
        start_index = int(zone["start_index"])
        # start_index cannot exceed len(bars) - 3 (detect_fvgs iterates
        # range(len(bars) - 2)), so the +3 upper bound stays in range.
        if zone["side"] == fvg_side and break_index - 2 <= start_index <= break_index + 3:
            fvg_confirmed = True
            break
    result.update(
        {
            "state": state,
            "level": round(level, 6),
            "confirmed_at": str(bars[break_index].get("timestamp", "")),
            "fvg_confirmed": fvg_confirmed,
        }
    )
    return result


def golden_pocket(
    swings: Sequence[Mapping[str, Any]],
    current_price: float,
    *,
    tolerance: float = GOLDEN_POCKET_TOLERANCE,
) -> dict[str, Any]:
    """Fibonacci 0.618 retracement of the last impulse wave.

    Long side (uptrend impulse low->high): the pocket is hit when price touches
    the 0.618 level or sits up to ``tolerance`` (0.5%) below it. Short side is
    mirrored (level touched or up to 0.5% above).
    """
    result: dict[str, Any] = {"direction": "none", "level": None, "in_pocket": False}
    trend = _trend_from_swings(swings)
    highs = [swing for swing in swings if swing.get("kind") == "high"]
    lows = [swing for swing in swings if swing.get("kind") == "low"]
    if not highs or not lows:
        return result
    if trend == "up":
        swing_high = highs[-1]
        candidates = [low for low in lows if int(low["index"]) < int(swing_high["index"])]
        if not candidates:
            return result
        swing_low = candidates[-1]
        span = float(swing_high["price"]) - float(swing_low["price"])
        if span <= 0.0:
            return result
        level = float(swing_high["price"]) - (GOLDEN_POCKET_RATIO * span)
        in_pocket = level * (1.0 - tolerance) <= current_price <= level
        result.update({"direction": "long", "level": round(level, 6), "in_pocket": in_pocket})
        return result
    if trend == "down":
        swing_low = lows[-1]
        candidates = [high for high in highs if int(high["index"]) < int(swing_low["index"])]
        if not candidates:
            return result
        swing_high = candidates[-1]
        span = float(swing_high["price"]) - float(swing_low["price"])
        if span <= 0.0:
            return result
        level = float(swing_low["price"]) + (GOLDEN_POCKET_RATIO * span)
        in_pocket = level <= current_price <= level * (1.0 + tolerance)
        result.update({"direction": "short", "level": round(level, 6), "in_pocket": in_pocket})
        return result
    return result


def liquidity_sweep_trigger(
    bars_1h: Sequence[Bar],
    *,
    pocket: Mapping[str, Any] | None = None,
    fvg_zones: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Liquidity-sweep reversal candle on the last two closed 1-hour bars.

    Long: low[0] < low[1] and close[0] > open[0] (sweep of the prior low that
    closes green); stop below the trigger-candle low. Short is mirrored. A doji
    (close == open) fires neither side.
    """
    result: dict[str, Any] = {
        "side": "none",
        "stop_level": None,
        "in_golden_pocket": False,
        "in_fvg_zone": False,
    }
    if len(bars_1h) < 2:
        return result
    current = bars_1h[-1]
    previous = bars_1h[-2]
    current_low = _bar_float(current, "low")
    current_high = _bar_float(current, "high")
    current_open = _bar_float(current, "open")
    current_close = _bar_float(current, "close")
    previous_low = _bar_float(previous, "low")
    previous_high = _bar_float(previous, "high")
    if None in (current_low, current_high, current_open, current_close, previous_low, previous_high):
        return result
    assert current_low is not None and current_high is not None
    assert current_open is not None and current_close is not None
    assert previous_low is not None and previous_high is not None
    side = "none"
    stop_level: float | None = None
    if current_low < previous_low and current_close > current_open:
        side = "long"
        stop_level = current_low
    elif current_high > previous_high and current_close < current_open:
        side = "short"
        stop_level = current_high
    if side == "none":
        return result
    in_golden_pocket = False
    if isinstance(pocket, Mapping) and pocket.get("level") is not None:
        level = float(pocket["level"])
        direction = str(pocket.get("direction", "none"))
        band_low = level * (1.0 - GOLDEN_POCKET_TOLERANCE)
        band_high = level * (1.0 + GOLDEN_POCKET_TOLERANCE)
        in_golden_pocket = (
            direction == side and current_low <= band_high and current_high >= band_low
        )
    in_fvg_zone = False
    wanted_side = "bullish" if side == "long" else "bearish"
    for zone in fvg_zones or ():
        if str(zone.get("side")) != wanted_side:
            continue
        top = zone.get("top")
        bottom = zone.get("bottom")
        if not isinstance(top, int | float) or not isinstance(bottom, int | float):
            continue
        if current_low <= float(top) and current_high >= float(bottom):
            in_fvg_zone = True
            break
    result.update(
        {
            "side": side,
            "stop_level": round(stop_level, 6) if stop_level is not None else None,
            "in_golden_pocket": in_golden_pocket,
            "in_fvg_zone": in_fvg_zone,
        }
    )
    return result


def wyckoff_state(
    bars_4h: Sequence[Bar],
    *,
    min_range_bars: int = WYCKOFF_MIN_RANGE_BARS,
    range_band_pct: float = WYCKOFF_RANGE_BAND_PCT,
) -> dict[str, Any]:
    """Wyckoff accumulation heuristic: trading range, spring, and LPS retest.

    A trading range exists when the ``min_range_bars`` bars preceding the last
    ``WYCKOFF_LPS_LOOKBACK_BARS`` stay within ``range_band_pct`` of their
    midpoint. A spring is a break below that range support that closes back
    inside within two bars; a spring within the last three bars reports phase
    ``spring``, while an older spring followed by the last bar retesting
    support from above reports ``lps``.
    """
    result: dict[str, Any] = {"phase": "none", "range_low": None, "range_high": None}
    if len(bars_4h) < min_range_bars + WYCKOFF_LPS_LOOKBACK_BARS:
        return result
    window = bars_4h[-(min_range_bars + WYCKOFF_LPS_LOOKBACK_BARS) : -WYCKOFF_LPS_LOOKBACK_BARS]
    highs = [value for bar in window if (value := _bar_float(bar, "high")) is not None]
    lows = [value for bar in window if (value := _bar_float(bar, "low")) is not None]
    if len(highs) < min_range_bars or len(lows) < min_range_bars:
        return result
    resistance = max(highs)
    support = min(lows)
    midpoint = (resistance + support) / 2.0
    if midpoint <= 0.0 or (resistance - support) / midpoint > range_band_pct * 2.0:
        return result
    result.update({"phase": "range", "range_low": round(support, 6), "range_high": round(resistance, 6)})
    tail = bars_4h[-WYCKOFF_LPS_LOOKBACK_BARS:]
    spring_offset: int | None = None
    for offset, bar in enumerate(tail):
        low = _bar_float(bar, "low")
        if low is None or low >= support:
            continue
        for follow in tail[offset : offset + WYCKOFF_SPRING_LOOKBACK_BARS]:
            close = _bar_float(follow, "close")
            if close is not None and close > support:
                spring_offset = offset
                break
    if spring_offset is None:
        return result
    if spring_offset >= len(tail) - WYCKOFF_SPRING_LOOKBACK_BARS:
        result["phase"] = "spring"
        return result
    last = tail[-1]
    last_low = _bar_float(last, "low")
    last_close = _bar_float(last, "close")
    if (
        last_low is not None
        and last_close is not None
        and last_low >= support
        and last_low <= support * (1.0 + WYCKOFF_LPS_SUPPORT_TOLERANCE)
        and last_close > support
    ):
        result["phase"] = "lps"
        return result
    result["phase"] = "spring"
    return result


def build_symbol_technicals(bars_1h: Sequence[Bar], *, as_of: str | None = None) -> dict[str, Any] | None:
    """Compact advisory technicals payload for one symbol, or None when there
    is not enough 1-hour history (< MIN_TRIGGER_BARS closed bars)."""
    del as_of
    ordered = _sorted_bars(bars_1h)
    if len(ordered) < MIN_TRIGGER_BARS:
        return None
    current_price = _bar_float(ordered[-1], "close")
    if current_price is None or current_price <= 0.0:
        return None
    bars_4h = aggregate_4h_bars(ordered)
    swings = detect_swings(bars_4h)
    fvg_zones = detect_fvgs(bars_4h, current_price=current_price)
    mss = detect_mss(bars_4h, swings)
    pocket = golden_pocket(swings, current_price)
    trigger = liquidity_sweep_trigger(ordered, pocket=pocket, fvg_zones=fvg_zones)
    wyckoff = wyckoff_state(bars_4h)
    score = 0.0
    factors: list[str] = []
    side = str(trigger["side"])
    if side != "none":
        mss_aligned = (side == "long" and mss["state"] == "bullish_mss") or (
            side == "short" and mss["state"] == "bearish_mss"
        )
        if mss_aligned:
            score += CONFLUENCE_WEIGHT_MSS
            factors.append("mss_aligned")
        if bool(trigger["in_golden_pocket"]):
            score += CONFLUENCE_WEIGHT_GOLDEN_POCKET
            factors.append("trigger_in_golden_pocket")
        if bool(trigger["in_fvg_zone"]):
            score += CONFLUENCE_WEIGHT_FVG
            factors.append("trigger_in_fvg_zone")
        if side == "long" and str(wyckoff["phase"]) in {"spring", "lps"}:
            score += CONFLUENCE_WEIGHT_WYCKOFF
            factors.append("wyckoff_accumulation")
    return {
        "mss_state": str(mss["state"]),
        "mss_level": mss["level"],
        "mss_fvg_confirmed": bool(mss["fvg_confirmed"]),
        "fvg_zones": [
            {
                "side": str(zone["side"]),
                "top": float(zone["top"]),
                "bottom": float(zone["bottom"]),
                "mitigated": bool(zone["mitigated"]),
            }
            for zone in fvg_zones
        ],
        "golden_pocket": {
            "direction": str(pocket["direction"]),
            "level": pocket["level"],
            "in_pocket": bool(pocket["in_pocket"]),
        },
        "reversal_trigger": {
            "side": side,
            "stop_level": trigger["stop_level"],
            "in_golden_pocket": bool(trigger["in_golden_pocket"]),
            "in_fvg_zone": bool(trigger["in_fvg_zone"]),
        },
        "wyckoff_phase": str(wyckoff["phase"]),
        "confluence_score": round(min(max(score, 0.0), 1.0), 4),
        "confluence_factors": factors,
    }


def _trend_from_swings(swings: Sequence[Mapping[str, Any]]) -> str:
    highs = [swing for swing in swings if swing.get("kind") == "high"]
    lows = [swing for swing in swings if swing.get("kind") == "low"]
    if len(highs) >= 2 and len(lows) >= 2:
        if float(highs[-1]["price"]) < float(highs[-2]["price"]) and float(
            lows[-1]["price"]
        ) < float(lows[-2]["price"]):
            return "down"
        if float(highs[-1]["price"]) > float(highs[-2]["price"]) and float(
            lows[-1]["price"]
        ) > float(lows[-2]["price"]):
            return "up"
    if highs and lows:
        return "up" if int(highs[-1]["index"]) > int(lows[-1]["index"]) else "down"
    return "none"


def _merge_bucket(bucket: Sequence[tuple[datetime, Bar]]) -> dict[str, Any]:
    bars = [bar for _, bar in bucket]
    return {
        "timestamp": str(bars[0].get("timestamp", "")),
        "open": float(bars[0]["open"]),
        "high": max(float(bar["high"]) for bar in bars),
        "low": min(float(bar["low"]) for bar in bars),
        "close": float(bars[-1]["close"]),
        "volume": sum(float(bar.get("volume", 0.0)) for bar in bars),
        "source": str(bars[0].get("source", "")),
    }


def _sorted_bars(bars: Sequence[Bar]) -> list[Bar]:
    dated: list[tuple[datetime, Bar]] = []
    for bar in bars:
        timestamp = _parse_timestamp(bar.get("timestamp"))
        if timestamp is not None and _bar_is_numeric(bar):
            dated.append((timestamp, bar))
    dated.sort(key=lambda item: item[0])
    return [bar for _, bar in dated]


def _bar_is_numeric(bar: Bar) -> bool:
    return all(_bar_float(bar, key) is not None for key in ("open", "high", "low", "close"))


def _bar_float(bar: Bar, key: str) -> float | None:
    value = bar.get(key)
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    if not math.isfinite(parsed):
        return None
    return parsed


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)
