"""Point-in-time research ledger and deterministic cost-aware equity replay."""

from __future__ import annotations

import hashlib
import json
import math
import os
import random
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import NormalDist
from typing import Any, Literal
from zoneinfo import ZoneInfo

from .contracts import LlmEquityContractError, normalize_symbol, reject_forbidden_fields

Horizon = Literal["30m", "60m", "close", "1d", "5d"]
Direction = Literal["long", "short"]
HORIZON_DELTA: Mapping[Horizon, timedelta] = {
    "30m": timedelta(minutes=30),
    "60m": timedelta(minutes=60),
    "close": timedelta(0),
    "1d": timedelta(days=1),
    "5d": timedelta(days=5),
}
NEW_YORK = ZoneInfo("America/New_York")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


class PointInTimeEventStore:
    """Append-only JSONL ledger with payload and chain hashes."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def append(
        self,
        event_type: str,
        observed_at: datetime,
        payload: Mapping[str, Any],
        *,
        event_key: str | None = None,
    ) -> dict[str, Any]:
        if not event_type.strip():
            raise ValueError("event_type is required")
        reject_forbidden_fields(payload)
        rows = self.read_all() if self.path.exists() else []
        previous = rows[-1]["event_hash"] if rows else None
        payload_hash = hashlib.sha256(_canonical_json(payload)).hexdigest()
        if event_key is not None:
            for row in rows:
                if row.get("event_key") == event_key:
                    if row["event_type"] != event_type or row["payload_hash"] != f"sha256:{payload_hash}":
                        raise ValueError("event idempotency key reused with different payload")
                    return row
        recorded_at = datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
        body = {
            "schema_version": 1,
            "event_type": event_type.strip()[:120],
            "observed_at": _utc(observed_at).isoformat().replace("+00:00", "Z"),
            "recorded_at": recorded_at,
            "payload_hash": f"sha256:{payload_hash}",
            "previous_hash": previous,
            "payload": dict(payload),
        }
        if event_key is not None:
            body["event_key"] = event_key
        body["event_hash"] = "sha256:" + hashlib.sha256(_canonical_json(body)).hexdigest()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(descriptor, _canonical_json(body) + b"\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return body

    def read_all(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        rows: list[dict[str, Any]] = []
        previous: str | None = None
        for line_number, line in enumerate(self.path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            parsed = json.loads(line)
            if not isinstance(parsed, dict):
                raise LlmEquityContractError(f"event row {line_number} must be an object")
            payload = parsed.get("payload")
            if not isinstance(payload, Mapping):
                raise LlmEquityContractError(f"event row {line_number} payload must be an object")
            expected_payload_hash = "sha256:" + hashlib.sha256(_canonical_json(payload)).hexdigest()
            if parsed.get("payload_hash") != expected_payload_hash:
                raise ValueError(f"event row {line_number} payload hash mismatch")
            if parsed.get("previous_hash") != previous:
                raise ValueError(f"event row {line_number} chain mismatch")
            claimed_hash = parsed.get("event_hash")
            hash_body = {key: value for key, value in parsed.items() if key != "event_hash"}
            expected_hash = "sha256:" + hashlib.sha256(_canonical_json(hash_body)).hexdigest()
            if claimed_hash != expected_hash:
                raise ValueError(f"event row {line_number} event hash mismatch")
            previous = str(claimed_hash)
            rows.append(parsed)
        return rows


@dataclass(frozen=True)
class ReplayObservation:
    as_of: datetime
    symbol: str
    bid: float
    ask: float
    session_open: float
    volume: float
    shortable: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "as_of", _utc(self.as_of))
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        if not all(
            math.isfinite(value) for value in (self.bid, self.ask, self.session_open, self.volume)
        ):
            raise ValueError("observation values must be finite")
        if self.bid <= 0 or self.ask < self.bid or self.session_open <= 0 or self.volume <= 0:
            raise ValueError("observation quote, open, and volume must be positive")

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0


@dataclass(frozen=True)
class ReplayIntent:
    decision_id: str
    as_of: datetime
    candidate_id: str
    symbol: str
    direction: Direction
    horizon: Horizon
    target_weight: float
    confidence: float
    model_cost_usd: float
    edge_class: str = "legacy_unclassified"
    expected_excess_return_bps: float | None = None
    probability_positive: float | None = None
    notional_usd: float | None = None
    entry_fill_price: float | None = None
    executed_quantity: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "as_of", _utc(self.as_of))
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        if not self.decision_id or not self.candidate_id:
            raise ValueError("decision_id and candidate_id are required")
        if self.direction not in {"long", "short"} or self.horizon not in HORIZON_DELTA:
            raise ValueError("unsupported replay direction or horizon")
        if not math.isfinite(self.target_weight) or abs(self.target_weight) > 0.95:
            raise ValueError("target_weight is invalid")
        if (self.direction == "long" and self.target_weight <= 0) or (
            self.direction == "short" and self.target_weight >= 0
        ):
            raise ValueError("target_weight sign must match direction")
        if not 0.0 <= self.confidence <= 1.0 or self.model_cost_usd < 0:
            raise ValueError("confidence or model cost is invalid")
        if self.edge_class not in {
            "positive_edge",
            "least_bad_forced",
            "risk_reduction",
            "legacy_unclassified",
        }:
            raise ValueError("edge class is invalid")
        if self.expected_excess_return_bps is not None and (
            not math.isfinite(self.expected_excess_return_bps)
            or not -5000.0 <= self.expected_excess_return_bps <= 5000.0
        ):
            raise ValueError("expected excess return is invalid")
        if self.probability_positive is not None and (
            not math.isfinite(self.probability_positive)
            or not 0.0 <= self.probability_positive <= 1.0
        ):
            raise ValueError("probability positive is invalid")
        for value, label in (
            (self.notional_usd, "notional"),
            (self.entry_fill_price, "entry fill price"),
            (self.executed_quantity, "executed quantity"),
        ):
            if value is not None and (not math.isfinite(value) or value <= 0.0):
                raise ValueError(f"{label} is invalid")


@dataclass(frozen=True)
class ReplayConfig:
    initial_cash_usd: float = 10_000.0
    max_single_name_weight: float = 0.15
    commission_bps: float = 0.5
    base_impact_bps: float = 2.0
    participation_impact_bps: float = 25.0
    borrow_apr: float = 0.05
    financing_apr: float = 0.03
    configured_fixed_cost_usd: float = 0.0
    max_entry_delay: timedelta = timedelta(minutes=1)
    max_intraday_exit_delay: timedelta = timedelta(minutes=40)
    max_daily_exit_delay: timedelta = timedelta(days=3)


@dataclass(frozen=True)
class ReplayTrade:
    decision_id: str
    candidate_id: str
    symbol: str
    direction: Direction
    horizon: Horizon
    edge_class: str
    entry_at: datetime
    exit_at: datetime
    notional_usd: float
    entry_mid: float
    exit_mid: float
    entry_fill: float
    exit_fill: float
    exit_notional_usd: float
    gross_pnl_usd: float
    spread_cost_usd: float
    impact_cost_usd: float
    execution_cost_usd: float
    commission_usd: float
    borrow_cost_usd: float
    financing_cost_usd: float
    model_cost_usd: float
    net_pnl_usd: float


@dataclass(frozen=True)
class SleeveReport:
    horizon: Horizon
    trades: list[ReplayTrade]
    skipped_intents: int
    model_cost_usd: float
    skipped_model_cost_usd: float
    net_pnl_usd: float
    ending_nav_usd: float
    max_drawdown_pct: float
    win_rate: float
    trade_sharpe: float
    turnover_usd: float


@dataclass(frozen=True)
class ReplayReport:
    sleeves: dict[Horizon, SleeveReport]
    total_model_cost_usd: float
    unattributed_model_cost_usd: float
    configured_fixed_cost_usd: float
    total_turnover_usd: float
    total_net_pnl_usd: float
    ending_nav_usd: float
    max_drawdown_pct: float


@dataclass(frozen=True)
class ReplayInputs:
    observations: list[ReplayObservation]
    intents: list[ReplayIntent]
    unattributed_model_cost_usd: float
    total_imputed_api_cost_usd: float = 0.0
    unattributed_model_cost_events: tuple[tuple[datetime, float], ...] = ()
    imputed_api_cost_events: tuple[tuple[datetime, float], ...] = ()


@dataclass(frozen=True)
class WalkForwardSplit:
    train_indices: tuple[int, ...]
    test_indices: tuple[int, ...]


_DEFAULT_REPLAY_CONFIG = ReplayConfig()


def run_replay(
    observations: Sequence[ReplayObservation],
    intents: Sequence[ReplayIntent],
    *,
    config: ReplayConfig = _DEFAULT_REPLAY_CONFIG,
    unattributed_model_cost_usd: float = 0.0,
) -> ReplayReport:
    if config.initial_cash_usd <= 0 or not 0 < config.max_single_name_weight <= 1:
        raise ValueError("replay capital and risk configuration are invalid")
    if min(
        config.max_entry_delay,
        config.max_intraday_exit_delay,
        config.max_daily_exit_delay,
    ) < timedelta(0):
        raise ValueError("replay timing configuration is invalid")
    if not math.isfinite(unattributed_model_cost_usd) or unattributed_model_cost_usd < 0.0:
        raise ValueError("unattributed model cost is invalid")
    if (
        not math.isfinite(config.configured_fixed_cost_usd)
        or config.configured_fixed_cost_usd < 0.0
    ):
        raise ValueError("configured fixed cost is invalid")
    by_symbol: dict[str, list[ReplayObservation]] = defaultdict(list)
    for observation in observations:
        by_symbol[observation.symbol].append(observation)
    for rows in by_symbol.values():
        rows.sort(key=lambda item: item.as_of)
    decision_counts: dict[str, int] = defaultdict(int)
    decision_costs: dict[str, float] = {}
    for intent in intents:
        decision_counts[intent.decision_id] += 1
        known_cost = decision_costs.setdefault(intent.decision_id, intent.model_cost_usd)
        if not math.isclose(known_cost, intent.model_cost_usd, rel_tol=1e-9, abs_tol=1e-9):
            raise ValueError("model cost must be consistent for every intent in a decision")
    grouped: dict[Horizon, list[ReplayTrade]] = defaultdict(list)
    skipped: dict[Horizon, int] = defaultdict(int)
    allocated_model_costs: dict[Horizon, float] = defaultdict(float)
    skipped_model_costs: dict[Horizon, float] = defaultdict(float)
    cashflows: dict[Horizon, list[tuple[datetime, float]]] = defaultdict(list)
    consolidated_cashflows: list[tuple[datetime, float]] = []
    for intent in sorted(
        intents, key=lambda item: (item.as_of, item.decision_id, item.candidate_id)
    ):
        model_cost = decision_costs[intent.decision_id] / decision_counts[intent.decision_id]
        allocated_model_costs[intent.horizon] += model_cost
        cashflows[intent.horizon].append((intent.as_of, -model_cost))
        consolidated_cashflows.append((intent.as_of, -model_cost))
        rows = by_symbol.get(intent.symbol, [])
        entry = _entry_observation(rows, intent.as_of, config.max_entry_delay)
        exit_mark = (
            _exit_observation(rows, entry, intent.horizon, config) if entry is not None else None
        )
        if (
            entry is None
            or exit_mark is None
            or (intent.direction == "short" and not entry.shortable)
        ):
            skipped[intent.horizon] += 1
            skipped_model_costs[intent.horizon] += model_cost
            continue
        notional = (
            intent.notional_usd
            if intent.notional_usd is not None
            else config.initial_cash_usd
            * min(abs(intent.target_weight), config.max_single_name_weight)
        )
        participation = notional / max(entry.mid * entry.volume, 1.0)
        impact_bps = config.base_impact_bps + participation * config.participation_impact_bps
        impact = impact_bps / 10_000.0
        if intent.direction == "long":
            simulated_entry_fill = entry.ask * (1.0 + impact)
            exit_fill = exit_mark.bid * (1.0 - impact)
            entry_after_spread = entry.ask
            exit_after_spread = exit_mark.bid
            direction = 1.0
        else:
            simulated_entry_fill = entry.bid * (1.0 - impact)
            exit_fill = exit_mark.ask * (1.0 + impact)
            entry_after_spread = entry.bid
            exit_after_spread = exit_mark.ask
            direction = -1.0
        entry_fill = intent.entry_fill_price or simulated_entry_fill
        quantity = intent.executed_quantity or notional / entry_fill
        gross_pnl = direction * quantity * (exit_mark.mid - entry.mid)
        after_spread = direction * quantity * (exit_after_spread - entry_after_spread)
        after_execution = direction * quantity * (exit_fill - entry_fill)
        spread_cost = max(gross_pnl - after_spread, 0.0)
        impact_cost = max(after_spread - after_execution, 0.0)
        execution_cost = spread_cost + impact_cost
        exit_notional = quantity * exit_fill
        commission = (notional + exit_notional) * config.commission_bps / 10_000.0
        elapsed_days = max((exit_mark.as_of - entry.as_of).total_seconds(), 0.0) / 86_400.0
        borrow_cost = (
            notional * config.borrow_apr * elapsed_days / 365.0
            if intent.direction == "short"
            else 0.0
        )
        financing_cost = notional * config.financing_apr * elapsed_days / 365.0
        net_pnl = after_execution - commission - borrow_cost - financing_cost - model_cost
        cashflows[intent.horizon].append((exit_mark.as_of, net_pnl + model_cost))
        consolidated_cashflows.append((exit_mark.as_of, net_pnl + model_cost))
        grouped[intent.horizon].append(
            ReplayTrade(
                decision_id=intent.decision_id,
                candidate_id=intent.candidate_id,
                symbol=intent.symbol,
                direction=intent.direction,
                horizon=intent.horizon,
                edge_class=intent.edge_class,
                entry_at=entry.as_of,
                exit_at=exit_mark.as_of,
                notional_usd=notional,
                entry_mid=entry.mid,
                exit_mid=exit_mark.mid,
                entry_fill=entry_fill,
                exit_fill=exit_fill,
                exit_notional_usd=exit_notional,
                gross_pnl_usd=gross_pnl,
                spread_cost_usd=spread_cost,
                impact_cost_usd=impact_cost,
                execution_cost_usd=execution_cost,
                commission_usd=commission,
                borrow_cost_usd=borrow_cost,
                financing_cost_usd=financing_cost,
                model_cost_usd=model_cost,
                net_pnl_usd=net_pnl,
            )
        )
    sleeves: dict[Horizon, SleeveReport] = {}
    requested_horizons = {intent.horizon for intent in intents}
    for horizon in sorted(requested_horizons):
        trades = grouped[horizon]
        trade_pnls = [trade.net_pnl_usd for trade in trades]
        net_pnl = sum(trade_pnls) - skipped_model_costs[horizon]
        nav = config.initial_cash_usd
        peak = nav
        max_drawdown = 0.0
        for _timestamp, cashflow in sorted(cashflows[horizon], key=lambda item: item[0]):
            nav += cashflow
            peak = max(peak, nav)
            max_drawdown = max(max_drawdown, (peak - nav) / peak if peak > 0 else 0.0)
        win_rate = sum(pnl > 0 for pnl in trade_pnls) / len(trade_pnls) if trade_pnls else 0.0
        sleeves[horizon] = SleeveReport(
            horizon=horizon,
            trades=trades,
            skipped_intents=skipped[horizon],
            model_cost_usd=allocated_model_costs[horizon],
            skipped_model_cost_usd=skipped_model_costs[horizon],
            net_pnl_usd=net_pnl,
            ending_nav_usd=nav,
            max_drawdown_pct=max_drawdown,
            win_rate=win_rate,
            trade_sharpe=_trade_sharpe(trade_pnls, config.initial_cash_usd),
            turnover_usd=round(
                sum(trade.notional_usd + trade.exit_notional_usd for trade in trades),
                6,
            ),
        )
    sleeve_model_cost = sum(sleeve.model_cost_usd for sleeve in sleeves.values())
    total_net_pnl = (
        sum(sleeve.net_pnl_usd for sleeve in sleeves.values())
        - unattributed_model_cost_usd
        - config.configured_fixed_cost_usd
    )
    ending_nav, max_drawdown = _consolidated_nav_metrics(
        consolidated_cashflows,
        initial_cash_usd=config.initial_cash_usd,
        terminal_cost_usd=unattributed_model_cost_usd + config.configured_fixed_cost_usd,
    )
    return ReplayReport(
        sleeves=sleeves,
        total_model_cost_usd=sleeve_model_cost + unattributed_model_cost_usd,
        unattributed_model_cost_usd=unattributed_model_cost_usd,
        configured_fixed_cost_usd=config.configured_fixed_cost_usd,
        total_turnover_usd=sum(sleeve.turnover_usd for sleeve in sleeves.values()),
        total_net_pnl_usd=total_net_pnl,
        ending_nav_usd=ending_nav,
        max_drawdown_pct=max_drawdown,
    )


def _consolidated_nav_metrics(
    cashflows: Sequence[tuple[datetime, float]],
    *,
    initial_cash_usd: float,
    terminal_cost_usd: float,
) -> tuple[float, float]:
    nav = initial_cash_usd
    peak = nav
    max_drawdown = 0.0
    amounts = [amount for _timestamp, amount in sorted(cashflows, key=lambda item: item[0])]
    if terminal_cost_usd > 0.0:
        amounts.append(-terminal_cost_usd)
    for amount in amounts:
        nav += amount
        peak = max(peak, nav)
        max_drawdown = max(max_drawdown, (peak - nav) / peak if peak > 0.0 else 0.0)
    return nav, max_drawdown


def replay_inputs_from_event_store(store: PointInTimeEventStore) -> ReplayInputs:
    observations: list[ReplayObservation] = []
    intents: list[ReplayIntent] = []
    unattributed_events: list[tuple[datetime, float]] = []
    imputed_events: list[tuple[datetime, float]] = []
    deferred_costs: dict[str, tuple[float, datetime]] = {}
    for event in store.read_all():
        if event.get("event_type") != "replay_cycle":
            continue
        payload = event.get("payload")
        if not isinstance(payload, Mapping) or payload.get("schema_version") not in {1, 2, 3}:
            raise ValueError("replay cycle payload is invalid")
        decision_id = str(payload.get("decision_id", "")).strip()
        call_cost = _replay_float(payload, "model_call_cost_usd", minimum=0.0)
        imputed_cost = (
            _replay_float(payload, "imputed_api_cost_usd", minimum=0.0)
            if "imputed_api_cost_usd" in payload
            else call_cost
        )
        cost_at = _replay_datetime_value(
            payload.get("as_of") or event.get("observed_at"),
            "cost.as_of",
        )
        disposition = str(payload.get("cost_disposition", "direct"))
        if disposition not in {"direct", "deferred", "linked", "none"}:
            raise ValueError("replay cycle cost disposition is invalid")
        if disposition == "deferred":
            if not decision_id:
                raise ValueError("deferred replay cost requires decision_id")
            previous = deferred_costs.setdefault(decision_id, (call_cost, cost_at))
            if not math.isclose(previous[0], call_cost, rel_tol=1e-9, abs_tol=1e-9):
                raise ValueError("deferred replay cost is inconsistent")
        if disposition in {"direct", "deferred"} and bool(payload.get("llm_invoked")):
            imputed_events.append((cost_at, imputed_cost))

        observation_rows = payload.get("observations", ())
        if not isinstance(observation_rows, Sequence) or isinstance(
            observation_rows, str | bytes | bytearray
        ):
            raise LlmEquityContractError("replay observations must be an array")
        for row in observation_rows:
            if not isinstance(row, Mapping):
                raise LlmEquityContractError("replay observation must be an object")
            observations.append(
                ReplayObservation(
                    as_of=_replay_datetime(row, "as_of"),
                    symbol=str(row.get("symbol", "")),
                    bid=_replay_float(row, "bid", minimum=0.0, exclusive=True),
                    ask=_replay_float(row, "ask", minimum=0.0, exclusive=True),
                    session_open=_replay_float(row, "session_open", minimum=0.0, exclusive=True),
                    volume=_replay_float(row, "volume", minimum=0.0, exclusive=True),
                    shortable=bool(row.get("shortable", False)),
                )
            )

        intent_rows = payload.get("intents", ())
        if not isinstance(intent_rows, Sequence) or isinstance(
            intent_rows, str | bytes | bytearray
        ):
            raise LlmEquityContractError("replay intents must be an array")
        intent_cost = call_cost
        if disposition == "linked":
            if not decision_id or decision_id not in deferred_costs:
                raise ValueError("linked replay cost is missing its deferred preparation")
            intent_cost = deferred_costs.pop(decision_id)[0]
        if intent_rows and not decision_id:
            raise ValueError("replay intents require decision_id")
        for row in intent_rows:
            if not isinstance(row, Mapping):
                raise LlmEquityContractError("replay intent must be an object")
            intents.append(
                ReplayIntent(
                    decision_id=decision_id,
                    as_of=_replay_datetime_value(
                        row.get("as_of") or payload.get("as_of") or event.get("observed_at"),
                        "intent.as_of",
                    ),
                    candidate_id=str(row.get("candidate_id", "")),
                    symbol=str(row.get("symbol", "")),
                    direction=str(row.get("direction", "")),  # type: ignore[arg-type]
                    horizon=str(row.get("horizon", "")),  # type: ignore[arg-type]
                    target_weight=_replay_float(row, "target_weight"),
                    confidence=_replay_float(row, "confidence", minimum=0.0),
                    model_cost_usd=intent_cost,
                    edge_class=str(row.get("edge_class", "legacy_unclassified")),
                    expected_excess_return_bps=(
                        _replay_float(row, "expected_excess_return_bps")
                        if "expected_excess_return_bps" in row
                        else None
                    ),
                    probability_positive=(
                        _replay_float(row, "probability_positive", minimum=0.0)
                        if "probability_positive" in row
                        else None
                    ),
                    notional_usd=(
                        _replay_float(row, "notional_usd", minimum=0.0, exclusive=True)
                        if "notional_usd" in row
                        else None
                    ),
                    entry_fill_price=(
                        _replay_float(row, "entry_fill_price", minimum=0.0, exclusive=True)
                        if "entry_fill_price" in row
                        else None
                    ),
                    executed_quantity=(
                        _replay_float(row, "executed_quantity", minimum=0.0, exclusive=True)
                        if "executed_quantity" in row
                        else None
                    ),
                )
            )
        if disposition == "direct" and not intent_rows and bool(payload.get("llm_invoked")):
            unattributed_events.append((cost_at, call_cost))
    unattributed_events.extend((cost_at, cost) for cost, cost_at in deferred_costs.values())
    unattributed_model_cost = sum(cost for _cost_at, cost in unattributed_events)
    total_imputed_api_cost = sum(cost for _cost_at, cost in imputed_events)
    return ReplayInputs(
        observations=observations,
        intents=intents,
        unattributed_model_cost_usd=unattributed_model_cost,
        total_imputed_api_cost_usd=total_imputed_api_cost,
        unattributed_model_cost_events=tuple(unattributed_events),
        imputed_api_cost_events=tuple(imputed_events),
    )


def _replay_datetime(payload: Mapping[str, Any], key: str) -> datetime:
    return _replay_datetime_value(payload.get(key), key)


def _replay_datetime_value(value: object, key: str) -> datetime:
    raw = str(value or "")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"replay {key} is invalid") from exc
    return _utc(parsed)


def _replay_float(
    payload: Mapping[str, Any],
    key: str,
    *,
    minimum: float | None = None,
    exclusive: bool = False,
) -> float:
    value = payload.get(key)
    if not isinstance(value, int | float) or not math.isfinite(float(value)):
        raise ValueError(f"replay {key} is invalid")
    result = float(value)
    if minimum is not None and (result <= minimum if exclusive else result < minimum):
        raise ValueError(f"replay {key} is invalid")
    return result


def _entry_observation(
    rows: Sequence[ReplayObservation],
    as_of: datetime,
    max_delay: timedelta,
) -> ReplayObservation | None:
    for row in rows:
        if as_of <= row.as_of <= as_of + max_delay:
            return row
    return None


def _exit_observation(
    rows: Sequence[ReplayObservation],
    entry: ReplayObservation,
    horizon: Horizon,
    config: ReplayConfig,
) -> ReplayObservation | None:
    if horizon == "close":
        entry_local = entry.as_of.astimezone(NEW_YORK)
        session_close = entry_local.replace(hour=16, minute=0, second=0, microsecond=0)
        same_session = [
            row
            for row in rows
            if row.as_of > entry.as_of
            and (local := row.as_of.astimezone(NEW_YORK)).date() == entry_local.date()
            and local <= session_close
        ]
        return same_session[-1] if same_session else None
    target = entry.as_of + HORIZON_DELTA[horizon]
    max_delay = (
        config.max_intraday_exit_delay if horizon in {"30m", "60m"} else config.max_daily_exit_delay
    )
    return next((row for row in rows if target <= row.as_of <= target + max_delay), None)


def _trade_sharpe(pnls: Sequence[float], initial_cash: float) -> float:
    if len(pnls) < 2:
        return 0.0
    returns = [pnl / initial_cash for pnl in pnls]
    mean = sum(returns) / len(returns)
    variance = sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
    if variance <= 0.0:
        return 0.0
    return mean / math.sqrt(variance)


def purged_walk_forward_splits(
    *,
    sample_count: int,
    train_size: int,
    test_size: int,
    purge_size: int,
    embargo_size: int,
) -> Iterable[WalkForwardSplit]:
    if min(sample_count, train_size, test_size) <= 0 or min(purge_size, embargo_size) < 0:
        raise ValueError("walk-forward sizes are invalid")
    train_start = 0
    while True:
        train_end = train_start + train_size
        test_start = train_end + purge_size
        test_end = test_start + test_size
        if test_end > sample_count:
            return
        yield WalkForwardSplit(
            train_indices=tuple(range(train_start, train_end)),
            test_indices=tuple(range(test_start, test_end)),
        )
        train_start = test_end + embargo_size


def probabilistic_sharpe_ratio(
    returns: Sequence[float],
    *,
    benchmark_sharpe: float = 0.0,
) -> float:
    """Probability that the observed per-period Sharpe exceeds a benchmark."""
    if len(returns) < 3 or any(not math.isfinite(value) for value in returns):
        return 0.0
    count = len(returns)
    mean = sum(returns) / count
    variance = sum((value - mean) ** 2 for value in returns) / (count - 1)
    if variance <= 0.0:
        return 1.0 if mean > benchmark_sharpe else 0.0
    deviation = math.sqrt(variance)
    sharpe = mean / deviation
    centered = [(value - mean) / deviation for value in returns]
    skew = sum(value**3 for value in centered) / count
    kurtosis = sum(value**4 for value in centered) / count
    denominator = math.sqrt(max(1.0 - skew * sharpe + ((kurtosis - 1.0) / 4.0) * sharpe**2, 1e-12))
    z_score = (sharpe - benchmark_sharpe) * math.sqrt(count - 1) / denominator
    return min(max(NormalDist().cdf(z_score), 0.0), 1.0)


def deflated_sharpe_ratio(returns: Sequence[float], *, trial_count: int) -> float:
    if trial_count <= 0:
        raise ValueError("trial_count must be positive")
    if trial_count == 1:
        return probabilistic_sharpe_ratio(returns)
    if len(returns) < 3:
        return 0.0
    quantile = 1.0 - (1.0 / trial_count)
    expected_null_max = NormalDist().inv_cdf(quantile) / math.sqrt(len(returns))
    return probabilistic_sharpe_ratio(returns, benchmark_sharpe=expected_null_max)


def bootstrap_mean_confidence_interval(
    values: Sequence[float],
    *,
    confidence: float = 0.95,
    samples: int = 2_000,
    seed: int = 0,
) -> tuple[float, float]:
    if not values or not 0.0 < confidence < 1.0 or samples < 100:
        raise ValueError("bootstrap inputs are invalid")
    if any(not math.isfinite(value) for value in values):
        raise ValueError("bootstrap values must be finite")
    generator = random.Random(seed)
    means = sorted(
        sum(generator.choice(values) for _ in values) / len(values) for _ in range(samples)
    )
    tail = (1.0 - confidence) / 2.0
    lower_index = max(min(int(tail * samples), samples - 1), 0)
    upper_index = max(min(int((1.0 - tail) * samples) - 1, samples - 1), 0)
    return means[lower_index], means[upper_index]
