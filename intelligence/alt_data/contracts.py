"""Future-state alternative-data and multi-asset intent contracts.

These validators are scaffolding for migration phases. They do not submit
orders, fetch data, or change the active equity dry-run runtime.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any, Final, Literal, TypeAlias, cast

from alt_data.catalog import SOURCE_ID_RE, STORAGE_POLICIES
from alt_data.registry import RegisteredSource, load_source_registry

SCHEMA_VERSION: Final = 1
TEXT_LIMIT: Final = 1000
REF_RE: Final = re.compile(r"^[A-Za-z0-9:/_.-]{1,120}$")
SHA256_RE: Final = re.compile(r"^sha256:[a-f0-9]{64}$")
_SECRET_ASSIGNMENT_RE: Final = re.compile(
    r"(?i)\b(api[_-]?key|secret|token|password)\s*[:=]\s*['\"]?[A-Za-z0-9_./+=-]{12,}"
)
ZERO_WEIGHT_TOLERANCE: Final = 1e-9

RAW_FORBIDDEN_KEYS: Final = {
    "api_key",
    "article_text",
    "credentials",
    "full_text",
    "password",
    "prompt",
    "prompts",
    "raw_log",
    "raw_logs",
    "raw_prompt",
    "raw_prompts",
    "secret",
    "stderr",
    "stdout",
    "token",
}

FRESHNESS_VALUES: Final = {"fresh", "stale", "unavailable"}
HORIZON_VALUES: Final = {"intraday", "short", "medium", "long"}
DIRECTION_HINT_VALUES: Final = {"bullish", "bearish", "neutral", "risk_off", "risk_on"}
TRADE_DIRECTIONS: Final = {"long", "short", "flat"}
INTENT_RISK_VERDICTS: Final = {"pass", "review", "hold", "reject"}
ORDER_VERDICTS: Final = {"approved", "rejected", "hold"}
ORDER_SIDES: Final = {"buy", "sell", "sell_short", "buy_to_cover", "hold"}
TIME_IN_FORCE_VALUES: Final = {"day", "gtc", "ioc", "fok"}

RAW_OBSERVATION_KEYS: Final = {
    "schema_version",
    "observation_id",
    "source_id",
    "retrieved_at",
    "event_time",
    "payload_hash",
    "storage_uri",
    "parser_version",
    "freshness",
    "http_status",
    "entity_refs",
    "instrument_refs",
    "geo_refs",
    "quality_flags",
    "storage_policy",
}
NORMALIZED_SIGNAL_KEYS: Final = {
    "schema_version",
    "signal_id",
    "source_id",
    "observation_id",
    "as_of",
    "event_time",
    "asset_refs",
    "entity_refs",
    "region_refs",
    "signal_family",
    "horizon",
    "direction_hint",
    "confidence",
    "magnitude",
    "freshness",
    "summary",
    "evidence_refs",
    "quality_flags",
}
CONTEXT_PACK_KEYS: Final = {
    "schema_version",
    "context_id",
    "cycle_id",
    "as_of",
    "portfolio_state_ref",
    "risk_state_ref",
    "approved_instrument_ids",
    "signal_refs",
    "graph_refs",
    "historical_analog_refs",
    "unavailable_source_ids",
    "stale_source_ids",
    "risk_constraints",
}
TRADE_INTENT_KEYS: Final = {
    "schema_version",
    "cycle_id",
    "sequence",
    "as_of",
    "model_id",
    "prompt_template_hash",
    "commands",
    "risk_assessment",
    "memory_updates",
}
TRADE_COMMAND_KEYS: Final = {
    "instrument_id",
    "direction",
    "target_weight",
    "max_notional_usd",
    "horizon",
    "confidence",
    "thesis",
    "evidence_refs",
}
INTENT_RISK_KEYS: Final = {"summary", "uncertainty", "risk_limit_verdict"}
RISK_APPROVED_ORDER_KEYS: Final = {
    "schema_version",
    "approval_id",
    "cycle_id",
    "intent_sequence",
    "as_of",
    "verdict",
    "reason",
    "instrument_id",
    "side",
    "quantity",
    "notional_usd",
    "route",
    "time_in_force",
    "context_id",
    "intent_ref",
    "risk_snapshot_ref",
    "instrument_master_version",
}

Freshness: TypeAlias = Literal["fresh", "stale", "unavailable"]
DirectionHint: TypeAlias = Literal["bullish", "bearish", "neutral", "risk_off", "risk_on"]
OrderVerdict: TypeAlias = Literal["approved", "rejected", "hold"]


class AltDataContractError(ValueError):
    """Raised when a future-state alt-data contract fails closed."""


@dataclass(frozen=True)
class RawObservationEvent:
    schema_version: int
    observation_id: str
    source_id: str
    retrieved_at: datetime
    event_time: datetime
    payload_hash: str
    storage_uri: str
    parser_version: str
    freshness: Freshness
    http_status: int
    entity_refs: tuple[str, ...]
    instrument_refs: tuple[str, ...]
    geo_refs: tuple[str, ...]
    quality_flags: tuple[str, ...]
    storage_policy: str


@dataclass(frozen=True)
class NormalizedSignalEvent:
    schema_version: int
    signal_id: str
    source_id: str
    observation_id: str
    as_of: datetime
    event_time: datetime
    asset_refs: tuple[str, ...]
    entity_refs: tuple[str, ...]
    region_refs: tuple[str, ...]
    signal_family: str
    horizon: str
    direction_hint: DirectionHint
    confidence: float
    magnitude: float
    freshness: Freshness
    summary: str
    evidence_refs: tuple[str, ...]
    quality_flags: tuple[str, ...]


@dataclass(frozen=True)
class LlmContextPack:
    schema_version: int
    context_id: str
    cycle_id: str
    as_of: datetime
    portfolio_state_ref: str
    risk_state_ref: str
    approved_instrument_ids: tuple[str, ...]
    signal_refs: tuple[str, ...]
    graph_refs: tuple[str, ...]
    historical_analog_refs: tuple[str, ...]
    unavailable_source_ids: tuple[str, ...]
    stale_source_ids: tuple[str, ...]
    risk_constraints: Mapping[str, float]


@dataclass(frozen=True)
class LlmTradeIntent:
    schema_version: int
    cycle_id: str
    sequence: int
    as_of: datetime
    model_id: str
    prompt_template_hash: str
    commands: tuple[Mapping[str, object], ...]
    risk_assessment: Mapping[str, object]
    memory_updates: tuple[str, ...]


@dataclass(frozen=True)
class RiskApprovedOrder:
    schema_version: int
    approval_id: str
    cycle_id: str
    intent_sequence: int
    as_of: datetime
    verdict: OrderVerdict
    reason: str
    instrument_id: str
    side: str
    quantity: float
    notional_usd: float
    route: str
    time_in_force: str
    context_id: str
    intent_ref: str
    risk_snapshot_ref: str
    instrument_master_version: str


def validate_raw_observation_event(payload: Mapping[str, object]) -> RawObservationEvent:
    _reject_unknown_or_forbidden(payload, RAW_OBSERVATION_KEYS, "raw observation")
    _require_schema_version(payload)
    source_id = _source_id(payload, "source_id")
    source = _require_known_source(source_id)
    storage_policy = _enum(payload, "storage_policy", STORAGE_POLICIES)
    if storage_policy != source.storage_policy:
        raise AltDataContractError(
            "storage_policy must match source registry policy for "
            f"{source_id}: {source.storage_policy}"
        )
    return RawObservationEvent(
        schema_version=SCHEMA_VERSION,
        observation_id=_ref(payload, "observation_id"),
        source_id=source_id,
        retrieved_at=_datetime(payload, "retrieved_at"),
        event_time=_datetime(payload, "event_time"),
        payload_hash=_sha256(payload, "payload_hash"),
        storage_uri=_bounded_string(payload, "storage_uri", limit=500),
        parser_version=_ref(payload, "parser_version"),
        freshness=cast(Freshness, _enum(payload, "freshness", FRESHNESS_VALUES)),
        http_status=_int_range(payload, "http_status", minimum=100, maximum=599),
        entity_refs=_ref_tuple(payload, "entity_refs", min_items=0, max_items=64),
        instrument_refs=_ref_tuple(payload, "instrument_refs", min_items=0, max_items=64),
        geo_refs=_ref_tuple(payload, "geo_refs", min_items=0, max_items=64),
        quality_flags=_ref_tuple(payload, "quality_flags", min_items=0, max_items=32),
        storage_policy=storage_policy,
    )


def validate_normalized_signal_event(payload: Mapping[str, object]) -> NormalizedSignalEvent:
    _reject_unknown_or_forbidden(payload, NORMALIZED_SIGNAL_KEYS, "normalized signal")
    _require_schema_version(payload)
    source_id = _source_id(payload, "source_id")
    _require_known_source(source_id)
    return NormalizedSignalEvent(
        schema_version=SCHEMA_VERSION,
        signal_id=_ref(payload, "signal_id"),
        source_id=source_id,
        observation_id=_ref(payload, "observation_id"),
        as_of=_datetime(payload, "as_of"),
        event_time=_datetime(payload, "event_time"),
        asset_refs=_ref_tuple(payload, "asset_refs", min_items=1, max_items=64),
        entity_refs=_ref_tuple(payload, "entity_refs", min_items=0, max_items=64),
        region_refs=_ref_tuple(payload, "region_refs", min_items=0, max_items=64),
        signal_family=_ref(payload, "signal_family"),
        horizon=_enum(payload, "horizon", HORIZON_VALUES),
        direction_hint=cast(DirectionHint, _enum(payload, "direction_hint", DIRECTION_HINT_VALUES)),
        confidence=_float_range(payload, "confidence", minimum=0.0, maximum=1.0),
        magnitude=_float_range(payload, "magnitude", minimum=-1.0, maximum=1.0),
        freshness=cast(Freshness, _enum(payload, "freshness", FRESHNESS_VALUES)),
        summary=_bounded_string(payload, "summary", limit=TEXT_LIMIT),
        evidence_refs=_ref_tuple(payload, "evidence_refs", min_items=1, max_items=64),
        quality_flags=_ref_tuple(payload, "quality_flags", min_items=0, max_items=32),
    )


def validate_llm_context_pack(payload: Mapping[str, object]) -> LlmContextPack:
    _reject_unknown_or_forbidden(payload, CONTEXT_PACK_KEYS, "LLM context pack")
    _require_schema_version(payload)
    unavailable = _source_id_tuple(payload, "unavailable_source_ids")
    stale = _source_id_tuple(payload, "stale_source_ids")
    return LlmContextPack(
        schema_version=SCHEMA_VERSION,
        context_id=_ref(payload, "context_id"),
        cycle_id=_ref(payload, "cycle_id"),
        as_of=_datetime(payload, "as_of"),
        portfolio_state_ref=_ref(payload, "portfolio_state_ref"),
        risk_state_ref=_ref(payload, "risk_state_ref"),
        approved_instrument_ids=_ref_tuple(
            payload, "approved_instrument_ids", min_items=1, max_items=256
        ),
        signal_refs=_ref_tuple(payload, "signal_refs", min_items=0, max_items=512),
        graph_refs=_ref_tuple(payload, "graph_refs", min_items=0, max_items=512),
        historical_analog_refs=_ref_tuple(
            payload, "historical_analog_refs", min_items=0, max_items=128
        ),
        unavailable_source_ids=unavailable,
        stale_source_ids=stale,
        risk_constraints=_float_mapping(payload, "risk_constraints"),
    )


def validate_llm_trade_intent(payload: Mapping[str, object]) -> LlmTradeIntent:
    _reject_unknown_or_forbidden(payload, TRADE_INTENT_KEYS, "LLM trade intent")
    _require_schema_version(payload)
    commands = tuple(_validate_trade_command(row) for row in _sequence(payload, "commands"))
    if not commands:
        raise AltDataContractError("commands must contain at least one command")
    risk_assessment = _mapping(payload, "risk_assessment")
    _reject_unknown_or_forbidden(risk_assessment, INTENT_RISK_KEYS, "intent risk_assessment")
    risk_assessment_out: dict[str, object] = {
        "summary": _bounded_string(risk_assessment, "summary", limit=TEXT_LIMIT),
        "uncertainty": _bounded_string(risk_assessment, "uncertainty", limit=TEXT_LIMIT),
        "risk_limit_verdict": _enum(
            risk_assessment, "risk_limit_verdict", INTENT_RISK_VERDICTS
        ),
    }
    return LlmTradeIntent(
        schema_version=SCHEMA_VERSION,
        cycle_id=_ref(payload, "cycle_id"),
        sequence=_int_range(payload, "sequence", minimum=1, maximum=2_147_483_647),
        as_of=_datetime(payload, "as_of"),
        model_id=_ref(payload, "model_id"),
        prompt_template_hash=_sha256(payload, "prompt_template_hash"),
        commands=commands,
        risk_assessment=risk_assessment_out,
        memory_updates=_string_tuple(payload, "memory_updates", min_items=1, max_items=16, limit=240),
    )


def validate_risk_approved_order(payload: Mapping[str, object]) -> RiskApprovedOrder:
    _reject_unknown_or_forbidden(payload, RISK_APPROVED_ORDER_KEYS, "risk-approved order")
    _require_schema_version(payload)
    verdict = cast(OrderVerdict, _enum(payload, "verdict", ORDER_VERDICTS))
    side = _enum(payload, "side", ORDER_SIDES)
    quantity = _float_range(payload, "quantity", minimum=0.0, maximum=1_000_000_000.0)
    notional_usd = _float_range(payload, "notional_usd", minimum=0.0, maximum=1_000_000_000.0)
    if verdict != "approved" and (side != "hold" or quantity != 0.0 or notional_usd != 0.0):
        raise AltDataContractError("rejected or hold orders must use hold side with zero size")
    if verdict == "approved" and (side == "hold" or quantity <= 0.0 or notional_usd <= 0.0):
        raise AltDataContractError("approved orders require a non-hold side and positive size")
    return RiskApprovedOrder(
        schema_version=SCHEMA_VERSION,
        approval_id=_ref(payload, "approval_id"),
        cycle_id=_ref(payload, "cycle_id"),
        intent_sequence=_int_range(payload, "intent_sequence", minimum=1, maximum=2_147_483_647),
        as_of=_datetime(payload, "as_of"),
        verdict=verdict,
        reason=_bounded_string(payload, "reason", limit=TEXT_LIMIT),
        instrument_id=_ref(payload, "instrument_id"),
        side=side,
        quantity=quantity,
        notional_usd=notional_usd,
        route=_ref(payload, "route"),
        time_in_force=_enum(payload, "time_in_force", TIME_IN_FORCE_VALUES),
        context_id=_ref(payload, "context_id"),
        intent_ref=_ref(payload, "intent_ref"),
        risk_snapshot_ref=_ref(payload, "risk_snapshot_ref"),
        instrument_master_version=_ref(payload, "instrument_master_version"),
    )


def _validate_trade_command(payload: object) -> Mapping[str, object]:
    row = _as_mapping(payload, "trade command")
    _reject_unknown_or_forbidden(row, TRADE_COMMAND_KEYS, "trade command")
    target_weight = _float_range(row, "target_weight", minimum=-0.95, maximum=0.95)
    direction = _enum(row, "direction", TRADE_DIRECTIONS)
    if direction == "long" and target_weight < 0:
        raise AltDataContractError("long commands cannot have negative target_weight")
    if direction == "short" and target_weight > 0:
        raise AltDataContractError("short commands cannot have positive target_weight")
    if direction == "flat" and abs(target_weight) > ZERO_WEIGHT_TOLERANCE:
        raise AltDataContractError("flat commands must use zero target_weight")
    if direction == "flat":
        target_weight = 0.0
    return {
        "instrument_id": _ref(row, "instrument_id"),
        "direction": direction,
        "target_weight": target_weight,
        "max_notional_usd": _float_range(
            row, "max_notional_usd", minimum=0.0, maximum=1_000_000_000.0
        ),
        "horizon": _enum(row, "horizon", HORIZON_VALUES),
        "confidence": _float_range(row, "confidence", minimum=0.0, maximum=1.0),
        "thesis": _bounded_string(row, "thesis", limit=TEXT_LIMIT),
        "evidence_refs": _ref_tuple(row, "evidence_refs", min_items=1, max_items=64),
    }


def _reject_unknown_or_forbidden(
    payload: Mapping[str, object], allowed_keys: set[str], label: str
) -> None:
    keys = set(payload)
    forbidden = sorted(keys & RAW_FORBIDDEN_KEYS)
    if forbidden:
        raise AltDataContractError(f"{label} contains forbidden raw/secret keys: {', '.join(forbidden)}")
    unknown = sorted(keys - allowed_keys)
    if unknown:
        raise AltDataContractError(f"{label} contains unknown keys: {', '.join(unknown)}")
    for key, value in payload.items():
        if isinstance(value, str) and _SECRET_ASSIGNMENT_RE.search(value):
            raise AltDataContractError(f"{label}.{key} contains a secret-like assignment")


def _require_schema_version(payload: Mapping[str, object]) -> None:
    version = _required(payload, "schema_version")
    if version != SCHEMA_VERSION:
        raise AltDataContractError(f"schema_version must be {SCHEMA_VERSION}")


def _require_known_source(source_id: str) -> RegisteredSource:
    source = _source_specs_by_id().get(source_id)
    if source is None:
        raise AltDataContractError(f"source_id is not in the source registry: {source_id}")
    return source


@lru_cache(maxsize=1)
def _known_source_ids() -> frozenset[str]:
    return frozenset(_source_specs_by_id())


@lru_cache(maxsize=1)
def _source_specs_by_id() -> Mapping[str, RegisteredSource]:
    return load_source_registry().by_source_id()


def _source_id_tuple(payload: Mapping[str, object], key: str) -> tuple[str, ...]:
    values = tuple(_source_id_value(value, f"{key}[]") for value in _sequence(payload, key))
    if len(set(values)) != len(values):
        raise AltDataContractError(f"{key} must not contain duplicate values")
    for source_id in values:
        _require_known_source(source_id)
    return values


def _source_id(payload: Mapping[str, object], key: str) -> str:
    return _source_id_value(_required(payload, key), key)


def _source_id_value(value: object, label: str) -> str:
    if not isinstance(value, str) or SOURCE_ID_RE.fullmatch(value) is None:
        raise AltDataContractError(f"{label} must be a valid source_id")
    return value


def _sha256(payload: Mapping[str, object], key: str) -> str:
    value = _bounded_string(payload, key, limit=80)
    if SHA256_RE.fullmatch(value) is None:
        raise AltDataContractError(f"{key} must be a sha256:<hex> digest")
    return value


def _datetime(payload: Mapping[str, object], key: str) -> datetime:
    value = _bounded_string(payload, key, limit=120)
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise AltDataContractError(f"{key} must be an ISO-8601 date-time") from exc
    if parsed.tzinfo is None:
        raise AltDataContractError(f"{key} must include timezone")
    return parsed


def _ref(payload: Mapping[str, object], key: str) -> str:
    value = _bounded_string(payload, key, limit=120)
    if REF_RE.fullmatch(value) is None:
        raise AltDataContractError(f"{key} contains an invalid reference: {value}")
    return value


def _ref_tuple(
    payload: Mapping[str, object],
    key: str,
    *,
    min_items: int,
    max_items: int,
) -> tuple[str, ...]:
    values = _string_tuple(payload, key, min_items=min_items, max_items=max_items, limit=120)
    for value in values:
        if REF_RE.fullmatch(value) is None:
            raise AltDataContractError(f"{key} contains an invalid reference: {value}")
    return values


def _string_tuple(
    payload: Mapping[str, object],
    key: str,
    *,
    min_items: int,
    max_items: int,
    limit: int,
) -> tuple[str, ...]:
    values = _sequence(payload, key)
    if not min_items <= len(values) <= max_items:
        raise AltDataContractError(f"{key} must contain between {min_items} and {max_items} items")
    output = tuple(_bounded_string_value(value, key, limit=limit) for value in values)
    if len(set(output)) != len(output):
        raise AltDataContractError(f"{key} must not contain duplicate values")
    return output


def _float_mapping(payload: Mapping[str, object], key: str) -> Mapping[str, float]:
    row = _mapping(payload, key)
    if not row:
        raise AltDataContractError(f"{key} must not be empty")
    output: dict[str, float] = {}
    for item_key, value in row.items():
        if not isinstance(item_key, str) or REF_RE.fullmatch(item_key) is None:
            raise AltDataContractError(f"{key} contains an invalid constraint key")
        output[item_key] = _float_value(value, f"{key}.{item_key}")
    return output


def _enum(payload: Mapping[str, object], key: str, values: frozenset[str] | set[str]) -> str:
    value = _bounded_string(payload, key, limit=120)
    if value not in values:
        raise AltDataContractError(f"{key} must be one of: {', '.join(sorted(values))}")
    return value


def _bounded_string(payload: Mapping[str, object], key: str, *, limit: int) -> str:
    return _bounded_string_value(_required(payload, key), key, limit=limit)


def _bounded_string_value(value: object, label: str, *, limit: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AltDataContractError(f"{label} must be a non-empty string")
    if len(value) > limit:
        raise AltDataContractError(f"{label} exceeds {limit} characters")
    if _SECRET_ASSIGNMENT_RE.search(value):
        raise AltDataContractError(f"{label} contains a secret-like assignment")
    return value


def _int_range(payload: Mapping[str, object], key: str, *, minimum: int, maximum: int) -> int:
    value = _required(payload, key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise AltDataContractError(f"{key} must be an integer")
    if not minimum <= value <= maximum:
        raise AltDataContractError(f"{key} must be between {minimum} and {maximum}")
    return value


def _float_range(
    payload: Mapping[str, object], key: str, *, minimum: float, maximum: float
) -> float:
    value = _float_value(_required(payload, key), key)
    if not minimum <= value <= maximum:
        raise AltDataContractError(f"{key} must be between {minimum} and {maximum}")
    return value


def _float_value(value: object, label: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise AltDataContractError(f"{label} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise AltDataContractError(f"{label} must be finite")
    return number


def _mapping(payload: Mapping[str, object], key: str) -> Mapping[str, object]:
    return _as_mapping(_required(payload, key), key)


def _as_mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise AltDataContractError(f"{label} must be an object")
    return cast(Mapping[str, object], value)


def _sequence(payload: Mapping[str, object], key: str) -> Sequence[object]:
    value = _required(payload, key)
    if not isinstance(value, list):
        raise AltDataContractError(f"{key} must be an array")
    return value


def _required(payload: Mapping[str, object], key: str) -> object:
    if key not in payload:
        raise AltDataContractError(f"missing required key: {key}")
    value: Any = payload[key]
    return value
