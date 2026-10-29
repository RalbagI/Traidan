"""Connector scaffolding for source-health and future context-pack evidence."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final

from alt_data.contracts import (
    NormalizedSignalEvent,
    RawObservationEvent,
    validate_normalized_signal_event,
    validate_raw_observation_event,
)
from alt_data.registry import RegisteredSource, SourceRegistry, TelegramWatchEntry

PARSER_VERSION: Final = "traidan-alt-data-gated-v1"
FIRST_CONNECTOR_WAVE_SOURCE_IDS: Final[frozenset[str]] = frozenset({
    "sec_edgar",
    "fred",
    "bureau_of_labor_statistics",
    "trading_economics",
    "gdelt",
    "stocktwits",
})
DEFAULT_ASSET_REFS: Final[tuple[str, ...]] = ("SPY",)


@dataclass(frozen=True)
class SourceHealth:
    source_id: str
    state: str
    freshness: str
    detail: str
    quality_flags: tuple[str, ...]


@dataclass(frozen=True)
class ConnectorPlan:
    source_id: str
    family: str
    wave: str
    enabled_for_paper: bool
    live_disabled: bool
    gating_reason: str


@dataclass(frozen=True)
class ConnectorResult:
    raw_observation: RawObservationEvent
    normalized_signals: tuple[NormalizedSignalEvent, ...]
    source_health: SourceHealth


def connector_plans(registry: SourceRegistry) -> tuple[ConnectorPlan, ...]:
    """Return deterministic paper-gate plans for every registered source."""

    rows: list[ConnectorPlan] = []
    for source in registry.sources:
        wave = (
            "first_connector_wave"
            if source.source_id in FIRST_CONNECTOR_WAVE_SOURCE_IDS
            else _family_wave(source.category)
        )
        if source.kind == "telegram":
            wave = "telegram_public_metadata"
        gating_reason = _gating_reason(source)
        rows.append(
            ConnectorPlan(
                source_id=source.source_id,
                family=source.category,
                wave=wave,
                enabled_for_paper=source.paper_context_enabled,
                live_disabled=source.live_disabled,
                gating_reason=gating_reason,
            )
        )
    return tuple(rows)


def unavailable_connector_result(
    source: RegisteredSource,
    as_of: datetime,
    *,
    reason: str,
) -> ConnectorResult:
    """Build a validated unavailable observation for a gated or failed source."""

    timestamp = _format_utc(as_of)
    safe_timestamp = _safe_timestamp(timestamp)
    observation_id = _observation_id(source.source_id, timestamp)
    raw_payload = {
        "schema_version": 1,
        "observation_id": observation_id,
        "source_id": source.source_id,
        "retrieved_at": timestamp,
        "event_time": timestamp,
        "payload_hash": _payload_hash({
            "source_id": source.source_id,
            "freshness": "unavailable",
            "reason": reason,
            "retrieved_at": timestamp,
        }),
        "storage_uri": f"metadata://source-health/{source.source_id}/{safe_timestamp}",
        "parser_version": PARSER_VERSION,
        "freshness": "unavailable",
        "http_status": 503,
        "entity_refs": [],
        "instrument_refs": [],
        "geo_refs": [],
        "quality_flags": [reason],
        "storage_policy": source.storage_policy,
    }
    return ConnectorResult(
        raw_observation=validate_raw_observation_event(raw_payload),
        normalized_signals=(),
        source_health=SourceHealth(
            source_id=source.source_id,
            state=reason,
            freshness="unavailable",
            detail=f"{source.site_name} is not promoted into paper context: {reason}.",
            quality_flags=(reason,),
        ),
    )


def telegram_metadata_result(
    entry: TelegramWatchEntry,
    as_of: datetime,
    *,
    message_id: int,
    posted_at: datetime,
    summary: str,
    cashtags: Sequence[str] = DEFAULT_ASSET_REFS,
    view_count: int | None = None,
    forward_count: int | None = None,
) -> ConnectorResult:
    """Convert one public Telegram channel post into metadata-only evidence."""

    retrieved_at = _format_utc(as_of)
    event_time = _format_utc(posted_at)
    source_id = entry.source_id
    observation_id = f"obs-{source_id}-{message_id}"
    safe_cashtags = tuple(_clean_ref(symbol) for symbol in cashtags if symbol.strip())
    entity_ref = f"telegram:{entry.channel_username.removeprefix('@')}"
    quality_flags = list(entry.quality_flags)
    if view_count is not None and view_count >= 0:
        quality_flags.append(f"views:{view_count}")
    if forward_count is not None and forward_count >= 0:
        quality_flags.append(f"forwards:{forward_count}")
    payload_hash = _payload_hash({
        "source_id": source_id,
        "message_id": message_id,
        "event_time": event_time,
        "summary": summary,
        "cashtags": safe_cashtags,
        "view_count": view_count,
        "forward_count": forward_count,
    })
    raw_payload = {
        "schema_version": 1,
        "observation_id": observation_id,
        "source_id": source_id,
        "retrieved_at": retrieved_at,
        "event_time": event_time,
        "payload_hash": payload_hash,
        "storage_uri": f"metadata://telegram/{entry.channel_username[1:]}/{message_id}",
        "parser_version": "telegram-public-metadata-v1",
        "freshness": "fresh",
        "http_status": 200,
        "entity_refs": [entity_ref],
        "instrument_refs": list(safe_cashtags or DEFAULT_ASSET_REFS),
        "geo_refs": [],
        "quality_flags": quality_flags,
        "storage_policy": entry.storage_policy,
    }
    signal_payload = {
        "schema_version": 1,
        "signal_id": f"sig-{source_id}-{message_id}",
        "source_id": source_id,
        "observation_id": observation_id,
        "as_of": retrieved_at,
        "event_time": event_time,
        "asset_refs": list(safe_cashtags or DEFAULT_ASSET_REFS),
        "entity_refs": [entity_ref],
        "region_refs": ["US"],
        "signal_family": "public_sentiment",
        "horizon": "intraday",
        "direction_hint": "neutral",
        "confidence": 0.35,
        "magnitude": 0.0,
        "freshness": "fresh",
        "summary": summary,
        "evidence_refs": [observation_id],
        "quality_flags": quality_flags,
    }
    return ConnectorResult(
        raw_observation=validate_raw_observation_event(raw_payload),
        normalized_signals=(validate_normalized_signal_event(signal_payload),),
        source_health=SourceHealth(
            source_id=source_id,
            state="fresh_metadata",
            freshness="fresh",
            detail=f"{entry.display_name} public Telegram metadata ingested without raw text storage.",
            quality_flags=entry.quality_flags,
        ),
    )


def public_summary_signal_result(
    source: RegisteredSource,
    as_of: datetime,
    *,
    summary: str,
    asset_refs: Sequence[str] = DEFAULT_ASSET_REFS,
    entity_refs: Sequence[str] = (),
    region_refs: Sequence[str] = ("US",),
    signal_family: str,
    direction_hint: str = "neutral",
    confidence: float = 0.35,
    magnitude: float = 0.0,
    quality_flags: Sequence[str] = ("summary_only", "public_source"),
) -> ConnectorResult:
    """Convert one public summary into validated metadata-only signal evidence."""

    timestamp = _format_utc(as_of)
    safe_assets = tuple(_clean_ref(symbol) for symbol in asset_refs if symbol.strip())
    safe_entities = tuple(str(entity).strip()[:120] for entity in entity_refs if str(entity).strip())
    safe_regions = tuple(str(region).strip()[:120] for region in region_refs if str(region).strip())
    flags = tuple(str(flag).strip()[:120] for flag in quality_flags if str(flag).strip())
    payload_hash = _payload_hash({
        "source_id": source.source_id,
        "event_time": timestamp,
        "summary": summary,
        "asset_refs": safe_assets,
        "entity_refs": safe_entities,
        "region_refs": safe_regions,
        "signal_family": signal_family,
        "direction_hint": direction_hint,
    })
    observation_id = _observation_id(source.source_id, payload_hash[-16:])
    raw_payload = {
        "schema_version": 1,
        "observation_id": observation_id,
        "source_id": source.source_id,
        "retrieved_at": timestamp,
        "event_time": timestamp,
        "payload_hash": payload_hash,
        "storage_uri": f"metadata://public-summary/{source.source_id}/{payload_hash[-16:]}",
        "parser_version": "public-summary-signal-v1",
        "freshness": "fresh",
        "http_status": 200,
        "entity_refs": list(safe_entities),
        "instrument_refs": list(safe_assets or DEFAULT_ASSET_REFS),
        "geo_refs": list(safe_regions),
        "quality_flags": list(flags),
        "storage_policy": source.storage_policy,
    }
    signal_payload = {
        "schema_version": 1,
        "signal_id": f"sig-{source.source_id}-{payload_hash[-16:]}",
        "source_id": source.source_id,
        "observation_id": observation_id,
        "as_of": timestamp,
        "event_time": timestamp,
        "asset_refs": list(safe_assets or DEFAULT_ASSET_REFS),
        "entity_refs": list(safe_entities),
        "region_refs": list(safe_regions),
        "signal_family": signal_family,
        "horizon": "intraday",
        "direction_hint": direction_hint,
        "confidence": max(min(float(confidence), 1.0), 0.0),
        "magnitude": max(min(float(magnitude), 1.0), -1.0),
        "freshness": "fresh",
        "summary": summary[:1000],
        "evidence_refs": [observation_id],
        "quality_flags": list(flags),
    }
    return ConnectorResult(
        raw_observation=validate_raw_observation_event(raw_payload),
        normalized_signals=(validate_normalized_signal_event(signal_payload),),
        source_health=SourceHealth(
            source_id=source.source_id,
            state="fresh_public_summary",
            freshness="fresh",
            detail=f"{source.site_name} public summary converted without raw payload storage.",
            quality_flags=flags,
        ),
    )


def dedupe_connector_results(results: Iterable[ConnectorResult]) -> tuple[ConnectorResult, ...]:
    """Keep the first observation for each source observation ID."""

    seen: set[str] = set()
    output: list[ConnectorResult] = []
    for result in results:
        observation_id = result.raw_observation.observation_id
        if observation_id in seen:
            continue
        seen.add(observation_id)
        output.append(result)
    return tuple(output)


def source_health_summary(results: Sequence[ConnectorResult]) -> Mapping[str, int]:
    counts = {"fresh": 0, "stale": 0, "unavailable": 0}
    for result in results:
        freshness = result.source_health.freshness
        if freshness in counts:
            counts[freshness] += 1
    return counts


def _family_wave(category: str) -> str:
    return {
        "core_financial_infrastructure": "wave_a",
        "global_macro": "wave_b",
        "public_sentiment": "wave_c",
        "alternative_real_world_assets": "wave_d",
        "systemic_interdependence": "wave_e",
    }.get(category, "future_wave")


def _gating_reason(source: RegisteredSource) -> str:
    if source.paper_context_enabled:
        return "paper_context_enabled"
    if not source.gates.get("legal_approved", False):
        return "legal_approval_required"
    if not source.gates.get("canary", False):
        return "canary_required"
    return "paper_context_disabled"


def _payload_hash(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _observation_id(source_id: str, timestamp: str) -> str:
    safe_timestamp = _safe_timestamp(timestamp)
    return f"obs-{source_id}-{safe_timestamp}"


def _safe_timestamp(timestamp: str) -> str:
    return timestamp.replace("-", "").replace(":", "").replace(".", "")


def _clean_ref(value: str) -> str:
    return value.strip().upper().removeprefix("$")


def _format_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
