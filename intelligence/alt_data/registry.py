"""Production-gated source registry for future LLM context expansion."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Literal, TypeAlias, cast

from alt_data.catalog import (
    CATALOG_PATH,
    SOURCE_ID_RE,
    STORAGE_POLICIES,
    SourceCatalog,
    SourceCatalogError,
    SourceSpec,
    load_source_catalog,
)

ROOT: Final = Path(__file__).resolve().parents[2]
TELEGRAM_WATCHLIST_PATH: Final = ROOT / "config" / "alt_data" / "telegram_watchlist.json"
REGISTRY_SCHEMA_VERSION: Final = 1
CONNECTOR_GATES: Final[tuple[str, ...]] = (
    "configured",
    "credentialed",
    "legal_approved",
    "canary",
    "paper_context_enabled",
    "live_disabled",
)
TELEGRAM_QUALITY_BLOCKLIST: Final[frozenset[str]] = frozenset({
    "anonymous_pump_group",
    "pump_group",
    "signal_calls",
    "unofficial_news_mirror",
    "private_channel",
})
TELEGRAM_USERNAME_RE: Final = re.compile(r"^@[A-Za-z][A-Za-z0-9_]{4,31}$")

SourceKind: TypeAlias = Literal["catalog", "telegram"]


class SourceRegistryError(ValueError):
    """Raised when source activation metadata is unsafe or inconsistent."""


@dataclass(frozen=True)
class TelegramWatchEntry:
    source_id: str
    watch_id: str
    channel_username: str
    channel_url: str
    display_name: str
    visibility: str
    verification_status: str
    storage_policy: str
    raw_text_allowed: bool
    license_approved: bool
    quality_flags: tuple[str, ...]
    production_gate: str


@dataclass(frozen=True)
class RegisteredSource:
    source_id: str
    site_name: str
    base_url: str
    category: str
    auth_mode: str
    data_shape: str
    storage_policy: str
    production_gate: str
    kind: SourceKind
    gates: Mapping[str, bool]

    @property
    def paper_context_enabled(self) -> bool:
        return bool(self.gates.get("paper_context_enabled", False))

    @property
    def live_disabled(self) -> bool:
        return bool(self.gates.get("live_disabled", True))


@dataclass(frozen=True)
class SourceRegistry:
    catalog: SourceCatalog
    telegram_watchlist: tuple[TelegramWatchEntry, ...]
    sources: tuple[RegisteredSource, ...]

    def by_source_id(self) -> Mapping[str, RegisteredSource]:
        return {source.source_id: source for source in self.sources}

    def active_paper_sources(self) -> tuple[RegisteredSource, ...]:
        return tuple(source for source in self.sources if source.paper_context_enabled)

    def live_enabled_sources(self) -> tuple[RegisteredSource, ...]:
        return tuple(source for source in self.sources if not source.live_disabled)


def load_source_registry(
    *,
    catalog_path: Path = CATALOG_PATH,
    telegram_path: Path = TELEGRAM_WATCHLIST_PATH,
) -> SourceRegistry:
    """Load the 50-source catalog plus public Telegram metadata sources."""

    catalog = load_source_catalog(catalog_path)
    telegram_watchlist = load_telegram_watchlist(telegram_path)
    catalog_sources = tuple(_registered_catalog_source(source) for source in catalog.sources)
    telegram_sources = tuple(_registered_telegram_source(entry) for entry in telegram_watchlist)
    sources = (*catalog_sources, *telegram_sources)
    _reject_duplicate_source_ids(sources)
    return SourceRegistry(
        catalog=catalog,
        telegram_watchlist=telegram_watchlist,
        sources=sources,
    )


def load_telegram_watchlist(path: Path = TELEGRAM_WATCHLIST_PATH) -> tuple[TelegramWatchEntry, ...]:
    payload = _read_json_object(path)
    if _int(payload, "schema_version") != REGISTRY_SCHEMA_VERSION:
        raise SourceRegistryError("unsupported telegram watchlist schema_version")
    if _string(payload, "runtime_status") != "public_metadata_watchlist":
        raise SourceRegistryError("telegram watchlist must remain metadata-only")
    entries = tuple(_parse_telegram_entry(row) for row in _sequence(payload, "entries"))
    if not entries:
        raise SourceRegistryError("telegram watchlist must contain at least one entry")
    return entries


def _registered_catalog_source(source: SourceSpec) -> RegisteredSource:
    gates = {
        "configured": True,
        "credentialed": source.auth_mode == "public",
        "legal_approved": False,
        "canary": False,
        "paper_context_enabled": False,
        "live_disabled": True,
    }
    return RegisteredSource(
        source_id=source.source_id,
        site_name=source.site_name,
        base_url=source.base_url,
        category=source.category,
        auth_mode=source.auth_mode,
        data_shape=source.data_shape,
        storage_policy=source.storage_policy,
        production_gate=source.production_gate,
        kind="catalog",
        gates=gates,
    )


def _registered_telegram_source(entry: TelegramWatchEntry) -> RegisteredSource:
    gates = {
        "configured": True,
        "credentialed": True,
        "legal_approved": entry.license_approved,
        "canary": False,
        "paper_context_enabled": False,
        "live_disabled": True,
    }
    return RegisteredSource(
        source_id=entry.source_id,
        site_name=entry.display_name,
        base_url=entry.channel_url,
        category="public_sentiment",
        auth_mode="public",
        data_shape="telegram_public_channel_metadata",
        storage_policy=entry.storage_policy,
        production_gate=entry.production_gate,
        kind="telegram",
        gates=gates,
    )


def _parse_telegram_entry(payload: object) -> TelegramWatchEntry:
    row = _as_mapping(payload, "telegram entry")
    entry = TelegramWatchEntry(
        source_id=_source_id(row, "source_id"),
        watch_id=_ref(row, "watch_id"),
        channel_username=_telegram_username(row, "channel_username"),
        channel_url=_https_url(row, "channel_url"),
        display_name=_string(row, "display_name"),
        visibility=_string(row, "visibility"),
        verification_status=_string(row, "verification_status"),
        storage_policy=_string(row, "storage_policy"),
        raw_text_allowed=_bool(row, "raw_text_allowed"),
        license_approved=_bool(row, "license_approved"),
        quality_flags=_string_tuple(row, "quality_flags"),
        production_gate=_string(row, "production_gate"),
    )
    _validate_telegram_entry(entry)
    return entry


def _validate_telegram_entry(entry: TelegramWatchEntry) -> None:
    if entry.visibility != "public":
        raise SourceRegistryError(f"telegram source must be public: {entry.source_id}")
    if entry.storage_policy != "metadata_summary_only":
        raise SourceRegistryError(f"telegram source must be metadata_summary_only: {entry.source_id}")
    if entry.storage_policy not in STORAGE_POLICIES:
        raise SourceRegistryError(f"invalid telegram storage_policy: {entry.storage_policy}")
    if entry.raw_text_allowed:
        raise SourceRegistryError(f"telegram raw text is disabled by default: {entry.source_id}")
    if entry.verification_status not in {"verified_public", "official_public"}:
        raise SourceRegistryError(f"telegram source must be verified or official: {entry.source_id}")
    blocked_flags = TELEGRAM_QUALITY_BLOCKLIST.intersection(entry.quality_flags)
    if blocked_flags:
        raise SourceRegistryError(
            f"telegram source has blocked quality flags: {', '.join(sorted(blocked_flags))}"
        )
    if not entry.production_gate.startswith(("Confirm", "Implement", "Use", "Prefer")):
        raise SourceRegistryError("telegram production_gate must be explicit")


def _reject_duplicate_source_ids(sources: Sequence[RegisteredSource]) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for source in sources:
        if source.source_id in seen:
            duplicates.add(source.source_id)
        seen.add(source.source_id)
    if duplicates:
        raise SourceRegistryError(f"duplicate registered source_id values: {', '.join(sorted(duplicates))}")


def _read_json_object(path: Path) -> Mapping[str, object]:
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise SourceRegistryError(f"unable to read source registry {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise SourceRegistryError(f"invalid JSON in source registry {path}: {exc}") from exc
    return _as_mapping(parsed, "source registry")


def _as_mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise SourceRegistryError(f"{label} must be an object")
    return cast(Mapping[str, object], value)


def _sequence(payload: Mapping[str, object], key: str) -> Sequence[object]:
    value = _required(payload, key)
    if not isinstance(value, list):
        raise SourceRegistryError(f"{key} must be an array")
    return value


def _string_tuple(payload: Mapping[str, object], key: str) -> tuple[str, ...]:
    values = _sequence(payload, key)
    if not all(isinstance(value, str) and value.strip() for value in values):
        raise SourceRegistryError(f"{key} must contain non-empty strings")
    output = tuple(str(value).strip() for value in values)
    if len(set(output)) != len(output):
        raise SourceRegistryError(f"{key} must not contain duplicates")
    return output


def _source_id(payload: Mapping[str, object], key: str) -> str:
    value = _string(payload, key)
    if SOURCE_ID_RE.fullmatch(value) is None:
        raise SourceCatalogError(f"invalid source_id: {value}")
    return value


def _telegram_username(payload: Mapping[str, object], key: str) -> str:
    value = _string(payload, key)
    if TELEGRAM_USERNAME_RE.fullmatch(value) is None:
        raise SourceRegistryError(f"{key} must be a public @username")
    return value


def _https_url(payload: Mapping[str, object], key: str) -> str:
    value = _string(payload, key)
    if not value.startswith("https://t.me/"):
        raise SourceRegistryError(f"{key} must be an HTTPS Telegram URL")
    return value


def _ref(payload: Mapping[str, object], key: str) -> str:
    value = _string(payload, key)
    if not all(character.isalnum() or character in "-_:" for character in value):
        raise SourceRegistryError(f"{key} contains invalid reference characters")
    return value


def _bool(payload: Mapping[str, object], key: str) -> bool:
    value = _required(payload, key)
    if not isinstance(value, bool):
        raise SourceRegistryError(f"{key} must be boolean")
    return value


def _int(payload: Mapping[str, object], key: str) -> int:
    value = _required(payload, key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise SourceRegistryError(f"{key} must be an integer")
    return value


def _string(payload: Mapping[str, object], key: str) -> str:
    value = _required(payload, key)
    if not isinstance(value, str) or not value.strip():
        raise SourceRegistryError(f"{key} must be a non-empty string")
    return value.strip()


def _required(payload: Mapping[str, object], key: str) -> object:
    if key not in payload:
        raise SourceRegistryError(f"missing required key: {key}")
    value: Any = payload[key]
    return value
