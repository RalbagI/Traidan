"""Typed loader for the future-state alternative-data source registry."""

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, TypeAlias, cast

ROOT: Final = Path(__file__).resolve().parents[2]
CATALOG_PATH: Final = ROOT / "config" / "alt_data" / "source_catalog.json"
EXPECTED_SOURCE_COUNT: Final = 50
SOURCE_ID_RE: Final = re.compile(r"^[a-z][a-z0-9_]{2,80}$")

Category: TypeAlias = str
AuthMode: TypeAlias = str
StoragePolicy: TypeAlias = str

CATEGORY_ORDER: Final[tuple[Category, ...]] = (
    "core_financial_infrastructure",
    "global_macro",
    "public_sentiment",
    "alternative_real_world_assets",
    "systemic_interdependence",
)

AUTH_MODES: Final[frozenset[AuthMode]] = frozenset({
    "api_key_and_token_query",
    "api_key_header",
    "api_key_payload",
    "api_key_query",
    "basic_or_custom_header",
    "bearer_token",
    "client_credentials",
    "client_key_secret_query",
    "hmac",
    "jwt",
    "oauth2",
    "oauth2_or_basic",
    "public",
    "registered_token",
    "registered_token_query",
})

STORAGE_POLICIES: Final[frozenset[StoragePolicy]] = frozenset({
    "metadata_summary_and_licensed_raw",
    "metadata_summary_and_public_raw",
    "metadata_summary_only",
    "no_raw_storage",
})


class SourceCatalogError(ValueError):
    """Raised when the alternative-data source catalog is invalid."""


@dataclass(frozen=True)
class SourceSpec:
    """One source from the future-state alternative-data registry."""

    catalog_number: int
    source_id: str
    site_name: str
    base_url: str
    category: Category
    extraction_signal: str
    access_strategy: str
    auth_mode: AuthMode
    data_shape: str
    storage_policy: StoragePolicy
    production_gate: str


@dataclass(frozen=True)
class SourceCatalog:
    """Validated source catalog for migration planning and connector scaffolding."""

    schema_version: int
    catalog_name: str
    research_input: str
    runtime_status: str
    sources: tuple[SourceSpec, ...]

    def by_source_id(self) -> Mapping[str, SourceSpec]:
        return {source.source_id: source for source in self.sources}

    def sources_by_category(self) -> Mapping[Category, tuple[SourceSpec, ...]]:
        grouped: dict[Category, list[SourceSpec]] = {category: [] for category in CATEGORY_ORDER}
        for source in self.sources:
            grouped[source.category].append(source)
        return {category: tuple(grouped[category]) for category in CATEGORY_ORDER}

    def category_counts(self) -> Mapping[Category, int]:
        counts = Counter(source.category for source in self.sources)
        return {category: counts[category] for category in CATEGORY_ORDER}


def load_source_catalog(path: Path = CATALOG_PATH) -> SourceCatalog:
    """Load and validate the source catalog."""

    payload = _read_json_object(path)
    schema_version = _int(payload, "schema_version")
    if schema_version != 1:
        raise SourceCatalogError(f"unsupported source catalog schema_version: {schema_version}")

    sources = tuple(_parse_source(row) for row in _sequence(payload, "sources"))
    catalog = SourceCatalog(
        schema_version=schema_version,
        catalog_name=_string(payload, "catalog_name"),
        research_input=_string(payload, "research_input"),
        runtime_status=_string(payload, "runtime_status"),
        sources=sources,
    )
    _validate_catalog(catalog)
    return catalog


def _validate_catalog(catalog: SourceCatalog) -> None:
    if catalog.runtime_status != "future_migration_registry":
        raise SourceCatalogError("source catalog must not be marked as an active runtime surface")
    if len(catalog.sources) != EXPECTED_SOURCE_COUNT:
        raise SourceCatalogError(f"expected {EXPECTED_SOURCE_COUNT} sources, got {len(catalog.sources)}")

    numbers = [source.catalog_number for source in catalog.sources]
    expected_numbers = list(range(1, EXPECTED_SOURCE_COUNT + 1))
    if sorted(numbers) != expected_numbers:
        raise SourceCatalogError("catalog numbers must be unique and contiguous from 1 to 50")
    if numbers != expected_numbers:
        raise SourceCatalogError("catalog sources must remain ordered by catalog_number")

    source_ids = [source.source_id for source in catalog.sources]
    duplicate_source_ids = [item for item, count in Counter(source_ids).items() if count > 1]
    if duplicate_source_ids:
        raise SourceCatalogError(f"duplicate source_id values: {', '.join(sorted(duplicate_source_ids))}")

    counts = catalog.category_counts()
    missing_categories = [category for category, count in counts.items() if count == 0]
    if missing_categories:
        raise SourceCatalogError(f"missing source categories: {', '.join(missing_categories)}")


def _parse_source(payload: object) -> SourceSpec:
    row = _as_mapping(payload, "source")
    source = SourceSpec(
        catalog_number=_int(row, "catalog_number"),
        source_id=_string(row, "source_id"),
        site_name=_string(row, "site_name"),
        base_url=_string(row, "base_url"),
        category=_string(row, "category"),
        extraction_signal=_string(row, "extraction_signal"),
        access_strategy=_string(row, "access_strategy"),
        auth_mode=_string(row, "auth_mode"),
        data_shape=_string(row, "data_shape"),
        storage_policy=_string(row, "storage_policy"),
        production_gate=_string(row, "production_gate"),
    )
    _validate_source(source)
    return source


def _validate_source(source: SourceSpec) -> None:
    if not SOURCE_ID_RE.fullmatch(source.source_id):
        raise SourceCatalogError(f"invalid source_id: {source.source_id}")
    if source.category not in CATEGORY_ORDER:
        raise SourceCatalogError(f"invalid category for {source.source_id}: {source.category}")
    if source.auth_mode not in AUTH_MODES:
        raise SourceCatalogError(f"invalid auth_mode for {source.source_id}: {source.auth_mode}")
    if source.storage_policy not in STORAGE_POLICIES:
        raise SourceCatalogError(
            f"invalid storage_policy for {source.source_id}: {source.storage_policy}"
        )
    if not source.base_url.startswith(("https://", "http://")):
        raise SourceCatalogError(f"invalid base_url for {source.source_id}: {source.base_url}")
    _require_nonempty(source.site_name, "site_name", source.source_id)
    _require_nonempty(source.extraction_signal, "extraction_signal", source.source_id)
    _require_nonempty(source.access_strategy, "access_strategy", source.source_id)
    _require_nonempty(source.data_shape, "data_shape", source.source_id)
    _require_nonempty(source.production_gate, "production_gate", source.source_id)


def _require_nonempty(value: str, field: str, source_id: str) -> None:
    if not value.strip():
        raise SourceCatalogError(f"{field} must be non-empty for {source_id}")


def _read_json_object(path: Path) -> Mapping[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise SourceCatalogError(f"unable to read source catalog {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise SourceCatalogError(f"invalid JSON in source catalog {path}: {exc}") from exc
    return _as_mapping(payload, "source catalog")


def _as_mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise SourceCatalogError(f"{label} must be an object")
    return cast(Mapping[str, object], value)


def _sequence(payload: Mapping[str, object], key: str) -> Sequence[object]:
    value = _required(payload, key)
    if not isinstance(value, list):
        raise SourceCatalogError(f"{key} must be an array")
    return value


def _string(payload: Mapping[str, object], key: str) -> str:
    value = _required(payload, key)
    if not isinstance(value, str) or not value.strip():
        raise SourceCatalogError(f"{key} must be a non-empty string")
    return value


def _int(payload: Mapping[str, object], key: str) -> int:
    value = _required(payload, key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise SourceCatalogError(f"{key} must be an integer")
    return value


def _required(payload: Mapping[str, object], key: str) -> object:
    if key not in payload:
        raise SourceCatalogError(f"missing required key: {key}")
    value: Any = payload[key]
    return value
