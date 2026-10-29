from __future__ import annotations

import json

from alt_data.catalog import CATALOG_PATH, CATEGORY_ORDER, load_source_catalog


def test_source_catalog_contains_exact_research_universe() -> None:
    catalog = load_source_catalog()

    assert catalog.schema_version == 1
    assert catalog.runtime_status == "future_migration_registry"
    assert catalog.research_input == "LLM Trading Data Source Catalog.md"
    assert len(catalog.sources) == 50
    assert [source.catalog_number for source in catalog.sources] == list(range(1, 51))
    assert len(catalog.by_source_id()) == 50


def test_source_catalog_preserves_family_waves() -> None:
    catalog = load_source_catalog()

    assert tuple(catalog.category_counts()) == CATEGORY_ORDER
    assert catalog.category_counts() == {
        "core_financial_infrastructure": 10,
        "global_macro": 10,
        "public_sentiment": 10,
        "alternative_real_world_assets": 10,
        "systemic_interdependence": 10,
    }


def test_source_catalog_includes_required_anchor_sources() -> None:
    sources = load_source_catalog().by_source_id()

    assert sources["sec_edgar"].base_url == "https://data.sec.gov"
    assert sources["jpx_jquants_v2"].auth_mode == "api_key_header"
    assert sources["openfigi"].data_shape == "identifier_mapping"
    assert sources["linkup"].storage_policy == "no_raw_storage"
    assert "invalid SOG" in sources["spire_maritime"].production_gate
    assert "actual average" in sources["electricity_maps"].production_gate
    assert sources["acled_cast"].catalog_number == 50


def test_source_catalog_does_not_store_secret_values() -> None:
    payload = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    serialized = json.dumps(payload, sort_keys=True).lower()

    forbidden_literals = (
        "sk-",
        "bearer ey",
        "api_key=",
        "api-token",
        "password=",
        "secret=",
    )
    assert all(literal not in serialized for literal in forbidden_literals)


def test_source_catalog_sets_production_gates_for_every_source() -> None:
    catalog = load_source_catalog()

    assert all(source.production_gate.startswith(("Confirm", "Implement", "Use", "Prefer")) for source in catalog.sources)
    assert all(source.storage_policy != "metadata_summary_and_licensed_raw" or "Confirm" in source.production_gate for source in catalog.sources)
