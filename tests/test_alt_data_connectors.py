from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from alt_data.connectors import (
    connector_plans,
    dedupe_connector_results,
    public_summary_signal_result,
    source_health_summary,
    telegram_metadata_result,
    unavailable_connector_result,
)
from alt_data.contracts import AltDataContractError
from alt_data.registry import load_source_registry

AS_OF = datetime(2026, 6, 8, 17, 0, tzinfo=UTC)


def test_connector_plans_cover_catalog_and_telegram_without_live_activation() -> None:
    registry = load_source_registry()
    plans = connector_plans(registry)
    by_source_id = {plan.source_id: plan for plan in plans}

    assert len(plans) == 51
    assert by_source_id["sec_edgar"].wave == "first_connector_wave"
    assert by_source_id["fred"].wave == "first_connector_wave"
    assert by_source_id["bureau_of_labor_statistics"].wave == "first_connector_wave"
    assert by_source_id["gdelt"].wave == "first_connector_wave"
    assert by_source_id["telegram_tradingview"].wave == "telegram_public_metadata"
    assert all(not plan.enabled_for_paper for plan in plans)
    assert all(plan.live_disabled for plan in plans)


def test_unavailable_connector_result_validates_source_health_contract() -> None:
    source = load_source_registry().by_source_id()["sec_edgar"]

    result = unavailable_connector_result(source, AS_OF, reason="legal_approval_required")

    assert result.raw_observation.source_id == "sec_edgar"
    assert result.raw_observation.freshness == "unavailable"
    assert result.raw_observation.storage_uri == "metadata://source-health/sec_edgar/20260608T170000Z"
    assert result.raw_observation.storage_policy == "metadata_summary_and_licensed_raw"
    assert result.normalized_signals == ()
    assert result.source_health.freshness == "unavailable"


def test_telegram_metadata_result_is_metadata_only_and_validated() -> None:
    entry = load_source_registry().telegram_watchlist[0]

    result = telegram_metadata_result(
        entry,
        AS_OF,
        message_id=42,
        posted_at=AS_OF - timedelta(minutes=3),
        summary="Public channel metadata summary mentioning $SPY and broad market breadth.",
        cashtags=("$SPY", "QQQ"),
        view_count=1_234,
        forward_count=12,
    )

    assert result.raw_observation.source_id == "telegram_tradingview"
    assert result.raw_observation.storage_uri == "metadata://telegram/tradingview/42"
    assert result.raw_observation.entity_refs == ("telegram:tradingview",)
    assert result.raw_observation.instrument_refs == ("SPY", "QQQ")
    assert result.raw_observation.quality_flags[-2:] == ("views:1234", "forwards:12")
    assert result.raw_observation.storage_policy == "metadata_summary_only"
    assert len(result.normalized_signals) == 1
    assert result.normalized_signals[0].entity_refs == ("telegram:tradingview",)
    assert "raw" not in result.normalized_signals[0].summary.lower()


def test_public_summary_signal_result_validates_summary_only_event() -> None:
    source = load_source_registry().by_source_id()["gdelt"]

    result = public_summary_signal_result(
        source,
        AS_OF,
        summary="Public event summary says Red Sea shipping risk is raising oil supply uncertainty.",
        asset_refs=("XLE", "GLD"),
        entity_refs=("red_sea_shipping",),
        region_refs=("MIDDLE_EAST",),
        signal_family="geopolitics",
        direction_hint="risk_off",
        confidence=0.72,
        magnitude=0.65,
    )

    assert result.raw_observation.source_id == "gdelt"
    assert result.raw_observation.storage_uri.startswith("metadata://public-summary/gdelt/")
    assert result.raw_observation.instrument_refs == ("XLE", "GLD")
    assert result.normalized_signals[0].signal_family == "geopolitics"
    assert result.normalized_signals[0].direction_hint == "risk_off"
    assert result.source_health.freshness == "fresh"


def test_connector_result_dedupe_and_health_summary() -> None:
    registry = load_source_registry()
    entry = registry.telegram_watchlist[0]
    first = telegram_metadata_result(
        entry,
        AS_OF,
        message_id=42,
        posted_at=AS_OF,
        summary="Public channel metadata summary.",
    )
    duplicate = telegram_metadata_result(
        entry,
        AS_OF,
        message_id=42,
        posted_at=AS_OF,
        summary="Public channel metadata summary.",
    )
    unavailable = unavailable_connector_result(
        registry.by_source_id()["sec_edgar"],
        AS_OF,
        reason="legal_approval_required",
    )

    deduped = dedupe_connector_results([first, duplicate, unavailable])

    assert deduped == (first, unavailable)
    assert source_health_summary(deduped) == {"fresh": 1, "stale": 0, "unavailable": 1}


def test_telegram_metadata_rejects_secret_like_summary() -> None:
    entry = load_source_registry().telegram_watchlist[0]

    with pytest.raises(AltDataContractError, match="secret-like assignment"):
        telegram_metadata_result(
            entry,
            AS_OF,
            message_id=99,
            posted_at=AS_OF,
            summary="api_key = NOT_A_REAL_SECRET",
        )
