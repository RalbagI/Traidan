from __future__ import annotations

import json
from pathlib import Path

import pytest
from alt_data.registry import (
    SourceRegistryError,
    load_source_registry,
    load_telegram_watchlist,
)


def test_source_registry_preserves_catalog_and_adds_telegram_watchlist() -> None:
    registry = load_source_registry()

    assert len(registry.catalog.sources) == 50
    assert len(registry.catalog.by_source_id()) == 50
    assert len(registry.telegram_watchlist) == 1
    assert len(registry.sources) == 51
    assert registry.by_source_id()["telegram_tradingview"].kind == "telegram"
    assert registry.by_source_id()["telegram_tradingview"].storage_policy == "metadata_summary_only"


def test_source_registry_starts_with_paper_disabled_and_live_disabled() -> None:
    registry = load_source_registry()

    assert registry.active_paper_sources() == ()
    assert registry.live_enabled_sources() == ()
    assert all(source.gates["configured"] for source in registry.sources)
    assert all(source.gates["live_disabled"] for source in registry.sources)


def test_telegram_watchlist_requires_public_metadata_only() -> None:
    entry = load_telegram_watchlist()[0]

    assert entry.source_id == "telegram_tradingview"
    assert entry.channel_username == "@tradingview"
    assert entry.visibility == "public"
    assert entry.storage_policy == "metadata_summary_only"
    assert entry.raw_text_allowed is False
    assert entry.license_approved is True


def test_telegram_watchlist_allows_mixed_case_public_usernames(tmp_path: Path) -> None:
    payload = {
        "schema_version": 1,
        "runtime_status": "public_metadata_watchlist",
        "entries": [{
            "source_id": "telegram_cnbc",
            "watch_id": "telegram-cnbc",
            "channel_username": "@CNBC_Official",
            "channel_url": "https://t.me/CNBC_Official",
            "display_name": "CNBC Official",
            "visibility": "public",
            "verification_status": "verified_public",
            "storage_policy": "metadata_summary_only",
            "raw_text_allowed": False,
            "license_approved": True,
            "quality_flags": ["official_verified"],
            "production_gate": "Confirm source licensing before any runtime use.",
        }],
    }
    path = tmp_path / "telegram_watchlist.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    assert load_telegram_watchlist(path)[0].channel_username == "@CNBC_Official"


def test_telegram_watchlist_rejects_private_or_pump_sources(tmp_path: Path) -> None:
    payload = {
        "schema_version": 1,
        "runtime_status": "public_metadata_watchlist",
        "entries": [{
            "source_id": "telegram_bad_pump",
            "watch_id": "bad-pump",
            "channel_username": "@badpump",
            "channel_url": "https://t.me/badpump",
            "display_name": "Bad Pump",
            "visibility": "private",
            "verification_status": "verified_public",
            "storage_policy": "metadata_summary_only",
            "raw_text_allowed": False,
            "license_approved": True,
            "quality_flags": ["pump_group"],
            "production_gate": "Confirm source licensing before any runtime use.",
        }],
    }
    path = tmp_path / "telegram_watchlist.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(SourceRegistryError, match="telegram source must be public"):
        load_telegram_watchlist(path)

    payload["entries"][0]["visibility"] = "public"  # type: ignore[index]
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(SourceRegistryError, match="blocked quality flags"):
        load_telegram_watchlist(path)


def test_telegram_watchlist_rejects_invalid_username_and_url(tmp_path: Path) -> None:
    payload = {
        "schema_version": 1,
        "runtime_status": "public_metadata_watchlist",
        "entries": [{
            "source_id": "telegram_bad_url",
            "watch_id": "bad-url",
            "channel_username": "@bad",
            "channel_url": "https://example.com/bad",
            "display_name": "Bad URL",
            "visibility": "public",
            "verification_status": "verified_public",
            "storage_policy": "metadata_summary_only",
            "raw_text_allowed": False,
            "license_approved": True,
            "quality_flags": ["official_verified"],
            "production_gate": "Confirm source licensing before any runtime use.",
        }],
    }
    path = tmp_path / "telegram_watchlist.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(SourceRegistryError, match="public @username"):
        load_telegram_watchlist(path)

    payload["entries"][0]["channel_username"] = "@valid_name"  # type: ignore[index]
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(SourceRegistryError, match="HTTPS Telegram URL"):
        load_telegram_watchlist(path)
