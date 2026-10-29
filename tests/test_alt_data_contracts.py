from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

import pytest
from alt_data.contracts import (
    AltDataContractError,
    validate_llm_context_pack,
    validate_llm_trade_intent,
    validate_normalized_signal_event,
    validate_raw_observation_event,
    validate_risk_approved_order,
)
from jsonschema import Draft202012Validator, ValidationError

AS_OF = datetime(2026, 6, 1, 14, 30, tzinfo=UTC).isoformat().replace("+00:00", "Z")
ROOT = Path(__file__).resolve().parents[1]
BLUEPRINT_PATH = ROOT / "docs" / "multi_asset_llm_pipeline_blueprint.md"


def schema_validator(name: str) -> Draft202012Validator:
    schema = json.loads((ROOT / "schemas" / name).read_text(encoding="utf-8"))
    return Draft202012Validator(schema)


def validate_schema(name: str, payload: dict[str, object]) -> None:
    schema_validator(name).validate(payload)


def assert_schema_rejects(name: str, payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        validate_schema(name, payload)


def valid_raw_observation() -> dict[str, object]:
    return {
        "schema_version": 1,
        "observation_id": "obs-sec-1",
        "source_id": "sec_edgar",
        "retrieved_at": AS_OF,
        "event_time": AS_OF,
        "payload_hash": "sha256:" + "a" * 64,
        "storage_uri": "s3://traidan-alt-data/raw/sec/obs-sec-1.json",
        "parser_version": "sec-edgar-v1",
        "freshness": "fresh",
        "http_status": 200,
        "entity_refs": ["CIK:0000320193"],
        "instrument_refs": ["FIGI:BBG000B9XRY4"],
        "geo_refs": [],
        "quality_flags": [],
        "storage_policy": "metadata_summary_and_licensed_raw",
    }


def valid_signal() -> dict[str, object]:
    return {
        "schema_version": 1,
        "signal_id": "sig-sec-1",
        "source_id": "sec_edgar",
        "observation_id": "obs-sec-1",
        "as_of": AS_OF,
        "event_time": AS_OF,
        "asset_refs": ["FIGI:BBG000B9XRY4"],
        "entity_refs": ["CIK:0000320193"],
        "region_refs": ["US"],
        "signal_family": "fundamental_event",
        "horizon": "medium",
        "direction_hint": "bearish",
        "confidence": 0.61,
        "magnitude": 0.2,
        "freshness": "fresh",
        "summary": "Risk-factor language became more cautious in the latest filing.",
        "evidence_refs": ["obs-sec-1"],
        "quality_flags": [],
    }


def valid_context_pack() -> dict[str, object]:
    return {
        "schema_version": 1,
        "context_id": "ctx-1",
        "cycle_id": "cycle-1",
        "as_of": AS_OF,
        "portfolio_state_ref": "portfolio-state-1",
        "risk_state_ref": "risk-state-1",
        "approved_instrument_ids": ["FIGI:BBG000B9XRY4"],
        "signal_refs": ["sig-sec-1"],
        "graph_refs": ["edge-issuer-sector-1"],
        "historical_analog_refs": ["analog-1"],
        "unavailable_source_ids": ["linkup"],
        "stale_source_ids": [],
        "risk_constraints": {
            "max_gross_exposure": 0.95,
            "max_single_instrument_weight": 0.12,
        },
    }


def valid_trade_intent() -> dict[str, object]:
    return {
        "schema_version": 1,
        "cycle_id": "cycle-1",
        "sequence": 1,
        "as_of": AS_OF,
        "model_id": "gpt-5.6-sol",
        "prompt_template_hash": "sha256:" + "b" * 64,
        "commands": [{
            "instrument_id": "FIGI:BBG000B9XRY4",
            "direction": "short",
            "target_weight": -0.05,
            "max_notional_usd": 500.0,
            "horizon": "medium",
            "confidence": 0.61,
            "thesis": "Validated filing evidence supports a small short-intent candidate.",
            "evidence_refs": ["sig-sec-1"],
        }],
        "risk_assessment": {
            "summary": "Small notional and evidence-backed thesis.",
            "uncertainty": "Single-source signal requires deterministic risk approval.",
            "risk_limit_verdict": "review",
        },
        "memory_updates": ["Watch whether follow-up disclosures confirm the thesis."],
    }


def flat_trade_intent() -> dict[str, object]:
    intent = valid_trade_intent()
    intent["commands"][0]["direction"] = "flat"  # type: ignore[index]
    intent["commands"][0]["target_weight"] = 1e-12  # type: ignore[index]
    return intent


def valid_risk_order() -> dict[str, object]:
    return {
        "schema_version": 1,
        "approval_id": "approval-1",
        "cycle_id": "cycle-1",
        "intent_sequence": 1,
        "as_of": AS_OF,
        "verdict": "approved",
        "reason": "Inside canary paper limits.",
        "instrument_id": "FIGI:BBG000B9XRY4",
        "side": "sell_short",
        "quantity": 1.0,
        "notional_usd": 500.0,
        "route": "paper",
        "time_in_force": "day",
        "context_id": "ctx-1",
        "intent_ref": "cycle-1:1",
        "risk_snapshot_ref": "risk-state-1",
        "instrument_master_version": "im-2026-06-01",
    }


def test_future_contracts_accept_minimal_valid_payloads() -> None:
    validate_schema("raw_observation_event.schema.json", valid_raw_observation())
    validate_schema("normalized_signal_event.schema.json", valid_signal())
    validate_schema("llm_context_pack.schema.json", valid_context_pack())
    validate_schema("llm_trade_intent.schema.json", valid_trade_intent())
    validate_schema("risk_approved_order.schema.json", valid_risk_order())

    assert validate_raw_observation_event(valid_raw_observation()).source_id == "sec_edgar"
    assert validate_normalized_signal_event(valid_signal()).direction_hint == "bearish"
    assert validate_llm_context_pack(valid_context_pack()).unavailable_source_ids == ("linkup",)
    assert validate_llm_trade_intent(valid_trade_intent()).commands[0]["direction"] == "short"
    assert validate_risk_approved_order(valid_risk_order()).verdict == "approved"


def test_future_contract_schema_files_are_valid_json_and_closed() -> None:
    schema_names = [
        "raw_observation_event.schema.json",
        "normalized_signal_event.schema.json",
        "llm_context_pack.schema.json",
        "llm_trade_intent.schema.json",
        "risk_approved_order.schema.json",
    ]

    for name in schema_names:
        schema = json.loads((ROOT / "schemas" / name).read_text(encoding="utf-8"))
        assert schema["additionalProperties"] is False
        assert schema["properties"]["schema_version"]["const"] == 1


def test_blueprint_references_implemented_future_contracts() -> None:
    blueprint = BLUEPRINT_PATH.read_text(encoding="utf-8")

    assert "intelligence/alt_data/contracts.py" in blueprint
    for schema_name in (
        "raw observations",
        "normalized signals",
        "LLM context packs",
        "LLM trade intent",
        "risk-approved orders",
    ):
        assert schema_name in blueprint


def test_future_contracts_reject_unknown_raw_and_secret_fields() -> None:
    raw = valid_raw_observation()
    raw["raw_prompt"] = "hidden"
    with pytest.raises(AltDataContractError, match="forbidden raw/secret keys"):
        validate_raw_observation_event(raw)

    signal = valid_signal()
    signal["secret"] = "hidden"
    with pytest.raises(AltDataContractError, match="forbidden raw/secret keys"):
        validate_normalized_signal_event(signal)

    context = valid_context_pack()
    context["debug_extra"] = "not allowed"
    with pytest.raises(AltDataContractError):
        validate_llm_context_pack(context)


def test_raw_observation_requires_catalog_storage_policy() -> None:
    raw = valid_raw_observation()
    raw["storage_policy"] = "metadata_summary_and_public_raw"

    with pytest.raises(AltDataContractError, match="storage_policy must match source registry"):
        validate_raw_observation_event(raw)


def test_trade_intent_requires_evidence_and_bounded_short_weight() -> None:
    intent = valid_trade_intent()
    intent["commands"][0]["evidence_refs"] = []  # type: ignore[index]
    with pytest.raises(AltDataContractError):
        validate_llm_trade_intent(intent)

    intent = valid_trade_intent()
    intent["commands"][0]["target_weight"] = -1.25  # type: ignore[index]
    with pytest.raises(AltDataContractError):
        validate_llm_trade_intent(intent)


def test_trade_intent_schema_rejects_direction_weight_mismatch() -> None:
    long_negative = deepcopy(valid_trade_intent())
    long_negative["commands"][0]["direction"] = "long"  # type: ignore[index]
    long_negative["commands"][0]["target_weight"] = -0.05  # type: ignore[index]

    short_positive = deepcopy(valid_trade_intent())
    short_positive["commands"][0]["target_weight"] = 0.05  # type: ignore[index]

    flat_non_zero = deepcopy(valid_trade_intent())
    flat_non_zero["commands"][0]["direction"] = "flat"  # type: ignore[index]
    flat_non_zero["commands"][0]["target_weight"] = 0.01  # type: ignore[index]

    for payload in (long_negative, short_positive, flat_non_zero):
        assert_schema_rejects("llm_trade_intent.schema.json", payload)
        with pytest.raises(AltDataContractError):
            validate_llm_trade_intent(payload)


def test_trade_intent_normalizes_near_zero_flat_weight() -> None:
    intent = validate_llm_trade_intent(flat_trade_intent())

    assert intent.commands[0]["target_weight"] == 0.0


def test_contracts_allow_secret_incident_summary_but_reject_literal_assignment() -> None:
    signal = valid_signal()
    signal["summary"] = "News mentions an API key leak: company X rotated credentials."
    assert validate_normalized_signal_event(signal).summary == signal["summary"]

    signal = valid_signal()
    signal["summary"] = "api_key = NOT_A_REAL_SECRET"
    with pytest.raises(AltDataContractError, match="secret-like assignment"):
        validate_normalized_signal_event(signal)


def test_context_pack_rejects_duplicate_reference_lists() -> None:
    context = valid_context_pack()
    context["stale_source_ids"] = ["fred", "fred"]

    assert_schema_rejects("llm_context_pack.schema.json", context)
    with pytest.raises(AltDataContractError, match="duplicate"):
        validate_llm_context_pack(context)


def test_risk_approved_order_requires_hold_shape_for_non_approved_verdicts() -> None:
    order = valid_risk_order()
    order["verdict"] = "rejected"
    order["quantity"] = 1.0
    assert_schema_rejects("risk_approved_order.schema.json", order)
    with pytest.raises(AltDataContractError):
        validate_risk_approved_order(order)

    order = valid_risk_order()
    order["verdict"] = "rejected"
    order["side"] = "hold"
    order["quantity"] = 0.0
    order["notional_usd"] = 0.0
    assert validate_risk_approved_order(order).verdict == "rejected"
