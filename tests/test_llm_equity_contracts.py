from __future__ import annotations

import builtins
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, ValidationError
from llm_equity.alpaca import AlpacaMarketDataClient, AlpacaMarketDataConfig
from llm_equity.contracts import (
    FORCED_RTH_CYCLE_KINDS,
    LlmEquityContractError,
    load_equity_universe,
    sanitize_memory_event,
    snapshot_requires_hold,
    validate_alpha_card,
    validate_cycle_audit_record,
    validate_llm_decision,
    validate_llm_review_result,
    validate_market_research_snapshot,
    validate_swarm_reflection,
    validate_trade_slate,
)

ROOT = Path(__file__).resolve().parents[1]


def test_forced_rth_cycle_kinds_are_shared_execution_contract() -> None:
    assert FORCED_RTH_CYCLE_KINDS == frozenset(
        {"run_once", "discovery_scan", "news_scan", "release"}
    )


def schema_validator(name: str) -> Draft202012Validator:
    schema = json.loads((ROOT / "schemas" / name).read_text(encoding="utf-8"))
    return Draft202012Validator(schema)


def schema_keywords(value: object, *, property_values: bool = False) -> set[str]:
    if isinstance(value, dict):
        keywords: set[str] = set()
        for key, child in value.items():
            if property_values:
                keywords.update(schema_keywords(child))
                continue
            keywords.add(key)
            keywords.update(schema_keywords(child, property_values=key == "properties"))
        return keywords
    if isinstance(value, list):
        return set().union(*(schema_keywords(child) for child in value))
    return set()


def valid_memory_event() -> dict[str, object]:
    return {
        "event_id": "memory-1",
        "cycle_id": "cycle-1",
        "created_at": "2026-05-28T14:30:00Z",
        "thesis": "ראיות טריות תומכות בכניסת סימולציה קטנה.",
        "outcome": "בהמתנה.",
        "conclusion": "לשמור על גודל קטן עד שההמשכיות משתפרת.",
        "prompt_adjustment": "לבדוק ראיות מיושנות לפני כל פקודה.",
        "next_cycle_watch_items": ["ריביות", "רוחב שוק"],
    }


def valid_decision_payload() -> dict[str, object]:
    as_of = datetime(2026, 5, 28, 14, 30, tzinfo=UTC).isoformat().replace("+00:00", "Z")
    return {
        "cycle_id": "cycle-1",
        "sequence": 1,
        "as_of": as_of,
        "market_session": "rth",
        "portfolio_nav_usd": 10_000.0,
        "commands": [
            {
                "action": "buy_notional",
                "symbol": "SPY",
                "rationale": "ראיות טריות תומכות בכניסת סימולציה קטנה.",
                "confidence": 0.7,
                "max_notional_usd": 100.0,
                "evidence_refs": ["ev-1"],
            }
        ],
        "evidence": [
            {
                "id": "ev-1",
                "source": "market_research_snapshot",
                "summary": "הקשר טרי.",
                "retrieved_at": as_of,
                "freshness": "fresh",
            }
        ],
        "risk_assessment": {
            "summary": "בתוך המגבלות.",
            "max_loss_scenario": "פקודת סימולציה קטנה.",
            "risk_limit_verdict": "pass",
        },
        "memory_updates": [valid_memory_event()],
    }


def valid_review_result_payload() -> dict[str, object]:
    return {
        "schema_version": 2,
        "verdict": "trade",
        "intents": [
            {
                "candidate_id": "candidate-1-SPY-long",
                "direction": "long",
                "horizon": "60m",
                "confidence": 0.72,
                "thesis": "האירוע והמחיר תומכים בעסקת נייר מוגבלת.",
                "invalidation": "המחיר חוזר מתחת לרמת האירוע.",
                "notional_usd": 25.0,
                "evidence_refs": ["snapshot"],
            }
        ],
        "risk_flags": ["event_decay"],
        "abstain_reason": None,
    }


def valid_review_result_v3_payload() -> dict[str, object]:
    payload = valid_review_result_payload()
    payload["schema_version"] = 3
    payload["intents"][0].update(  # type: ignore[index, union-attr]
        {
            "edge_class": "positive_edge",
            "expected_excess_return_bps": 24.0,
            "probability_positive": 0.61,
            "return_p10_bps": -35.0,
            "return_p50_bps": 18.0,
            "return_p90_bps": 85.0,
        }
    )
    return payload


def valid_snapshot_payload() -> dict[str, object]:
    as_of = datetime(2026, 5, 28, 14, 30, tzinfo=UTC).isoformat().replace("+00:00", "Z")
    return {
        "snapshot_id": "snap-1",
        "as_of": as_of,
        "market_session": "rth",
        "symbols": ["SPY"],
        "latest_bars": {
            "SPY": {
                "timestamp": as_of,
                "open": 500.0,
                "high": 501.0,
                "low": 499.0,
                "close": 500.0,
                "volume": 1_000_000,
                "source": "alpaca_market_data",
            },
        },
        "latest_quotes": {
            "SPY": {
                "timestamp": as_of,
                "bid": 499.95,
                "ask": 500.05,
                "source": "alpaca_market_data",
            },
        },
        "market_calendar_state": {
            "source": "alpaca_market_clock",
            "retrieved_at": as_of,
            "is_open": True,
            "next_open": "2026-05-29T13:30:00Z",
            "next_close": "2026-05-28T20:00:00Z",
            "freshness": "fresh",
        },
        "trend_summaries": {"SPY": "שטוח."},
        "source_freshness": [
            {"source": "alpaca_market_data", "freshness": "fresh"},
            {"source": "alpaca_market_clock", "freshness": "fresh"},
            {"source": "macro_calendar", "freshness": "fresh"},
            {"source": "news_summary", "freshness": "fresh"},
        ],
        "macro_news_summaries": [
            {
                "source": "macro_calendar",
                "url": "https://example.com/calendar",
                "retrieved_at": as_of,
                "summary": "הקשר מאקרו טרי.",
                "freshness": "fresh",
            },
            {
                "source": "news_summary",
                "url": "https://example.com/news",
                "retrieved_at": as_of,
                "summary": "הקשר חדשות טרי.",
                "freshness": "fresh",
            },
        ],
        "unavailable_data_flags": [],
    }


def valid_cycle_audit_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "cycle_kind": "run_once",
        "status": "posted",
        "cycle_id": "snap-1",
        "created_at": "2026-05-28T14:30:01Z",
        "target_release_at": None,
        "market_session": "rth",
        "as_of": "2026-05-28T14:30:00Z",
        "symbols_sample": ["SPY"],
        "context": {
            "sha256": "a" * 64,
            "json_bytes": 12345,
            "symbol_count": 1,
        },
        "decision": {
            "sha256": "b" * 64,
            "json_bytes": 2048,
        },
        "attempts": [
            {
                "attempt": 1,
                "status": "valid",
                "decision_sha256": "b" * 64,
                "decision_json_bytes": 2048,
            }
        ],
        "costs": {
            "cycle_cost_usd": 0.01,
            "monthly_model_cost_usd": 1.47,
            "required_monthly_alpha_usd": 101.47,
        },
        "rust_response": {
            "status": "success",
            "cycle_id": "snap-1",
            "path": "/internal/llm-equity/cycle",
        },
        "artifact_refs": {
            "audit_record": "cycle-audit/snap-1.json",
            "latest_decision": "latest-decision.json",
        },
        "swarm_agent_count": 8,
        "alpha_card_count": 8,
        "consensus_score": 0.76,
        "edge_gate_status": "pass",
        "selected_strategy_book": "small_consensus",
        "no_trade_reason": None,
        "portfolio_nav_usd": 10_000.0,
        "total_pnl_usd": 0.0,
        "gross_exposure_pct": 0.0,
        "net_alpha_usd": {"SPY": -0.01, "QQQ": -0.01},
        "llm_invoked": True,
        "cost_basis": "actual_llm_invocation",
        "validation_repair_count": 0,
        "exploration_mode": "max_discovery",
        "speculative_candidate_count": 2,
        "exploration_candidate_count": 1,
        "speculative_command_count": 1,
        "speculative_command_notional_usd": 25.0,
        "discovery_lane_counts": {"extreme_momentum": 1},
        "risk_budget_status": "within_budget",
        "review_attestation": {
            "required": True,
            "status": "compliant",
            "model": "gpt-5.6-sol",
            "reasoning_effort": "xhigh",
            "invocation_count": 1,
            "order_action": "buy_notional",
            "order_notional_usd": 25.0,
            "accepted_side": "buy",
            "accepted_notional_usd": 25.0,
        },
    }


def valid_cycle_audit_v2_payload() -> dict[str, object]:
    payload = valid_cycle_audit_payload()
    payload["schema_version"] = 2
    payload["mandatory_trade_required"] = True
    payload["mandatory_trade_status"] = "accepted"
    payload["parent_cycle_id"] = "snap-1"
    payload["recovery_attempt_count"] = 0
    payload["rust_rejection_reasons"] = []
    payload["mandatory_accepted_notional_usd"] = 25.0
    payload["mandatory_required_total_notional_usd"] = 25.0
    payload["mandatory_accepted_total_notional_usd"] = 25.0
    payload["mandatory_remaining_notional_usd"] = 0.0
    payload["dashboard_summary_thesis"] = (
        "Fresh breadth and macro context supported a minimum paper buy."
    )
    payload["execution_attempts"] = [
        {
            "cycle_id": "snap-1",
            "requested_notional_usd": 25.0,
            "accepted_notional_usd": 25.0,
            "remaining_notional_usd": 0.0,
            "rust_rejection_reasons": [],
        }
    ]
    payload["accepted_trades"] = [
        {
            "symbol": "SPY",
            "action": "buy",
            "accepted_notional_usd": 25.0,
            "horizon": "60m",
            "outcome_status": "pending",
            "exposure_effect": "add",
        }
    ]
    payload["review_attestation"] = {
        **payload["review_attestation"],  # type: ignore[dict-item]
        "accepted_total_notional_usd": 25.0,
    }
    return payload


def valid_cycle_audit_v3_payload() -> dict[str, object]:
    payload = valid_cycle_audit_v2_payload()
    payload.update(
        {
            "schema_version": 3,
            "strategy_epoch_id": "epoch-0123456789abcdef",
            "strategy_fingerprint": "a" * 64,
            "risk_mode": "forced_aggressive",
            "forecast": [
                {
                    "edge_class": "positive_edge",
                    "expected_excess_return_bps": 24.0,
                    "probability_positive": 0.61,
                    "return_p10_bps": -35.0,
                    "return_p50_bps": 18.0,
                    "return_p90_bps": 85.0,
                }
            ],
            "source_health_reasons": [],
            "cost_basis": "subscription_zero",
            "imputed_api_cost_usd": 0.42,
            "forced_trade_status": "accepted",
        }
    )
    return payload


def test_compact_review_result_contract_supports_trade_and_abstention() -> None:
    trade = valid_review_result_payload()
    assert validate_llm_review_result(trade)["verdict"] == "trade"

    abstain = {
        "schema_version": 2,
        "verdict": "abstain",
        "intents": [],
        "risk_flags": ["insufficient_edge"],
        "abstain_reason": "היתרון אינו מכסה עלויות ואי ודאות.",
    }
    assert validate_llm_review_result(abstain)["verdict"] == "abstain"

    schema_validator("llm_review_result_v2.schema.json").validate(trade)
    schema_validator("llm_review_result_v2.schema.json").validate(abstain)

    with pytest.raises(ValidationError):
        schema_validator("llm_review_result_v2.schema.json").validate({**trade, "intents": []})
    with pytest.raises(ValidationError):
        schema_validator("llm_review_result_v2.schema.json").validate(
            {**abstain, "intents": trade["intents"]}
        )

    invalid = {**trade, "model_id": "model-owned-metadata"}
    with pytest.raises(LlmEquityContractError, match="review result schema violation"):
        validate_llm_review_result(invalid)


def test_review_result_v3_requires_a_finite_ordered_forecast() -> None:
    payload = valid_review_result_v3_payload()

    assert validate_llm_review_result(payload) == payload
    schema_validator("llm_review_result_v3.schema.json").validate(payload)

    for key, value in (
        ("probability_positive", 1.01),
        ("expected_excess_return_bps", 5_001.0),
        ("return_p10_bps", float("nan")),
    ):
        invalid = json.loads(json.dumps(payload))
        invalid["intents"][0][key] = value
        with pytest.raises(LlmEquityContractError):
            validate_llm_review_result(invalid)

    unordered = json.loads(json.dumps(payload))
    unordered["intents"][0].update(
        {"return_p10_bps": 20.0, "return_p50_bps": 10.0, "return_p90_bps": 30.0}
    )
    with pytest.raises(LlmEquityContractError, match="p10 <= p50 <= p90"):
        validate_llm_review_result(unordered)


def test_codex_review_v3_transport_and_strict_schema_remain_separate() -> None:
    transport = schema_validator("llm_review_result_v3.codex.schema.json")
    strict = schema_validator("llm_review_result_v3.schema.json")
    payload = valid_review_result_v3_payload()

    transport.validate(payload)
    strict.validate(payload)

    out_of_range = json.loads(json.dumps(payload))
    out_of_range["intents"][0]["probability_positive"] = 2.0
    transport.validate(out_of_range)
    with pytest.raises(ValidationError):
        strict.validate(out_of_range)


def test_codex_review_output_schema_uses_only_supported_composition() -> None:
    transport = schema_validator("llm_review_result_v2.codex.schema.json")
    transport.validate(valid_review_result_payload())

    assert schema_keywords(transport.schema) == {
        "$schema",
        "$id",
        "title",
        "type",
        "additionalProperties",
        "required",
        "properties",
        "enum",
        "maxItems",
        "items",
    }


def test_codex_transport_schema_fails_closed_through_the_strict_contract() -> None:
    transport = schema_validator("llm_review_result_v2.codex.schema.json")
    strict = schema_validator("llm_review_result_v2.schema.json")
    trade = valid_review_result_payload()
    abstain = {
        "schema_version": 2,
        "verdict": "abstain",
        "intents": [],
        "risk_flags": ["insufficient_edge"],
        "abstain_reason": "היתרון אינו מכסה עלויות ואי ודאות.",
    }

    for payload in (
        {**trade, "intents": [{**trade["intents"][0], "confidence": 1.1}]},  # type: ignore[index]
        {**trade, "intents": []},
        {**abstain, "intents": trade["intents"]},
    ):
        transport.validate(payload)
        with pytest.raises(ValidationError):
            strict.validate(payload)
        with pytest.raises(LlmEquityContractError):
            validate_llm_review_result(payload)


def test_compact_review_notional_has_a_floor_but_no_python_maximum() -> None:
    trade = valid_review_result_payload()
    too_small = {**trade, "intents": [{**trade["intents"][0], "notional_usd": 24.99}]}  # type: ignore[index]
    with pytest.raises(LlmEquityContractError, match="review result schema violation"):
        validate_llm_review_result(too_small)

    unbounded = {**trade, "intents": [{**trade["intents"][0], "notional_usd": 9_999_999.0}]}  # type: ignore[index]
    assert validate_llm_review_result(unbounded)["intents"][0]["notional_usd"] == 9_999_999.0  # type: ignore[index]


def valid_alpha_card_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "symbol": "SPY",
        "side": "long",
        "horizon_minutes": 60,
        "edge_type": "news_drift",
        "thesis": "ראיות טריות מרובות מקורות תומכות בלונג נייר קטן.",
        "evidence_refs": ["market_data", "news"],
        "confidence": 0.73,
        "expected_alpha_bps": 90.0,
        "downside_bps": 45.0,
        "spread_bps": 2.0,
        "liquidity_score": 0.9,
        "invalidation": "המרווח מתרחב או שהחדשות מתהפכות.",
        "agent_role": "catalyst_scout",
    }


def valid_trade_slate_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "cycle_id": "snap-1",
        "generated_at": "2026-05-28T14:30:00Z",
        "market_session": "rth",
        "alpha_cards": [valid_alpha_card_payload()],
        "swarm_agent_count": 8,
        "alpha_card_count": 1,
        "consensus_score": 0.73,
        "evidence_family_count": 2,
        "cost_threshold_bps": 32.0,
        "edge_gate_status": "pass",
        "edge_gate_reason": "edge_passed",
        "selected_symbol": "SPY",
        "selected_side": "long",
        "selected_strategy_book": "small_consensus",
        "no_trade_reason": None,
        "strategy_books": {
            "small_consensus": {"nav_usd": 10000.0, "alpha_usd": 0.0},
            "kelly_aggressive": {"nav_usd": 10000.0, "alpha_usd": 0.0},
        },
    }


def valid_swarm_reflection_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "cycle_id": "snap-1",
        "created_at": "2026-05-28T14:30:01Z",
        "strategy_book": "small_consensus",
        "lesson": "לשמור את paper swarm ממוקד ב-edge טרי מרובה מקורות.",
        "outcome": "שער ה-edge עבר עם מרווח נמוך.",
        "next_watch_items": ["מרווח", "היפוך חדשות"],
        "source": "llm_swarm_reflection",
    }


class FakeAlpacaResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


class FakeAlpacaHttpClient:
    def __init__(self) -> None:
        self.requests: list[tuple[str, dict[str, str]]] = []

    def get(
        self,
        url: str,
        *,
        headers: dict[str, str],
        params: dict[str, str],
        timeout: float,
    ) -> FakeAlpacaResponse:
        self.requests.append((url, dict(params)))
        if url.endswith("/v2/stocks/snapshots"):
            return FakeAlpacaResponse(
                {
                    "SPY": {
                        "minuteBar": {
                            "t": "2026-05-28T14:30:00Z",
                            "o": 500.0,
                            "h": 501.0,
                            "l": 499.0,
                            "c": 500.5,
                            "v": 10_000,
                        },
                        "dailyBar": {
                            "t": "2026-05-28T04:00:00Z",
                            "o": 490.0,
                            "h": 501.0,
                            "l": 488.0,
                            "c": 500.5,
                            "v": 2_000_000,
                        },
                        "prevDailyBar": {
                            "t": "2026-05-27T04:00:00Z",
                            "o": 485.0,
                            "h": 492.0,
                            "l": 484.0,
                            "c": 491.0,
                            "v": 3_000_000,
                        },
                        "latestQuote": {
                            "t": "2026-05-28T14:30:00Z",
                            "bp": 500.45,
                            "ap": 500.55,
                            "bs": 10.0,
                            "as": 20.0,
                            "bx": "V",
                            "ax": "Q",
                            "c": ["R"],
                        },
                    }
                }
            )
        if url.endswith("/v2/assets"):
            return FakeAlpacaResponse(
                [
                    {
                        "class": "us_equity",
                        "symbol": "BRK.B",
                        "status": "active",
                        "tradable": True,
                        "name": "Berkshire Hathaway Inc. Class B",
                    },
                    {
                        "class": "us_equity",
                        "symbol": "HALT",
                        "status": "inactive",
                        "tradable": True,
                        "name": "Inactive placeholder",
                    },
                    {
                        "class": "us_equity",
                        "symbol": "BAD/SYM",
                        "status": "active",
                        "tradable": True,
                        "name": "Malformed symbol placeholder",
                    },
                ]
            )
        if url.endswith("/v1beta1/screener/stocks/movers"):
            return FakeAlpacaResponse(
                {
                    "gainers": [
                        {"symbol": "NVDA", "percent_change": 4.2},
                        {"symbol": "BAD/SYM", "percent_change": 9.9},
                    ],
                    "losers": [
                        {"symbol": "TSLA", "percent_change": -3.1},
                        {"symbol": "BADPCT", "percent_change": "n/a"},
                    ],
                }
            )
        return FakeAlpacaResponse(
            {
                "quotes": {
                    "SPY": {
                        "t": "2026-05-28T14:30:00Z",
                        "bp": 499.95,
                        "ap": 0.0,
                    },
                },
            }
        )


def test_alpaca_client_construction_does_not_import_httpx(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_import = builtins.__import__

    def guarded_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "httpx":
            raise AssertionError("httpx must be imported only when a request is made")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    AlpacaMarketDataClient(AlpacaMarketDataConfig(key_id="key", secret_key="secret"))


def test_alpaca_client_uses_default_attempts_for_invalid_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRAIDAN_ALPACA_READ_ATTEMPTS", "off")
    client = AlpacaMarketDataClient(
        AlpacaMarketDataConfig(key_id="key", secret_key="secret"),
        http_client=FakeAlpacaHttpClient(),
    )

    quotes = client.latest_quotes(["SPY"])

    assert quotes["SPY"]["timestamp"] == "2026-05-28T14:30:00Z"


def test_equity_universe_is_dynamic_us_equity_paper_scope() -> None:
    universe = load_equity_universe()

    assert universe.initial_cash_usd == 10_000.0
    assert universe.provider == "alpaca"
    assert universe.require_symbol("SPY") == "SPY"
    assert universe.require_symbol("BRK.B") == "BRK.B"
    assert universe.research_symbol_limit == 100
    with pytest.raises(LlmEquityContractError):
        universe.require_symbol("BAD SYMBOL")


def test_llm_decision_accepts_dynamic_researched_symbol_syntax() -> None:
    payload = valid_decision_payload()
    payload["commands"][0]["symbol"] = "ZZZZZ"  # type: ignore[index]

    assert validate_llm_decision(payload).commands[0].symbol == "ZZZZZ"

    payload["commands"][0]["symbol"] = "BAD SYMBOL"  # type: ignore[index]
    with pytest.raises(LlmEquityContractError, match="invalid equity symbol"):
        validate_llm_decision(payload)


def test_llm_decision_retains_optional_forecast_and_accepts_historical_commands() -> None:
    historical = validate_llm_decision(valid_decision_payload())
    assert historical.commands[0].forecast is None

    payload = valid_decision_payload()
    payload["commands"][0]["forecast"] = {  # type: ignore[index]
        "edge_class": "least_bad_forced",
        "expected_excess_return_bps": -4.0,
        "probability_positive": 0.48,
        "return_p10_bps": -75.0,
        "return_p50_bps": -2.0,
        "return_p90_bps": 50.0,
    }
    command = validate_llm_decision(payload).commands[0]
    assert command.forecast is not None
    assert command.forecast.edge_class == "least_bad_forced"

    payload["commands"][0]["forecast"]["unknown"] = True  # type: ignore[index]
    with pytest.raises(LlmEquityContractError, match="unsupported fields"):
        validate_llm_decision(payload)


def test_equity_universe_instrument_accepts_dynamic_symbol() -> None:
    universe = load_equity_universe()

    instrument = universe.instrument("ZZZZZ")

    assert instrument.symbol == "ZZZZZ"
    assert instrument.asset_class == "equity_stock"
    assert instrument.venue == "us_equities_paper"
    assert instrument.currency == "USD"
    assert instrument.timezone == universe.timezone
    assert instrument.lot_size == 1
    assert instrument.tick_size == 0.01
    with pytest.raises(LlmEquityContractError, match="invalid equity symbol"):
        universe.instrument("BAD SYMBOL")


def test_market_snapshot_accepts_bounded_public_source_records() -> None:
    snapshot = valid_snapshot_payload()
    news_row = next(
        row
        for row in snapshot["macro_news_summaries"]  # type: ignore[union-attr]
        if row["source"] == "news_summary"
    )
    news_row["source_records"] = [
        {
            "url": "https://example.com/source-four",
            "published_at": "2026-05-28T14:00:00Z",
            "content_hash": "sha256:" + "a" * 64,
            "summary": "Micron DRAM supply tightened.",
        }
    ]

    schema_validator("market_research_snapshot.schema.json").validate(snapshot)
    validate_market_research_snapshot(snapshot)


def test_llm_contracts_reject_unknown_and_raw_fields() -> None:
    decision = valid_decision_payload()
    decision["debug_extra"] = "not part of the schema"
    with pytest.raises(LlmEquityContractError):
        validate_llm_decision(decision)

    decision = valid_decision_payload()
    decision["commands"][0]["debug_extra"] = "not part of the schema"  # type: ignore[index]
    with pytest.raises(LlmEquityContractError):
        validate_llm_decision(decision)

    decision = valid_decision_payload()
    decision["evidence"][0]["raw_prompt"] = "hidden"  # type: ignore[index]
    with pytest.raises(LlmEquityContractError):
        validate_llm_decision(decision)

    snapshot = valid_snapshot_payload()
    snapshot["latest_quotes"]["SPY"]["full_text"] = "forbidden"  # type: ignore[index]
    with pytest.raises(LlmEquityContractError):
        validate_market_research_snapshot(snapshot)

    snapshot = valid_snapshot_payload()
    snapshot["market_calendar_state"]["debug_extra"] = "not part of the schema"  # type: ignore[index]
    with pytest.raises(LlmEquityContractError):
        validate_market_research_snapshot(snapshot)


def test_llm_equity_cycle_audit_schema_accepts_scrubbed_metadata() -> None:
    payload = valid_cycle_audit_payload()
    schema_validator("llm_equity_cycle_audit.schema.json").validate(payload)
    assert validate_cycle_audit_record(payload) is payload


def test_llm_equity_cycle_audit_schema_keeps_old_rows_backward_compatible() -> None:
    payload = valid_cycle_audit_payload()
    for key in (
        "portfolio_nav_usd",
        "total_pnl_usd",
        "gross_exposure_pct",
        "net_alpha_usd",
        "llm_invoked",
        "cost_basis",
        "validation_repair_count",
        "exploration_mode",
        "speculative_candidate_count",
        "exploration_candidate_count",
        "speculative_command_count",
        "speculative_command_notional_usd",
        "discovery_lane_counts",
        "risk_budget_status",
        "review_attestation",
    ):
        payload.pop(key)

    schema_validator("llm_equity_cycle_audit.schema.json").validate(payload)
    assert validate_cycle_audit_record(payload) is payload


def test_llm_equity_cycle_audit_v2_keeps_v1_reader_compatibility() -> None:
    legacy_v1 = valid_cycle_audit_payload()
    v2 = valid_cycle_audit_v2_payload()

    schema_validator("llm_equity_cycle_audit.schema.json").validate(legacy_v1)
    assert validate_cycle_audit_record(legacy_v1) is legacy_v1
    schema_validator("llm_equity_cycle_audit_v2.schema.json").validate(v2)
    assert validate_cycle_audit_record(v2) is v2
    with pytest.raises(ValidationError):
        schema_validator("llm_equity_cycle_audit.schema.json").validate(v2)


@pytest.mark.parametrize("model", ["gpt-5.6-sol", "gpt-6-astra"])
def test_llm_equity_cycle_audit_v3_retains_v1_v2_reader_compatibility(model: str) -> None:
    v1 = valid_cycle_audit_payload()
    v2 = valid_cycle_audit_v2_payload()
    v3 = valid_cycle_audit_v3_payload()

    for payload in (v1, v2, v3):
        payload["review_attestation"]["model"] = model  # type: ignore[index]

    assert validate_cycle_audit_record(v1) is v1
    assert validate_cycle_audit_record(v2) is v2
    assert validate_cycle_audit_record(v3) is v3

    for payload in (v1, v2, v3):
        payload["review_attestation"]["model"] = "unsupported-model"  # type: ignore[index]
        with pytest.raises(LlmEquityContractError, match="cycle audit schema violation"):
            validate_cycle_audit_record(payload)
        payload["review_attestation"]["model"] = model  # type: ignore[index]

    v3["unexpected"] = True
    with pytest.raises(LlmEquityContractError, match="cycle audit schema violation"):
        validate_cycle_audit_record(v3)


def test_llm_equity_cycle_audit_v2_rejects_raw_fields_in_accepted_trades() -> None:
    payload = valid_cycle_audit_v2_payload()
    payload["accepted_trades"][0]["reason"] = "raw authority detail"  # type: ignore[index]

    with pytest.raises(ValidationError):
        schema_validator("llm_equity_cycle_audit_v2.schema.json").validate(payload)


def test_llm_equity_cycle_audit_attestation_keeps_rust_rejection_separate() -> None:
    payload = valid_cycle_audit_payload()
    payload["status"] = "blocked"
    payload["review_attestation"] = {
        "required": True,
        "status": "compliant",
        "model": "gpt-5.6-sol",
        "reasoning_effort": "xhigh",
        "invocation_count": 3,
        "order_action": "sell_notional",
        "order_notional_usd": 25.0,
        "accepted_side": None,
        "accepted_notional_usd": None,
    }

    schema_validator("llm_equity_cycle_audit.schema.json").validate(payload)
    assert validate_cycle_audit_record(payload) is payload


def test_llm_swarm_contracts_accept_scrubbed_payloads() -> None:
    alpha_card = valid_alpha_card_payload()
    trade_slate = valid_trade_slate_payload()
    reflection = valid_swarm_reflection_payload()

    schema_validator("llm_alpha_card.schema.json").validate(alpha_card)
    schema_validator("llm_trade_slate.schema.json").validate(trade_slate)
    schema_validator("llm_swarm_reflection.schema.json").validate(reflection)
    assert validate_alpha_card(alpha_card) is alpha_card
    assert validate_trade_slate(trade_slate) is trade_slate
    assert validate_swarm_reflection(reflection) is reflection


def test_trade_slate_rejects_inconsistent_selection_state() -> None:
    pass_slate = valid_trade_slate_payload()
    pass_slate["selected_symbol"] = None
    with pytest.raises((ValidationError, LlmEquityContractError)):
        schema_validator("llm_trade_slate.schema.json").validate(pass_slate)
        validate_trade_slate(pass_slate)

    non_pass_slate = valid_trade_slate_payload()
    non_pass_slate["edge_gate_status"] = "low_consensus"
    non_pass_slate["edge_gate_reason"] = "consensus_below_0_65"
    non_pass_slate["no_trade_reason"] = "consensus_below_0_65"
    with pytest.raises((ValidationError, LlmEquityContractError)):
        schema_validator("llm_trade_slate.schema.json").validate(non_pass_slate)
        validate_trade_slate(non_pass_slate)

    skipped = valid_trade_slate_payload()
    skipped.update(
        {
            "edge_gate_status": "low_consensus",
            "edge_gate_reason": "consensus_below_0_65",
            "selected_symbol": None,
            "selected_side": None,
            "selected_strategy_book": None,
            "no_trade_reason": "consensus_below_0_65",
        }
    )
    schema_validator("llm_trade_slate.schema.json").validate(skipped)
    assert validate_trade_slate(skipped) is skipped


def test_llm_swarm_contracts_reject_raw_fields_and_schema_drift() -> None:
    alpha_card = valid_alpha_card_payload()
    alpha_card["raw_prompt"] = "hidden"
    with pytest.raises(LlmEquityContractError):
        validate_alpha_card(alpha_card)

    trade_slate = valid_trade_slate_payload()
    trade_slate["alpha_cards"][0]["full_text"] = "forbidden"  # type: ignore[index]
    with pytest.raises(LlmEquityContractError):
        validate_trade_slate(trade_slate)

    reflection = valid_swarm_reflection_payload()
    reflection["debug_extra"] = "not part of the schema"
    with pytest.raises(LlmEquityContractError, match="swarm reflection schema violation"):
        validate_swarm_reflection(reflection)


def test_llm_equity_cycle_audit_schema_rejects_raw_runtime_payloads() -> None:
    payload = valid_cycle_audit_payload()
    payload["market_research_snapshot"] = valid_snapshot_payload()

    with pytest.raises(ValidationError):
        schema_validator("llm_equity_cycle_audit.schema.json").validate(payload)


def test_validate_cycle_audit_record_rejects_schema_drift() -> None:
    payload = valid_cycle_audit_payload()
    payload["debug_extra"] = "not part of the schema"

    with pytest.raises(LlmEquityContractError, match="cycle audit schema violation"):
        validate_cycle_audit_record(payload)


def test_llm_decision_requires_memory_update() -> None:
    decision = valid_decision_payload()
    decision["memory_updates"] = []

    with pytest.raises(LlmEquityContractError):
        validate_llm_decision(decision)


def test_llm_decision_requires_directional_positive_notional() -> None:
    decision = valid_decision_payload()
    decision["commands"][0]["action"] = "hold"  # type: ignore[index]

    with pytest.raises(LlmEquityContractError, match="unsupported LLM trading command"):
        validate_llm_decision(decision)

    decision = valid_decision_payload()
    decision["commands"][0]["max_notional_usd"] = 25.0  # type: ignore[index]

    assert validate_llm_decision(decision).commands[0].max_notional_usd == 25.0

    decision = valid_decision_payload()
    decision["commands"][0]["max_notional_usd"] = 24.99  # type: ignore[index]

    with pytest.raises(LlmEquityContractError, match="mandatory order"):
        validate_llm_decision(decision)

    decision = valid_decision_payload()
    first_command = dict(decision["commands"][0])  # type: ignore[index]
    first_command["max_notional_usd"] = 12.5
    second_command = dict(first_command)
    second_command["action"] = "sell_notional"
    decision["commands"] = [first_command, second_command]

    with pytest.raises(LlmEquityContractError, match="mandatory order"):
        validate_llm_decision(decision)

    decision = valid_decision_payload()
    first_command = dict(decision["commands"][0])  # type: ignore[index]
    first_command["max_notional_usd"] = 12.5
    second_command = dict(first_command)
    second_command["action"] = "sell_notional"
    second_command["max_notional_usd"] = 25.0
    decision["commands"] = [first_command, second_command]

    validated = validate_llm_decision(decision)
    assert [command.max_notional_usd for command in validated.commands] == [12.5, 25.0]
    assert validated.commands[1].action == "sell_notional"

    decision = valid_decision_payload()
    decision["commands"][0]["action"] = "sell_notional"  # type: ignore[index]
    decision["commands"][0]["max_notional_usd"] = 100.0  # type: ignore[index]

    assert validate_llm_decision(decision).commands[0].max_notional_usd == 100.0

    decision = valid_decision_payload()
    decision["commands"][0]["action"] = "sell_notional"  # type: ignore[index]
    decision["commands"][0]["max_notional_usd"] = 24.99  # type: ignore[index]

    with pytest.raises(LlmEquityContractError, match="mandatory order"):
        validate_llm_decision(decision)

    decision = valid_decision_payload()
    decision["commands"][0]["max_notional_usd"] = 0.0  # type: ignore[index]

    with pytest.raises(LlmEquityContractError, match="max_notional_usd"):
        validate_llm_decision(decision)

    decision = valid_decision_payload()
    decision["commands"][0]["target_weight"] = 0.1  # type: ignore[index]

    with pytest.raises(LlmEquityContractError, match="unsupported fields"):
        validate_llm_decision(decision)


def test_llm_decision_schema_requires_evidence_rows() -> None:
    decision = valid_decision_payload()
    decision["evidence"] = []
    evidence_schema = schema_validator("llm_trading_decision.schema.json").schema["properties"][
        "evidence"
    ]

    with pytest.raises(ValidationError):
        Draft202012Validator(evidence_schema).validate(decision["evidence"])
    with pytest.raises(LlmEquityContractError, match="decision evidence must not be empty"):
        validate_llm_decision(decision)


def test_memory_event_redacts_raw_prompt_and_credentials() -> None:
    event = valid_memory_event()
    event["raw_prompt"] = "hidden"

    with pytest.raises(LlmEquityContractError):
        sanitize_memory_event(event)


def test_alpaca_config_accepts_project_and_standard_env_aliases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for key in (
        "ALPACA_API_KEY_ID",
        "ALPACA_API_SECRET_KEY",
        "APCA_API_KEY_ID",
        "APCA_API_SECRET_KEY",
    ):
        monkeypatch.delenv(key, raising=False)

    monkeypatch.setenv("APCA_API_KEY_ID", "standard-key")
    monkeypatch.setenv("APCA_API_SECRET_KEY", "standard-secret")
    config = AlpacaMarketDataConfig.from_env()
    assert config.key_id == "standard-key"
    assert config.secret_key == "standard-secret"

    monkeypatch.setenv("ALPACA_API_KEY_ID", "project-key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "project-secret")
    monkeypatch.setenv("ALPACA_TRADING_BASE_URL", "https://paper-api.alpaca.markets/v2")
    config = AlpacaMarketDataConfig.from_env()
    assert config.key_id == "project-key"
    assert config.secret_key == "project-secret"
    assert config.trading_base_url == "https://paper-api.alpaca.markets"

    event = valid_memory_event()
    event["conclusion"] = "api_key = NOT_A_REAL_SECRET"
    with pytest.raises(LlmEquityContractError):
        sanitize_memory_event(event)


def test_alpaca_zero_ask_quote_is_marked_unavailable() -> None:
    client = AlpacaMarketDataClient(
        AlpacaMarketDataConfig(key_id="key", secret_key="secret"),
        http_client=FakeAlpacaHttpClient(),
    )

    quote = client.latest_quotes(["SPY"])["SPY"]

    assert quote["bid"] == 0.0
    assert quote["ask"] == 0.0
    assert quote["source"] == "alpaca_market_data_unavailable"


class PositiveQuoteMetadataHttpClient:
    def get(
        self,
        url: str,
        *,
        headers: dict[str, str],
        params: dict[str, str],
        timeout: float,
    ) -> FakeAlpacaResponse:
        assert url.endswith("/v2/stocks/quotes/latest")
        return FakeAlpacaResponse(
            {
                "quotes": {
                    "SPY": {
                        "t": "2026-05-28T14:30:00Z",
                        "bp": 500.0,
                        "ap": 500.1,
                        "bs": 10.0,
                        "as": 20.0,
                        "bx": "V",
                        "ax": "Q",
                        "c": ["R"],
                    }
                }
            }
        )


def test_alpaca_live_quotes_match_closed_resident_contract() -> None:
    client = AlpacaMarketDataClient(
        AlpacaMarketDataConfig(key_id="key", secret_key="secret"),
        http_client=PositiveQuoteMetadataHttpClient(),
    )

    quote = client.latest_quotes(["SPY"])["SPY"]

    assert quote == {
        "timestamp": "2026-05-28T14:30:00Z",
        "bid": 500.0,
        "ask": 500.1,
        "source": "alpaca_market_data",
    }


def test_alpaca_client_discovers_active_assets_and_movers() -> None:
    http = FakeAlpacaHttpClient()
    client = AlpacaMarketDataClient(
        AlpacaMarketDataConfig(key_id="key", secret_key="secret"),
        http_client=http,
    )

    assets = client.active_assets()
    movers = client.top_movers(top=250)

    assert [asset["symbol"] for asset in assets] == ["BRK.B"]
    assert [row["symbol"] for row in movers] == ["NVDA", "TSLA"]
    mover_request = next(request for request in http.requests if request[0].endswith("/movers"))
    assert mover_request[1]["top"] == "50"


def test_alpaca_snapshots_keep_minute_and_session_bars_distinct() -> None:
    client = AlpacaMarketDataClient(
        AlpacaMarketDataConfig(key_id="key", secret_key="secret"),
        http_client=FakeAlpacaHttpClient(),
    )

    snapshot = client.stock_snapshots(["SPY"])["SPY"]

    assert snapshot["minute_bar"]["open"] == 500.0
    assert snapshot["session_bar"]["open"] == 490.0
    assert snapshot["previous_daily_bar"]["close"] == 491.0
    assert snapshot["latest_quote"]["bid"] == 500.45
    assert set(snapshot["latest_quote"]) == {"timestamp", "bid", "ask", "source"}


class PaginatingBarsHttpClient:
    def __init__(self) -> None:
        self.requests: list[dict[str, str]] = []

    def get(
        self,
        url: str,
        *,
        headers: dict[str, str],
        params: dict[str, str],
        timeout: float,
    ) -> FakeAlpacaResponse:
        assert url.endswith("/v2/stocks/bars")
        self.requests.append(dict(params))
        if params.get("page_token") == "page-2":
            return FakeAlpacaResponse(
                {
                    "bars": {
                        "SPY": [
                            {
                                "t": "2026-05-28T15:30:00Z",
                                "o": 501.0,
                                "h": 502.0,
                                "l": 500.0,
                                "c": 501.5,
                                "v": 900.0,
                            }
                        ]
                    },
                    "next_page_token": None,
                }
            )
        return FakeAlpacaResponse(
            {
                "bars": {
                    "SPY": [
                        {
                            "t": "2026-05-28T14:30:00Z",
                            "o": 500.0,
                            "h": 501.0,
                            "l": 499.0,
                            "c": 500.5,
                            "v": 1000.0,
                        }
                    ]
                },
                "next_page_token": "page-2",
            }
        )


class MalformedBarsHttpClient:
    def get(
        self,
        url: str,
        *,
        headers: dict[str, str],
        params: dict[str, str],
        timeout: float,
    ) -> FakeAlpacaResponse:
        return FakeAlpacaResponse({"bars": "not-an-object"})


class PaginatingQuotesHttpClient:
    def __init__(self) -> None:
        self.requests: list[dict[str, str]] = []

    def get(
        self,
        url: str,
        *,
        headers: dict[str, str],
        params: dict[str, str],
        timeout: float,
    ) -> FakeAlpacaResponse:
        assert url.endswith("/v2/stocks/quotes")
        self.requests.append(dict(params))
        timestamp = (
            "2026-05-28T14:30:01Z"
            if params.get("page_token") == "page-2"
            else "2026-05-28T14:30:00Z"
        )
        return FakeAlpacaResponse(
            {
                "quotes": {
                    "SPY": [
                        {
                            "t": timestamp,
                            "bp": 500.0,
                            "ap": 500.1,
                            "bs": 10.0,
                            "as": 20.0,
                            "bx": "V",
                            "ax": "Q",
                            "c": ["R"],
                        }
                    ]
                },
                "next_page_token": (
                    None if params.get("page_token") == "page-2" else "page-2"
                ),
            }
        )


class CorporateActionsHttpClient:
    def __init__(self) -> None:
        self.requests: list[dict[str, str]] = []

    def get(
        self,
        url: str,
        *,
        headers: dict[str, str],
        params: dict[str, str],
        timeout: float,
    ) -> FakeAlpacaResponse:
        assert url.endswith("/v1/corporate-actions")
        self.requests.append(dict(params))
        return FakeAlpacaResponse(
            {
                "corporate_actions": {
                    "forward_splits": [
                        {
                            "id": "action-2" if params.get("page_token") else "action-1",
                            "symbol": "SPY",
                            "process_date": "2026-05-01",
                            "ex_date": "2026-05-04",
                            "old_rate": 1.0,
                            "new_rate": 2.0,
                        }
                    ]
                },
                "next_page_token": None if params.get("page_token") else "page-2",
            }
        )
def test_alpaca_historical_bars_paginates_and_normalizes() -> None:
    http_client = PaginatingBarsHttpClient()
    client = AlpacaMarketDataClient(
        AlpacaMarketDataConfig(key_id="key", secret_key="secret"),
        http_client=http_client,
    )

    bars = client.historical_bars(
        ["SPY"],
        timeframe="1Hour",
        start=datetime(2026, 5, 1, tzinfo=UTC),
    )

    assert [bar["close"] for bar in bars["SPY"]] == [500.5, 501.5]
    assert bars["SPY"][0]["timestamp"] == "2026-05-28T14:30:00Z"
    assert bars["SPY"][0]["source"] == "alpaca_market_data"
    assert len(http_client.requests) == 2
    first_request = http_client.requests[0]
    assert first_request["timeframe"] == "1Hour"
    assert first_request["start"] == "2026-05-01T00:00:00Z"
    assert first_request["adjustment"] == "split"
    assert first_request["sort"] == "asc"
    assert "page_token" not in first_request
    assert http_client.requests[1]["page_token"] == "page-2"


def test_alpaca_historical_quote_pages_stream_without_aggregation() -> None:
    http_client = PaginatingQuotesHttpClient()
    client = AlpacaMarketDataClient(
        AlpacaMarketDataConfig(key_id="key", secret_key="secret"),
        http_client=http_client,
    )

    pages = list(
        client.iter_historical_quote_pages(
            ["SPY"],
            start=datetime(2026, 5, 28, 14, 30, tzinfo=UTC),
            max_pages=2,
        )
    )

    assert [page["SPY"][0]["timestamp"] for page in pages] == [
        "2026-05-28T14:30:00Z",
        "2026-05-28T14:30:01Z",
    ]
    assert pages[0]["SPY"][0]["bid_size"] == 10.0
    assert pages[0]["SPY"][0]["ask_size"] == 20.0
    assert pages[0]["SPY"][0]["bid_exchange"] == "V"
    assert pages[0]["SPY"][0]["ask_exchange"] == "Q"
    assert pages[0]["SPY"][0]["conditions"] == ["R"]
    assert "page_token" not in http_client.requests[0]
    assert http_client.requests[1]["page_token"] == "page-2"


def test_alpaca_historical_corporate_actions_paginates_without_current_state() -> None:
    http_client = CorporateActionsHttpClient()
    client = AlpacaMarketDataClient(
        AlpacaMarketDataConfig(key_id="key", secret_key="secret"),
        http_client=http_client,
    )

    actions = client.historical_corporate_actions(
        ["SPY"], start=date(2026, 5, 1), end=date(2026, 5, 31), max_pages=2
    )

    assert [row["id"] for row in actions] == ["action-1", "action-2"]
    assert {row["family"] for row in actions} == {"forward_splits"}
    assert http_client.requests[0]["symbols"] == "SPY"
    assert http_client.requests[1]["page_token"] == "page-2"


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"corporate_actions": []}, "response is malformed"),
        ({"corporate_actions": {"splits": "bad"}}, "family is malformed"),
        (
            {"corporate_actions": {"splits": [{"family": "reserved"}]}},
            "reserved field",
        ),
    ],
)
def test_alpaca_historical_corporate_actions_reject_malformed_payloads(
    payload: dict[str, object], message: str
) -> None:
    class MalformedCorporateActionsHttpClient:
        def get(
            self,
            url: str,
            *,
            headers: dict[str, str],
            params: dict[str, str],
            timeout: float,
        ) -> FakeAlpacaResponse:
            return FakeAlpacaResponse(payload)

    client = AlpacaMarketDataClient(
        AlpacaMarketDataConfig(key_id="key", secret_key="secret"),
        http_client=MalformedCorporateActionsHttpClient(),
    )

    with pytest.raises(LlmEquityContractError, match=message):
        client.historical_corporate_actions(
            ["SPY"], start=date(2026, 5, 1), end=date(2026, 5, 31)
        )


def test_alpaca_historical_corporate_actions_reject_backwards_window() -> None:
    client = AlpacaMarketDataClient(
        AlpacaMarketDataConfig(key_id="key", secret_key="secret"),
        http_client=CorporateActionsHttpClient(),
    )

    with pytest.raises(LlmEquityContractError, match="end cannot precede start"):
        client.historical_corporate_actions(
            ["SPY"], start=date(2026, 5, 2), end=date(2026, 5, 1)
        )


def test_alpaca_historical_surfaces_fail_closed_on_page_limit() -> None:
    class NeverCompleteHistoricalHttpClient:
        def get(
            self,
            url: str,
            *,
            headers: dict[str, str],
            params: dict[str, str],
            timeout: float,
        ) -> FakeAlpacaResponse:
            if url.endswith("/v2/stocks/bars"):
                payload: dict[str, object] = {"bars": {}}
            elif url.endswith("/v2/stocks/quotes"):
                payload = {"quotes": {}}
            elif url.endswith("/v1beta1/news"):
                payload = {"news": []}
            else:
                payload = {"corporate_actions": {}}
            return FakeAlpacaResponse({**payload, "next_page_token": "more"})

    client = AlpacaMarketDataClient(
        AlpacaMarketDataConfig(key_id="key", secret_key="secret"),
        http_client=NeverCompleteHistoricalHttpClient(),
    )
    start = datetime(2026, 5, 1, tzinfo=UTC)

    with pytest.raises(LlmEquityContractError, match="bars exceeded"):
        client.historical_bars(["SPY"], start=start, max_pages=1, require_complete=True)
    with pytest.raises(LlmEquityContractError, match="quotes exceeded"):
        client.historical_quotes(["SPY"], start=start, max_pages=1, require_complete=True)
    with pytest.raises(LlmEquityContractError, match="news exceeded"):
        client.historical_news(["SPY"], start=start, max_pages=1, require_complete=True)
    with pytest.raises(LlmEquityContractError, match="corporate actions exceeded"):
        client.historical_corporate_actions(
            ["SPY"],
            start=date(2026, 5, 1),
            end=date(2026, 5, 31),
            max_pages=1,
            require_complete=True,
        )


class EmptyBarsHttpClient:
    def __init__(self) -> None:
        self.calls = 0

    def get(
        self,
        url: str,
        *,
        headers: dict[str, str],
        params: dict[str, str],
        timeout: float,
    ) -> FakeAlpacaResponse:
        self.calls += 1
        return FakeAlpacaResponse({"bars": {}, "next_page_token": "more"})


def test_alpaca_historical_bars_empty_pages_stay_bounded() -> None:
    http_client = EmptyBarsHttpClient()
    client = AlpacaMarketDataClient(
        AlpacaMarketDataConfig(key_id="key", secret_key="secret"),
        http_client=http_client,
    )

    bars = client.historical_bars(
        ["SPY"],
        start=datetime(2026, 5, 1, tzinfo=UTC),
        max_pages=3,
    )

    assert bars == {}
    assert http_client.calls == 3


def test_alpaca_read_retries_transient_http_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import httpx

    class TransientHttpClient:
        def __init__(self) -> None:
            self.calls = 0

        def get(
            self,
            url: str,
            *,
            headers: dict[str, str],
            params: dict[str, str],
            timeout: float,
        ) -> FakeAlpacaResponse:
            self.calls += 1
            if self.calls < 3:
                request = httpx.Request("GET", url)
                response = httpx.Response(
                    503,
                    request=request,
                    headers={"retry-after": "0"},
                )
                raise httpx.HTTPStatusError(
                    "transient service failure", request=request, response=response
                )
            return FakeAlpacaResponse(
                {
                    "bars": {
                        "SPY": {
                            "t": "2026-05-28T14:30:00Z",
                            "o": 500.0,
                            "h": 501.0,
                            "l": 499.0,
                            "c": 500.5,
                            "v": 1000.0,
                        }
                    }
                }
            )

    delays: list[float] = []
    monkeypatch.setattr("llm_equity.alpaca.time.sleep", delays.append)
    http_client = TransientHttpClient()
    client = AlpacaMarketDataClient(
        AlpacaMarketDataConfig(key_id="key", secret_key="secret"),
        http_client=http_client,
    )

    bars = client.latest_bars(["SPY"])

    assert bars["SPY"]["close"] == 500.5
    assert http_client.calls == 3
    assert delays == [0.25, 0.5]


class BooleanOhlcHttpClient:
    def get(
        self,
        url: str,
        *,
        headers: dict[str, str],
        params: dict[str, str],
        timeout: float,
    ) -> FakeAlpacaResponse:
        return FakeAlpacaResponse(
            {
                "bars": {
                    "SPY": [
                        {
                            "t": "2026-05-28T14:30:00Z",
                            "o": True,
                            "h": 501.0,
                            "l": 499.0,
                            "c": 500.5,
                            "v": 1000.0,
                        }
                    ]
                },
                "next_page_token": None,
            }
        )


def test_alpaca_historical_bars_rejects_boolean_ohlc_values() -> None:
    client = AlpacaMarketDataClient(
        AlpacaMarketDataConfig(key_id="key", secret_key="secret"),
        http_client=BooleanOhlcHttpClient(),
    )

    with pytest.raises(LlmEquityContractError, match="must be numeric"):
        client.historical_bars(["SPY"], start=datetime(2026, 5, 1, tzinfo=UTC))


def test_alpaca_historical_bars_rejects_malformed_payload() -> None:
    client = AlpacaMarketDataClient(
        AlpacaMarketDataConfig(key_id="key", secret_key="secret"),
        http_client=MalformedBarsHttpClient(),
    )

    with pytest.raises(LlmEquityContractError, match="historical bars"):
        client.historical_bars(["SPY"], start=datetime(2026, 5, 1, tzinfo=UTC))


def test_snapshot_missing_required_freshness_sources_forces_hold_and_fails_contract() -> None:
    snapshot = valid_snapshot_payload()
    snapshot["source_freshness"] = [
        row
        for row in snapshot["source_freshness"]  # type: ignore[union-attr]
        if row["source"] != "alpaca_market_clock"
    ]
    freshness_schema = schema_validator("market_research_snapshot.schema.json").schema[
        "properties"
    ]["source_freshness"]

    assert snapshot_requires_hold(snapshot)
    with pytest.raises(ValidationError):
        Draft202012Validator(freshness_schema).validate(snapshot["source_freshness"])
    with pytest.raises(LlmEquityContractError, match="source_freshness missing required"):
        validate_market_research_snapshot(snapshot)


def test_snapshot_requires_macro_and_news_summary_rows() -> None:
    snapshot = valid_snapshot_payload()
    snapshot["macro_news_summaries"] = [
        row
        for row in snapshot["macro_news_summaries"]  # type: ignore[union-attr]
        if row["source"] != "news_summary"
    ]
    macro_schema = schema_validator("market_research_snapshot.schema.json").schema["properties"][
        "macro_news_summaries"
    ]

    assert snapshot_requires_hold(snapshot)
    with pytest.raises(ValidationError):
        Draft202012Validator(macro_schema).validate(snapshot["macro_news_summaries"])
    with pytest.raises(LlmEquityContractError, match="macro_news_summaries missing required"):
        validate_market_research_snapshot(snapshot)


def test_snapshot_rejects_technical_signals_key() -> None:
    # technical_signals is LLM-context-only advisory data; the snapshot posted
    # to the Rust authority must never carry it (deny_unknown_fields there).
    snapshot = valid_snapshot_payload()
    snapshot["technical_signals"] = {"symbols": {}}
    with pytest.raises(LlmEquityContractError):
        validate_market_research_snapshot(snapshot)


def test_profit_policy_allows_dust_and_preserves_reduce_only() -> None:
    payload = valid_decision_payload()
    payload["execution_policy"] = "profit_driven_v1"
    payload["commands"][0]["max_notional_usd"] = 0.25
    payload["commands"][0]["reduce_only"] = True
    decision = validate_llm_decision(payload)
    assert decision.commands[0].reduce_only is True
    assert decision.execution_policy == "profit_driven_v1"
    payload["commands"][0]["reduce_only"] = "true"
    with pytest.raises(LlmEquityContractError, match="reduce_only must be a boolean"):
        validate_llm_decision(payload)
    payload["commands"][0]["reduce_only"] = True
    payload["execution_policy"] = "unbounded"
    with pytest.raises(LlmEquityContractError, match="execution_policy"):
        validate_llm_decision(payload)
