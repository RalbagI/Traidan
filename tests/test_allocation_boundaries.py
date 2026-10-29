"""Allocation contract and production-entry boundary regressions."""

import copy

import pytest
from llm_equity.accounting import model_cost_events
from llm_equity.contracts import (
    LlmEquityContractError,
    validate_llm_decision,
    validate_market_research_snapshot,
)
from test_fully_invested import review
from test_fully_invested_integration import packet
from test_fully_invested_service import AT, Client
from test_fully_invested_service import setup as allocation_setup

from llm_equity import fully_invested as policy
from llm_equity import fully_invested_service as service
from llm_equity import hourly_service as s

setup = allocation_setup


@pytest.mark.parametrize("corruption", ["weight_cap", "weight_sum", "duplicate", "wrong_policy"])
def test_complete_allocation_contract_rejects_invalid_targets(corruption):
    decision = policy.decision_from_review(review(), packet(), sequence=1)
    if corruption == "weight_cap":
        decision["target_portfolio"]["holdings"][0]["weight_bps"] = 501
    elif corruption == "weight_sum":
        decision["target_portfolio"]["holdings"][0]["weight_bps"] = 499
    elif corruption == "duplicate":
        decision["target_portfolio"]["holdings"][1]["symbol"] = "STK0"
    else:
        decision["execution_policy"] = "profit_driven_v1"
    with pytest.raises(LlmEquityContractError):
        validate_llm_decision(decision)


def test_signed_source_inventory_validates_and_unknown_asset_class_does_not():
    context = packet([{"symbol": "STK39", "quantity": -5, "avg_price": 100}], cash=10500)
    decision = policy.decision_from_review(review(), context, sequence=1)
    validate_llm_decision(decision)
    context["market_research_snapshot"]["asset_metadata"]["STK0"]["investment_kind"] = "cash"
    with pytest.raises(LlmEquityContractError, match="investment_kind"):
        validate_market_research_snapshot(context["market_research_snapshot"])


def test_nan_forecast_and_missing_quote_cannot_be_allocated():
    allocation = review()
    allocation["holdings"][0]["expected_excess_return_bps"] = float("nan")
    with pytest.raises(LlmEquityContractError, match="finite"):
        policy.validate_review(allocation, {f"STK{i}" for i in range(20)})
    context = packet()
    context["market_research_snapshot"]["latest_quotes"].pop("STK0")
    assert "STK0" not in {row["symbol"] for row in policy.review_context(context)["candidates"]}


def test_oversized_held_inventory_is_rejected_without_truncating_actual_account():
    context = packet()
    context["portfolio"]["positions"] = [
        {"symbol": f"STK{i}", "quantity": 1, "audit_note": "x" * 6000} for i in range(20)
    ]
    before = copy.deepcopy(context)
    with pytest.raises(LlmEquityContractError, match="context budget"):
        policy.review_context(context)
    assert context == before


def test_codex_adapter_uses_allocation_prompt_and_schema(tmp_path, monkeypatch):
    client = s.CodexLlmClient(tmp_path, 180)
    observed = []

    def invoke(prompt, schema):
        observed.append((prompt, schema))
        return review()

    monkeypatch.setattr(client, "_invoke", invoke)
    assert client.decide_portfolio(packet()) == review()
    assert observed[0][1] == policy.SCHEMA_PATH
    assert "Negative" in observed[0][0] or "negative" in observed[0][0]
    assert "10000" in observed[0][0]


def test_requote_market_closure_preserves_inventory(setup, monkeypatch):
    cfg, post, _ = setup
    original = s.build_service_context
    count = 0

    def closed_after_review(*a, **kw):
        nonlocal count
        count += 1
        store, snap, context = original(*a, **kw)
        if count % 2 == 0:
            snap["market_calendar_state"]["is_open"] = False
        return store, snap, context

    monkeypatch.setattr(s, "build_service_context", closed_after_review)
    result = service.run_cycle(cfg, now=AT, poster=post, llm_client=Client([review()] * 3))
    assert result["status"] == "blocked"
    assert post.cycles == []


def test_rejected_response_is_never_a_completed_allocation(setup):
    cfg, post, _ = setup

    def rejected(path, payload):
        if path == s.LLM_EQUITY_CYCLE_PATH:
            return {"status": "rejected", "rejected_orders": 1}
        return post(path, payload)

    with pytest.raises(LlmEquityContractError, match="not accepted"):
        service.run_cycle(cfg, now=AT, poster=rejected, llm_client=Client([review()]))
    assert service.pending_path(cfg).exists()


def test_public_entrypoints_route_fully_invested_policy(setup, monkeypatch):
    cfg, post, _ = setup
    result = s.run_once(cfg, now=AT, poster=post, llm_client=Client([review()]))
    assert result["status"] == "success"
    calls = []
    monkeypatch.setattr(service, "run_forever", lambda config: calls.append(config))
    s.run_forever(cfg)
    assert calls == [cfg]


def test_fully_invested_drops_stale_optional_core_but_retains_held_valuation(setup):
    _, _, snap = setup
    bars = {"STK0": snap["latest_bars"]["STK0"], "TLT": snap["latest_bars"]["STK0"]}
    quotes = {
        "STK0": snap["latest_quotes"]["STK0"],
        "TLT": {**snap["latest_quotes"]["STK0"], "timestamp": "2026-10-22T12:00:00Z"},
    }
    assert s.usable_research_symbols(
        ["STK0", "TLT"], bars, quotes, as_of=AT, protected_symbols=["TLT"]
    ) == ["STK0"]
    assert s.usable_research_symbols(
        ["STK0", "TLT"], bars, quotes, as_of=AT, required_symbols=["TLT"]
    ) == ["STK0", "TLT"]


def test_outgoing_holdings_requote_requires_reapproval_and_audits_all_attempts(setup, monkeypatch):
    cfg, post, _ = setup
    original = s.build_service_context
    count = 0

    def outgoing_changes(*a, **kw):
        nonlocal count
        count += 1
        store, snap, context = original(*a, **kw)
        context["portfolio"]["positions"] = [{"symbol": "STK24", "quantity": 5, "avg_price": 100}]
        context["profitability_scorecard"] = {"costs": {"cycle_cost_usd": 0.02}}
        if count % 2 == 0:
            snap["latest_quotes"]["STK24"]["bid"] *= 0.9
            snap["latest_quotes"]["STK24"]["ask"] *= 0.9
        return store, snap, context

    monkeypatch.setattr(s, "build_service_context", outgoing_changes)
    client = Client([review()] * 3)
    client.last_usage = {"usage": {"input_tokens": 100, "output_tokens": 20}}
    assert service.run_cycle(cfg, now=AT, poster=post, llm_client=client)["status"] == "blocked"
    assert client.calls == 3
    assert post.cycles == []
    rows = s.read_cycle_audit_ledger(cfg.state_dir)
    assert len(rows) == 1
    assert rows[0]["review_attestation"]["invocation_count"] == 3
    assert rows[0]["costs"]["actual_cycle_cost_usd"] == pytest.approx(0.06)
    assert rows[0]["costs"]["codex_input_tokens"] == 300
    events = s.PointInTimeEventStore(
        s.strategy_epoch_dir(cfg.state_dir) / s.RESEARCH_EVENT_LEDGER_NAME
    ).read_all()
    assert sum(row["event_type"] == "replay_cycle" for row in events) == 3
    assert len(model_cost_events(events)) == 3
    assert sum(cost for _, cost in model_cost_events(events)) == pytest.approx(0.06)


@pytest.mark.parametrize(
    "failure_stage", ["policy_frame", "replay_cycle", "cycle_audit", "summary"]
)
def test_receipt_retry_repairs_every_audit_stage_without_duplicate_fills_or_costs(
    setup, monkeypatch, failure_stage
):
    cfg, post, _ = setup
    append = s.PointInTimeEventStore.append
    published = []
    broken = True

    def append_with_failure(self, event_type, *a, **kw):
        if broken and event_type == failure_stage:
            raise OSError("audit stage unavailable")
        return append(self, event_type, *a, **kw)

    def publish(path, payload):
        if path == s.LLM_EQUITY_CYCLE_SUMMARY_PATH:
            if broken and failure_stage == "summary":
                raise OSError("audit stage unavailable")
            published.append(payload)
        response = post(path, payload)
        if path == s.LLM_EQUITY_CYCLE_PATH:
            response["order_results"] = [
                {
                    "symbol": row["symbol"],
                    "side": "buy",
                    "status": "accepted",
                    "qty": 5,
                    "avg_price": 100,
                    "notional_usd": 500,
                }
                for row in payload["llm_trading_decision"]["target_portfolio"]["holdings"]
            ]
        return response

    monkeypatch.setattr(s.PointInTimeEventStore, "append", append_with_failure)
    with pytest.raises(OSError, match="audit stage unavailable"):
        service.run_cycle(cfg, now=AT, poster=publish, llm_client=Client([review()]))
    assert service.pending_path(cfg).exists()
    assert len(s.read_cycle_audit_ledger(cfg.state_dir)) == 1
    broken = False
    assert (
        service.run_cycle(cfg, now=AT, poster=publish, llm_client=Client([]))["status"] == "success"
    )
    assert post.cycles[0] == post.cycles[1]
    assert len(s.read_cycle_audit_ledger(cfg.state_dir)) == 1
    events = s.PointInTimeEventStore(
        s.strategy_epoch_dir(cfg.state_dir) / s.RESEARCH_EVENT_LEDGER_NAME
    ).read_all()
    assert [row["event_type"] for row in events] == ["policy_frame", "replay_cycle", "cycle_audit"]
    assert len(published) == 1
    assert not service.pending_path(cfg).exists()


def test_rejected_review_audit_retry_never_resubmits_rejected_orders(setup, monkeypatch):
    from io import BytesIO
    from urllib.error import HTTPError

    cfg, post, _ = setup
    original = s.PointInTimeEventStore.append
    attempts = []

    def reject(path, payload):
        if path == s.LLM_EQUITY_CYCLE_PATH:
            attempts.append(payload)
            raise HTTPError(
                "http://localhost", 422, "invalid", {}, BytesIO(b'{"reason":"stale_market_data"}')
            )
        return post(path, payload)

    def broken(*a, **kw):
        raise OSError("audit unavailable")

    monkeypatch.setattr(s.PointInTimeEventStore, "append", broken)
    with pytest.raises(OSError, match="audit unavailable"):
        service.run_cycle(cfg, now=AT, poster=reject, llm_client=Client([review()]))
    monkeypatch.setattr(s.PointInTimeEventStore, "append", original)
    assert (
        service.run_cycle(cfg, now=AT, poster=reject, llm_client=Client([]))["status"] == "blocked"
    )
    assert len(attempts) == 1
    rows = s.read_cycle_audit_ledger(cfg.state_dir)
    assert len(rows) == 1
    assert rows[0]["review_attestation"]["invocation_count"] == 1
    assert not service.pending_path(cfg).exists()


def test_event_idempotency_rejects_conflicting_reuse(tmp_path):
    store = s.PointInTimeEventStore(tmp_path / "events.jsonl")
    original = store.append("audit", AT, {"result": 1}, event_key="same")
    assert store.append("audit", AT, {"result": 1}, event_key="same") == original
    with pytest.raises(ValueError, match="different payload"):
        store.append("audit", AT, {"result": 2}, event_key="same")
    assert len(store.read_all()) == 1


def test_failed_codex_calls_capture_only_their_own_usage(setup, monkeypatch):
    import json
    import subprocess
    from pathlib import Path

    cfg, post, _ = setup
    calls = 0

    def invoke(command, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            output = Path(command[command.index("--output-last-message") + 1])
            output.write_text(json.dumps({"verdict": "abstain"}))
        usage = {
            1: {"input_tokens": 100, "output_tokens": 20},
            2: {"input_tokens": 7, "output_tokens": 2},
        }.get(calls)
        stdout = json.dumps({"type": "turn.completed", "usage": usage}) if usage else ""
        return subprocess.CompletedProcess(
            command, 0 if calls == 1 else 1, stdout, "failed" if calls > 1 else ""
        )

    monkeypatch.setattr(s.subprocess, "run", invoke)
    monkeypatch.setattr(s, "codex_model_catalog_hash", lambda *a: None)
    client = s.CodexLlmClient(cfg.state_dir, 180)
    assert service.run_cycle(cfg, now=AT, poster=post, llm_client=client)["status"] == "blocked"
    assert calls == 3
    row = s.read_cycle_audit_ledger(cfg.state_dir)[-1]
    assert row["costs"]["codex_input_tokens"] == 107
    assert row["costs"]["codex_output_tokens"] == 22
    assert client.last_usage == {}


@pytest.mark.parametrize(
    ("mode", "configured", "expected"),
    [
        ("fully_invested", 3300, 600),
        ("fully_invested", 120, 120),
        ("profit_driven", 3300, 180),
        ("bounded", 999, 999),
    ],
)
def test_full_portfolio_review_has_a_bounded_budget_sized_for_twenty_holdings(
    tmp_path, monkeypatch, mode, configured, expected
):
    import subprocess

    monkeypatch.setenv(s.LLM_EQUITY_FILL_MODE_ENV, "paper")
    monkeypatch.setenv(s.LLM_EQUITY_RISK_MODE_ENV, mode)
    observed = []

    def invoke(command, **kwargs):
        observed.append(kwargs["timeout"])
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr(s.subprocess, "run", invoke)
    client = s.CodexLlmClient(tmp_path, configured)
    with pytest.raises(subprocess.TimeoutExpired):
        client.decide_portfolio(packet())
    assert observed == [expected]


def test_requote_repair_receives_previous_model_proposal(setup, monkeypatch):
    cfg, post, _ = setup
    original = s.build_service_context
    contexts = []
    count = 0

    def requote(*args, **kwargs):
        nonlocal count
        count += 1
        store, snap, context = original(*args, **kwargs)
        if count == 2:
            snap["latest_quotes"]["STK0"]["bid"] *= 1.02
            snap["latest_quotes"]["STK0"]["ask"] *= 1.02
        return store, snap, context

    monkeypatch.setattr(s, "build_service_context", requote)

    class RevisingClient(Client):
        def decide_portfolio(self, context):
            contexts.append(copy.deepcopy(context))
            return super().decide_portfolio(context)

    client = RevisingClient([review(), review()])
    assert service.run_cycle(cfg, now=AT, poster=post, llm_client=client)["status"] == "success"
    assert len(contexts) == 2
    assert contexts[1]["previous_allocation_review"] == review()
    assert "material price change" in contexts[1]["allocation_repair_reason"]
    assert len(post.cycles) == 1
