"""Validated model selections survive the bounded cycle without authorizing fills."""

import json
from datetime import UTC, datetime, timedelta

import pytest
from test_allocation_requote import RequoteClient, moving_quotes
from test_fully_invested import review
from test_fully_invested_service import AT
from test_fully_invested_service import setup as allocation_setup

from llm_equity import fully_invested_service as service
from llm_equity import hourly_service as s

setup = allocation_setup


def state_path(cfg):
    return s.strategy_epoch_dir(cfg.state_dir) / "allocation-scheduler.json"


def save_selection(cfg, post, monkeypatch):
    stable = s.build_service_context
    moving_quotes(monkeypatch, keep_moving=True)
    client = RequoteClient([review()], [True, True])
    assert service.run_cycle(cfg, now=AT, poster=post, llm_client=client)["status"] == "blocked"
    assert client.calls == 1 and len(client.packets) == 2 and not post.cycles
    state = json.loads(state_path(cfg).read_text())
    assert state["pending_proposal"]["review"] == review()
    delay = datetime.fromisoformat(state["next_attempt_at"]) - datetime.now(UTC)
    assert timedelta(seconds=20) < delay <= timedelta(seconds=30)
    monkeypatch.setattr(s, "build_service_context", stable)
    return state


def test_new_cycle_reapproves_saved_target_without_reselecting_or_duplicate_usage(
    setup, monkeypatch
):
    cfg, post, _ = setup
    saved = save_selection(cfg, post, monkeypatch)
    client = RequoteClient([], [True])
    assert (
        service.run_cycle(cfg, now=AT + timedelta(minutes=1), poster=post, llm_client=client)[
            "status"
        ]
        == "success"
    )
    assert client.calls == 0 and len(client.packets) == 1 and len(post.cycles) == 1
    assert json.loads(state_path(cfg).read_text())["pending_proposal"] is None
    ledger = s.read_cycle_audit_ledger(cfg.state_dir)
    assert len(ledger) == 2
    assert sorted(row["review_attestation"]["invocation_count"] for row in ledger) == [1, 3]
    assert sum(row["costs"]["codex_input_tokens"] for row in ledger) == 400
    audits = [
        json.loads(p.read_text())
        for p in s.strategy_epoch_dir(cfg.state_dir).glob("allocation-reviews/*.json")
    ]
    accepted = next(row for row in audits if row["status"] == "accepted")
    assert accepted["proposal_origin_cycle_id"] == saved["pending_proposal"]["origin_cycle_id"]


@pytest.mark.parametrize("corruption", ["expired", "future", "weight", "quote", "quote_shape"])
def test_old_or_corrupted_selection_requires_fresh_full_review(setup, monkeypatch, corruption):
    cfg, post, _ = setup
    saved = save_selection(cfg, post, monkeypatch)
    now = AT + timedelta(minutes=1)
    if corruption == "expired":
        now = AT + timedelta(minutes=16)
    if corruption == "future":
        now = AT - timedelta(seconds=1)
    if corruption == "weight":
        saved["pending_proposal"]["review"]["holdings"][0]["weight_bps"] = 501
    if corruption == "quote":
        saved["pending_proposal"]["snapshot"]["latest_quotes"]["STK0"]["bid"] = 0
    if corruption == "quote_shape":
        saved["pending_proposal"]["snapshot"]["latest_quotes"] = list(
            saved["pending_proposal"]["snapshot"]["latest_quotes"]
        )
    state_path(cfg).write_text(json.dumps(saved))
    client = RequoteClient([review()], [])
    assert service.run_cycle(cfg, now=now, poster=post, llm_client=client)["status"] == "success"
    assert client.calls == 1 and not client.packets


def test_newly_ineligible_saved_target_goes_back_to_full_selection(setup, monkeypatch):
    cfg, post, _ = setup
    save_selection(cfg, post, monkeypatch)
    original = s.build_service_context

    def ineligible(*args, **kwargs):
        store, snap, context = original(*args, **kwargs)
        snap["asset_metadata"]["STK0"]["investment_kind"] = "excluded"
        context["market_research_snapshot"] = snap
        return store, snap, context

    monkeypatch.setattr(s, "build_service_context", ineligible)
    replacement = review()
    replacement["holdings"][0]["symbol"] = "STK20"
    client = RequoteClient([replacement], [])
    assert (
        service.run_cycle(cfg, now=AT + timedelta(minutes=1), poster=post, llm_client=client)[
            "status"
        ]
        == "success"
    )
    assert client.calls == 1 and not client.packets
    assert (
        post.cycles[0]["llm_trading_decision"]["target_portfolio"]["holdings"][0]["symbol"]
        == "STK20"
    )


def test_denial_keeps_previous_target_visible_to_new_full_selection(setup, monkeypatch):
    cfg, post, _ = setup
    saved = save_selection(cfg, post, monkeypatch)

    class Inspecting(RequoteClient):
        def decide_portfolio(self, context):
            assert context["previous_allocation_review"] == saved["pending_proposal"]["review"]
            assert "approval declined" in context["allocation_repair_reason"]
            return super().decide_portfolio(context)

    client = Inspecting([review()], [False])
    assert (
        service.run_cycle(cfg, now=AT + timedelta(minutes=1), poster=post, llm_client=client)[
            "status"
        ]
        == "success"
    )
    assert client.calls == 1 and len(client.packets) == 1 and len(post.cycles) == 1


def test_reduced_saved_universe_below_twenty_returns_to_broad_selection(setup, monkeypatch):
    cfg, post, _ = setup
    save_selection(cfg, post, monkeypatch)
    original = s.build_service_context
    modes = []

    def reduced(*args, **kwargs):
        modes.append(kwargs.get("required_only", False))
        store, snap, context = original(*args, **kwargs)
        if kwargs.get("required_only"):
            symbols = set(kwargs["required_symbols"])
            snap["symbols"] = sorted(symbols)
            for field in ("latest_quotes", "latest_bars", "asset_metadata"):
                snap[field] = {k: v for k, v in snap[field].items() if k in symbols}
        if "STK0" in snap["asset_metadata"]:
            snap["asset_metadata"]["STK0"]["investment_kind"] = "excluded"
        context["market_research_snapshot"] = snap
        return store, snap, context

    monkeypatch.setattr(s, "build_service_context", reduced)
    replacement = review()
    replacement["holdings"][0]["symbol"] = "STK20"
    client = RequoteClient([replacement], [])
    assert (
        service.run_cycle(cfg, now=AT + timedelta(minutes=1), poster=post, llm_client=client)[
            "status"
        ]
        == "success"
    )
    assert modes == [True, False, True]
    assert client.calls == 1 and not client.packets and len(post.cycles) == 1


@pytest.mark.parametrize("due", [False, True])
def test_saved_proposal_is_scheduled_after_stop_recovers_when_retry_is_due(setup, monkeypatch, due):
    from llm_equity.simulator import EquityPaperPortfolio, EquityPosition

    cfg, post, snap = setup
    saved = save_selection(cfg, post, monkeypatch)
    saved.update(
        last_review_at=AT.isoformat(),
        next_attempt_at=(AT + timedelta(seconds=30)).isoformat(),
        plans={
            r["symbol"]: {"stop_price": 0, "review_at": (AT + timedelta(days=1)).isoformat()}
            for r in review()["holdings"]
        },
    )
    state_path(cfg).write_text(json.dumps(saved))
    portfolio = EquityPaperPortfolio.seeded(s.load_equity_universe())
    portfolio.cash_usd = 0
    portfolio.positions = {
        r["symbol"]: EquityPosition(symbol=r["symbol"], quantity=5, avg_price=100)
        for r in review()["holdings"]
    }
    monkeypatch.setattr(s, "LocalRustPoster", lambda *a: post)
    monkeypatch.setattr(s, "authoritative_portfolio_for_prompt", lambda *a, **kw: portfolio)
    monkeypatch.setattr(s, "build_market_research_snapshot", lambda *a, **kw: snap)
    monkeypatch.setattr(s, "scheduled_cycles_for_date", lambda *a, **kw: [])
    calls = []
    monkeypatch.setattr(service, "run_cycle", lambda *a, **kw: calls.append(kw))
    service.poll(cfg, now=AT + timedelta(minutes=1) if due else AT)
    assert len(calls) == int(due)
    assert not post.cycles
