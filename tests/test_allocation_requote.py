"""Price-only model reapproval keeps the execution quote guard intact."""

import copy
import json
import subprocess

import pytest
from test_fully_invested import review
from test_fully_invested_integration import packet
from test_fully_invested_service import AT, Client
from test_fully_invested_service import setup as allocation_setup

from llm_equity import fully_invested as policy
from llm_equity import fully_invested_service as service
from llm_equity import hourly_service as s

setup = allocation_setup


class RequoteClient(Client):
    def __init__(self, responses, approvals):
        super().__init__(responses)
        self.approvals = iter(approvals)
        self.packets = []
        self.last_usage = {"usage": {"input_tokens": 100, "output_tokens": 20}}

    def approve_portfolio_requote(self, context):
        self.packets.append(copy.deepcopy(context))
        result = next(self.approvals)
        return {
            "approval_id": context["approval_id"],
            "approved": result,
            "rationale": "Rechecked prices",
        }


def moving_quotes(monkeypatch, *, keep_moving=False, outgoing=False):
    original = s.build_service_context
    calls = []

    def build(*args, **kwargs):
        calls.append(kwargs)
        store, snap, context = original(*args, **kwargs)
        # The production snapshot and context reference the same market data.
        context["market_research_snapshot"] = snap
        if outgoing:
            context["portfolio"]["positions"] = [
                {"symbol": "STK24", "quantity": 5, "avg_price": 100}
            ]
        symbol = "STK24" if outgoing else "STK0"
        move = 1.02 ** (len(calls) - 1) if keep_moving else 1.02 if len(calls) > 1 else 1
        for field in ("bid", "ask"):
            snap["latest_quotes"][symbol][field] *= move
        return store, snap, context

    monkeypatch.setattr(s, "build_service_context", build)
    return calls


@pytest.mark.parametrize("outgoing", [False, True])
def test_short_approval_executes_exact_target_after_slow_selection(setup, monkeypatch, outgoing):
    cfg, post, _ = setup
    calls = moving_quotes(monkeypatch, outgoing=outgoing)
    selected = review()
    client = RequoteClient([selected], [True])
    result = service.run_cycle(cfg, now=AT, poster=post, llm_client=client)
    assert result["status"] == "success"
    assert client.calls == 1 and len(client.packets) == 1 and len(post.cycles) == 1
    assert calls[2]["required_only"] is True
    assert set(calls[2]["required_symbols"]) == {r["symbol"] for r in selected["holdings"]}
    approved = client.packets[0]
    assert approved["proposed_allocation"] == selected
    changed = next(
        q for q in approved["quotes"] if q["symbol"] == ("STK24" if outgoing else "STK0")
    )
    assert changed["change_bps"] == pytest.approx(200)
    target = post.cycles[0]["llm_trading_decision"]["target_portfolio"]["holdings"]
    assert target == [{k: r[k] for k in ("symbol", "weight_bps")} for r in selected["holdings"]]
    ledger = s.read_cycle_audit_ledger(cfg.state_dir)
    assert ledger[0]["review_attestation"]["invocation_count"] == 2
    assert ledger[0]["costs"]["codex_input_tokens"] == 200
    audit = next(s.strategy_epoch_dir(cfg.state_dir).glob("allocation-reviews/*.json"))
    assert '"price_approval"' in audit.read_text()


def test_declined_price_approval_requires_new_full_selection(setup, monkeypatch):
    cfg, post, _ = setup
    moving_quotes(monkeypatch)
    replacement = review()
    replacement["holdings"][-1]["symbol"] = "STK20"
    client = RequoteClient([review(), replacement], [False])
    assert service.run_cycle(cfg, now=AT, poster=post, llm_client=client)["status"] == "success"
    assert client.calls == 2 and len(client.packets) == 1
    assert (
        post.cycles[0]["llm_trading_decision"]["target_portfolio"]["holdings"][-1]["symbol"]
        == "STK20"
    )


def test_further_price_moves_still_block_all_fills_and_bound_model_calls(setup, monkeypatch):
    cfg, post, _ = setup
    moving_quotes(monkeypatch, keep_moving=True)
    client = RequoteClient([review()], [True, True])
    assert service.run_cycle(cfg, now=AT, poster=post, llm_client=client)["status"] == "blocked"
    assert client.calls == 1 and len(client.packets) == 2 and not post.cycles
    assert client.packets[0]["approval_id"] != client.packets[1]["approval_id"]
    assert (
        s.read_cycle_audit_ledger(cfg.state_dir)[0]["review_attestation"]["invocation_count"] == 3
    )


@pytest.mark.parametrize("corruption", ["identity", "boolean", "extra"])
def test_invalid_reapproval_never_submits(setup, monkeypatch, corruption):
    cfg, post, _ = setup
    moving_quotes(monkeypatch)

    class Invalid(RequoteClient):
        def approve_portfolio_requote(self, context):
            answer = super().approve_portfolio_requote(context)
            if corruption == "identity":
                answer["approval_id"] = "0" * 64
            if corruption == "boolean":
                answer["approved"] = "true"
            if corruption == "extra":
                answer["holdings"] = review()["holdings"]
            return answer

    client = Invalid([review()], [True, True])
    assert service.run_cycle(cfg, now=AT, poster=post, llm_client=client)["status"] == "blocked"
    assert not post.cycles


def test_requote_adapter_uses_small_schema_and_bounded_timeout(tmp_path, monkeypatch):
    monkeypatch.setenv(s.LLM_EQUITY_FILL_MODE_ENV, "paper")
    monkeypatch.setenv(s.LLM_EQUITY_RISK_MODE_ENV, "fully_invested")
    context = packet()
    approval = policy.requote_context(review(), context["market_research_snapshot"], context)
    observed = []

    def invoke(command, **kwargs):
        path = command[command.index("--output-schema") + 1]
        from pathlib import Path

        observed.append(json.loads(Path(path).read_text()))
        assert kwargs["timeout"] == 180
        assert approval["approval_id"] in kwargs["input"]
        assert "Do not repeat portfolio" in kwargs["input"]
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr(s.subprocess, "run", invoke)
    with pytest.raises(subprocess.TimeoutExpired):
        s.CodexLlmClient(tmp_path, 3300).approve_portfolio_requote(approval)
    assert observed == [policy.REQUOTE_SCHEMA]
