import copy
import json
import subprocess
from datetime import UTC, datetime

import pytest
from test_llm_equity_accounting import _event, _state, _write
from test_llm_equity_epoch import seed
from test_llm_equity_profit_execution import command, context

from llm_equity import accounting as a
from llm_equity import epoch as e
from llm_equity import hourly_service as s
from llm_equity import policy as p


@pytest.fixture
def profit(monkeypatch):
    monkeypatch.setenv(s.LLM_EQUITY_FILL_MODE_ENV, "paper")
    monkeypatch.setenv(s.LLM_EQUITY_RISK_MODE_ENV, "profit_driven")


@pytest.mark.parametrize(
    "field,value,message",
    [("side", "hold", "side"), ("quantity", 0, "positive"), ("notional_usd", 101, "reconcile")],
)
def test_account_rejects_malformed_fill(tmp_path, field, value, message):
    _write(tmp_path, [_state("2026-10-09T14:00:00Z", 9900, 1, 100, ["buy"])])
    event = _event("buy", "buy", 1, 100)
    event["payload"]["order_results"][0][field] = value
    with pytest.raises(ValueError, match=message):
        a.account_report(tmp_path, [event])


def test_account_validates_costs_inputs_and_empty_windows(tmp_path):
    with pytest.raises(ValueError, match="timezone"):
        a.timestamp("2026-10-09T14:00:00")
    bad = tmp_path / "bad.json"
    bad.write_text("[]")
    with pytest.raises(TypeError, match="object"):
        a._read_object(bad)
    with pytest.raises(ValueError, match="precede"):
        a.account_report(tmp_path, [], start="2026-10-10", end="2026-10-09")
    _write(tmp_path, [])
    assert a.account_report(tmp_path, [])["status"] == "authority_history_unavailable"
    row = _state("2026-10-09T14:00:00Z", 10000, 0, 100, [])
    _write(tmp_path, [row])
    assert a.account_report(tmp_path, [], start="2026-10-10")["status"] == "no_marks_in_window"
    assert a.session_returns(row, [row]) == []
    for policy, error in [
        ({"commission_bps": -1}, ValueError),
        ({"exclude_operating_costs": "yes"}, TypeError),
        ({"exclude_operating_costs": True, "monthly_model_usd": 1}, ValueError),
    ]:
        (tmp_path / "economic-cost-policy.json").write_text(json.dumps(policy))
        with pytest.raises(error):
            a.account_report(tmp_path, [])
    rejected = _event("no", "buy", 1, 100)
    rejected["payload"]["order_results"][0]["status"] = "rejected"
    assert a._fills([rejected]) == {}


def test_account_adjustments_reconcile_split_dividend_and_symbol_change(tmp_path):
    positions = {"ABC": 2.0}
    assert (
        a._adjust(positions, 10, {"symbol": "ABC", "action_type": "dividend", "cash_amount_usd": 2})
        == 14
    )
    a._adjust(positions, 14, {"symbol": "ABC", "action_type": "split", "ratio": 2})
    a._adjust(positions, 14, {"symbol": "ABC", "action_type": "symbol_change", "new_symbol": "XYZ"})
    assert positions == {"XYZ": 4}
    for payload in (
        {"action_type": "split", "ratio": 0},
        {"action_type": "symbol_change", "new_symbol": "XYZ"},
        {"action_type": "unknown"},
    ):
        with pytest.raises(ValueError):
            a._adjust(positions, 14, {"symbol": "XYZ", **payload})
    states = [
        _state("2026-10-08T14:00:00Z", 9900, 1, 100, ["buy"]),
        _state("2026-10-09T14:00:00Z", 9902, 1, 100, ["buy"]),
    ]
    _write(tmp_path, states)
    events = [
        _event("buy", "buy", 1, 100),
        {
            "event_type": "account_adjustment",
            "observed_at": "2026-10-09T13:00:00Z",
            "payload": {"symbol": "ABC", "action_type": "dividend", "cash_amount_usd": 2},
        },
    ]
    assert a.account_report(tmp_path, events)["reconciliation"]["status"] == "matched"
    states[-1]["seen_cycle_ids"].extend(["missing1", "missing2"])
    _write(tmp_path, states)
    assert a.account_report(tmp_path, events)["reconciliation"]["status"] == "mismatch"


def test_dashboard_marks_extend_valuation_and_opening_book(tmp_path):
    _write(
        tmp_path,
        [
            _state("2026-10-08T14:00:00Z", 9900, 1, 100, ["buy"]),
            _state("2026-10-09T14:00:00Z", 9900, 1, 101, ["buy"]),
        ],
    )
    rows = []
    for at, price in [("2026-10-08T19:00:00Z", 102), ("2026-10-09T19:00:00Z", 103)]:
        state = _state(at, 9900, 1, price, ["buy"])
        rows.append({**state, "updated_at": at, "prices": state["last_prices"]})
    (tmp_path / "rust-dashboard-state.json").write_text(json.dumps({"portfolio_history": rows}))
    report = a.account_report(tmp_path, [_event("buy", "buy", 1, 100)], start="2026-10-09")
    assert report["opening_nav_usd"] == 10002
    assert report["ending_nav_usd"] == 10003


def test_model_cost_integrity_rejects_negative_and_conflicting_invocations():
    event = {
        "event_type": "replay_cycle",
        "observed_at": "2026-10-09T14:00:00Z",
        "payload": {"model_call_cost_usd": -1},
    }
    with pytest.raises(ValueError, match="nonnegative"):
        a.model_cost_events([event])
    event["payload"]["model_call_cost_usd"] = 1
    other = copy.deepcopy(event)
    other["payload"]["model_call_cost_usd"] = 2
    with pytest.raises(ValueError, match="conflicting"):
        a.model_cost_events([event, other])


@pytest.mark.parametrize("which", ["target", "source", "authority", "same", "existing"])
def test_migration_invalid_or_unreconciled_state_is_rejected(tmp_path, which):
    old, _state = seed(tmp_path)
    target = {"epoch_id": "epoch-bbbbbbbbbbbbbbbb"}
    if which == "target":
        target["epoch_id"] = "../bad"
    if which == "source":
        (tmp_path / "strategy-epoch.json").write_text(json.dumps({"epoch_id": "../bad"}))
    if which == "authority":
        (old / "rust-authority-state.json").write_text(json.dumps({"schema_version": 0}))
    if which == "same":
        target["epoch_id"] = old.name
    if which == "existing":
        path = tmp_path / "epochs" / target["epoch_id"]
        path.mkdir()
        (path / "rust-authority-state.json").write_text("{}")
    with pytest.raises(ValueError):
        e.migrate_account_epoch(tmp_path, target, at=datetime.now(UTC))


def test_migration_copies_policy_and_detects_source_race(tmp_path, monkeypatch):
    old, _state = seed(tmp_path)
    (old / "economic-cost-policy.json").write_text('{"commission_bps":0.5}')
    runtime = tmp_path / "runtime"
    (runtime / "pids").mkdir(parents=True)
    (runtime / "pids/rust.pid").write_text("99999999")
    monkeypatch.setenv("TRAIDAN_LOCAL_DRY_RUN_STATE_DIR", str(runtime))
    original = e._write

    def racing(path, value):
        original(path, value)
        if path.name == "account-opening-exit-plans.json":
            (old / "rust-authority-state.json").write_text("{}")

    monkeypatch.setattr(e, "_write", racing)
    with pytest.raises(RuntimeError, match="changed"):
        e.migrate_account_epoch(
            tmp_path, {"epoch_id": "epoch-bbbbbbbbbbbbbbbb"}, at=datetime.now(UTC)
        )


def test_policy_rejects_duplicates_and_invalid_close_and_handles_reductions():
    with pytest.raises(ValueError, match="one command"):
        p.cap_entries({"commands": [command(), command()]}, context())
    with pytest.raises(ValueError, match="calendar"):
        p.cap_entries({"commands": [{**command(), "horizon": "close"}]}, context())
    ctx = context()
    ctx["portfolio"]["positions"] = [{"symbol": "SPY", "quantity": 0, "market_value_usd": 0}]
    assert p.exit_reasons(ctx, {}, datetime.now(UTC)) == {}
    assert p.exit_reasons(
        context(quantity=1),
        {"SPY": {"side": 1, "stop_price": 0, "exit_at": "2026-10-09"}},
        datetime.now(UTC),
    ) == {"SPY": "missing_exit_plan"}
    response = {
        "order_results": [{"status": "accepted", "command_id": "unrelated", "symbol": "SPY"}]
    }
    decision = {"cycle_id": "c", "commands": [command("sell_notional")]}
    assert p.update_exit_plans({}, decision, response, context(quantity=2), datetime.now(UTC)) == {}
    response["order_results"][0].update(command_id="c-1", side="sell", quantity=1, avg_price=100)
    assert p.update_exit_plans(
        {"SPY": {"side": 1}}, decision, response, context(quantity=2), datetime.now(UTC)
    ) == {"SPY": {"side": 1}}
    assert (
        p.risk_exit_payload(
            {
                "market_research_snapshot": {
                    "as_of": "2026-10-09T14:00:00Z",
                    "snapshot_id": "x",
                    "market_session": "rth",
                },
                "portfolio": context(quantity=1)["portfolio"],
            },
            {},
            sequence=1,
        )["commands"]
        == []
    )
    for error, kind in [
        (subprocess.TimeoutExpired("model", 1), "model_timeout"),
        (RuntimeError("401 authentication"), "model_authentication"),
        (s.LlmEquityContractError("bad"), "model_contract_invalid"),
        (OSError("connection"), "model_transport_failure"),
    ]:
        assert p.failure_kind(error)[0] == kind


def test_model_cache_and_cooldown_never_reinvoke_on_unchanged_or_failed_evidence(
    tmp_path, monkeypatch, profit
):
    from test_hourly_llm_equity_service import ForcedAggressiveReviewLlmClient

    ctx = context()
    ctx["ranked_trade_candidates"] = [
        {"rank": 1, "symbol": "SPY", "side": "long", "edge_gate_status": "pass"}
    ]
    review = ForcedAggressiveReviewLlmClient().decide(ctx)
    calls = []

    def invoke(self, context):
        calls.append(1)
        return review

    monkeypatch.setattr(s.CodexLlmClient, "_decide", invoke)
    client = s.CodexLlmClient(tmp_path, 10)
    assert client.decide(ctx) == review
    with pytest.raises(s.LlmNoEdgeVeto, match="unchanged"):
        client.decide(ctx)
    ctx["portfolio"]["cash_usd"] = 9990

    def broken(self, context):
        raise TimeoutError("slow")

    monkeypatch.setattr(s.CodexLlmClient, "_decide", broken)
    with pytest.raises(RuntimeError, match="model_timeout"):
        client.decide(ctx)
    with pytest.raises(s.LlmNoEdgeVeto, match="cooldown"):
        client.decide(ctx)
    assert len(calls) == 1
