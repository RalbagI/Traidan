import copy
import json
from datetime import UTC, datetime, timedelta

import pytest
from llm_equity.accounting import account_report
from llm_equity.epoch import migrate_account_epoch
from llm_equity.profitability import build_profitability_report
from llm_equity.replay import PointInTimeEventStore
from test_hourly_llm_equity_service import ForcedAggressiveReviewLlmClient
from test_llm_equity_accounting import _state, _write
from test_llm_equity_epoch import seed
from test_llm_equity_profit_execution import command
from test_llm_equity_profit_service import AT, setup_context
from test_llm_equity_profitability import EPOCH_A, _initialize_epoch

from llm_equity import hourly_service as s


@pytest.fixture(autouse=True)
def profit(monkeypatch):
    monkeypatch.setenv(s.LLM_EQUITY_FILL_MODE_ENV, "paper")
    monkeypatch.setenv(s.LLM_EQUITY_RISK_MODE_ENV, "profit_driven")


@pytest.mark.parametrize(
    "fault",
    [
        "seed_only",
        "missing_authority",
        "bad_identity",
        "bad_hash",
        "bad_seed",
        "bad_authority",
        None,
    ],
)
def test_activation_requires_complete_matching_migration(tmp_path, fault):
    old, original = seed(tmp_path)
    descriptor = s.strategy_epoch_descriptor()
    target = migrate_account_epoch(tmp_path, descriptor, at=AT)
    (tmp_path / "strategy-epoch.json").write_text(json.dumps({"epoch_id": old.name}))
    receipt = json.loads((target / "policy-transition.json").read_text())
    if fault == "seed_only":
        (target / "policy-transition.json").unlink()
        (target / "rust-authority-state.json").unlink()
    elif fault == "missing_authority":
        (target / "rust-authority-state.json").unlink()
    elif fault in ("bad_identity", "bad_hash"):
        receipt["to_epoch" if fault == "bad_identity" else "source_sha256"] = "invalid"
        (target / "policy-transition.json").write_text(json.dumps(receipt))
    elif fault in ("bad_seed", "bad_authority"):
        broken = copy.deepcopy(original)
        broken["portfolio"]["cash_usd"] = 10000
        name = "account-opening-state.json" if fault == "bad_seed" else "rust-authority-state.json"
        (target / name).write_text(json.dumps(broken))
    if fault:
        with pytest.raises(RuntimeError, match="epoch incomplete"):
            s.ensure_strategy_epoch(tmp_path)
        assert json.loads((tmp_path / "strategy-epoch.json").read_text())["epoch_id"] == old.name
    else:
        assert s.ensure_strategy_epoch(tmp_path) == descriptor
        assert json.loads((target / "rust-authority-state.json").read_text()) == original
        (target / "rust-authority-state.json").unlink()
        with pytest.raises(RuntimeError, match="epoch incomplete"):
            s.ensure_strategy_epoch(tmp_path)


def test_accepted_receipt_is_durable_before_exit_plan_write(tmp_path, monkeypatch):
    config, post, snapshot, context, memory = setup_context(tmp_path)
    monkeypatch.setattr(
        s, "build_service_context", lambda *args, **kwargs: (memory, snapshot, context)
    )
    write = s.write_json_atomically

    def fail_plan(path, value):
        if path.name == "position-exit-plans.json":
            audits = s.read_cycle_audit_ledger(tmp_path)
            assert audits[-1]["rust_response"]["accepted_orders"] == 1
            raise PermissionError("derivative store unavailable")
        return write(path, value)

    monkeypatch.setattr(s, "write_json_atomically", fail_plan)
    with pytest.raises(PermissionError, match="derivative"):
        s._execute_contextual_trade_cycle(
            config,
            cycle_kind="run_once",
            current=AT,
            post=post,
            universe=s.load_equity_universe(),
            memory_store=memory,
            snapshot=snapshot,
            context=context,
            client=ForcedAggressiveReviewLlmClient(),
        )
    audits = s.read_cycle_audit_ledger(tmp_path)
    assert [row["status"] for row in audits] == ["posted", "failed"]
    assert all(row["rust_response"]["accepted_orders"] == 1 for row in audits)
    epoch = s.strategy_epoch_dir(tmp_path)
    events = PointInTimeEventStore(epoch / "research-events.jsonl").read_all()
    fill = next(
        row["payload"]["order_results"][0] for row in events if row["event_type"] == "replay_cycle"
    )
    state = _state(AT.isoformat(), 10000 - fill["notional_usd"], 0, 100, [audits[-1]["cycle_id"]])
    state["portfolio"]["positions"] = {
        "SPY": {"quantity": fill["quantity"], "avg_price": fill["avg_price"]}
    }
    state["last_prices"]["SPY"] = fill["avg_price"]
    _write(epoch, [state])
    report = account_report(epoch, events)
    assert report["reconciliation"]["status"] == "matched"
    assert report["accepted_fill_count"] == 1


def write_session_clocks(epoch, marks):
    store = PointInTimeEventStore(epoch / "research-events.jsonl")
    for mark in marks:
        at = datetime.fromisoformat(mark["saved_at"])
        store.append(
            "policy_frame",
            at - timedelta(minutes=1),
            {
                "snapshot": {
                    "as_of": (at - timedelta(minutes=1)).isoformat(),
                    "market_calendar_state": {
                        "freshness": "fresh",
                        "is_open": True,
                        "next_close": at.replace(hour=20, minute=0, second=0).isoformat(),
                    },
                }
            },
        )


def forward_report_epoch(tmp_path):
    epoch = _initialize_epoch(tmp_path, EPOCH_A)
    start = datetime(2026, 8, 3, 20, tzinfo=UTC)
    opening = _state((start - timedelta(days=3)).isoformat(), 9900, 1, 100, [])
    (epoch / "account-opening-state.json").write_text(json.dumps(opening))
    (epoch / "profit-trial-registry.json").write_text(json.dumps({"forward_start": "2026-08-03"}))
    (epoch / "economic-cost-policy.json").write_text(
        json.dumps(
            {
                "commission_bps": 0,
                "borrow_apr": 0,
                "financing_apr": 0,
                "monthly_model_usd": 0,
                "monthly_data_usd": 0,
                "monthly_hosting_usd": 0,
                "exclude_operating_costs": True,
            }
        )
    )
    days = [
        start + timedelta(days=i) for i in range(35) if (start + timedelta(days=i)).weekday() < 5
    ][:21]
    marks = [_state(day.isoformat(), 9900, 1, 60 + i, []) for i, day in enumerate(days)]
    for mark in marks:
        mark["last_prices"].update(SPY=100, QQQ=100)
    _write(epoch, marks)
    write_session_clocks(epoch, marks)
    store = PointInTimeEventStore(epoch / "forecast-outcomes.jsonl")
    for i in range(100):
        store.append(
            "entry_forecast_settled",
            days[-1] + timedelta(minutes=1),
            {
                "cycle_id": f"entry-{i}",
                "command_index": 1,
                "opened_at": days[0].isoformat(),
                "settled_at": days[-1].isoformat(),
            },
        )
    return epoch, days


def test_report_start_cannot_remove_registered_forward_losses(tmp_path):
    _epoch, days = forward_report_epoch(tmp_path)
    full = build_profitability_report(tmp_path)
    selected = build_profitability_report(tmp_path, start=days[1].date().isoformat())
    assert full["account"]["economic"]["net_pnl_usd"] == -20
    assert selected["account"]["economic"]["net_pnl_usd"] == 20
    assert selected["forward_promotion"] == full["forward_promotion"]
    assert not full["forward_promotion"]["gates"]["positive_economic_profit"]
    assert not full["forward_promotion"]["eligible"]
    early = build_profitability_report(tmp_path, end="2026-08-01")
    assert early["forward_promotion"]["future_sessions"] == 0


@pytest.mark.parametrize("cost", [1, 100])
@pytest.mark.parametrize("later_mark", ["none", "partial", "complete"])
def test_promotion_accounts_for_costs_after_last_completed_session(tmp_path, cost, later_mark):
    epoch, days = forward_report_epoch(tmp_path)
    marks = [_state(day.isoformat(), 9900, 1, 100 + i, []) for i, day in enumerate(days)]
    if later_mark != "none":
        at = days[-1] + timedelta(days=1, hours=-4 if later_mark == "partial" else 0)
        marks.append(_state(at.isoformat(), 9900, 1, 120, []))
    for mark in marks:
        mark["last_prices"].update(SPY=100, QQQ=100)
    _write(epoch, marks)
    write_session_clocks(epoch, marks)
    path = epoch / "economic-cost-policy.json"
    policy = json.loads(path.read_text())
    policy["exclude_operating_costs"] = False
    path.write_text(json.dumps(policy))
    at = days[-1] + timedelta(minutes=1)
    PointInTimeEventStore(epoch / "research-events.jsonl").append(
        "replay_cycle",
        at,
        {
            "decision_id": "late-cost",
            "schema_version": 3,
            "as_of": at.isoformat(),
            "llm_invoked": True,
            "model_call_cost_usd": cost,
            "model_cost_basis": "configured_api",
            "cost_disposition": "direct",
        },
    )
    full = build_profitability_report(tmp_path)
    selected = build_profitability_report(tmp_path, start=days[1].date().isoformat())
    assert full["account"]["reconciliation"]["status"] == "matched"
    assert full["account"]["economic"]["net_pnl_usd"] == 20 - cost
    gate = full["forward_promotion"]
    assert gate["gates"]["all_costs_in_completed_sessions"] is (later_mark == "complete")
    assert gate["gates"]["positive_economic_profit"] is (cost == 1)
    assert gate["eligible"] is (cost == 1 and later_mark == "complete")
    assert selected["forward_promotion"] == gate


@pytest.mark.parametrize("kind", ["fixed", "borrow", "financing"])
def test_recurring_costs_continue_through_assessment_without_new_marks(tmp_path, kind):
    epoch, days = forward_report_epoch(tmp_path)
    quantity, cash = (
        (-1, 10100) if kind == "borrow" else (101, -100) if kind == "financing" else (1, 9900)
    )
    opening = _state((days[0] - timedelta(days=3)).isoformat(), cash, quantity, 100, [])
    (epoch / "account-opening-state.json").write_text(json.dumps(opening))
    marks = [
        _state(day.isoformat(), cash, quantity, 100 + i / quantity, [])
        for i, day in enumerate(days)
    ]
    for mark in marks:
        mark["last_prices"].update(SPY=100, QQQ=100)
    _write(epoch, marks)
    policy_path = epoch / "economic-cost-policy.json"
    policy = json.loads(policy_path.read_text())
    if kind == "fixed":
        policy.update(exclude_operating_costs=False, monthly_hosting_usd=15)
        daily_cost = 0.5
    else:
        policy[kind + "_apr"] = 2
        daily_cost = (80 if kind == "borrow" else 100) * 2 / 365
    policy_path.write_text(json.dumps(policy))
    cutoff = datetime(2026, 10, 1, tzinfo=UTC)
    events = PointInTimeEventStore(epoch / "research-events.jsonl").read_all()
    account = account_report(epoch, events, start="2026-08-03", end=cutoff.isoformat())
    trailing_days = (cutoff - days[-1]).total_seconds() / 86400
    assert account["economic"]["unallocated_session_cost_usd"] == pytest.approx(
        daily_cost * trailing_days
    )
    assert account["economic"]["net_pnl_usd"] < 0
    if kind == "fixed":
        assert account["economic"]["fixed_cost_usd"] == 29.5
        assert account["economic"]["net_pnl_usd"] == -9.5
    report = build_profitability_report(tmp_path, end=cutoff.isoformat())
    selected = build_profitability_report(
        tmp_path, start=days[1].date().isoformat(), end=cutoff.isoformat()
    )
    assert report["forward_promotion"] == selected["forward_promotion"]
    assert not report["forward_promotion"]["gates"]["all_costs_in_completed_sessions"]
    assert not report["forward_promotion"]["gates"]["positive_economic_profit"]
    assert not report["forward_promotion"]["eligible"]


def test_startup_retries_pending_opening_slot_until_completed(tmp_path, monkeypatch):
    config = s.HourlyServiceConfig(state_dir=tmp_path)
    cycle = s.ScheduledCycle(kind="release", run_at=AT, market_session=s.MarketSession.RTH)
    monkeypatch.setattr(s, "stale_trade_cycles_for_now", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        s, "due_cycles_for_now", lambda *args, **kwargs: [cycle] if not kwargs["seen"] else []
    )
    answers = iter([{"status": "readiness_pending"}, {"status": "held"}])
    monkeypatch.setattr(s, "run_scheduled_cycle_safely", lambda *args, **kwargs: next(answers))
    seen = set()
    s.run_startup_cycle(config, now=AT, seen=seen)
    assert seen == set()
    s.run_due_cycles_once(config, now=AT + timedelta(seconds=30), seen=seen)
    assert seen == {s.cycle_key(cycle)}


def test_close_forecast_uses_early_calendar_close_and_completed_bar(tmp_path):
    _config, _post, snapshot, context, _memory = setup_context(tmp_path)
    at = datetime(2026, 11, 27, 17, tzinfo=UTC)
    snapshot.update(as_of=at.isoformat())
    snapshot["market_calendar_state"].update(next_close="2026-11-27T18:00:00Z")
    outcome = s._pending_outcome_from_command(
        {**command(), "horizon": "close"}, context=context, strategy_book="core", notional_share=1
    )
    assert outcome["settle_after"] == "2026-11-27T18:00:00Z"
    target = datetime(2026, 11, 27, 18, tzinfo=UTC)
    assert (
        s._exit_price_from_history(
            {"SPY": [{"timestamp": "2026-11-27T17:59:00Z", "close": 100}]},
            "SPY",
            target,
            as_of=target,
        )
        == 100
    )
    snapshot["market_calendar_state"]["freshness"] = "unavailable"
    assert (
        s._pending_outcome_from_command(
            {**command(), "horizon": "close"},
            context=context,
            strategy_book="core",
            notional_share=1,
        )
        is None
    )
