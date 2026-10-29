from __future__ import annotations

import json
from pathlib import Path

import pytest
from llm_equity.accounting import account_report, deduplicate_audits, position_effect


def _state(at: str, cash: float, quantity: float, price: float, ids: list[str]) -> dict:
    return {
        "saved_at": at, "highest_sequence": len(ids), "seen_cycle_ids": ids,
        "portfolio": {"initial_cash_usd": 10000.0, "cash_usd": cash,
                      "positions": {"ABC": {"quantity": quantity, "avg_price": 100.0}}},
        "last_prices": {"ABC": price, "SPY": price, "QQQ": price},
    }


def _event(cid: str, side: str, qty: float, price: float) -> dict:
    return {"event_type": "replay_cycle", "payload": {
        "decision_id": cid, "model_cost_basis": "subscription_zero",
        "model_call_cost_usd": 0.0, "order_results": [{
            "command_id": cid + "-1", "status": "accepted", "symbol": "ABC",
            "side": side, "quantity": qty, "avg_price": price,
            "notional_usd": qty * price,
        }]}}


def _write(path: Path, rows: list[dict]) -> None:
    (path / "rust-authority-state.jsonl").write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n"
    )


def test_account_reconstructs_partial_exit_without_inventing_short(tmp_path: Path) -> None:
    _write(tmp_path, [
        _state("2026-09-14T14:00:00Z", 9900, 1, 100, ["buy"]),
        _state("2026-09-15T14:00:00Z", 9955, .5, 110, ["buy", "sell"]),
    ])
    events = [_event("buy", "buy", 1, 100), _event("sell", "sell", .5, 110)]
    result = account_report(tmp_path, events)
    assert result["account_pnl_usd"] == pytest.approx(10)
    assert result["reconciliation"]["status"] == "matched"
    assert result["fill_counts"] == {"open_long": 1, "reduce_long": 1}
    assert result["economic"]["status"] == "cost_unknown"
    assert result["economic"]["net_pnl_usd"] is None


def test_window_carries_opening_position_and_uses_half_open_end(tmp_path: Path) -> None:
    _write(tmp_path, [
        _state("2026-09-14T14:00:00Z", 9900, 1, 100, ["buy"]),
        _state("2026-09-15T14:00:00Z", 9955, .5, 110, ["buy", "sell"]),
        _state("2026-09-16T14:00:00Z", 10015, 0, 120, ["buy", "sell", "close"]),
    ])
    events = [_event("buy", "buy", 1, 100), _event("sell", "sell", .5, 110),
              _event("close", "sell", .5, 120)]
    result = account_report(tmp_path, events, start="2026-09-15", end="2026-09-16")
    assert result["account_pnl_usd"] == pytest.approx(10)
    assert result["opening_nav_usd"] == 10000
    assert result["accepted_fill_count"] == 1
    assert result["benchmarks"]["SPY"]["return_fraction"] == pytest.approx(.1)


def test_duplicate_fill_is_idempotent_and_conflicting_fill_fails(tmp_path: Path) -> None:
    _write(tmp_path, [_state("2026-09-14T14:00:00Z", 9900, 1, 100, ["buy"])])
    event = _event("buy", "buy", 1, 100)
    assert account_report(tmp_path, [event, event])["accepted_fill_count"] == 1
    with pytest.raises(ValueError, match="conflicting fill"):
        account_report(tmp_path, [event, _event("buy", "buy", 2, 100)])


def test_missing_fill_evidence_blocks_reconciliation(tmp_path: Path) -> None:
    _write(tmp_path, [_state("2026-09-14T14:00:00Z", 9900, 1, 100, ["buy"])])
    result = account_report(tmp_path, [])
    assert result["reconciliation"]["status"] == "mismatch"
    assert result["promotion_eligible"] is False


@pytest.mark.parametrize(("before", "after", "effect"), [
    (0, 1, "open_long"), (0, -1, "open_short"), (1, 2, "increase_long"),
    (2, 1, "reduce_long"), (1, 0, "close_long"), (-1, 0, "close_short"),
    (-2, -1, "cover_short"), (-1, -2, "increase_short"), (1, -1, "reverse"),
])
def test_position_effect(before: float, after: float, effect: str) -> None:
    assert position_effect(before, after) == effect


def test_audit_deduplication_preserves_invocation_and_failure_evidence() -> None:
    rich = {"cycle_id": "a", "status": "failed", "llm_invoked": True,
            "source_health_reasons": ["stale:quotes"], "review_attestation": {"invocation_count": 1}}
    duplicate = {"cycle_id": "a", "status": "failed", "llm_invoked": False}
    assert deduplicate_audits([rich, duplicate]) == [rich]


def test_model_cost_charged_once_when_prepare_is_linked_to_release(tmp_path):
    from llm_equity.accounting import model_cost_events
    events = [
        {"event_type": "replay_cycle", "payload": {"decision_id": "slot", "as_of": "2026-10-09T13:15:00Z", "model_call_cost_usd": 2, "llm_invoked": True, "cost_disposition": "deferred"}},
        {"event_type": "replay_cycle", "payload": {"decision_id": "slot", "as_of": "2026-10-09T13:30:00Z", "model_call_cost_usd": 2, "llm_invoked": False, "cost_disposition": "linked"}},
    ]
    assert sum(cost for _, cost in model_cost_events(events + events)) == 2


def test_exposure_matched_benchmark_uses_prior_day_inventory():
    from llm_equity.accounting import session_returns
    states = [
        _state("2026-10-08T20:00:00Z", 9500, .5, 1000, []),
        _state("2026-10-09T20:00:00Z", 0, 10, 1010, []),
    ]
    states[0]["last_prices"]["QQQ"] = 100
    states[1]["last_prices"]["QQQ"] = 102
    rows = session_returns(states[0], states[1:])
    assert rows[0]["SPY_exposure_matched"] == pytest.approx(.05 * .01)
    assert rows[0]["QQQ_exposure_matched"] == pytest.approx(.05 * .02)


def test_owner_exclusion_retains_execution_costs_and_observed_session_allocation(tmp_path):
    rows = [_state("2026-10-08T20:00:00Z", 10000, 0, 100, []),
            _state("2026-10-09T19:59:00Z", 9900, 1, 100, ["buy"]),
            _state("2026-10-12T19:59:00Z", 10010, 0, 110, ["buy", "sell"])]
    _write(tmp_path, rows)
    (tmp_path / "economic-cost-policy.json").write_text(json.dumps({
        "commission_bps": 10, "borrow_apr": .05, "financing_apr": .03,
        "monthly_model_usd": 0, "monthly_data_usd": 0, "monthly_hosting_usd": 0,
        "exclude_operating_costs": True}))
    buy, sell = _event("buy", "buy", 1, 100), _event("sell", "sell", 1, 110)
    buy["payload"].update(as_of="2026-10-09T19:59:00Z", model_call_cost_usd=5)
    result = account_report(tmp_path, [buy, sell], start="2026-10-09")
    assert result["economic"]["operating_costs"] == "excluded_by_owner"
    assert result["economic"]["model_usd"] == 0
    assert result["economic"]["net_pnl_usd"] == pytest.approx(9.79)
    assert [row["economic_cost_usd"] for row in result["sessions"]] == pytest.approx([.1, .11])
    assert result["promotion_eligible"]


def test_missing_position_mark_reports_incomplete_evidence(tmp_path):
    row = _state("2026-10-09T20:00:00Z", 9900, 1, 100, ["buy"])
    del row["last_prices"]["ABC"]
    _write(tmp_path, [row])
    result = account_report(tmp_path, [_event("buy", "buy", 1, 100)])
    assert result["status"] == "holding_marks_unavailable"
    assert result["promotion_eligible"] is False
