import json
import os
from datetime import UTC, datetime

import pytest
from llm_equity.epoch import migrate_account_epoch


def seed(tmp_path):
    old = tmp_path / "epochs" / "epoch-aaaaaaaaaaaaaaaa"
    old.mkdir(parents=True)
    state = {"schema_version": 1, "saved_at": "2026-10-12T14:00:00Z", "highest_sequence": 99,
        "seen_cycle_ids": ["old-fill"], "portfolio": {"initial_cash_usd": 10000,
            "cash_usd": 9500, "positions": {"SPY": {"quantity": 5, "avg_price": 99}},
            "peak_nav_usd": 10010, "day_start_nav_usd": 10005}, "last_prices": {"SPY": 101}}
    (old / "rust-authority-state.json").write_text(json.dumps(state))
    (tmp_path / "strategy-epoch.json").write_text(json.dumps({"epoch_id": old.name}))
    return old, state


def test_migration_preserves_cash_positions_sequences_and_is_idempotent(tmp_path):
    old, state = seed(tmp_path)
    target = {"epoch_id": "epoch-bbbbbbbbbbbbbbbb", "components": {"risk_policy_version": "profit_driven_v1"}}
    at = datetime(2026, 10, 12, 15, tzinfo=UTC)
    path = migrate_account_epoch(tmp_path, target, at=at)
    assert json.loads((path / "rust-authority-state.json").read_text()) == state
    opening = json.loads((path / "account-opening-state.json").read_text())
    assert opening["portfolio"] == state["portfolio"]
    assert opening["seen_cycle_ids"] == ["old-fill"]
    assert json.loads((path / "policy-transition.json").read_text())["from_epoch"] == old.name
    assert migrate_account_epoch(tmp_path, target, at=at) == path


def test_migration_refuses_a_running_writer(tmp_path):
    seed(tmp_path)
    pids = tmp_path.parent / "local-dry-run" / "pids"
    pids.mkdir(parents=True, exist_ok=True)
    pid = pids / "rust.pid"
    pid.write_text(str(os.getpid()))
    try:
        with pytest.raises(RuntimeError, match="stop paper services"):
            migrate_account_epoch(tmp_path, {"epoch_id": "epoch-bbbbbbbbbbbbbbbb"}, at=datetime.now(UTC))
    finally:
        pid.unlink()


def test_fully_invested_migration_preserves_account_without_blanket_expiry(tmp_path):
    _, state = seed(tmp_path)
    target = {"epoch_id": "epoch-bbbbbbbbbbbbbbbb", "components": {"risk_policy_version": "fully_invested_v1"}}
    path = migrate_account_epoch(tmp_path, target, at=datetime(2026, 10, 22, 15, tzinfo=UTC))
    assert json.loads((path / "rust-authority-state.json").read_text()) == state
    assert json.loads((path / "position-exit-plans.json").read_text()) == {}
    assert json.loads((path / "account-opening-exit-plans.json").read_text()) == {}
