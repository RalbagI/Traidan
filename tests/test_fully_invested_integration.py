"""Exercise the production allocator across the Python/Rust process boundary."""

import json
import shutil
from datetime import UTC, datetime, timedelta

import pytest
from llm_equity.research_simulation import RustResearchHarness
from test_fully_invested import review

from llm_equity import fully_invested as f
from llm_equity import hourly_service as s

pytestmark = pytest.mark.skipif(
    shutil.which("cargo") is None, reason="Rust harness requires the Rust toolchain"
)
AT = datetime(2026, 10, 22, 14, tzinfo=UTC)


def packet(positions=(), cash=10000):
    symbols = [f"STK{i}" for i in range(40)]
    snap = s.build_market_research_snapshot(
        s.load_equity_universe(), AT, fixture_mode=True, required_symbols=symbols
    )
    for symbol in symbols:
        snap["asset_metadata"][symbol]["investment_kind"] = "equity_stock"
    return {
        "portfolio": {"cash_usd": cash, "nav_usd": 10000, "positions": list(positions)},
        "market_research_snapshot": snap,
        "ranked_trade_candidates": [],
    }


def test_python_allocation_rust_rotation_receipt_and_complete_audit(tmp_path, monkeypatch):
    monkeypatch.setenv(s.LLM_EQUITY_FILL_MODE_ENV, "paper")
    monkeypatch.setenv(s.LLM_EQUITY_RISK_MODE_ENV, "fully_invested")
    state_file = tmp_path / "authority.json"
    ctx = packet()
    first = f.decision_from_review(review(), ctx, sequence=1)
    with RustResearchHarness(policy="fully_invested", state_file=state_file) as harness:
        filled = harness.apply(first, ctx["market_research_snapshot"], AT)
        portfolio = filled["portfolio"]
        assert len(portfolio["positions"]) == 20
        assert 0 <= portfolio["cash_usd"] <= 0.01
        # Restart paths must preserve actual funds and never synthesize another $10k.
    with RustResearchHarness(policy="fully_invested", state_file=state_file) as harness:
        assert (
            harness.apply(first, ctx["market_research_snapshot"], AT + timedelta(days=1)) == filled
        )
        ctx = packet(portfolio["positions"].values(), portfolio["cash_usd"])
        replacement = review()
        for i, row in enumerate(replacement["holdings"]):
            row["symbol"] = f"STK{i + 20}"
        decision = f.decision_from_review(replacement, ctx, sequence=2)
        result = harness.apply(decision, ctx["market_research_snapshot"], AT)
        assert len(result["accepted_orders"]) == 40
        assert 0 <= result["portfolio"]["cash_usd"] <= 0.01
        assert all(int(symbol[3:]) >= 20 for symbol in result["portfolio"]["positions"])
    response = {
        "status": "success",
        "accepted_orders": 40,
        "rejected_orders": 0,
        "order_results": result["accepted_orders"],
        "portfolio_nav_usd": 9900,
    }
    audit = s.record_cycle_audit(
        tmp_path,
        cycle_kind="run_once",
        status="posted",
        snapshot=ctx["market_research_snapshot"],
        decision=decision,
        rust_response=response,
    )
    assert len(audit["accepted_trades"]) == 40
    summary = s.cycle_audit_dashboard_summary(audit)
    assert len(summary["symbols"]) == 40
    assert len(summary["actions"]) == 40
    assert summary["accepted_notional_usd"] == pytest.approx(
        sum(o["notional_usd"] for o in result["accepted_orders"]), abs=0.0001
    )


def test_process_retains_committed_allocation_after_journal_error(tmp_path):
    state_file = tmp_path / "authority.json"
    state_file.with_suffix(".jsonl").mkdir()
    ctx = packet()
    decision = f.decision_from_review(review(), ctx, sequence=1)
    with RustResearchHarness(policy="fully_invested", state_file=state_file) as harness:
        with pytest.raises(RuntimeError, match="persistence failed"):
            harness.apply(decision, ctx["market_research_snapshot"], AT)
        durable = json.loads(state_file.read_text())
        assert len(durable["portfolio"]["positions"]) == 20
        assert harness.export_state()["portfolio"] == durable["portfolio"]
        state_file.with_suffix(".jsonl").rmdir()
        replay = harness.apply(decision, ctx["market_research_snapshot"], AT)
        assert replay["portfolio"] == durable["portfolio"]
        assert harness.export_state()["highest_sequence"] == 1
