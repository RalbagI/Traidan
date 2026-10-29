from datetime import UTC, datetime

import pytest
from llm_equity.policy import cap_entries, exit_reasons, update_exit_plans


def context(quantity=0.0, price=100.0):
    return {"portfolio": {"nav_usd": 10000, "positions": [
        {"symbol": "SPY", "quantity": quantity, "market_value_usd": quantity * price,
         "avg_price": 100.0}] if quantity else []},
        "profitability_scorecard": {"risk": {"daily_pnl_usd": 0, "max_drawdown_pct": 0}},
        "market_research_snapshot": {"as_of": "2026-10-12T15:00:00Z"}}


def command(action="buy_notional", notional=1000):
    return {"symbol": "SPY", "action": action, "max_notional_usd": notional,
            "horizon": "1d", "rationale": "approved thesis", "forecast": {
                "edge_class": "positive_edge", "expected_excess_return_bps": 20,
                "return_p10_bps": -500}}


def test_cap_is_remaining_position_capacity_and_stop_budget():
    decision = {"commands": [command()]}
    capped = cap_entries(decision, context(quantity=4))
    assert capped["commands"][0]["max_notional_usd"] == 100
    decision["commands"][0]["forecast"]["return_p10_bps"] = -1000
    assert cap_entries(decision, context())["commands"][0]["max_notional_usd"] == 250


def test_opposing_order_can_only_reduce_and_no_quota():
    capped = cap_entries({"commands": [command("sell_notional")]}, context(quantity=.001))
    assert capped["commands"][0]["reduce_only"]
    assert capped["commands"][0]["max_notional_usd"] == pytest.approx(.105)
    assert cap_entries({"commands": [command()]}, context(quantity=5))["commands"] == []


def test_no_forecast_or_nonpositive_entry_is_rejected():
    for forecast in (None, {"edge_class": "least_bad_forced", "expected_excess_return_bps": -1}):
        row = command()
        row["forecast"] = forecast
        with pytest.raises(ValueError, match="positive after-cost"):
            cap_entries({"commands": [row]}, context())


def test_explicit_exit_plans_only_from_accepted_entries_and_do_not_extend():
    at = datetime(2026, 10, 12, 15, tzinfo=UTC)
    decision = {"commands": [command()], "cycle_id": "c1"}
    rejected = {"order_results": [{"status": "rejected"}]}
    assert update_exit_plans({}, decision, rejected, context(), at) == {}
    response = {"order_results": [{"status": "accepted", "command_id": "c1-1",
        "symbol": "SPY", "side": "buy", "quantity": 5, "avg_price": 100}]}
    plans = update_exit_plans({}, decision, response, context(), at)
    assert plans["SPY"]["stop_price"] == 95
    assert plans["SPY"]["exit_at"] == "2026-10-13T15:00:00+00:00"
    assert exit_reasons(context(quantity=5), plans, at) == {}
    assert exit_reasons(context(quantity=5, price=94), plans, at) == {"SPY": "position_stop"}
    later = datetime(2026, 10, 13, 15, tzinfo=UTC)
    assert exit_reasons(context(quantity=5), plans, later) == {"SPY": "thesis_horizon"}
    assert update_exit_plans(plans, decision, response, context(quantity=1), later)["SPY"]["exit_at"] == plans["SPY"]["exit_at"]


def test_aggregate_risk_exit_works_without_a_model_or_thesis():
    ctx = context(quantity=5)
    ctx["profitability_scorecard"]["risk"]["max_drawdown_pct"] = .10
    assert exit_reasons(ctx, {}, datetime.now(UTC)) == {"SPY": "drawdown_limit"}


def test_review_signature_ignores_timestamp_but_tracks_prices_and_portfolio():
    from llm_equity.policy import failure_kind, review_signature
    ctx = context()
    ctx["market_research_snapshot"]["latest_quotes"] = {"SPY": {"bid": 99.99, "ask": 100.01}}
    first = review_signature(ctx)
    ctx["market_research_snapshot"]["as_of"] = "2026-10-12T16:00:00Z"
    assert review_signature(ctx) == first
    ctx["market_research_snapshot"]["latest_quotes"]["SPY"] = {"bid": 101, "ask": 101.02}
    assert review_signature(ctx) != first
    assert failure_kind(TimeoutError())[0] == "model_timeout"
    assert failure_kind(RuntimeError("usage limit reached"))[0] == "model_rate_limit"


def test_entry_revalidation_rejects_material_price_move():
    from llm_equity.policy import revalidate_entry_prices
    old = context()
    old["market_research_snapshot"]["latest_quotes"] = {"SPY": {"bid": 99.99, "ask": 100.01}}
    new = context()
    new["market_research_snapshot"]["latest_quotes"] = {"SPY": {"bid": 100.99, "ask": 101.01}}
    with pytest.raises(ValueError, match="material price change"):
        revalidate_entry_prices({"commands": [command()]}, old, new)
    revalidate_entry_prices({"commands": [{**command(), "reduce_only": True}]}, old, new)


def test_reduce_only_approval_never_becomes_entry_when_book_changes():
    row = {**command("sell_notional"), "reduce_only": True}
    assert cap_entries({"commands": [row]}, context())["commands"] == []
    assert cap_entries({"commands": [row]}, context(quantity=-1))["commands"] == []
    reduced = cap_entries({"commands": [{**command("buy_notional", .1), "reduce_only": True}]},
                          context(quantity=-.001))["commands"][0]
    assert reduced["max_notional_usd"] > .1  # cover the ask; authority clamps quantity
