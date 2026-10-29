from datetime import UTC, datetime, timedelta

from llm_equity import hourly_service as service


def test_profit_policy_has_no_quota_and_bounded_context(monkeypatch):
    monkeypatch.setenv("TRAIDAN_LLM_EQUITY_FILL_MODE", "paper")
    monkeypatch.setenv("TRAIDAN_LLM_EQUITY_RISK_MODE", "profit_driven")
    assert service.profit_driven_risk_mode_enabled()
    assert not service._mandatory_trade_required({"market_session": "rth"}, cycle_kind="news_scan")
    assert not service._least_bad_forced_allowed()
    assert service.review_context_max_bytes() == 96_000
    assert service._context_review_candidate_limit({}, {"symbols": list(range(250))}) <= 5


def test_horizon_settlement_rejects_late_bars_and_orders_history():
    target = datetime(2026, 10, 9, 20, tzinfo=UTC)
    bars = {"SPY": [
        {"timestamp": (target + timedelta(days=3)).isoformat(), "close": 110},
        {"timestamp": (target + timedelta(minutes=2)).isoformat(), "close": 101},
        {"timestamp": target.isoformat(), "close": 100},
    ]}
    assert service._exit_price_from_history(bars, "SPY", target, as_of=target + timedelta(days=3)) == 100
    assert service._exit_price_from_history({"SPY": bars["SPY"][:1]}, "SPY", target) is None


def test_opening_readiness_waits_for_completed_bar_but_has_a_deadline(monkeypatch):
    monkeypatch.setenv("TRAIDAN_LLM_EQUITY_FILL_MODE", "paper")
    monkeypatch.setenv("TRAIDAN_LLM_EQUITY_RISK_MODE", "profit_driven")
    target = datetime(2026, 10, 12, 13, 30, tzinfo=UTC)
    snapshot = {"market_calendar_state": {"freshness": "fresh", "is_open": True}}
    assert service._opening_readiness_pending(snapshot, target, target)
    assert service._opening_readiness_pending(snapshot, target, target + timedelta(seconds=299))
    assert not service._opening_readiness_pending(snapshot, target, target + timedelta(minutes=5))
    snapshot["market_calendar_state"].update(is_open=False, next_open="2026-10-13T13:30:00Z")
    assert not service._opening_readiness_pending(snapshot, target, target)


def test_profit_policy_drops_missing_optional_candidate_but_keeps_held_stale(monkeypatch):
    monkeypatch.setenv("TRAIDAN_LLM_EQUITY_FILL_MODE", "paper")
    monkeypatch.setenv("TRAIDAN_LLM_EQUITY_RISK_MODE", "profit_driven")
    now = datetime(2026, 10, 12, 14, tzinfo=UTC)
    bars = {s: {"timestamp": now.isoformat(), "source": "alpaca_market_data"} for s in ["SPY", "AAPL"]}
    quotes = {s: dict(bars[s]) for s in bars}
    quotes["AAPL"]["source"] = "unavailable"
    assert service.usable_research_symbols(["SPY", "AAPL"], bars, quotes, as_of=now,
        protected_symbols=["SPY", "AAPL"]) == ["SPY"]
    assert service.usable_research_symbols(["SPY", "AAPL"], bars, quotes, as_of=now,
        required_symbols=["AAPL"], protected_symbols=["SPY", "AAPL"]) == ["SPY", "AAPL"]


def test_profit_risk_exit_payload_cannot_generate_new_exposure(tmp_path, monkeypatch):
    from llm_equity.contracts import load_equity_universe, validate_llm_decision
    from llm_equity.policy import risk_exit_payload
    universe = load_equity_universe()
    at = datetime(2026, 10, 12, 14, 30, tzinfo=UTC)
    snapshot = service.build_market_research_snapshot(universe, at, fixture_mode=True)
    context = {"market_research_snapshot": snapshot, "portfolio": {"nav_usd": 10000,
        "positions": [{"symbol": "SPY", "quantity": .001, "market_value_usd": .5}]}}
    decision = risk_exit_payload(context, {"SPY": "position_stop"}, sequence=1)
    valid = validate_llm_decision(decision)
    assert valid.commands[0].reduce_only
    assert valid.commands[0].max_notional_usd < 1
    assert valid.commands[0].action == "sell_notional"
    assert len(valid.commands) == 1


def test_profit_cycle_summary_keeps_sub_dollar_exit():
    row = {"cycle_kind": "run_once", "market_session": "rth", "cycle_id": "dust",
        "as_of": "2026-10-12T14:30:00Z", "status": "posted", "risk_mode": "profit_driven",
        "mandatory_trade_status": "not_applicable", "rust_response": {"order_results": [
            {"symbol": "SPY", "side": "sell", "status": "accepted", "notional_usd": .25,
             "reason": "simulated_fill"}]}}
    summary = service.cycle_audit_dashboard_summary(row)
    assert summary is not None
    assert summary["accepted_notional_usd"] == .25
    assert summary["required_notional_usd"] == 0
