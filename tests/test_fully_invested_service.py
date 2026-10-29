import copy
from datetime import UTC, datetime

import pytest
from test_fully_invested import review

from llm_equity import fully_invested_service as f
from llm_equity import hourly_service as s

AT = datetime(2026, 10, 22, 14, 0, tzinfo=UTC)


class Client:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = 0
        self.last_usage = {}

    def decide_portfolio(self, context):
        self.calls += 1
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        return response


class Poster:
    def __init__(self):
        self.cycles = []
        self.fail = False

    def __call__(self, path, payload):
        if path == s.LLM_EQUITY_CYCLE_PATH:
            self.cycles.append(payload)
            if self.fail:
                raise OSError("connection interrupted")
            return {
                "status": "success",
                "cycle_id": payload["llm_trading_decision"]["cycle_id"],
                "accepted_orders": 20,
                "rejected_orders": 0,
                "order_results": [],
            }
        return {"status": "ok"}


@pytest.fixture
def setup(tmp_path, monkeypatch):
    monkeypatch.setenv(s.LLM_EQUITY_FILL_MODE_ENV, "paper")
    monkeypatch.setenv(s.LLM_EQUITY_RISK_MODE_ENV, "fully_invested")
    cfg = s.HourlyServiceConfig(state_dir=tmp_path)
    symbols = [f"STK{i}" for i in range(25)]
    snap = s.build_market_research_snapshot(
        s.load_equity_universe(), AT, fixture_mode=True, required_symbols=symbols
    )
    for symbol in symbols:
        snap["asset_metadata"][symbol]["investment_kind"] = "equity_stock"
    context = {
        "portfolio": {"cash_usd": 10000, "nav_usd": 10000, "positions": []},
        "market_research_snapshot": snap,
        "ranked_trade_candidates": [],
    }
    monkeypatch.setattr(
        s,
        "build_service_context",
        lambda *a, **k: (None, copy.deepcopy(snap), copy.deepcopy(context)),
    )
    return cfg, Poster(), snap


def test_repair_before_any_order_and_persist_review(setup):
    cfg, post, _ = setup
    client = Client([{"verdict": "abstain", "abstain_reason": "negative forecasts"}, review()])
    result = f.run_cycle(cfg, now=AT, poster=post, llm_client=client)
    assert result["status"] == "success"
    assert client.calls == 2
    assert len(post.cycles) == 1
    audit = list(s.strategy_epoch_dir(cfg.state_dir).glob("allocation-reviews/*.json"))
    assert len(audit) == 1
    assert "negative forecasts" in audit[0].read_text()
    assert not f.pending_path(cfg).exists()


def test_all_model_failures_preserve_account_and_are_not_success(setup):
    cfg, post, _ = setup
    result = f.run_cycle(cfg, now=AT, poster=post, llm_client=Client([RuntimeError("offline")] * 3))
    assert result["status"] == "blocked"
    assert post.cycles == []
    assert (
        "offline"
        in next(s.strategy_epoch_dir(cfg.state_dir).glob("allocation-reviews/*.json")).read_text()
    )


def test_ambiguous_post_is_retried_identically_without_another_model_review(setup):
    cfg, post, _ = setup
    post.fail = True
    result = f.run_cycle(cfg, now=AT, poster=post, llm_client=Client([review()]))
    assert result["status"] == "pending"
    assert f.pending_path(cfg).exists()
    original = copy.deepcopy(post.cycles[0])
    post.fail = False
    client = Client([])
    assert f.run_cycle(cfg, now=AT, poster=post, llm_client=client)["status"] == "success"
    assert post.cycles[-1] == original
    assert client.calls == 0


def test_closed_market_never_posts(setup):
    cfg, post, snap = setup
    snap["market_calendar_state"]["is_open"] = False
    assert f.run_cycle(cfg, now=AT, poster=post, llm_client=Client([]))["status"] == "blocked"
    assert not post.cycles


def test_committed_allocation_recovery_does_not_duplicate_ledger_or_model_cost(setup, monkeypatch):
    cfg, post, _ = setup
    write = s.write_json_atomically

    def fail_scheduler(path, value):
        if path.name == "allocation-scheduler.json":
            raise OSError("disk full after receipt")
        return write(path, value)

    monkeypatch.setattr(s, "write_json_atomically", fail_scheduler)
    with pytest.raises(OSError, match="disk full"):
        f.run_cycle(cfg, now=AT, poster=post, llm_client=Client([review()]))
    assert f.pending_path(cfg).exists()
    assert len(s.read_cycle_audit_ledger(cfg.state_dir)) == 1
    monkeypatch.setattr(s, "write_json_atomically", write)
    assert f.run_cycle(cfg, now=AT, poster=post, llm_client=Client([]))["status"] == "success"
    ledger = s.read_cycle_audit_ledger(cfg.state_dir)
    assert len(ledger) == 1
    assert ledger[0]["review_attestation"]["invocation_count"] == 1
    assert ledger[0]["review_attestation"]["status"] == "compliant"


@pytest.mark.parametrize("code", [400, 503])
def test_http_errors_preserve_ambiguous_requests_and_record_authority_rejection(setup, code):
    from io import BytesIO
    from urllib.error import HTTPError

    cfg, post, _ = setup

    def failing(path, payload):
        if path == s.LLM_EQUITY_CYCLE_PATH:
            raise HTTPError(
                "http://localhost", code, "rejected", {}, BytesIO(b'{"error":"turnover_limit"}')
            )
        return post(path, payload)

    if code == 400:
        assert (
            f.run_cycle(cfg, now=AT, poster=failing, llm_client=Client([review()]))["status"]
            == "blocked"
        )
        assert not f.pending_path(cfg).exists()
    else:
        assert (
            f.run_cycle(cfg, now=AT, poster=failing, llm_client=Client([review()]))["status"]
            == "pending"
        )
        assert f.pending_path(cfg).exists()


def test_requote_requires_reapproval_on_material_price_change(setup, monkeypatch):
    cfg, post, _ = setup
    original = s.build_service_context
    calls = 0

    def moving(*a, **kw):
        nonlocal calls
        calls += 1
        store, snapshot, context = original(*a, **kw)
        if calls % 2 == 0:
            snapshot["latest_quotes"]["STK0"]["ask"] *= 1.02
            context["market_research_snapshot"] = snapshot
        return store, snapshot, context

    monkeypatch.setattr(s, "build_service_context", moving)
    result = f.run_cycle(cfg, now=AT, poster=post, llm_client=Client([review()] * 3))
    assert result["status"] == "blocked"
    assert "material price change" in result["reason"]
    assert post.cycles == []


@pytest.mark.parametrize(
    "trigger",
    ["cash", "first", "stop", "expiry", "schedule", "none", "closed", "retry", "stale", "pending"],
)
def test_poll_replaces_expired_or_stopped_positions_without_standalone_sales(
    setup, monkeypatch, trigger
):
    from datetime import timedelta

    from llm_equity.simulator import EquityPaperPortfolio, EquityPosition

    cfg, post, snap = setup
    portfolio = EquityPaperPortfolio.seeded(s.load_equity_universe())
    portfolio.cash_usd = 10 if trigger == "cash" else 0.00001
    portfolio.positions = {"STK0": EquityPosition(symbol="STK0", quantity=100, avg_price=100)}
    when = AT if trigger != "closed" else AT.replace(hour=1)
    state = {
        "last_review_at": (when - timedelta(minutes=1)).isoformat(),
        "plans": {"STK0": {"review_at": (when + timedelta(days=1)).isoformat(), "stop_price": 0}},
    }
    if trigger == "first":
        state = {}
    if trigger == "stop":
        state["plans"]["STK0"]["stop_price"] = 1e9
    if trigger == "expiry":
        state["plans"]["STK0"]["review_at"] = when.isoformat()
    if trigger == "schedule":
        state["last_review_at"] = (when - timedelta(days=1)).isoformat()
    if trigger == "retry":
        state["next_attempt_at"] = (when + timedelta(minutes=5)).isoformat()
    if trigger == "stale":
        snap["market_calendar_state"]["is_open"] = False
    s.ensure_strategy_epoch(cfg.state_dir)
    s.write_json_atomically(f._state_path(cfg), state)
    if trigger == "pending":
        s.write_json_atomically(f.pending_path(cfg), {})
    monkeypatch.setattr(s, "LocalRustPoster", lambda *a: post)
    monkeypatch.setattr(s, "authoritative_portfolio_for_prompt", lambda *a, **kw: portfolio)
    monkeypatch.setattr(s, "build_market_research_snapshot", lambda *a, **kw: snap)
    monkeypatch.setattr(s, "run_news_scan", lambda *a, **kw: {})
    calls = []
    monkeypatch.setattr(f, "run_cycle", lambda *a, **kw: calls.append(kw))
    f.poll(cfg, now=when)
    assert len(calls) == int(trigger not in {"none", "closed", "retry", "stale"})
    assert post.cycles == []


def test_scheduler_reports_failure_and_retries_next_poll(setup, monkeypatch):
    cfg, post, _ = setup
    calls = []
    monkeypatch.setattr(
        f, "poll", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("broken data"))
    )
    monkeypatch.setattr(s, "LocalRustPoster", lambda *a: post)
    monkeypatch.setattr(f, "_status", lambda *a: calls.append(a))

    def end_sleep(seconds):
        assert seconds == 30
        raise KeyboardInterrupt

    monkeypatch.setattr(f.time, "sleep", end_sleep)
    with pytest.raises(KeyboardInterrupt):
        f.run_forever(cfg)
    assert calls[0][2] == "allocation_blocked"
    assert "broken data" in calls[0][3]


def test_corrupt_scheduler_state_is_not_silently_reset(tmp_path):
    path = tmp_path / "scheduler.json"
    path.write_text("[]")
    with pytest.raises(s.LlmEquityContractError, match="must be an object"):
        f._read(path)


@pytest.mark.parametrize("code", [400, 422, 500])
def test_real_adapter_distinguishes_definitive_rejection_from_ambiguous_commit(
    setup, monkeypatch, code
):
    from io import BytesIO
    from urllib.error import HTTPError

    cfg, _, _ = setup
    post = s.LocalRustPoster("http://localhost:8090", "synthetic")

    def reject(*a, **kw):
        raise HTTPError(
            "http://localhost:8090", code, "rejected", {}, BytesIO(b'{"error":"stale_market_data"}')
        )

    monkeypatch.setattr(s.request, "urlopen", reject)
    result = f.run_cycle(cfg, now=AT, poster=post, llm_client=Client([review()]))
    assert result["status"] == ("pending" if code == 500 else "blocked")
    assert f.pending_path(cfg).exists() is (code == 500)


def test_offhours_hourly_scan_runs_once_and_never_allocates(setup, monkeypatch):
    cfg, post, _ = setup
    from llm_equity.simulator import EquityPaperPortfolio

    when = AT.replace(hour=21, minute=5)
    monkeypatch.setattr(s, "LocalRustPoster", lambda *a: post)
    monkeypatch.setattr(
        s,
        "authoritative_portfolio_for_prompt",
        lambda *a, **kw: EquityPaperPortfolio.seeded(s.load_equity_universe()),
    )
    scans = []
    monkeypatch.setattr(s, "run_news_scan", lambda *a, **kw: scans.append(kw))
    f.poll(cfg, now=when)
    f.poll(cfg, now=when)
    assert len(scans) == 1
    assert scans[0]["cycle"].kind == "news_scan"
    assert post.cycles == []


def test_allocation_audit_preserves_model_cost_horizon_and_sale_attribution(setup, monkeypatch):
    cfg, post, _ = setup
    original = s.build_service_context

    def context_with_position(*a, **kw):
        store, snap, context = original(*a, **kw)
        context["portfolio"]["positions"] = [{"symbol": "STK24", "quantity": 5, "avg_price": 100}]
        context["profitability_scorecard"] = {"costs": {"cycle_cost_usd": 0.03}}
        return store, snap, context

    monkeypatch.setattr(s, "build_service_context", context_with_position)

    def fills(path, payload):
        response = post(path, payload)
        if path == s.LLM_EQUITY_CYCLE_PATH:
            response["order_results"] = [
                {"status": "accepted", "side": "sell", "symbol": "STK24", "notional_usd": 50},
                {"status": "accepted", "side": "buy", "symbol": "STK0", "notional_usd": 100},
            ]
        return response

    allocation = review()
    allocation["holdings"][0]["horizon"] = "1d"
    client = Client([allocation])
    client.last_usage = {"usage": {"input_tokens": 123, "output_tokens": 45}}
    assert f.run_cycle(cfg, now=AT, poster=fills, llm_client=client)["status"] == "success"
    row = s.read_cycle_audit_ledger(cfg.state_dir)[-1]
    assert row["costs"]["actual_cycle_cost_usd"] == 0.03
    assert row["costs"]["codex_input_tokens"] == 123
    assert row["costs"]["codex_output_tokens"] == 45
    trades = {trade["symbol"]: trade for trade in row["accepted_trades"]}
    assert trades["STK24"]["exposure_effect"] == "reduce"
    assert trades["STK0"]["horizon"] == "1d"


@pytest.mark.parametrize("scheduled_session", [s.MarketSession.RTH, s.MarketSession.AFTER_HOURS])
def test_offhours_high_impact_news_remains_research_only(setup, monkeypatch, scheduled_session):
    from dataclasses import replace

    cfg, post, _ = setup
    cfg = replace(cfg, fixture_mode=True)
    when = AT.replace(hour=21, minute=5)
    cycle = s.ScheduledCycle("news_scan", when, scheduled_session)
    monkeypatch.setattr(
        s,
        "build_public_event_intelligence",
        lambda *a, **kw: {
            "schema_version": 1,
            "as_of": when.isoformat(),
            "max_event_opportunity_score": 1.0,
            "events": [],
        },
    )
    result = s.run_news_scan(cfg, cycle=cycle, now=when, poster=post)
    assert result["status"] == "research_only"
    assert result["reason"] == "allocation_waiting_market"
    assert post.cycles == []
