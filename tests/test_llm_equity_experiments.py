import shutil
from datetime import UTC, datetime, timedelta

import pytest
from llm_equity.experiments import promotion_gate, purged_partitions, register_trials


def test_trial_registration_is_immutable_and_partitions_are_purged(tmp_path):
    registered = register_trials(tmp_path, at=datetime(2026, 10, 12, tzinfo=UTC), baseline="abc")
    assert len(registered["variants"]) == 4
    assert register_trials(tmp_path, at=datetime.now(UTC), baseline="abc") == registered
    with pytest.raises(ValueError, match="baseline"):
        register_trials(tmp_path, at=datetime.now(UTC), baseline="different")
    days = [f"2026-10-{day:02}" for day in range(1, 31)]
    split = purged_partitions(days, purge_sessions=5)
    assert set(split["train"]).isdisjoint(split["validation"])
    assert set(split["validation"]).isdisjoint(split["lockbox"])
    assert len(split["purged"]) == 10


def test_forward_promotion_requires_both_benchmarks_costs_and_maturity():
    start = datetime(2026, 10, 13, 21, tzinfo=UTC)
    sessions = [{"session": (start + timedelta(days=i)).date().isoformat(),
        "mark_at": (start + timedelta(days=i)).isoformat(), "opening_nav_usd": 10000, "session_close_at": (start + timedelta(days=i)).isoformat(), "economic_return": .003,
        "SPY_exposure_matched": .001, "QQQ_exposure_matched": .002} for i in range(28) if (start + timedelta(days=i)).weekday() < 5]
    account = {"promotion_eligible": True, "economic": {"net_pnl_usd": 100},
        "sampled_max_drawdown_fraction": .01, "sessions": sessions}
    registry = {"forward_start": "2026-10-13"}
    assert not promotion_gate(account, registry, as_of=datetime(2026, 12, 1, tzinfo=UTC), matured_forecasts=99)["eligible"]
    assert promotion_gate(account, registry, as_of=datetime(2026, 12, 1, tzinfo=UTC), matured_forecasts=100)["eligible"]
    sessions[-1]["QQQ_exposure_matched"] = .5
    assert not promotion_gate(account, registry, as_of=datetime(2026, 12, 1, tzinfo=UTC), matured_forecasts=100)["eligible"]
    account["promotion_eligible"] = False
    assert not promotion_gate(account, registry, as_of=datetime(2026, 12, 1, tzinfo=UTC), matured_forecasts=100)["eligible"]


@pytest.mark.skipif(shutil.which("cargo") is None, reason="Rust harness requires the Rust toolchain")
def test_registered_variants_use_real_isolated_rust_harness(tmp_path, monkeypatch):
    from llm_equity.contracts import load_equity_universe
    from llm_equity.experiments import compare_registered_variants
    from llm_equity.replay import PointInTimeEventStore
    from test_llm_equity_contracts import valid_decision_payload

    from llm_equity import hourly_service as service
    monkeypatch.delenv("TRAIDAN_RESEARCH_RUST_COMMAND", raising=False)
    epoch = tmp_path / "epoch"
    at = datetime(2026, 5, 28, 14, 30, tzinfo=UTC)
    register_trials(epoch, at=at, baseline="fixture")
    store = PointInTimeEventStore(epoch / "research-events.jsonl")
    decision = valid_decision_payload()
    decision["commands"][0].update(horizon="1d", forecast={
        "edge_class": "positive_edge", "expected_excess_return_bps": 100,
        "probability_positive": .6, "return_p10_bps": -500,
        "return_p50_bps": 100, "return_p90_bps": 600})
    for i in range(2):
        now = at + timedelta(minutes=i * 10)
        if i:
            decision["commands"][0].update(action="sell_notional", reduce_only=True)
        snapshot = service.build_market_research_snapshot(load_equity_universe(), now, fixture_mode=True)
        store.append("policy_frame", now, {"snapshot": snapshot, "decision": decision,
            "entry_approval": "llm" if i == 0 else "risk_rule_or_hold"})
    report = compare_registered_variants(epoch, tmp_path / "research")
    assert report["status"] == "complete"
    assert report["forward_promotion_eligible"] is False
    assert len(report["variants"]) == 8
    assert all(row["unpriceable_frames"] == 0 for row in report["variants"])
    quota = tmp_path / "research/quota_free_explicit_exits-cost1"
    import json
    assert json.loads((quota / "authority-state.json").read_text())["portfolio"]["positions"] == {}
    for variant in ("frozen_execution_reference", "quota_free_explicit_exits"):
        events = PointInTimeEventStore(tmp_path / "research" / f"{variant}-cost1/research-events.jsonl").read_all()
        assert sum(event["event_type"] == "rust_execution" for event in events) == 2


def test_frozen_bundle_includes_opening_account_and_plans(tmp_path):
    import hashlib
    import json

    from llm_equity.experiments import compare_registered_variants
    register_trials(tmp_path, at=datetime(2026, 10, 12, tzinfo=UTC), baseline='a')
    seed = {'cash': 123, 'positions': {'SPY': 1}}
    plans = {'SPY': {'stop_price': 50}}
    (tmp_path / 'account-opening-state.json').write_text(json.dumps(seed))
    (tmp_path / 'account-opening-exit-plans.json').write_text(json.dumps(plans))
    result = compare_registered_variants(tmp_path, tmp_path / 'out')
    data = (tmp_path / 'out/frozen-inputs.json').read_bytes()
    assert json.loads(data)['opening_authority'] == seed
    assert json.loads(data)['opening_exit_plans'] == plans
    assert hashlib.sha256(data).hexdigest() == result['source_sha256']
    assert result['status'] == 'awaiting_frames'


@pytest.mark.skipif(shutil.which('cargo') is None, reason='Rust harness requires the Rust toolchain')
def test_seeded_variants_reconcile_exits_skip_duplicate_and_unpriceable_frames(tmp_path):
    import json

    from llm_equity.contracts import load_equity_universe
    from llm_equity.experiments import compare_registered_variants
    from llm_equity.replay import PointInTimeEventStore
    from llm_equity.research_simulation import RustResearchHarness

    from llm_equity import hourly_service as service
    epoch = tmp_path / 'epoch'
    at = datetime(2026, 5, 29, 14, 30, tzinfo=UTC)
    assert register_trials(epoch, at=at, baseline='fixture')['forward_start'] == '2026-06-01'
    assert purged_partitions(['a', 'b'])['purged'] == ['a', 'b']
    snapshot = service.build_market_research_snapshot(load_equity_universe(), at, fixture_mode=True)
    with RustResearchHarness(policy='profit_driven') as harness:
        state = harness.export_state()
    price = sum(snapshot['latest_quotes']['SPY'][key] for key in ('bid', 'ask')) / 2
    state['portfolio']['positions'] = {'SPY': {'symbol': 'SPY', 'quantity': 1, 'avg_price': price}}
    state['portfolio']['cash_usd'] -= price
    state['last_prices'] = {'SPY': price}
    state['saved_at'] = (at - timedelta(minutes=5)).isoformat()
    (epoch / 'account-opening-state.json').write_text(json.dumps(state))
    (epoch / 'account-opening-exit-plans.json').write_text(json.dumps({'SPY': {
        'side': 1, 'stop_price': price * .95, 'exit_at': (at - timedelta(minutes=1)).isoformat()}}))
    (epoch / 'economic-cost-policy.json').write_text(json.dumps({'commission_bps': .5,
        'borrow_apr': .05, 'financing_apr': .03, 'exclude_operating_costs': True,
        'monthly_model_usd': 0, 'monthly_data_usd': 0, 'monthly_hosting_usd': 0}))
    store = PointInTimeEventStore(epoch / 'research-events.jsonl')
    store.append('policy_frame', at, {'snapshot': snapshot, 'decision': None})
    store.append('policy_frame', at, {'snapshot': snapshot, 'decision': None})
    bad = service.build_market_research_snapshot(load_equity_universe(), at + timedelta(minutes=1), fixture_mode=True)
    bad['market_calendar_state']['is_open'] = False
    store.append('policy_frame', at + timedelta(minutes=1), {'snapshot': bad})
    result = compare_registered_variants(epoch, tmp_path / 'out')
    assert all(row['unpriceable_frames'] == 1 for row in result['variants'])
    assert all(row['reconciliation']['status'] == 'matched' for row in result['variants'])
    assert all(row['economic_pnl_usd'] is not None for row in result['variants'])
    for row in result['variants']:
        state = json.loads((tmp_path / 'out' / f"{row['id']}-cost{row['execution_cost_multiplier']}" / 'authority-state.json').read_text())
        assert bool(state['portfolio']['positions']) is (row['id'] == 'frozen_execution_reference')
