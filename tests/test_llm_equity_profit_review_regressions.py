import json
import os
import shutil
from datetime import UTC, datetime, timedelta

import pytest
from llm_equity.accounting import session_returns
from llm_equity.contracts import load_equity_universe
from llm_equity.epoch import migrate_account_epoch
from llm_equity.policy import cap_entries, exit_reasons, update_exit_plans
from test_llm_equity_accounting import _state
from test_llm_equity_epoch import seed
from test_llm_equity_profit_execution import command, context

from llm_equity import hourly_service as service


@pytest.fixture
def profit(monkeypatch):
    monkeypatch.setenv('TRAIDAN_LLM_EQUITY_FILL_MODE', 'paper')
    monkeypatch.setenv('TRAIDAN_LLM_EQUITY_RISK_MODE', 'profit_driven')


def test_migration_refuses_custom_runtime_writer(tmp_path, monkeypatch):
    seed(tmp_path)
    runtime = tmp_path / 'custom-runtime'
    (runtime / 'pids').mkdir(parents=True)
    (runtime / 'pids/rust.pid').write_text(str(os.getpid()))
    monkeypatch.setenv('TRAIDAN_LOCAL_DRY_RUN_STATE_DIR', str(runtime))
    with pytest.raises(RuntimeError, match='stop paper services'):
        migrate_account_epoch(tmp_path, {'epoch_id': 'epoch-bbbbbbbbbbbbbbbb'}, at=datetime.now(UTC))


def test_migration_seeds_inherited_holding_protection(tmp_path):
    seed(tmp_path)
    path = migrate_account_epoch(tmp_path, {'epoch_id': 'epoch-bbbbbbbbbbbbbbbb'},
                                 at=datetime(2026, 10, 12, 15, tzinfo=UTC))
    plans = json.loads((path / 'position-exit-plans.json').read_text())
    assert plans['SPY']['stop_price'] == pytest.approx(101 * .95)
    assert plans['SPY']['side'] == 1
    assert json.loads((path / 'account-opening-exit-plans.json').read_text()) == plans


def test_close_plan_exits_before_authoritative_early_close():
    at = datetime(2026, 11, 27, 17, tzinfo=UTC)
    ctx = context()
    ctx['market_research_snapshot']['market_calendar_state'] = {'next_close': '2026-11-27T18:00:00Z'}
    decision = {'cycle_id': 'c1', 'commands': [{**command(), 'horizon': 'close'}]}
    response = {'order_results': [{'status': 'accepted', 'command_id': 'c1-1',
        'symbol': 'SPY', 'side': 'buy', 'quantity': 1, 'avg_price': 100}]}
    plans = update_exit_plans({}, decision, response, ctx, at)
    assert plans['SPY']['exit_at'] == '2026-11-27T17:55:00+00:00'
    assert exit_reasons(context(quantity=1), plans, at + timedelta(minutes=55)) == {'SPY': 'thesis_horizon'}
    ctx['market_research_snapshot']['as_of'] = '2026-11-27T17:56:00Z'
    with pytest.raises(ValueError, match='close horizon'):
        cap_entries(decision, ctx)


@pytest.mark.parametrize('plans', [{}, {'SPY': None}, {'SPY': {'side': -1}},
                                  {'SPY': {'side': 1, 'stop_price': 'bad', 'exit_at': 'bad'}}])
def test_restart_without_valid_exit_plan_closes_accepted_inventory(plans):
    assert exit_reasons(context(quantity=5), plans, datetime.now(UTC)) == {'SPY': 'missing_exit_plan'}


def test_fresh_gate_allows_only_reduction_of_held_symbol(profit):
    ctx = context(quantity=1)
    ctx['ranked_trade_candidates'] = [{'symbol': 'SPY', 'side': 'long', 'edge_gate_status': 'skipped_no_edge'}]
    with pytest.raises(ValueError, match='edge'):
        service.validate_decision_against_ranked_candidates({'commands': [command()]}, ctx, require_passing_gate=True)
    service.validate_decision_against_ranked_candidates(
        {'commands': [{**command('sell_notional'), 'reduce_only': True}]}, ctx, require_passing_gate=True)


def test_closing_session_evidence_survives_after_hours_telemetry():
    opening = _state('2026-10-08T19:59:00Z', 9900, 1, 100, [])
    close = _state('2026-10-09T19:59:00Z', 9900, 1, 101, [])
    late = _state('2026-10-09T21:00:00Z', 9900, 1, 500, [])
    rows = session_returns(opening, [close, late], session_closes={'2026-10-09': '2026-10-09T20:00:00Z'})
    assert rows[0]['mark_at'] == close['saved_at']
    assert rows[0]['account_return'] == pytest.approx(.0001)


def test_minute_settlement_uses_completed_bar_timestamp():
    target = datetime(2026, 10, 12, 15, 35, tzinfo=UTC)
    history = {'SPY': [{'timestamp': '2026-10-12T15:34:00Z', 'close': 100},
                       {'timestamp': '2026-10-12T15:35:00Z', 'close': 999}]}
    assert service._exit_price_from_history(history, 'SPY', target, as_of=target) == 100


@pytest.mark.skipif(shutil.which('cargo') is None, reason='Rust harness requires the Rust toolchain')
def test_research_marks_persist_risk_latch_and_reject_after_close(tmp_path):
    from llm_equity.research_simulation import RustResearchHarness
    from test_llm_equity_contracts import valid_decision_payload
    at = datetime(2026, 5, 28, 19, 59, 50, tzinfo=UTC)
    snapshot = service.build_market_research_snapshot(load_equity_universe(), at, fixture_mode=True)
    state_file = tmp_path / 'authority.json'
    with RustResearchHarness(policy='profit_driven', state_file=state_file) as harness:
        state = dict(harness.export_state())
        state['portfolio']['peak_nav_usd'] = 12000
        harness.restore_state(state)
        harness.mark(snapshot, at)
        assert state_file.exists()
    with RustResearchHarness(policy='profit_driven', state_file=state_file) as harness:
        assert harness.export_state()['drawdown_halted'] is True
    with RustResearchHarness(policy='profit_driven') as harness:
        decision = valid_decision_payload()
        decision.update(as_of=snapshot['as_of'], execution_policy='profit_driven_v1')
        result = harness.apply(decision, snapshot, at + timedelta(seconds=20))
        assert result['accepted_orders'] == []


def test_settlement_fetch_uses_minute_bars_with_bounded_horizon_window(tmp_path):
    class Client:
        def historical_bars(self, symbols, **kwargs):
            assert set(symbols) == {'SPY', 'QQQ', 'AAPL'}
            assert kwargs['timeframe'] == '1Min'
            assert kwargs['start'] == datetime(2026, 10, 12, 15, 34, tzinfo=UTC)
            assert kwargs['end'] == datetime(2026, 10, 12, 15, 40, tzinfo=UTC)
            return {'AAPL': [{'timestamp': '2026-10-12T15:34:00Z', 'close': 100}]}
    learning = {'pending_outcomes': [{'symbol': 'AAPL', 'settle_after': '2026-10-12T15:35:00Z'}]}
    assert service.fetch_settlement_history(Client(), learning, current=datetime(2026, 10, 12, 16, tzinfo=UTC),
        fixture_mode=False, state_dir=tmp_path)['AAPL'][0]['close'] == 100
