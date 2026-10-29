"""Paper-policy orchestration, recovery, and evidence boundary regressions."""
import copy
import json
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta

import pytest
from llm_equity.contracts import load_equity_universe
from llm_equity.replay import PointInTimeEventStore
from test_hourly_llm_equity_service import CapturingPoster, ForcedAggressiveReviewLlmClient

from llm_equity import hourly_service as s

AT = datetime(2026, 5, 28, 14, 30, tzinfo=UTC)


@pytest.fixture(autouse=True)
def profit(monkeypatch):
    monkeypatch.setenv(s.LLM_EQUITY_FILL_MODE_ENV, 'paper')
    monkeypatch.setenv(s.LLM_EQUITY_RISK_MODE_ENV, 'profit_driven')


class FillPoster(CapturingPoster):
    def __call__(self, path, payload):
        result = super().__call__(path, payload)
        for row in result.get('order_results', []):
            quote = payload['market_research_snapshot']['latest_quotes'][row['symbol']]
            price = (quote['bid'] + quote['ask']) / 2
            row.update(quantity=row['notional_usd'] / price, avg_price=price)
        return result


def setup_context(tmp_path, *, held=False):
    universe = load_equity_universe()
    config = s.HourlyServiceConfig(state_dir=tmp_path)
    s.ensure_strategy_epoch(tmp_path)
    post = FillPoster()
    if held:
        post.status_payload['llm_equity']['portfolio']['positions'] = [
            {'symbol': 'SPY', 'quantity': 1, 'avg_price': 500}]
    snapshot = s.build_market_research_snapshot(universe, AT, fixture_mode=True)
    portfolio = s.authoritative_portfolio_for_prompt(post, universe, fixture_mode=False)
    context = dict(s.build_llm_context(portfolio, snapshot, universe=universe))
    context['profitability_scorecard'] = s.build_profitability_scorecard(portfolio, snapshot, universe)
    context['ranked_trade_candidates'] = [{'rank': 1, 'symbol': 'SPY', 'side': 'long',
        'edge_gate_status': 'pass', 'enforced_notional_usd': 25}]
    context['alpha_swarm'] = {'edge_gate_status': 'pass'}
    return config, post, snapshot, context, s.MemoryStore(tmp_path / 'memory.json')


@pytest.mark.parametrize('held,stale,move', [(False, False, False), (True, False, False),
                                           (False, True, False), (False, False, True)])
def test_execution_rechecks_market_and_preserves_exit_protection(tmp_path, monkeypatch, held, stale, move):
    config, post, snapshot, context, memory = setup_context(tmp_path, held=held)
    refreshed = copy.deepcopy(snapshot)
    if stale:
        refreshed['market_calendar_state']['freshness'] = 'unavailable'
    if move:
        for key in ('bid', 'ask'):
            refreshed['latest_quotes']['SPY'][key] *= 1.01
    fresh_context = {**context, 'market_research_snapshot': refreshed}
    monkeypatch.setattr(s, 'build_service_context', lambda *args, **kwargs: (memory, refreshed, fresh_context))
    client = ForcedAggressiveReviewLlmClient()
    with pytest.raises(s.LlmEquityContractError, match='fresh LLM approval') if move else nullcontext():
        result = s._execute_contextual_trade_cycle(config, cycle_kind='run_once', current=AT,
            post=post, universe=load_equity_universe(), memory_store=memory, snapshot=snapshot,
            context=context, client=client)
    if move:
        assert post.payload is None
        return
    if stale or move:
        assert post.payload is None
        assert result['status'] != 'success'
    else:
        assert result['accepted_orders'] == 1
        decision = post.payload['llm_trading_decision']
        assert decision['execution_policy'] == 'profit_driven_v1'
        assert decision['commands'][0]['reduce_only'] is held
        plans = s._profit_exit_plans(tmp_path)
        assert ('SPY' in plans) is not held
        assert len(client.contexts) == (0 if held else 1)
    assert any(event['event_type'] == 'policy_frame' for event in
               PointInTimeEventStore(s.strategy_epoch_dir(tmp_path) / 'research-events.jsonl').read_all())


def test_monitor_recovers_missing_plan_and_flat_marks_without_model(tmp_path, monkeypatch):
    config, post, snapshot, _context, _memory = setup_context(tmp_path, held=True)
    monkeypatch.setattr(s, 'LocalRustPoster', lambda *args: post)
    monkeypatch.setattr(s, 'build_market_research_snapshot', lambda *args, **kwargs: snapshot)
    path = s.strategy_epoch_dir(tmp_path) / 'position-exit-plans.json'
    path.write_text('{broken')
    s.run_profit_risk_check(config, now=AT)
    assert post.payload['llm_trading_decision']['commands'][0]['reduce_only'] is True
    assert json.loads(path.read_text()) == {}
    post.payload = None
    post.status_payload['llm_equity']['portfolio']['positions'] = []
    s.run_profit_risk_check(config, now=AT)
    assert post.payload is None
    assert s.read_cycle_audit_ledger(tmp_path)[-1]['status'] == 'held'
    s.run_profit_risk_check(config, now=AT.replace(hour=1))
    s.run_profit_risk_check(s.HourlyServiceConfig(state_dir=tmp_path, fixture_mode=True), now=AT)
    monkeypatch.setenv(s.LLM_EQUITY_RISK_MODE_ENV, 'bounded')
    s.run_profit_risk_check(config, now=AT)


def test_scheduler_logs_monitor_failure_and_continues_scheduled_work(tmp_path, monkeypatch):
    calls = []
    def risk(*args, **kwargs):
        calls.append('risk')
        if len(calls) > 1:
            raise OSError('paper state unavailable')
    monkeypatch.setattr(s, 'run_profit_risk_check', risk)
    monkeypatch.setattr(s, 'run_startup_cycle', lambda *args, **kwargs: calls.append('startup'))
    monkeypatch.setattr(s, 'run_due_cycles_once', lambda *args, **kwargs: calls.append('scheduled'))
    class StopLoop(Exception):
        pass
    def stop(*args):
        raise StopLoop
    monkeypatch.setattr(s.time_module, 'sleep', stop)
    with pytest.raises(StopLoop):
        s.run_forever(s.HourlyServiceConfig(state_dir=tmp_path))
    assert calls == ['risk', 'startup', 'risk', 'scheduled']
    assert 'profit_risk_monitor' in (tmp_path / 'service-errors.jsonl').read_text()


def test_premarket_research_does_not_authorize_execution_and_requires_context():
    snapshot = s.build_market_research_snapshot(load_equity_universe(), AT, fixture_mode=True)
    assert not s._premarket_research_eligible(snapshot)
    snapshot['market_session'] = 'premarket'
    assert s._premarket_research_eligible(snapshot)
    assert s.release_snapshot_block_reason(snapshot) == 'release_snapshot_not_rth'
    snapshot['as_of'] = 'invalid'
    assert not s._premarket_research_eligible(snapshot)
    snapshot['as_of'] = AT.isoformat()
    snapshot['source_freshness'].append({'source': 'news', 'freshness': 'stale'})
    assert not s._premarket_research_eligible(snapshot)


@pytest.mark.parametrize('prepared', [False, True])
def test_opening_wait_preserves_same_slot_until_completed_bar(tmp_path, monkeypatch, prepared):
    config, post, snapshot, context, memory = setup_context(tmp_path)
    target = AT
    cycle = s.ScheduledCycle(kind='release', run_at=target, market_session=s.MarketSession.RTH)
    monkeypatch.setattr(s, 'build_service_context', lambda *args, **kwargs: (memory, snapshot, context))
    if prepared:
        monkeypatch.setattr(s, 'load_pending_release_intent', lambda *args: {'decision': {'commands': []}})
    result = s.release_prepared_intent(config, cycle, poster=post, now=target)
    assert result['status'] == 'readiness_pending'
    assert not s.release_ledger_contains(tmp_path, target)
    monkeypatch.setattr(s, 'due_cycles_for_now', lambda *args, **kwargs: [cycle])
    assert s.sleep_seconds_until_next_cycle(target, seen=set(), config=config) == 30
    snapshot['as_of'] = (target + timedelta(minutes=1)).isoformat()
    assert s.release_snapshot_block_reason(snapshot, target_release_at=target) is None
    snapshot['market_calendar_state'].update(is_open=False, next_open='2026-05-29T13:30:00Z')
    assert not s._opening_readiness_pending(snapshot, target, target)


def test_forecast_ledger_survives_bounded_index_and_deduplicates(tmp_path):
    store = s.StrategyLearningStore(tmp_path / 'learning.json')
    rows = [{'cycle_id': f'c-{i}', 'command_index': 0, 'status': 'settled',
             'settled_at': AT.isoformat()} for i in range(150)]
    store._write({'recent_outcomes': rows})
    store._write({'recent_outcomes': rows[-10:]})
    ledger = PointInTimeEventStore(tmp_path / 'forecast-outcomes.jsonl').read_all()
    assert len(ledger) == 150
    assert len(json.loads((tmp_path / 'learning.json').read_text())['recent_outcomes']) == 10


def test_settlement_data_failure_is_observable(tmp_path):
    class Client:
        def historical_bars(self, *args, **kwargs):
            raise OSError('upstream unavailable')
    result = s.fetch_settlement_history(Client(), {'pending_outcomes': [
        {'symbol': 'SPY', 'settle_after': AT.isoformat()}]}, current=AT + timedelta(hours=1),
        fixture_mode=False, state_dir=tmp_path)
    assert result is None
    assert 'forecast_settlement_history' in (tmp_path / 'service-errors.jsonl').read_text()


def test_epoch_cli_preserves_book_and_inherits_owner_cost_policy(tmp_path, capsys):
    from test_llm_equity_epoch import seed
    _old, state = seed(tmp_path)
    policy = {'exclude_operating_costs': True}
    (tmp_path / 'economic-cost-policy.json').write_text(json.dumps(policy))
    with pytest.raises(RuntimeError, match='migrate-policy-epoch'):
        s.ensure_strategy_epoch(tmp_path)
    assert s.main(['migrate-policy-epoch', '--state-dir', str(tmp_path)]) == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'account_preserved'
    s.ensure_strategy_epoch(tmp_path)
    epoch = s.strategy_epoch_dir(tmp_path)
    assert json.loads((epoch / 'economic-cost-policy.json').read_text()) == policy
    assert s._opening_epoch_nav(epoch) == state['portfolio']['cash_usd'] + 5 * 101
    assert s.main(['profit-experiments', '--epoch-dir', str(epoch), '--output-dir', str(tmp_path / 'out')]) == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'awaiting_frames'


def test_profit_prompt_and_zero_fill_summary_are_explicit(tmp_path):
    _config, _post, _snapshot, context, _memory = setup_context(tmp_path)
    context['runtime_controls'] = {'risk_mode': 'profit_driven'}
    assert 'no minimum trade notional' in s.build_prompt(context)
    assert len(s._review_candidates_for_context(context)) == 1
    assert s.cycle_audit_dashboard_summary({'risk_mode': 'profit_driven', 'status': 'posted',
        'cycle_kind': 'run_once', 'rust_response': {'order_results': [
            {'status': 'accepted', 'symbol': 'SPY', 'side': 'sell', 'notional_usd': 0}]}}) is None


@pytest.mark.parametrize('reviewed', [False, True])
def test_prepared_release_reuses_only_price_validated_approval(tmp_path, monkeypatch, reviewed):
    config, post, snapshot, context, memory = setup_context(tmp_path)
    client = ForcedAggressiveReviewLlmClient()
    decision = s.request_valid_llm_decision(client, context, load_equity_universe(), require_passing_gate=True)
    intent = {'decision': decision, 'reviewed_quotes': snapshot['latest_quotes'] if reviewed else {}}
    monkeypatch.setattr(s, 'load_pending_release_intent', lambda *args: intent)
    monkeypatch.setattr(s, 'build_service_context', lambda *args, **kwargs: (memory, snapshot, context))
    captured = []
    def execute(*args, **kwargs):
        captured.append(kwargs['prepared_decision'])
        return {'status': 'held', 'order_results': []}
    monkeypatch.setattr(s, '_execute_contextual_trade_cycle', execute)
    cycle = s.ScheduledCycle(kind='release', run_at=AT - timedelta(minutes=1), market_session=s.MarketSession.RTH)
    snapshot['as_of'] = (AT + timedelta(minutes=1)).isoformat()
    s.release_prepared_intent(config, cycle, poster=post, llm_client=client, now=AT + timedelta(minutes=1))
    assert (captured[0] is not None) is reviewed


def test_unreadable_exit_plan_directory_is_fail_closed(tmp_path, monkeypatch):
    s.ensure_strategy_epoch(tmp_path)
    def unreadable(*args):
        raise PermissionError('state mount unreadable')
    monkeypatch.setattr(s, '_read_json_object', unreadable)
    (s.strategy_epoch_dir(tmp_path) / 'position-exit-plans.json').write_text('{}')
    assert s._profit_exit_plans(tmp_path) == {}


@pytest.mark.parametrize('completed', [True, False])
def test_snapshot_filters_optional_incomplete_minutes_before_execution(tmp_path, monkeypatch, completed):
    now = AT + timedelta(minutes=1)
    bars, quotes = s.fixture_market_data(['SPY', 'QQQ'], AT.isoformat())
    if not completed:
        for row in bars.values():
            row['timestamp'] = now.isoformat()
    class Client:
        def latest_bars(self, symbols):
            return bars
        def latest_quotes(self, symbols):
            return quotes
        def market_clock(self):
            return {'is_open': True, 'timestamp': now.isoformat(), 'next_open': '2026-05-29T13:30:00Z',
                    'next_close': '2026-05-28T20:00:00Z'}
    monkeypatch.setattr(s, '_refreshed_market_snapshot_time', lambda *args: now)
    monkeypatch.setattr(s, 'discover_research_symbols_with_health', lambda *args, **kwargs: (['SPY', 'QQQ'], {}))
    snapshot = s.build_market_research_snapshot(load_equity_universe(), now, data_client=Client(),
        fixture_mode=False, macro_rows_override=[])
    assert (snapshot['source_freshness'][0]['freshness'] == 'fresh') is completed
