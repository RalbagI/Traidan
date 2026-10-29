import assert from 'node:assert/strict';
import test from 'node:test';

import { MESSAGE_TYPES } from './messages.js';
import { applyEngineMessage, createOperationsState } from './operationsState.js';

test('records the stream generation that supplied each frame', () => {
  const state = applyEngineMessage(createOperationsState(), {
    type: MESSAGE_TYPES.EQUITY_PORTFOLIO_STATE,
    data: {
      initial_equity_usd: 10000,
      cash_usd: 10000,
      nav_usd: 10000,
      gross_exposure_usd: 0,
      daily_pnl_usd: 0,
      max_drawdown_pct: 0,
      positions: [],
      timestamp_ns: '1700000000000000000',
    },
  }, 1000, 7);

  assert.equal(state.equityPortfolio.streamGeneration, 7);
});

test('records equity portfolio and cycle frames', () => {
  let state = createOperationsState();
  state = applyEngineMessage(state, {
    type: MESSAGE_TYPES.EQUITY_PORTFOLIO_STATE,
    data: {
      initial_equity_usd: 10000,
      cash_usd: 9000,
      nav_usd: 10000,
      gross_exposure_usd: 1000,
      daily_pnl_usd: 0,
      max_drawdown_pct: 0,
      positions: [],
      timestamp_ns: '1700000000000000000',
    },
  }, 1000);
  state = applyEngineMessage(state, {
    type: MESSAGE_TYPES.LLM_EQUITY_CYCLE,
    data: {
      cycle_id: 'cycle-1',
      timestamp_ns: '1700000000000000001',
    },
  }, 2000);

  assert.equal(state.equityPortfolio.cash_usd, 9000);
  assert.equal(state.equityCycle.cycle_id, 'cycle-1');
  assert.equal(state.equityPortfolioHistory.length, 1);
  assert.equal(state.equityCycles.length, 1);
  assert.equal(state.equityCycles[0].cycle_id, 'cycle-1');
});

test('records bounded trade cycle summary frames', () => {
  let state = createOperationsState();

  for (let index = 0; index < 22; index += 1) {
    state = applyEngineMessage(state, {
      type: MESSAGE_TYPES.LLM_EQUITY_CYCLE_SUMMARY,
      data: {
        cycle_id: `cycle-${index}`,
        cycle_kind: 'news_scan',
        as_of: '2026-07-24T15:35:00Z',
        market_session: 'rth',
        status: 'accepted',
        mandatory_trade_status: 'accepted',
        required_notional_usd: 25,
        requested_notional_usd: 30,
        accepted_notional_usd: 25 + index,
        actions: ['buy'],
        symbols: ['SPY'],
        rationale: 'Fresh breadth and macro context supported a minimum paper buy.',
        reason: 'simulated_fill',
        timestamp_ns: String(1_700_000_000_000_000_000n + BigInt(index)),
      },
    }, 1000 + index);
  }

  assert.equal(state.cycleSummaries.length, 20);
  assert.equal(state.cycleSummaries[0].cycle_id, 'cycle-21');
  assert.equal(state.cycleSummaries[19].cycle_id, 'cycle-2');
});

test('retains distinct terminal research statuses as deduplicated service activities', () => {
  let state = createOperationsState();
  const first = {
    type: MESSAGE_TYPES.LLM_EQUITY_SERVICE_STATUS,
    data: {
      status: 'llm_research_only',
      detail: 'Public-news scan completed without a qualifying event.',
      market_session: 'rth',
      activity_key: 'news-scan:stable-opportunity',
      activity_repeat_count: 1,
      timestamp_ns: '1700000000000000000',
    },
  };

  state = applyEngineMessage(state, first, 1000);
  state = applyEngineMessage(state, first, 2000);
  state = applyEngineMessage(state, {
    ...first,
    data: {
      ...first.data,
      activity_repeat_count: 2,
      timestamp_ns: '1700003600000000000',
    },
  }, 3000);
  state = applyEngineMessage(state, {
    type: MESSAGE_TYPES.LLM_EQUITY_SERVICE_STATUS,
    data: {
      status: 'llm_edge_gate_skipped',
      detail: 'The edge gate rejected the candidate slate.',
      market_session: 'rth',
      activity_key: 'edge-gate:candidate-slate',
      timestamp_ns: '1700007200000000000',
    },
  }, 4000);
  state = applyEngineMessage(state, {
    type: MESSAGE_TYPES.LLM_EQUITY_SERVICE_STATUS,
    data: {
      status: 'llm_scheduler_waiting',
      detail: 'Waiting for the next scheduled scan.',
      market_session: 'rth',
      timestamp_ns: '1700010800000000000',
    },
  }, 5000);

  assert.equal(state.serviceActivities.length, 2);
  assert.equal(state.serviceActivities[0].status, 'llm_edge_gate_skipped');
  assert.equal(state.serviceActivities[1].status, 'llm_research_only');
  assert.equal(state.serviceActivities[1].receivedAtMs, 3000);
  assert.equal(state.serviceActivities[1].repeatCount, 2);
  assert.equal(state.serviceStatus.status, 'llm_scheduler_waiting');
});

test('keeps legacy research statuses without activity metadata visible by timestamp', () => {
  let state = createOperationsState();
  const first = {
    type: MESSAGE_TYPES.LLM_EQUITY_SERVICE_STATUS,
    data: {
      status: 'llm_research_only',
      detail: 'Legacy producer completed a research-only scan.',
      market_session: 'rth',
      activity_repeat_count: 0,
      timestamp_ns: '1700000000000000000',
    },
  };

  state = applyEngineMessage(state, first, 1000);
  state = applyEngineMessage(state, first, 2000);
  state = applyEngineMessage(state, {
    ...first,
    data: {
      ...first.data,
      timestamp_ns: '1700003600000000000',
    },
  }, 3000);

  assert.equal(state.serviceActivities.length, 2);
  assert.equal(state.serviceActivities[0].activityId, '1700003600000000000');
  assert.equal(state.serviceActivities[0].repeatCount, 1);
  assert.equal(state.serviceActivities[1].activityId, '1700000000000000000');
  assert.equal(state.serviceActivities[1].repeatCount, 1);
});

test('never decreases a keyed activity count after intervening statuses', () => {
  let state = createOperationsState();
  const status = (activityKey, repeatCount, timestampNs) => ({
    type: MESSAGE_TYPES.LLM_EQUITY_SERVICE_STATUS,
    data: {
      status: 'llm_research_only',
      detail: 'Research activity completed.',
      activity_key: activityKey,
      activity_repeat_count: repeatCount,
      timestamp_ns: timestampNs,
    },
  });

  state = applyEngineMessage(
    state,
    status('news-scan:stable-opportunity', 2, '1700000000000000000'),
    1000,
  );
  state = applyEngineMessage(
    state,
    status('edge-gate:other-opportunity', 1, '1700003600000000000'),
    2000,
  );
  state = applyEngineMessage(
    state,
    status('news-scan:stable-opportunity', 1, '1700007200000000000'),
    3000,
  );

  const stable = state.serviceActivities.find(
    (activity) => activity.activityId === 'news-scan:stable-opportunity',
  );
  assert.equal(stable.repeatCount, 2);
  assert.equal(stable.timestamp_ns, '1700007200000000000');
});

test('does not keep duplicate simulated order ids', () => {
  let state = createOperationsState();
  const message = {
    type: MESSAGE_TYPES.EQUITY_ORDER_SIMULATED,
    data: {
      order_id: 'order-1',
      cycle_id: 'cycle-1',
      symbol: 'SPY',
      side: 'buy',
      status: 'accepted',
      qty: 2,
      avg_price: 500,
      notional_usd: 1000,
      reason: 'simulated_fill',
      timestamp_ns: '1700000000000000000',
    },
  };

  state = applyEngineMessage(state, message, 1000);
  state = applyEngineMessage(state, message, 2000);

  assert.equal(state.equityOrders.length, 1);
});

test('keeps portfolio frames with same timestamp when values differ', () => {
  let state = createOperationsState();
  const base = {
    type: MESSAGE_TYPES.EQUITY_PORTFOLIO_STATE,
    data: {
      initial_equity_usd: 10000,
      cash_usd: 9000,
      nav_usd: 10000,
      gross_exposure_usd: 1000,
      daily_pnl_usd: 0,
      max_drawdown_pct: 0,
      positions: [],
      timestamp_ns: '1700000000000000000',
    },
  };

  state = applyEngineMessage(state, base, 1000);
  state = applyEngineMessage(state, {
    ...base,
    data: {
      ...base.data,
      nav_usd: 10025,
      daily_pnl_usd: 25,
    },
  }, 2000);

  assert.equal(state.equityPortfolioHistory.length, 2);
  assert.equal(state.equityPortfolioHistory[0].nav_usd, 10025);
});

test('keeps portfolio frames with same timestamp when benchmark price differs', () => {
  let state = createOperationsState();
  const base = {
    type: MESSAGE_TYPES.EQUITY_PORTFOLIO_STATE,
    data: {
      initial_equity_usd: 10000,
      cash_usd: 9000,
      nav_usd: 10000,
      gross_exposure_usd: 1000,
      daily_pnl_usd: 0,
      max_drawdown_pct: 0,
      positions: [],
      benchmark_symbol: 'SPY',
      benchmark_label: 'S&P500',
      benchmark_price_usd: 500,
      timestamp_ns: '1700000000000000000',
    },
  };

  state = applyEngineMessage(state, base, 1000);
  state = applyEngineMessage(state, {
    ...base,
    data: {
      ...base.data,
      benchmark_price_usd: 505,
    },
  }, 2000);

  assert.equal(state.equityPortfolioHistory.length, 2);
  assert.equal(state.equityPortfolioHistory[0].benchmark_price_usd, 505);
});

test('retains portfolio frames beyond the old short sparkline window', () => {
  let state = createOperationsState();

  for (let index = 0; index < 72; index += 1) {
    state = applyEngineMessage(state, {
      type: MESSAGE_TYPES.EQUITY_PORTFOLIO_STATE,
      data: {
        initial_equity_usd: 10000,
        cash_usd: 9000,
        nav_usd: 10000 + index,
        gross_exposure_usd: 1000,
        daily_pnl_usd: index,
        max_drawdown_pct: 0,
        positions: [],
        timestamp_ns: String(index),
      },
    }, 1000 + index);
  }

  assert.equal(state.equityPortfolioHistory.length, 72);
  assert.equal(state.equityPortfolioHistory[0].nav_usd, 10071);
  assert.equal(state.equityPortfolioHistory[71].nav_usd, 10000);
});
