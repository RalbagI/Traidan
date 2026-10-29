import assert from 'node:assert/strict';
import test from 'node:test';

import { MESSAGE_TYPES, parseEngineMessage } from './messages.js';

const logger = {
  error() {},
  warn() {},
};

function validCycleMessage() {
  return {
    type: MESSAGE_TYPES.LLM_EQUITY_CYCLE,
    data: {
      cycle_id: 'cycle-1',
      sequence: 1,
      as_of: '2026-05-28T14:30:00Z',
      market_session: 'rth',
      portfolio_nav_usd: 10000,
      commands: [{
        action: 'buy_notional',
        symbol: 'SPY',
        rationale: 'Breadth improved and evidence is fresh.',
        confidence: 0.72,
        max_notional_usd: 1000,
        horizon: '60m',
        evidence_refs: ['ev-1'],
      }],
      evidence: [{
        id: 'ev-1',
        source: 'market_research_snapshot',
        summary: 'Fresh bars and quotes.',
        retrieved_at: '2026-05-28T14:29:00Z',
        freshness: 'fresh',
      }],
      risk_assessment: {
        summary: 'Inside all dry-run limits.',
        max_loss_scenario: 'Gap risk only.',
        risk_limit_verdict: 'pass',
      },
      memory_updates: [],
      timestamp_ns: '1700000000000000000',
    },
  };
}

test('parses readable LLM equity cycle payloads', () => {
  const parsed = parseEngineMessage(validCycleMessage(), { logger });

  assert.equal(parsed.type, MESSAGE_TYPES.LLM_EQUITY_CYCLE);
  assert.equal(parsed.data.commands[0].symbol, 'SPY');
  assert.equal(parsed.data.commands[0].horizon, '60m');
});

test('validates the optional market session on service-status frames', () => {
  const valid = {
    type: MESSAGE_TYPES.LLM_EQUITY_SERVICE_STATUS,
    data: {
      status: 'llm_research_only',
      detail: 'Public-news research completed without a trade cycle.',
      market_session: 'rth',
      has_market_research_snapshot: false,
      activity_key: 'news-scan:stable-opportunity',
      activity_repeat_count: 2,
      last_cycle_id: null,
      last_error: 'no_public_event_score_above_threshold',
      timestamp_ns: '1700000000000000000',
    },
  };

  assert.equal(parseEngineMessage(valid, { logger }).data.market_session, 'rth');
  assert.equal(parseEngineMessage(valid, { logger }).data.has_market_research_snapshot, false);
  assert.equal(parseEngineMessage(valid, { logger }).data.activity_key, 'news-scan:stable-opportunity');
  assert.equal(parseEngineMessage(valid, { logger }).data.activity_repeat_count, 2);

  const invalid = {
    ...valid,
    data: { ...valid.data, market_session: 'weekend' },
  };
  assert.equal(parseEngineMessage(invalid, { logger }), null);

  const invalidActivity = {
    ...valid,
    data: { ...valid.data, activity_key: '' },
  };
  assert.equal(parseEngineMessage(invalidActivity, { logger }), null);

  const invalidRepeatCount = {
    ...valid,
    data: { ...valid.data, activity_repeat_count: -1 },
  };
  assert.equal(parseEngineMessage(invalidRepeatCount, { logger }), null);

  const invalidSnapshotPresence = {
    ...valid,
    data: { ...valid.data, has_market_research_snapshot: 'no' },
  };
  assert.equal(parseEngineMessage(invalidSnapshotPresence, { logger }), null);
});

test('accepts only the five production command horizons', () => {
  for (const horizon of ['30m', '60m', 'close', '1d', '5d']) {
    const message = validCycleMessage();
    message.data.commands[0].horizon = horizon;
    assert.equal(parseEngineMessage(message, { logger }).data.commands[0].horizon, horizon);
  }

  const missing = validCycleMessage();
  delete missing.data.commands[0].horizon;
  assert.equal(parseEngineMessage(missing, { logger }), null);

  const unsupported = validCycleMessage();
  unsupported.data.commands[0].horizon = '2h';
  assert.equal(parseEngineMessage(unsupported, { logger }), null);
});

test('rejects non-directional and non-positive LLM command frames', () => {
  const hold = validCycleMessage();
  hold.data.commands[0].action = 'hold';
  assert.equal(parseEngineMessage(hold, { logger }), null);

  const smallPositive = validCycleMessage();
  smallPositive.data.commands[0].max_notional_usd = 0.01;
  assert.equal(parseEngineMessage(smallPositive, { logger }).data.commands[0].max_notional_usd, 0.01);

  const zero = validCycleMessage();
  zero.data.commands[0].max_notional_usd = 0;
  assert.equal(parseEngineMessage(zero, { logger }), null);

  const oldTargetWeight = validCycleMessage();
  oldTargetWeight.data.commands[0].target_weight = 0.1;
  assert.equal(parseEngineMessage(oldTargetWeight, { logger }), null);
});

test('rejects oversized LLM cycle arrays against schema caps', () => {
  const commandTemplate = validCycleMessage().data.commands[0];
  const evidenceTemplate = validCycleMessage().data.evidence[0];
  const memoryTemplate = {
    event_id: 'memory-1',
    cycle_id: 'cycle-1',
    conclusion: 'Keep watching the dynamic equity sweep.',
  };

  const tooManyCommands = validCycleMessage();
  tooManyCommands.data.commands = Array.from({ length: 17 }, (_, index) => ({
    ...commandTemplate,
    symbol: `A${index.toString().padStart(3, '0')}`,
  }));
  assert.equal(parseEngineMessage(tooManyCommands, { logger }), null);

  const tooManyEvidence = validCycleMessage();
  tooManyEvidence.data.evidence = Array.from({ length: 65 }, (_, index) => ({
    ...evidenceTemplate,
    id: `ev-${index}`,
  }));
  assert.equal(parseEngineMessage(tooManyEvidence, { logger }), null);

  const tooManyMemoryUpdates = validCycleMessage();
  tooManyMemoryUpdates.data.memory_updates = Array.from({ length: 17 }, (_, index) => ({
    ...memoryTemplate,
    event_id: `memory-${index}`,
  }));
  assert.equal(parseEngineMessage(tooManyMemoryUpdates, { logger }), null);
});

test('rejects removed stream message types', () => {
  const parsed = parseEngineMessage({
    type: ['ai', 'signal'].join('_'),
    data: {},
  }, { logger });

  assert.equal(parsed, null);
});

test('accepts signed short position market values', () => {
  const parsed = parseEngineMessage({
    type: MESSAGE_TYPES.EQUITY_PORTFOLIO_STATE,
    data: {
      initial_equity_usd: 10000,
      cash_usd: 11000,
      nav_usd: 10000,
      gross_exposure_usd: 1000,
      daily_pnl_usd: 0,
      max_drawdown_pct: 0,
      positions: [{
        symbol: 'SPY',
        quantity: -2,
        avg_price: 500,
        market_value_usd: -1000,
        weight_pct: -0.1,
      }],
      timestamp_ns: '1700000000000000000',
    },
  }, { logger });

  assert.equal(parsed.type, MESSAGE_TYPES.EQUITY_PORTFOLIO_STATE);
  assert.equal(parsed.data.positions[0].quantity, -2);
});

test('accepts finite non-positive portfolio NAV frames', () => {
  const parsed = parseEngineMessage({
    type: MESSAGE_TYPES.EQUITY_PORTFOLIO_STATE,
    data: {
      initial_equity_usd: 10000,
      cash_usd: -50,
      nav_usd: -25,
      gross_exposure_usd: 25,
      daily_pnl_usd: -10025,
      max_drawdown_pct: 1.0025,
      positions: [{
        symbol: 'SPY',
        quantity: 25,
        avg_price: 500,
        market_value_usd: 25,
        weight_pct: 0,
      }],
      timestamp_ns: '1700000000000000000',
    },
  }, { logger });

  assert.equal(parsed.type, MESSAGE_TYPES.EQUITY_PORTFOLIO_STATE);
  assert.equal(parsed.data.nav_usd, -25);
});

test('accepts optional S&P500 benchmark portfolio fields', () => {
  const parsed = parseEngineMessage({
    type: MESSAGE_TYPES.EQUITY_PORTFOLIO_STATE,
    data: {
      initial_equity_usd: 10000,
      cash_usd: 9000,
      nav_usd: 10025,
      gross_exposure_usd: 1000,
      daily_pnl_usd: 25,
      total_pnl_usd: 25,
      total_pnl_pct: 0.0025,
      realized_pnl_usd: 0,
      unrealized_pnl_usd: 25,
      max_drawdown_pct: 0,
      positions: [],
      benchmark_symbol: 'SPY',
      benchmark_label: 'S&P500',
      benchmark_status: 'stale',
      benchmark_price_usd: 505.25,
      timestamp_ns: '1700000000000000000',
    },
  }, { logger });

  assert.equal(parsed.type, MESSAGE_TYPES.EQUITY_PORTFOLIO_STATE);
  assert.equal(parsed.data.benchmark_symbol, 'SPY');
  assert.equal(parsed.data.benchmark_label, 'S&P500');
  assert.equal(parsed.data.benchmark_status, 'stale');
  assert.equal(parsed.data.benchmark_price_usd, 505.25);
});

test('accepts visible unavailable S&P500 benchmark state', () => {
  const parsed = parseEngineMessage({
    type: MESSAGE_TYPES.EQUITY_PORTFOLIO_STATE,
    data: {
      initial_equity_usd: 10000,
      cash_usd: 10000,
      nav_usd: 10000,
      gross_exposure_usd: 0,
      daily_pnl_usd: 0,
      total_pnl_usd: 0,
      total_pnl_pct: 0,
      realized_pnl_usd: 0,
      unrealized_pnl_usd: 0,
      max_drawdown_pct: 0,
      positions: [],
      benchmark_symbol: 'SPY',
      benchmark_label: 'S&P500',
      benchmark_status: 'unavailable',
      timestamp_ns: '1700000000000000000',
    },
  }, { logger });

  assert.equal(parsed.type, MESSAGE_TYPES.EQUITY_PORTFOLIO_STATE);
  assert.equal(parsed.data.benchmark_symbol, 'SPY');
  assert.equal(parsed.data.benchmark_status, 'unavailable');
  assert.equal(parsed.data.benchmark_price_usd, undefined);
});

test('accepts cycle summary frames for the trade ledger', () => {
  const parsed = parseEngineMessage({
    type: MESSAGE_TYPES.LLM_EQUITY_CYCLE_SUMMARY,
    data: {
      cycle_id: 'llm-equity-2026-07-24T15:35:00Z',
      cycle_kind: 'news_scan',
      as_of: '2026-07-24T15:35:00Z',
      market_session: 'rth',
      status: 'accepted',
      mandatory_trade_status: 'accepted',
      required_notional_usd: 25,
      requested_notional_usd: 30,
      accepted_notional_usd: 25.5,
      actions: ['buy'],
      symbols: ['SPY'],
      rationale: 'Fresh breadth and macro context supported a minimum paper buy.',
      reason: 'simulated_fill',
      timestamp_ns: '1784907315000000000',
    },
  }, { logger });

  assert.equal(parsed.type, MESSAGE_TYPES.LLM_EQUITY_CYCLE_SUMMARY);
  assert.equal(parsed.data.accepted_notional_usd, 25.5);
  assert.deepEqual(parsed.data.symbols, ['SPY']);
});

test('rejects partial or invalid benchmark portfolio fields', () => {
  const frame = {
    type: MESSAGE_TYPES.EQUITY_PORTFOLIO_STATE,
    data: {
      initial_equity_usd: 10000,
      cash_usd: 9000,
      nav_usd: 10025,
      gross_exposure_usd: 1000,
      daily_pnl_usd: 25,
      max_drawdown_pct: 0,
      positions: [],
      benchmark_symbol: 'SPY',
      benchmark_label: 'S&P500',
      benchmark_status: 'live',
      benchmark_price_usd: 505.25,
      timestamp_ns: '1700000000000000000',
    },
  };

  assert.equal(parseEngineMessage({
    ...frame,
    data: {
      ...frame.data,
      benchmark_price_usd: 0,
    },
  }, { logger }), null);
  assert.equal(parseEngineMessage({
    ...frame,
    data: {
      ...frame.data,
      benchmark_status: 'live',
      benchmark_label: undefined,
    },
  }, { logger }), null);
  assert.equal(parseEngineMessage({
    ...frame,
    data: {
      ...frame.data,
      benchmark_symbol: 'QQQ',
    },
  }, { logger }), null);
  assert.equal(parseEngineMessage({
    ...frame,
    data: {
      ...frame.data,
      benchmark_label: 'Nasdaq 100',
    },
  }, { logger }), null);
});

test('accepts a complete allocation and rejects incomplete or duplicate target weights', () => {
  const message = validCycleMessage();
  message.data.commands = [];
  message.data.execution_policy = 'fully_invested_v1';
  message.data.target_portfolio = {
    schema_version: 1,
    holdings: Array.from({ length: 20 }, (_, i) => ({ symbol: `STK${i}`, weight_bps: 500 })),
  };
  assert.ok(parseEngineMessage(message, { logger }));
  message.data.target_portfolio.holdings[0].weight_bps = 499;
  assert.equal(parseEngineMessage(message, { logger }), null);
  message.data.target_portfolio.holdings[0].weight_bps = 500;
  message.data.target_portfolio.holdings[0].symbol = 'STK1';
  assert.equal(parseEngineMessage(message, { logger }), null);
});
