import assert from 'node:assert/strict';
import test from 'node:test';

import { createDashboardViewModel, gatePath } from './dashboardShell.js';
import { createOperationsState } from './operationsState.js';

test('waiting desk is not marked unavailable while backend is connected', () => {
  const dashboard = createDashboardViewModel({
    operationsState: createOperationsState(),
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.equal(dashboard.service.label, 'ממתין');
  assert.notEqual(dashboard.service.label, 'לא זמין');
  assert.match(dashboard.latestReasoning.thesis, /ללא שינוי/);
});

test('waiting desk exposes truthful loading states instead of synthetic account data', () => {
  const dashboard = createDashboardViewModel({
    operationsState: createOperationsState(),
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.equal(dashboard.portfolio.available, false);
  assert.equal(dashboard.portfolio.nav, null);
  assert.equal(dashboard.portfolio.navLabel, '—');
  assert.deepEqual(dashboard.navSeries, []);
  assert.deepEqual(dashboard.navHistory, []);
  assert.equal(dashboard.panels.portfolio.status, 'loading');
  assert.equal(dashboard.panels.cycles.status, 'loading');
  assert.equal(dashboard.panels.memory.status, 'loading');
  assert.deepEqual(dashboard.memory, []);
});

test('disconnected desk reports an incident and never presents missing data as current', () => {
  const emptyDashboard = createDashboardViewModel({
    operationsState: createOperationsState(),
    connectionStatus: 'disconnected',
    reconnectDelayMs: 4000,
    nowMs: 5000,
  });

  assert.equal(emptyDashboard.health.severity, 'critical');
  assert.equal(emptyDashboard.health.incident.kind, 'stream_disconnected');
  assert.equal(emptyDashboard.health.retryInMs, 4000);
  assert.equal(emptyDashboard.panels.portfolio.status, 'error');
  assert.equal(emptyDashboard.portfolio.navLabel, '—');

  const retainedState = createOperationsState();
  retainedState.equityPortfolio = {
    initial_equity_usd: 10000,
    cash_usd: 9000,
    nav_usd: 10025,
    gross_exposure_usd: 1025,
    daily_pnl_usd: 25,
    max_drawdown_pct: 0,
    positions: [],
    receivedAtMs: 3000,
  };
  const retainedDashboard = createDashboardViewModel({
    operationsState: retainedState,
    connectionStatus: 'disconnected',
    nowMs: 5000,
  });

  assert.equal(retainedDashboard.portfolio.navLabel, '$10,025.00');
  assert.equal(retainedDashboard.panels.portfolio.status, 'stale');
  assert.equal(retainedDashboard.panels.portfolio.updatedAtMs, 3000);
});

test('retained panels stay stale until their domain receives the current stream generation', () => {
  const retainedState = createOperationsState();
  retainedState.equityPortfolio = {
    initial_equity_usd: 10000,
    cash_usd: 9000,
    nav_usd: 10025,
    gross_exposure_usd: 1025,
    daily_pnl_usd: 25,
    max_drawdown_pct: 0,
    positions: [],
    receivedAtMs: 3000,
    streamGeneration: 1,
  };

  for (const connectionStatus of ['disconnected', 'connecting', 'connected']) {
    const dashboard = createDashboardViewModel({
      operationsState: retainedState,
      connectionStatus,
      streamGeneration: 2,
      nowMs: 5000,
    });
    assert.equal(dashboard.panels.portfolio.status, 'stale', connectionStatus);
  }

  retainedState.equityPortfolio = {
    ...retainedState.equityPortfolio,
    receivedAtMs: 5100,
    streamGeneration: 2,
  };
  const refreshed = createDashboardViewModel({
    operationsState: retainedState,
    connectionStatus: 'connected',
    streamGeneration: 2,
    nowMs: 5200,
  });

  assert.equal(refreshed.panels.portfolio.status, 'ready');
});

test('service failures are separated from historical panel data', () => {
  const state = createOperationsState();
  state.serviceStatus = {
    status: 'state_persistence_failed',
    detail: 'שמירת מצב הסמכות נכשלה.',
    receivedAtMs: 4000,
  };
  state.equityPortfolio = {
    initial_equity_usd: 10000,
    cash_usd: 9000,
    nav_usd: 10064,
    gross_exposure_usd: 1064,
    daily_pnl_usd: 64,
    max_drawdown_pct: 0,
    positions: [],
    receivedAtMs: 3500,
  };

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    adminApiStatus: 'available',
    nowMs: 5000,
  });

  assert.equal(dashboard.health.severity, 'critical');
  assert.equal(dashboard.health.incident.kind, 'service_failed');
  assert.equal(dashboard.health.incident.detail, 'שמירת מצב הסמכות נכשלה.');
  assert.equal(dashboard.panels.portfolio.status, 'stale');
  assert.equal(dashboard.portfolio.navLabel, '$10,064.00');
});

test('service status distinguishes active review from failed review', () => {
  const reviewingState = createOperationsState();
  reviewingState.serviceStatus = {
    status: 'llm_review_in_progress',
    detail: 'סריקת GPT-5.6-Sol xhigh למניות פועלת.',
    receivedAtMs: 1000,
  };

  const reviewing = createDashboardViewModel({
    operationsState: reviewingState,
    connectionStatus: 'connected',
    nowMs: 1500,
  });

  assert.equal(reviewing.service.label, 'בבדיקה');
  assert.equal(reviewing.service.state, 'warn');

  const failedState = createOperationsState();
  failedState.serviceStatus = {
    status: 'llm_review_failed',
    detail: 'הסריקה נכשלה.',
    last_error: 'invalid strict decision JSON',
    receivedAtMs: 1000,
  };

  const failed = createDashboardViewModel({
    operationsState: failedState,
    connectionStatus: 'connected',
    nowMs: 1500,
  });

  assert.equal(failed.service.label, 'נכשל');
  assert.equal(failed.service.state, 'bad');
});

test('service status supplies the session before any trade cycle exists', () => {
  const state = createOperationsState();
  state.serviceStatus = {
    status: 'llm_research_only',
    detail: 'סריקת החדשות הסתיימה ללא אירוע חזק מספיק.',
    market_session: 'rth',
    receivedAtMs: 1000,
  };

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1500,
  });

  assert.equal(dashboard.service.session, 'rth');
  assert.equal(dashboard.cycles.length, 1);
  assert.equal(dashboard.currentCycle.status, 'held');
  assert.equal(dashboard.currentCycle.serviceActivity, true);
  assert.equal(dashboard.currentCycle.session, 'rth');
  assert.equal(dashboard.currentCycle.thesis, state.serviceStatus.detail);
  assert.equal(dashboard.currentCycle.risk.verdict, 'not_evaluated');
  assert.ok(dashboard.currentCycle.gates.every((gate) => gate.state === 'na'));
  assert.equal(dashboard.panels.cycles.status, 'ready');
  assert.equal(dashboard.panels.decision.status, 'ready');
  assert.equal(dashboard.panels.freshness.status, 'empty');
});

test('quiet status does not present a retained market snapshot as current', () => {
  const state = createOperationsState();
  state.marketResearchSnapshot = {
    snapshot_id: 'prior-full-cycle',
    as_of: '2026-05-28T14:30:00Z',
    market_session: 'rth',
    source_freshness: [{ source: 'alpaca_market_data', freshness: 'fresh' }],
    unavailable_data_flags: [],
    receivedAtMs: 2000,
    streamGeneration: 2,
  };
  state.serviceStatus = {
    status: 'llm_research_only',
    detail: 'Quiet scan completed without fetching a full market snapshot.',
    market_session: 'rth',
    has_market_research_snapshot: false,
    timestamp_ns: '1700007200000000000',
    timestampMs: 1700007200000,
    receivedAtMs: 3000,
    streamGeneration: 2,
  };

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    streamGeneration: 2,
    nowMs: 3500,
  });

  assert.equal(dashboard.currentCycle.researchOnly, true);
  assert.equal(dashboard.panels.freshness.status, 'empty');
  assert.equal(dashboard.freshness[0].freshness, 'waiting');
  assert.doesNotMatch(dashboard.freshness[0].detail, /מעודכן/);
});

test('retained research activities render newest first as distinct completed scans', () => {
  const state = createOperationsState();
  state.serviceStatus = {
    status: 'llm_scheduler_waiting',
    detail: 'Waiting for the next scheduled scan.',
    market_session: 'rth',
    receivedAtMs: 3000,
  };
  state.serviceActivities = [{
    status: 'llm_research_only',
    detail: 'First public-news scan completed.',
    market_session: 'rth',
    timestamp_ns: '1700000000000000000',
    timestampMs: 1700000000000,
    receivedAtMs: 1000,
    repeatCount: 4,
  }, {
    status: 'llm_edge_gate_skipped',
    detail: 'Second scan found candidates but no sufficient edge.',
    market_session: 'rth',
    timestamp_ns: '1700003600000000000',
    timestampMs: 1700003600000,
    receivedAtMs: 2000,
  }];

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1700007200000,
  });

  assert.equal(dashboard.cycles.length, 2);
  assert.equal(dashboard.cycles[0].activityStatus, 'llm_edge_gate_skipped');
  assert.equal(dashboard.cycles[0].fresh, true);
  assert.equal(dashboard.cycles[1].activityStatus, 'llm_research_only');
  assert.equal(dashboard.cycles[1].fresh, false);
  assert.equal(dashboard.cycles[1].repeatCount, 4);
});

test('primary cycle ledger uses trade summaries and excludes research-only activity', () => {
  const state = createOperationsState();
  state.serviceStatus = {
    status: 'llm_research_only',
    detail: 'Quiet public-news scan completed.',
    market_session: 'rth',
    activity_key: 'news-scan:quiet',
    timestamp_ns: '1784900000000000000',
    timestampMs: 1784900000000,
    receivedAtMs: 1000,
  };
  state.serviceActivities = [state.serviceStatus];
  state.cycleSummaries = Array.from({ length: 20 }, (_, index) => ({
    cycle_id: `trade-cycle-${index}`,
    cycle_kind: 'news_scan',
    as_of: `2026-07-24T${String(10 + Math.floor(index / 2)).padStart(2, '0')}:35:00Z`,
    market_session: 'rth',
    status: index % 3 === 0 ? 'blocked' : 'accepted',
    mandatory_trade_status: index % 3 === 0 ? 'blocked' : 'accepted',
    required_notional_usd: 25,
    requested_notional_usd: 30,
    accepted_notional_usd: index % 3 === 0 ? 0 : 25 + index,
    actions: index === 19 ? ['buy', 'buy', 'sell'] : [index % 2 === 0 ? 'buy' : 'sell'],
    symbols: index === 19 ? ['SPY', 'QQQ', 'IWM'] : [index % 2 === 0 ? 'SPY' : 'QQQ'],
    rationale: `Cycle ${index} rationale`,
    reason: index % 3 === 0 ? 'mandatory_order_notional_required' : 'simulated_fill',
    timestamp_ns: String(1_784_900_000_000_000_000n + BigInt(index)),
    timestampMs: 1_784_900_000_000 + index,
    receivedAtMs: 2_000 + index,
  })).reverse();

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1_784_900_100_000,
  });

  assert.equal(dashboard.cycles.length, 20);
  assert.equal(dashboard.cycles.some((cycle) => cycle.researchOnly), false);
  assert.equal(dashboard.cycles[0].id, 'trade-cycle-19');
  assert.equal(dashboard.cycles[0].actualNotionalLabel, '$44.00');
  assert.equal(dashboard.cycles[0].requiredNotionalLabel, '$25.00');
  assert.match(dashboard.cycles[0].thesis, /Cycle 19 rationale/);
  assert.deepEqual(
    dashboard.cycles[0].commands.map(({ symbol, action }) => [symbol, action]),
    [
      ['SPY', 'buy_notional'],
      ['QQQ', 'buy_notional'],
      ['IWM', 'sell_notional'],
    ],
  );
});

test('service status surfaces authority recovery and persistence failures', () => {
  for (const status of ['state_recovery_failed', 'state_persistence_failed']) {
    const state = createOperationsState();
    state.serviceStatus = {
      status,
      detail: `Authority reported ${status}.`,
      receivedAtMs: 1000,
    };

    const dashboard = createDashboardViewModel({
      operationsState: state,
      connectionStatus: 'connected',
      nowMs: 1500,
    });

    assert.equal(dashboard.service.label, 'נכשל');
    assert.equal(dashboard.service.state, 'bad');
  }
});

test('latest reasoning renders commands, evidence, risk, and memory prose', () => {
  const state = createOperationsState();
  state.equityCycle = {
    cycle_id: 'cycle-2',
    commands: [{
      action: 'buy_notional',
      symbol: 'SPY',
      rationale: 'רוחב השוק והנזילות תומכים בכניסת סימולציה קטנה.',
      confidence: 0.7,
      max_notional_usd: 1000,
    }],
    evidence: [{
      id: 'ev-1',
      source: 'market_research_snapshot',
      freshness: 'fresh',
      summary: 'snapshot טרי של ציטוט ו-bar.',
    }],
    risk_assessment: {
      summary: 'בתוך מגבלות הפקודה ורזרבת המזומן.',
      risk_limit_verdict: 'pass',
    },
    memory_updates: [{
      conclusion: 'לעקוב אם המשכיות רוחב השוק נשמרת.',
    }],
  };

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.equal(dashboard.latestReasoning.commands[0].symbol, 'SPY');
  assert.match(dashboard.latestReasoning.risk, /בתוך מגבלות/);
  assert.match(dashboard.latestReasoning.memory, /רוחב השוק/);
});

test('rejected positive-notional directional sweep is surfaced in the summary', () => {
  const state = createOperationsState();
  state.equityPortfolio = {
    nav_usd: 10000,
    cash_usd: 10000,
    gross_exposure_usd: -0,
    daily_pnl_usd: -0,
    max_drawdown_pct: -0,
    positions: [],
  };
  state.serviceStatus = {
    status: 'cycle_applied',
    detail: 'הסריקה השעתית הושלמה; פקודת הסימולציה נדחתה.',
    last_cycle_id: 'llm-equity-2026-06-01T19:30:00Z',
    receivedAtMs: 1000,
  };
  state.equityCycle = {
    cycle_id: 'llm-equity-2026-06-01T19:30:00Z',
    commands: [{
      action: 'buy_notional',
      symbol: 'SPY',
      rationale: 'ראיות טריות תומכות בכניסת סימולציה עם notional חיובי.',
      confidence: 0.72,
      max_notional_usd: 10,
    }],
    evidence: [{
      id: 'ev-1',
      source: 'market_research_snapshot',
      freshness: 'fresh',
      summary: 'snapshot טרי של ציטוט ו-bar.',
    }],
    risk_assessment: {
      summary: 'בתוך מגבלות הפקודה ורזרבת המזומן.',
      risk_limit_verdict: 'pass',
    },
    memory_updates: [],
  };
  state.equityOrders = [{
    order_id: 'order-1',
    cycle_id: 'llm-equity-2026-06-01T19:30:00Z',
    symbol: 'SPY',
    side: 'buy',
    status: 'rejected',
    qty: 0,
    avg_price: 500,
    notional_usd: 10,
    reason: 'orders_allowed_only_during_rth',
  }];

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1500,
  });

  assert.equal(dashboard.portfolio.exposureLabel, '$0.00');
  assert.equal(dashboard.portfolio.pnlLabel, '$0.00');
  assert.equal(dashboard.sweep.decision, 'SPY קנייה $10.00');
  assert.equal(dashboard.sweep.orderEffect, '1 פקודות סימולציה נדחו.');
  assert.match(dashboard.sweep.portfolioEffect, /מזומן בלבד/);
});

test('desk model normalizes percent weights and infers freshness gates', () => {
  const state = createOperationsState();
  state.equityPortfolio = {
    initial_equity_usd: 10000,
    cash_usd: 9000,
    nav_usd: 10000,
    gross_exposure_usd: 1000,
    daily_pnl_usd: 0,
    max_drawdown_pct: 0,
    positions: [{
      symbol: 'SPY',
      quantity: 2,
      avg_price: 500,
      market_value_usd: 1000,
      weight_pct: 10,
    }],
  };
  state.marketResearchSnapshot = {
    market_session: 'rth',
    source_freshness: [{ source: 'alpaca_market_data', freshness: 'stale' }],
    unavailable_data_flags: [],
  };
  state.equityCycle = {
    cycle_id: 'cycle-stale',
    sequence: 7,
    as_of: '2026-05-28T14:30:00Z',
    market_session: 'rth',
    commands: [{
      action: 'buy_notional',
      symbol: 'SPY',
      rationale: 'טריות הנתונים אמורה לחסום את הכוונה הזו.',
      confidence: 0.7,
      max_notional_usd: 1000,
      evidence_refs: ['ev-1'],
    }],
    evidence: [{
      id: 'ev-1',
      source: 'alpaca_market_data',
      freshness: 'stale',
      summary: 'נתוני השוק מיושנים.',
      retrieved_at: '2026-05-28T14:29:00Z',
    }],
    risk_assessment: {
      summary: 'הסיכון לא נבדק כי הטריות נכשלה.',
      risk_limit_verdict: 'pass',
    },
    memory_updates: [],
  };
  state.equityCycles = [state.equityCycle];

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.equal(dashboard.portfolio.positions[0].weightPct, 0.1);
  assert.equal(dashboard.currentCycle.model, 'GPT-6 Astra xhigh');
  assert.equal(dashboard.currentCycle.blockedGate, 'freshness');
  assert.equal(dashboard.currentCycle.gates.find((gate) => gate.key === 'freshness').state, 'block');
});

test('portfolio model uses total run pnl and position worth fields', () => {
  const state = createOperationsState();
  state.equityPortfolio = {
    initial_equity_usd: 10000,
    cash_usd: 9000,
    nav_usd: 10020,
    gross_exposure_usd: 1020,
    daily_pnl_usd: 10,
    total_pnl_usd: 20,
    total_pnl_pct: 0.002,
    realized_pnl_usd: 4,
    unrealized_pnl_usd: 20,
    max_drawdown_pct: 0,
    positions: [{
      symbol: 'SPY',
      quantity: 2,
      avg_price: 500,
      current_price_usd: 510,
      cost_basis_usd: 1000,
      market_value_usd: 1020,
      weight_pct: 0.10179640718562875,
      unrealized_pnl_usd: 20,
      unrealized_pnl_pct: 0.02,
    }],
  };

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.equal(dashboard.portfolio.totalPnl, 20);
  assert.equal(dashboard.portfolio.pnlLabel, '$20.00');
  assert.equal(dashboard.portfolio.dailyPnlLabel, '$10.00');
  assert.equal(dashboard.portfolio.positions[0].marketValueLabel, '$1,020.00');
  assert.equal(dashboard.portfolio.positions[0].costBasisLabel, '$1,000.00');
  assert.equal(dashboard.portfolio.positions[0].unrealizedPnlLabel, '$20.00');
  assert.equal(dashboard.portfolio.positions[0].currentPriceLabel, '$510.00');
});

test('portfolio nav history starts at initial dry-run equity and samples portfolio changes once per day', () => {
  const state = createOperationsState();
  const startMs = Date.UTC(2026, 0, 1, 14, 30, 0);
  const dayMs = 86_400_000;
  const frames = [0, 1, 2].flatMap((dayIndex) => [0, 1].map((intraDayIndex) => {
    const sequence = dayIndex * 2 + intraDayIndex;
    return {
      initial_equity_usd: 10000,
      cash_usd: 9000 - sequence * 10,
      nav_usd: 10001 + sequence,
      gross_exposure_usd: 1000 + sequence * 20,
      daily_pnl_usd: sequence,
      max_drawdown_pct: 0,
      positions: [],
      timestamp_ns: String(BigInt(startMs + dayIndex * dayMs + intraDayIndex * 3_600_000) * 1_000_000n),
    };
  }));
  state.equityPortfolio = frames[frames.length - 1];
  state.equityPortfolioHistory = [...frames].reverse();

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.deepEqual(dashboard.navHistory, [10000, 10002, 10004, 10006]);
  assert.equal(dashboard.navSeries[0].label, 'תחילה');
  assert.equal(dashboard.navSeries[0].nav, 10000);
  assert.deepEqual(dashboard.navSeries.slice(1).map((point) => point.nav), [10002, 10004, 10006]);
  assert.deepEqual(dashboard.navSeries.slice(1).map((point) => point.cash), [8990, 8970, 8950]);
  assert.deepEqual(dashboard.navSeries.slice(1).map((point) => point.grossExposure), [1020, 1060, 1100]);
  assert.equal(dashboard.navSeries.length, 4);
});

test('portfolio nav series normalizes S&P500 benchmark prices to initial equity', () => {
  const state = createOperationsState();
  const startMs = Date.UTC(2026, 0, 1, 14, 30, 0);
  const nextMs = Date.UTC(2026, 0, 2, 14, 30, 0);
  state.equityPortfolio = {
    initial_equity_usd: 10000,
    cash_usd: 8975,
    nav_usd: 10060,
    gross_exposure_usd: 1000,
    daily_pnl_usd: 60,
    max_drawdown_pct: 0,
    positions: [],
    benchmark_symbol: 'SPY',
    benchmark_label: 'S&P500',
    benchmark_price_usd: 510,
    timestamp_ns: String(BigInt(nextMs) * 1_000_000n),
  };
  state.equityPortfolioHistory = [{
    ...state.equityPortfolio,
    nav_usd: 10020,
    benchmark_price_usd: 500,
    timestamp_ns: String(BigInt(startMs) * 1_000_000n),
  }, state.equityPortfolio];

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.deepEqual(
    dashboard.navSeries.map((point) => point.benchmarkNav),
    [10000, 10000, 10200],
  );
  assert.deepEqual(
    dashboard.navSeries.map((point) => point.benchmarkLabel),
    ['S&P500', 'S&P500', 'S&P500'],
  );
  assert.equal(dashboard.navSeries[2].benchmarkPrice, 510);
});

test('portfolio nav series keeps S&P500 comparison when the first sample lacks benchmark price', () => {
  const state = createOperationsState();
  const startMs = Date.UTC(2026, 0, 1, 14, 30, 0);
  const nextMs = Date.UTC(2026, 0, 2, 14, 30, 0);
  state.equityPortfolio = {
    initial_equity_usd: 10000,
    cash_usd: 8975,
    nav_usd: 10060,
    gross_exposure_usd: 1000,
    daily_pnl_usd: 60,
    max_drawdown_pct: 0,
    positions: [],
    benchmark_symbol: 'SPY',
    benchmark_label: 'S&P500',
    benchmark_price_usd: 510,
    timestamp_ns: String(BigInt(nextMs) * 1_000_000n),
  };
  state.equityPortfolioHistory = [{
    ...state.equityPortfolio,
    nav_usd: 10020,
    benchmark_symbol: undefined,
    benchmark_label: undefined,
    benchmark_price_usd: undefined,
    timestamp_ns: String(BigInt(startMs) * 1_000_000n),
  }, state.equityPortfolio];

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.deepEqual(
    dashboard.navSeries.map((point) => point.benchmarkNav),
    [10000, null, 10000],
  );
  assert.deepEqual(
    dashboard.navSeries.map((point) => point.benchmarkLabel),
    ['S&P500', 'S&P500', 'S&P500'],
  );
});

test('portfolio nav series keeps repeated NAV frames when timestamps fall on different days', () => {
  const state = createOperationsState();
  const dayMs = 86_400_000;
  const startMs = Date.UTC(2026, 0, 1, 14, 30, 0);
  state.equityPortfolio = {
    initial_equity_usd: 10000,
    cash_usd: 9000,
    nav_usd: 10025,
    gross_exposure_usd: 1000,
    daily_pnl_usd: 25,
    max_drawdown_pct: 0.01,
    positions: [{ symbol: 'SPY' }],
    timestamp_ns: String(BigInt(startMs + dayMs) * 1_000_000n),
  };
  state.equityPortfolioHistory = [{
    ...state.equityPortfolio,
    timestamp_ns: String(BigInt(startMs + dayMs) * 1_000_000n),
  }, {
    ...state.equityPortfolio,
    timestamp_ns: String(BigInt(startMs) * 1_000_000n),
  }];

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  const repeated = dashboard.navSeries.filter((point) => point.nav === 10025);
  assert.equal(repeated.length, 2);
  assert.notEqual(repeated[0].timestampMs, repeated[1].timestampMs);
  assert.equal(repeated[0].positionCount, 1);
  assert.equal(repeated[0].maxDrawdownPct, 0.01);
});

test('portfolio nav series keeps the latest same-day NAV frame', () => {
  const state = createOperationsState();
  const startMs = Date.UTC(2026, 0, 1, 14, 30, 0);
  state.equityPortfolio = {
    initial_equity_usd: 10000,
    cash_usd: 8975,
    nav_usd: 10025,
    gross_exposure_usd: 1000,
    daily_pnl_usd: 25,
    max_drawdown_pct: 0.01,
    positions: [{ symbol: 'SPY' }],
    timestamp_ns: String(BigInt(startMs + 3_600_000) * 1_000_000n),
  };
  state.equityPortfolioHistory = [{
    ...state.equityPortfolio,
    nav_usd: 10025,
    cash_usd: 8975,
    timestamp_ns: String(BigInt(startMs + 3_600_000) * 1_000_000n),
  }, {
    ...state.equityPortfolio,
    nav_usd: 10010,
    cash_usd: 8990,
    timestamp_ns: String(BigInt(startMs) * 1_000_000n),
  }];

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.deepEqual(dashboard.navSeries.map((point) => point.nav), [10000, 10025]);
  assert.equal(dashboard.navSeries[1].cash, 8975);
});

test('portfolio nav series marks accepted LLM buy and sell decisions on portfolio points', () => {
  const state = createOperationsState();
  const startMs = Date.UTC(2026, 0, 1, 14, 30, 0);
  const buyFrame = {
    initial_equity_usd: 10000,
    cash_usd: 9500,
    nav_usd: 10010,
    gross_exposure_usd: 500,
    daily_pnl_usd: 10,
    max_drawdown_pct: 0,
    positions: [{
      symbol: 'SPY',
      quantity: 1,
      avg_price: 500,
      market_value_usd: 500,
      weight_pct: 0.05,
    }],
    timestamp_ns: String(BigInt(startMs) * 1_000_000n),
  };
  const sellFrame = {
    ...buyFrame,
    cash_usd: 10500,
    nav_usd: 10020,
    gross_exposure_usd: 500,
    daily_pnl_usd: 20,
    positions: [{
      symbol: 'QQQ',
      quantity: -1,
      avg_price: 500,
      market_value_usd: -500,
      weight_pct: -0.05,
    }],
    timestamp_ns: String(BigInt(startMs + 3_600_000) * 1_000_000n),
  };
  state.equityPortfolio = sellFrame;
  state.equityPortfolioHistory = [sellFrame, buyFrame];
  state.equityCycles = [{
    cycle_id: 'cycle-sell-short',
    as_of: '2026-01-01T15:30:00Z',
    commands: [{
      action: 'sell_notional',
      symbol: 'QQQ',
      confidence: 0.62,
      max_notional_usd: 500,
      rationale: 'פתיחת שורט קטן בסימולציה.',
    }],
  }, {
    cycle_id: 'cycle-buy-long',
    as_of: '2026-01-01T14:30:00Z',
    commands: [{
      action: 'buy_notional',
      symbol: 'SPY',
      confidence: 0.74,
      max_notional_usd: 500,
      rationale: 'פתיחת לונג קטן בסימולציה.',
    }],
  }];
  state.equityCycle = state.equityCycles[0];
  state.equityOrders = [{
    order_id: 'order-sell-short',
    cycle_id: 'cycle-sell-short',
    symbol: 'QQQ',
    side: 'sell',
    status: 'accepted',
    qty: 1,
    avg_price: 500,
    notional_usd: 500,
    reason: 'simulated_fill',
    timestamp_ns: String(BigInt(startMs + 3_600_000) * 1_000_000n),
  }, {
    order_id: 'order-buy-long',
    cycle_id: 'cycle-buy-long',
    symbol: 'SPY',
    side: 'buy',
    status: 'accepted',
    qty: 1,
    avg_price: 500,
    notional_usd: 500,
    reason: 'simulated_fill',
    timestamp_ns: String(BigInt(startMs) * 1_000_000n),
  }];

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.deepEqual(dashboard.navSeries.map((point) => point.nav), [10000, 10010, 10020]);
  assert.equal(dashboard.navSeries[1].tradeMarkers[0].label, 'קניית לונג');
  assert.equal(dashboard.navSeries[1].tradeMarkers[0].symbol, 'SPY');
  assert.equal(dashboard.navSeries[2].tradeMarkers[0].label, 'מכירת שורט');
  assert.equal(dashboard.navSeries[2].tradeMarkers[0].symbol, 'QQQ');
});

test('portfolio nav trade markers do not borrow command metadata from other symbols', () => {
  const state = createOperationsState();
  const startMs = Date.UTC(2026, 0, 1, 14, 30, 0);
  state.equityPortfolio = {
    initial_equity_usd: 10000,
    cash_usd: 9500,
    nav_usd: 10010,
    gross_exposure_usd: 500,
    daily_pnl_usd: 10,
    max_drawdown_pct: 0,
    positions: [{
      symbol: 'QQQ',
      quantity: 1,
      avg_price: 500,
      market_value_usd: 500,
      weight_pct: 0.05,
    }],
    timestamp_ns: String(BigInt(startMs) * 1_000_000n),
  };
  state.equityPortfolioHistory = [state.equityPortfolio];
  state.equityCycle = {
    cycle_id: 'cycle-mixed',
    as_of: '2026-01-01T14:30:00Z',
    commands: [{
      action: 'buy_notional',
      symbol: 'SPY',
      confidence: 0.91,
      max_notional_usd: 900,
      rationale: 'פקודת לונג לא קשורה.',
    }, {
      action: 'sell_notional',
      symbol: 'AAPL',
      confidence: 0.42,
      max_notional_usd: 300,
      rationale: 'פקודת שורט לא קשורה.',
    }],
  };
  state.equityCycles = [state.equityCycle];
  state.equityOrders = [{
    order_id: 'order-qqq',
    cycle_id: 'cycle-mixed',
    symbol: 'QQQ',
    side: 'buy',
    status: 'accepted',
    qty: 1,
    avg_price: 500,
    notional_usd: 0,
    reason: 'simulated_fill',
    timestamp_ns: String(BigInt(startMs) * 1_000_000n),
  }];

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.equal(dashboard.navSeries[1].tradeMarkers[0].symbol, 'QQQ');
  assert.equal(dashboard.navSeries[1].tradeMarkers[0].notional, 0);
  assert.equal(dashboard.navSeries[1].tradeMarkers[0].confidence, 0);
});

test('portfolio nav trade markers ignore a single unrelated command', () => {
  const state = createOperationsState();
  const startMs = Date.UTC(2026, 0, 1, 14, 30, 0);
  state.equityPortfolio = {
    initial_equity_usd: 10000,
    cash_usd: 9500,
    nav_usd: 10010,
    gross_exposure_usd: 500,
    daily_pnl_usd: 10,
    max_drawdown_pct: 0,
    positions: [{
      symbol: 'QQQ',
      quantity: 1,
      avg_price: 500,
      market_value_usd: 500,
      weight_pct: 0.05,
    }],
    timestamp_ns: String(BigInt(startMs) * 1_000_000n),
  };
  state.equityPortfolioHistory = [state.equityPortfolio];
  state.equityCycle = {
    cycle_id: 'cycle-single-unrelated',
    as_of: '2026-01-01T14:30:00Z',
    commands: [{
      action: 'sell_notional',
      symbol: 'SPY',
      confidence: 0.91,
      max_notional_usd: 900,
      rationale: 'פקודת שורט לא קשורה.',
    }],
  };
  state.equityCycles = [state.equityCycle];
  state.equityOrders = [{
    order_id: 'order-qqq-single',
    cycle_id: 'cycle-single-unrelated',
    symbol: 'QQQ',
    side: 'buy',
    status: 'accepted',
    qty: 1,
    avg_price: 500,
    notional_usd: 0,
    reason: 'simulated_fill',
    timestamp_ns: String(BigInt(startMs) * 1_000_000n),
  }];

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.equal(dashboard.navSeries[1].tradeMarkers[0].side, 'buy');
  assert.equal(dashboard.navSeries[1].tradeMarkers[0].notional, 0);
  assert.equal(dashboard.navSeries[1].tradeMarkers[0].confidence, 0);
});

test('portfolio nav trade markers use matching command notional when accepted fill notional is zero', () => {
  const state = createOperationsState();
  const startMs = Date.UTC(2026, 0, 1, 14, 30, 0);
  state.equityPortfolio = {
    initial_equity_usd: 10000,
    cash_usd: 9500,
    nav_usd: 10010,
    gross_exposure_usd: 500,
    daily_pnl_usd: 10,
    max_drawdown_pct: 0,
    positions: [{
      symbol: 'SPY',
      quantity: 1,
      avg_price: 500,
      market_value_usd: 500,
      weight_pct: 0.05,
    }],
    timestamp_ns: String(BigInt(startMs) * 1_000_000n),
  };
  state.equityPortfolioHistory = [state.equityPortfolio];
  state.equityCycle = {
    cycle_id: 'cycle-zero-fill',
    as_of: '2026-01-01T14:30:00Z',
    commands: [{
      action: 'buy_notional',
      symbol: 'SPY',
      confidence: 0.74,
      max_notional_usd: 500,
      rationale: 'פקודה מתוכננת תואמת.',
    }],
  };
  state.equityCycles = [state.equityCycle];
  state.equityOrders = [{
    order_id: 'order-spy-zero',
    cycle_id: 'cycle-zero-fill',
    symbol: 'SPY',
    side: 'buy',
    status: 'accepted',
    qty: 0,
    avg_price: 500,
    notional_usd: 0,
    reason: 'simulated_fill',
    timestamp_ns: String(BigInt(startMs) * 1_000_000n),
  }];

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.equal(dashboard.navSeries[1].tradeMarkers[0].notional, 500);
  assert.equal(dashboard.navSeries[1].tradeMarkers[0].confidence, 0.74);
});

test('portfolio nav trade markers label flat sells as long trims unless short exposure remains', () => {
  const state = createOperationsState();
  const startMs = Date.UTC(2026, 0, 1, 14, 30, 0);
  state.equityPortfolio = {
    initial_equity_usd: 10000,
    cash_usd: 10000,
    nav_usd: 10000,
    gross_exposure_usd: 0,
    daily_pnl_usd: 0,
    max_drawdown_pct: 0,
    positions: [],
    timestamp_ns: String(BigInt(startMs) * 1_000_000n),
  };
  state.equityPortfolioHistory = [state.equityPortfolio];
  state.equityCycle = {
    cycle_id: 'cycle-flat-sell',
    as_of: '2026-01-01T14:30:00Z',
    commands: [{
      action: 'sell_notional',
      symbol: 'SPY',
      confidence: 0.6,
      max_notional_usd: 250,
      rationale: 'סגירת לונג בסימולציה.',
    }],
  };
  state.equityCycles = [state.equityCycle];
  state.equityOrders = [{
    order_id: 'order-flat-sell',
    cycle_id: 'cycle-flat-sell',
    symbol: 'SPY',
    side: 'sell',
    status: 'accepted',
    qty: 0.5,
    avg_price: 500,
    notional_usd: 250,
    reason: 'simulated_fill',
    timestamp_ns: String(BigInt(startMs) * 1_000_000n),
  }];

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.equal(dashboard.navSeries[1].tradeMarkers[0].label, 'מכירת לונג');
});

test('portfolio nav series chooses the latest same-day frame by timestamp', () => {
  const state = createOperationsState();
  const startMs = Date.UTC(2026, 0, 1, 14, 30, 0);
  const olderFrame = {
    initial_equity_usd: 10000,
    cash_usd: 8990,
    nav_usd: 10010,
    gross_exposure_usd: 1000,
    daily_pnl_usd: 10,
    max_drawdown_pct: 0.01,
    positions: [{ symbol: 'SPY' }],
    timestamp_ns: String(BigInt(startMs) * 1_000_000n),
  };
  const newerFrame = {
    ...olderFrame,
    cash_usd: 8975,
    nav_usd: 10025,
    daily_pnl_usd: 25,
    timestamp_ns: String(BigInt(startMs + 3_600_000) * 1_000_000n),
  };
  state.equityPortfolio = olderFrame;
  state.equityPortfolioHistory = [newerFrame];

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.deepEqual(dashboard.navSeries.map((point) => point.nav), [10000, 10025]);
  assert.equal(dashboard.navSeries[1].cash, 8975);
});

test('portfolio nav series tolerates missing and invalid timestamps', () => {
  const state = createOperationsState();
  state.equityPortfolio = {
    initial_equity_usd: 10000,
    cash_usd: 9025,
    nav_usd: 10025,
    gross_exposure_usd: 1000,
    daily_pnl_usd: 25,
    max_drawdown_pct: 0,
    positions: [],
    timestamp_ns: '',
    receivedAtMs: Date.UTC(2026, 0, 2, 14, 30, 0),
  };
  state.equityPortfolioHistory = [{
    ...state.equityPortfolio,
  }, {
    ...state.equityPortfolio,
    nav_usd: 10010,
    timestamp_ns: 'not-a-timestamp',
    receivedAtMs: Date.UTC(2026, 0, 1, 14, 30, 0),
  }];

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.equal(dashboard.navSeries.length, 3);
  assert.equal(dashboard.navSeries[1].nav, 10010);
  assert.equal(dashboard.navSeries[1].timestampMs, null);
  assert.equal(dashboard.navSeries[1].receivedAtMs, Date.UTC(2026, 0, 1, 14, 30, 0));
  assert.equal(dashboard.navSeries[2].timestampMs, null);
  assert.equal(dashboard.navSeries[2].receivedAtMs, Date.UTC(2026, 0, 2, 14, 30, 0));
});

test('portfolio nav series keeps non-positive NAV frames', () => {
  const state = createOperationsState();
  const startMs = Date.UTC(2026, 0, 1, 14, 30, 0);
  state.equityPortfolio = {
    initial_equity_usd: 10000,
    cash_usd: -50,
    nav_usd: -25,
    gross_exposure_usd: 25,
    daily_pnl_usd: -10025,
    max_drawdown_pct: 1.0025,
    positions: [{ symbol: 'SPY' }],
    timestamp_ns: String(BigInt(startMs) * 1_000_000n),
  };
  state.equityPortfolioHistory = [state.equityPortfolio];

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.deepEqual(dashboard.navHistory, [10000, -25]);
  assert.equal(dashboard.navSeries.length, 2);
  assert.equal(dashboard.navSeries[1].nav, -25);
  assert.equal(dashboard.navSeries[1].cash, -50);
});

test('pricing gate inference is specific and replay takes precedence', () => {
  const state = createOperationsState();
  state.equityCycle = {
    cycle_id: 'cycle-risk',
    sequence: 8,
    as_of: '2026-05-28T14:30:00Z',
    market_session: 'rth',
    commands: [{
      action: 'buy_notional',
      symbol: 'SPY',
      rationale: 'Requested dynamic symbol lacks a usable release-time price.',
      confidence: 0.7,
      max_notional_usd: 1000,
      evidence_refs: ['ev-1'],
    }],
    evidence: [{
      id: 'ev-1',
      source: 'alpaca_market_data',
      freshness: 'fresh',
      summary: 'ציטוטים טריים.',
      retrieved_at: '2026-05-28T14:29:00Z',
    }],
    risk_assessment: {
      summary: 'Command symbol pricing was unavailable.',
      risk_limit_verdict: 'pass',
    },
    memory_updates: [],
  };
  state.equityCycles = [state.equityCycle];
  state.equityOrders = [{
    order_id: 'order-risk',
    cycle_id: 'cycle-risk',
    symbol: 'SPY',
    side: 'buy',
    status: 'rejected',
    qty: 0,
    avg_price: 500,
    notional_usd: 1000,
    reason: 'market_price_unavailable',
  }];

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.equal(dashboard.currentCycle.blockedGate, 'pricing');

  state.equityOrders[0].reason = 'invalid_symbol_sequence_replay';
  const replayDashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.equal(replayDashboard.currentCycle.blockedGate, 'replay');
});

test('backend rejection reasons map to authority gates', () => {
  const state = createOperationsState();
  state.marketResearchSnapshot = {
    as_of: '2026-05-28T14:30:00Z',
    market_session: 'rth',
    source_freshness: [{ source: 'alpaca_market_data', freshness: 'fresh' }],
    unavailable_data_flags: [],
  };
  state.equityCycle = {
    cycle_id: 'cycle-mismatch',
    sequence: 9,
    as_of: '2026-05-28T14:30:00Z',
    market_session: 'rth',
    commands: [{
      action: 'buy_notional',
      symbol: 'SPY',
      rationale: 'Snapshot and decision times diverged.',
      confidence: 0.7,
      max_notional_usd: 1000,
      evidence_refs: ['ev-1'],
    }],
    evidence: [{
      id: 'ev-1',
      source: 'market_research_snapshot',
      freshness: 'fresh',
      summary: 'ציטוטים טריים.',
      retrieved_at: '2026-05-28T14:29:00Z',
    }],
    risk_assessment: {
      summary: 'מגבלות הסיכון עברו.',
      risk_limit_verdict: 'pass',
    },
    memory_updates: [],
  };
  state.equityCycles = [state.equityCycle];
  state.equityOrders = [{
    order_id: 'order-mismatch',
    cycle_id: 'cycle-mismatch',
    symbol: 'SPY',
    side: 'buy',
    status: 'rejected',
    qty: 0,
    avg_price: 500,
    notional_usd: 1000,
    reason: 'snapshot_decision_time_mismatch',
  }];

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.equal(dashboard.currentCycle.status, 'rejected');
  assert.equal(dashboard.currentCycle.blockedGate, 'freshness');
  assert.equal(dashboard.currentCycle.gates.find((gate) => gate.key === 'freshness').state, 'block');
  assert.match(dashboard.currentCycle.gates.find((gate) => gate.key === 'freshness').detail, /snapshot_decision_time_mismatch/);
});

test('later stale snapshots do not rewrite accepted historical cycle gates', () => {
  const state = createOperationsState();
  state.marketResearchSnapshot = {
    as_of: '2026-05-28T15:30:00Z',
    market_session: 'rth',
    source_freshness: [{ source: 'alpaca_market_data', freshness: 'stale' }],
    unavailable_data_flags: [],
  };
  state.equityCycle = {
    cycle_id: 'cycle-filled',
    sequence: 10,
    as_of: '2026-05-28T14:30:00Z',
    market_session: 'rth',
    commands: [{
      action: 'buy_notional',
      symbol: 'SPY',
      rationale: 'ראיות טריות תמכו בכניסה שמולאה.',
      confidence: 0.7,
      max_notional_usd: 1000,
      evidence_refs: ['ev-1'],
    }],
    evidence: [{
      id: 'ev-1',
      source: 'market_research_snapshot',
      freshness: 'fresh',
      summary: 'snapshot טרי של ציטוט ו-bar.',
      retrieved_at: '2026-05-28T14:30:00Z',
    }],
    risk_assessment: {
      summary: 'בתוך מגבלות הפקודה ורזרבת המזומן.',
      risk_limit_verdict: 'pass',
    },
    memory_updates: [],
  };
  state.equityCycles = [state.equityCycle];
  state.equityOrders = [{
    order_id: 'cycle-filled-1',
    cycle_id: 'cycle-filled',
    symbol: 'SPY',
    side: 'buy',
    status: 'accepted',
    qty: 2,
    avg_price: 500,
    notional_usd: 1000,
    reason: 'simulated_fill',
  }];

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.equal(dashboard.currentCycle.status, 'accepted');
  assert.equal(dashboard.currentCycle.blockedGate, null);
  assert.equal(dashboard.currentCycle.gates.find((gate) => gate.key === 'freshness').state, 'pass');
});

test('accepted simulated fill overrides stale advisory evidence gate', () => {
  const state = createOperationsState();
  state.marketResearchSnapshot = {
    as_of: '2026-06-22T15:45:00Z',
    market_session: 'rth',
    source_freshness: [{ source: 'alpaca_market_data', freshness: 'fresh' }],
    unavailable_data_flags: [],
  };
  state.equityCycle = {
    cycle_id: 'cycle-filled-stale-evidence',
    sequence: 11,
    as_of: '2026-06-22T15:00:00Z',
    market_session: 'rth',
    commands: [{
      action: 'sell_notional',
      symbol: 'SOXL',
      rationale: 'Trim exposure while one advisory evidence row was stale.',
      confidence: 0.6,
      max_notional_usd: 150,
      evidence_refs: ['market_data_soxl'],
    }],
    evidence: [{
      id: 'market_data_soxl',
      source: 'alpaca_market_data',
      freshness: 'stale',
      summary: 'Advisory evidence warned that market data was stale.',
      retrieved_at: '2026-06-22T14:45:00Z',
    }],
    risk_assessment: {
      summary: 'Rust may block acceptance if freshness fails.',
      risk_limit_verdict: 'pass',
    },
    memory_updates: [],
  };
  state.equityCycles = [state.equityCycle];
  state.equityOrders = [{
    order_id: 'cycle-filled-stale-evidence-1',
    cycle_id: 'cycle-filled-stale-evidence',
    symbol: 'SOXL',
    side: 'sell',
    status: 'accepted',
    qty: 0.5,
    avg_price: 294.61,
    notional_usd: 150,
    reason: 'simulated_fill',
  }];

  const dashboard = createDashboardViewModel({
    operationsState: state,
    connectionStatus: 'connected',
    nowMs: 1000,
  });

  assert.equal(dashboard.currentCycle.status, 'accepted');
  assert.equal(dashboard.currentCycle.blockedGate, null);
  assert.equal(dashboard.currentCycle.gates.find((gate) => gate.key === 'freshness').state, 'pass');
});

test('gatePath marks later gates unevaluated after a block', () => {
  const gates = gatePath('pricing');

  assert.equal(gates.find((gate) => gate.key === 'pricing').state, 'block');
  assert.equal(gates.find((gate) => gate.key === 'replay').state, 'na');
});

test('fully invested policy distinguishes successful allocation from pending and blocked states', () => {
  for (const [status, expected] of [['fully_invested', 'good'], ['allocation_pending', 'warn'], ['allocation_waiting_market', 'warn'], ['allocation_blocked', 'bad']]) {
    const state = createOperationsState();
    state.serviceStatus = { status, detail: 'Allocation reason', receivedAtMs: 1000 };
    const dashboard = createDashboardViewModel({ operationsState: state, connectionStatus: 'connected', nowMs: 1500 });
    assert.equal(dashboard.service.state, expected);
    assert.equal(dashboard.service.detail, 'Allocation reason');
  }
});


test('fully invested decisions retain model rationale and all target weights', () => {
  const state = createOperationsState();
  const holdings = Array.from({ length: 20 }, (_, i) => ({ symbol: `STK${i}`, weight_bps: 500 }));
  state.equityCycle = {
    cycle_id: 'allocation-42', sequence: 42, execution_policy: 'fully_invested_v1',
    commands: [], target_portfolio: { holdings },
    risk_assessment: { summary: 'Model selected complete diversified portfolio', risk_limit_verdict: 'pass' },
  };
  const dashboard = createDashboardViewModel({ operationsState: state, connectionStatus: 'connected', nowMs: 1000 });
  assert.equal(dashboard.latestReasoning.thesis, state.equityCycle.risk_assessment.summary);
  assert.deepEqual(dashboard.latestReasoning.allocation, holdings);
  const cycle = dashboard.cycles.find((row) => row.id === 'allocation-42');
  assert.deepEqual(cycle.allocation, holdings);
  assert.equal(cycle.thesis, state.equityCycle.risk_assessment.summary);
});

test('rotation summaries preserve sell then buy actions', () => {
  const state = createOperationsState();
  state.cycleSummaries = [{ cycle_id: 'rotation', cycle_kind: 'run_once', status: 'accepted',
    symbols: ['OLD', 'NEW'], actions: ['sell', 'buy'], accepted_notional_usd: 1000,
    rationale: 'Complete replacement', as_of: '2026-10-22T14:00:00Z' }];
  const dashboard = createDashboardViewModel({ operationsState: state, connectionStatus: 'connected', nowMs: 1000 });
  assert.deepEqual(dashboard.cycles[0].commands.map((row) => row.action), ['sell_notional', 'buy_notional']);
});
