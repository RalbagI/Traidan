import { isServiceActivityStatus } from './operationsState.js';

export const GATE_SEQUENCE = Object.freeze([
  { key: 'schema', label: 'סכמה', sub: 'בדיקה', icon: 'doc' },
  { key: 'freshness', label: 'טריות', sub: 'SLA נתונים', icon: 'wave' },
  { key: 'session', label: 'מסחר', sub: 'RTH בלבד', icon: 'clock' },
  { key: 'pricing', label: 'תמחור', sub: 'ציטוט סימבול', icon: 'list' },
  { key: 'replay', label: 'בדיקת Replay', sub: 'רצף', icon: 'loop' },
]);

const NUMBER_FORMAT = new Intl.NumberFormat('en-US', {
  maximumFractionDigits: 4,
});

const MONEY_FORMAT = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  maximumFractionDigits: 2,
});

const MONEY_ZERO_FORMAT = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  maximumFractionDigits: 0,
});

const PCT_FORMAT = new Intl.NumberFormat('en-US', {
  style: 'percent',
  maximumFractionDigits: 2,
});

export const SESSION_LABELS = Object.freeze({
  rth: 'מסחר רציף',
  premarket: 'טרום מסחר',
  after_hours: 'אחרי המסחר',
  closed: 'סגור',
});

const ACTION_LABELS = Object.freeze({
  buy_notional: 'קנייה',
  sell_notional: 'מכירה',
  hold: 'החזקה',
  close_position: 'סגירה',
  set_target_weight: 'יעד',
});

export const CYCLE_STATUS_LABELS = Object.freeze({
  accepted: 'אושר',
  held: 'הוחזק',
  rejected: 'נדחה',
  reviewing: 'בבדיקה',
});

export const FRESHNESS_LABELS = Object.freeze({
  fresh: 'טרי',
  stale: 'מיושן',
  unavailable: 'לא זמין',
  waiting: 'בהמתנה',
});

export const ORDER_SIDE_LABELS = Object.freeze({
  buy: 'קנייה',
  sell: 'מכירה',
});

export const ORDER_STATUS_LABELS = Object.freeze({
  accepted: 'התקבל',
  rejected: 'נדחה',
  pending: 'בהמתנה',
});

export const RISK_VERDICT_LABELS = Object.freeze({
  within_limits: 'בתוך המגבלות',
  reduced: 'הוקטן',
  blocked: 'חסום',
  not_evaluated: 'לא נבדק',
});

export function createDashboardViewModel({
  operationsState,
  connectionStatus = 'connecting',
  adminApiStatus = 'token_required',
  reconnectDelayMs = null,
  streamGeneration = null,
  nowMs = Date.now(),
}) {
  const state = operationsState ?? {};
  const currentMarketResearchSnapshot = state.serviceStatus?.has_market_research_snapshot === false
    ? null
    : state.marketResearchSnapshot;
  const orderRows = createOrderRows(state.equityOrders ?? []);
  const rawCycles = state.equityCycles?.length ? state.equityCycles : state.equityCycle ? [state.equityCycle] : [];
  const summaryCycles = createCycleSummaryRows(state.cycleSummaries ?? [], nowMs);
  const serviceActivities = serviceActivityFrames(state);
  const tradeCycles = createCycleRows({
    cycles: rawCycles,
    orders: orderRows,
    snapshot: state.marketResearchSnapshot,
    serviceStatus: state.serviceStatus,
    nowMs,
  });
  const cycles = (summaryCycles.length > 0
    ? summaryCycles
    : [
      ...tradeCycles,
      ...createServiceActivityRows(serviceActivities, nowMs),
    ])
    .sort((left, right) => right.asOf - left.asOf)
    .slice(0, 20)
    .map((cycle, index) => ({ ...cycle, fresh: index === 0 }));
  const decisionCycles = (tradeCycles.length > 0 ? tradeCycles : cycles)
    .sort((left, right) => right.asOf - left.asOf)
    .map((cycle, index) => ({ ...cycle, fresh: index === 0 }));
  const currentCycle = decisionCycles[0] ?? null;
  const service = createServiceState(
    state.serviceStatus,
    connectionStatus,
    adminApiStatus,
    nowMs,
    currentCycle ?? cycles[0],
  );
  const portfolio = createPortfolioState(state.equityPortfolio);
  const freshness = createFreshnessRows(currentMarketResearchSnapshot);
  const memory = createMemoryRows(state.llmMemoryUpdates ?? []);
  const events = createEventRows(state.operatorEvents ?? []);
  const health = createHealthState({
    state,
    service,
    connectionStatus,
    adminApiStatus,
    reconnectDelayMs,
  });
  const panels = createPanelStates({
    state,
    health,
    decisionCycles,
    cycles,
    orders: orderRows,
    memory,
    events,
    streamGeneration,
    marketResearchSnapshot: currentMarketResearchSnapshot,
    activityFrames: [...rawCycles, ...(state.cycleSummaries ?? []), ...serviceActivities],
    researchOnly: isServiceActivityStatus(state.serviceStatus?.status)
      && !currentMarketResearchSnapshot,
  });
  const latestReasoning = createLatestReasoning(
    state.equityCycle,
    currentMarketResearchSnapshot,
    state.llmMemoryUpdates ?? [],
  );
  const navSeries = createNavSeries(
    state.equityPortfolioHistory ?? [],
    state.equityPortfolio,
    portfolio,
    { cycles: rawCycles, orders: orderRows },
  );

  return {
    health,
    panels,
    service,
    portfolio,
    sweep: createSweepSummary({
      service,
      cycle: state.equityCycle,
      orders: orderRows,
      portfolio,
    }),
    latestReasoning,
    currentCycle,
    cycles,
    orders: orderRows,
    freshness,
    memory,
    events,
    navHistory: createNavHistory(navSeries, portfolio),
    navSeries,
    hasLiveData: Boolean(state.equityCycle || state.equityPortfolio || state.serviceStatus),
  };
}

function createHealthState({
  state,
  service,
  connectionStatus,
  adminApiStatus,
  reconnectDelayMs,
}) {
  const streamState = connectionStatus === 'connected'
    ? 'good'
    : connectionStatus === 'disconnected'
      ? 'bad'
      : 'warn';
  const accessState = adminApiStatus === 'available'
    ? 'good'
    : adminApiStatus === 'unauthorized'
      ? 'bad'
      : 'warn';
  const severity = streamState === 'bad' || service.state === 'bad'
    ? 'critical'
    : streamState === 'warn' || service.state === 'warn' || accessState !== 'good'
      ? 'warning'
      : 'ok';
  let incident = null;
  if (connectionStatus === 'disconnected') {
    incident = {
      kind: 'stream_disconnected',
      tone: 'critical',
      title: 'זרם הנתונים מנותק',
      detail: service.detail,
    };
  } else if (service.state === 'bad') {
    incident = {
      kind: 'service_failed',
      tone: 'critical',
      title: 'שירות ההחלטות דורש תשומת לב',
      detail: service.detail,
    };
  } else if (connectionStatus === 'connecting') {
    incident = {
      kind: 'stream_connecting',
      tone: 'warning',
      title: 'מתחבר לזרם הנתונים',
      detail: 'הנתונים יוצגו לאחר יצירת חיבור מאומת לשרת.',
    };
  } else if (adminApiStatus === 'unauthorized') {
    incident = {
      kind: 'admin_unauthorized',
      tone: 'warning',
      title: 'גישת Admin API נדחתה',
      detail: 'הדשבורד ממשיך במצב קריאה; יש לבדוק את טוקן המפעיל.',
    };
  }

  return {
    severity,
    streamState,
    serviceState: service.state,
    accessState,
    connectionStatus,
    adminApiStatus,
    retryInMs: Number.isFinite(reconnectDelayMs) ? reconnectDelayMs : null,
    lastUpdateAtMs: latestReceiptMs(state),
    incident,
  };
}

function createPanelStates({
  state,
  health,
  decisionCycles,
  cycles,
  orders,
  memory,
  events,
  streamGeneration,
  marketResearchSnapshot,
  activityFrames,
  researchOnly,
}) {
  const serviceKnown = Boolean(state.serviceStatus);
  const cycleFrames = activityFrames ?? [];
  const hasDecisionCycle = (decisionCycles ?? []).length > 0;
  return {
    decision: panelState(hasDecisionCycle, serviceKnown && !hasDecisionCycle, health, latestReceiptMs(cycleFrames), streamValueIsCurrent(cycleFrames, streamGeneration)),
    cycles: panelState(cycles.length > 0, serviceKnown && cycles.length === 0, health, latestReceiptMs(cycleFrames), streamValueIsCurrent(cycleFrames, streamGeneration)),
    portfolio: panelState(Boolean(state.equityPortfolio), false, health, state.equityPortfolio?.receivedAtMs, streamValueIsCurrent(state.equityPortfolio, streamGeneration)),
    freshness: panelState(Boolean(marketResearchSnapshot), researchOnly, health, marketResearchSnapshot?.receivedAtMs ?? latestReceiptMs(cycleFrames), streamValueIsCurrent(marketResearchSnapshot, streamGeneration)),
    orders: panelState(orders.length > 0, cycles.length > 0 || serviceKnown, health, latestReceiptMs(state.equityOrders), streamValueIsCurrent(state.equityOrders, streamGeneration)),
    memory: panelState(memory.length > 0, cycles.length > 0 || serviceKnown, health, latestReceiptMs(state.llmMemoryUpdates), streamValueIsCurrent(state.llmMemoryUpdates, streamGeneration)),
    events: panelState(events.length > 0, serviceKnown, health, latestReceiptMs(state.operatorEvents), streamValueIsCurrent(state.operatorEvents, streamGeneration)),
  };
}

function panelState(hasData, knownEmpty, health, updatedAtMs, currentStreamData = true) {
  let status = 'loading';
  if (hasData) {
    status = health.severity === 'critical'
      || health.connectionStatus !== 'connected'
      || !currentStreamData
      ? 'stale'
      : 'ready';
  } else if (health.connectionStatus === 'disconnected') {
    status = 'error';
  } else if (knownEmpty) {
    status = 'empty';
  }
  return {
    status,
    updatedAtMs: Number.isFinite(updatedAtMs) ? updatedAtMs : null,
  };
}

function streamValueIsCurrent(value, streamGeneration) {
  if (!Number.isInteger(streamGeneration)) return true;
  const rows = Array.isArray(value) ? value : [value];
  return rows.some((row) => row?.streamGeneration === streamGeneration);
}

function latestReceiptMs(value) {
  const frames = Array.isArray(value)
    ? value
    : value && typeof value === 'object'
      ? Object.values(value).flatMap((item) => (Array.isArray(item) ? item : [item]))
      : [];
  const values = frames
    .map((frame) => frame?.receivedAtMs)
    .filter(Number.isFinite);
  return values.length > 0 ? Math.max(...values) : null;
}

function createServiceState(status, connectionStatus, adminApiStatus, nowMs, currentCycle) {
  if (connectionStatus === 'disconnected') {
    return {
      label: 'לא זמין',
      state: 'bad',
      detail: 'זרם הנתונים מהשרת מנותק.',
      cycleId: status?.last_cycle_id ?? currentCycle?.id ?? 'none',
      ageLabel: 'לא מקוון',
      session: status?.market_session ?? currentCycle?.session ?? 'closed',
      adminApiStatus,
    };
  }

  if (!status) {
    return {
      label: connectionStatus === 'connected' ? 'ממתין' : 'מתחבר',
      state: connectionStatus === 'connected' ? 'warn' : 'idle',
      detail: connectionStatus === 'connected'
        ? 'זרם הנתונים מחובר; עדיין לא התקבל סטטוס שירות.'
        : 'פותח זרם נתונים מהשרת.',
      cycleId: currentCycle?.id ?? 'none',
      ageLabel: 'בהמתנה',
      session: currentCycle?.session ?? 'closed',
      adminApiStatus,
    };
  }

  const ageMs = Number.isFinite(status.receivedAtMs) ? Math.max(0, nowMs - status.receivedAtMs) : 0;
  const isRejected = status.status === 'cycle_rejected';
  const isFailed = [
    'llm_review_failed',
    'state_recovery_failed',
    'state_persistence_failed',
    'allocation_blocked',
  ].includes(status.status);
  const isReviewing = status.status === 'llm_review_in_progress';
  const isWaiting = ['waiting_for_hourly_cycle', 'allocation_pending', 'allocation_required', 'allocation_waiting_market'].includes(status.status);
  const label = status.status === 'fully_invested' ? 'מושקע במלואו' : serviceLabel({ isRejected, isFailed, isReviewing, isWaiting });
  return {
    label,
    state: isRejected || isFailed ? 'bad' : isWaiting || isReviewing ? 'warn' : 'good',
    detail: status.detail,
    cycleId: status.last_cycle_id ?? currentCycle?.id ?? 'none',
    ageLabel: formatAge(ageMs),
    session: status.market_session ?? currentCycle?.session ?? 'closed',
    adminApiStatus,
  };
}

function serviceLabel({ isRejected, isFailed, isReviewing, isWaiting }) {
  if (isRejected) return 'נדחה';
  if (isFailed) return 'נכשל';
  if (isReviewing) return 'בבדיקה';
  if (isWaiting) return 'ממתין';
  return 'זמין';
}

function createPortfolioState(portfolio) {
  const fallback = {
    available: false,
    initialEquity: null,
    nav: null,
    cash: null,
    gross: null,
    dailyPnl: null,
    totalPnl: null,
    realizedPnl: null,
    unrealizedPnl: null,
    maxDrawdownPct: null,
    navLabel: '—',
    cashLabel: '—',
    exposureLabel: '—',
    pnlLabel: '—',
    dailyPnlLabel: '—',
    realizedPnlLabel: '—',
    unrealizedPnlLabel: '—',
    drawdownLabel: '—',
    positions: [],
  };
  if (!portfolio) return fallback;

  const initialEquity = Number.isFinite(portfolio.initial_equity_usd)
    ? normalizedFinite(portfolio.initial_equity_usd)
    : 10000;
  const nav = normalizedFinite(portfolio.nav_usd);
  const totalPnl = Number.isFinite(portfolio.total_pnl_usd)
    ? normalizedFinite(portfolio.total_pnl_usd)
    : nav - initialEquity;
  const dailyPnl = normalizedFinite(portfolio.daily_pnl_usd);
  const realizedPnl = normalizedFinite(portfolio.realized_pnl_usd);
  const unrealizedPnl = normalizedFinite(portfolio.unrealized_pnl_usd);
  const positions = [...(portfolio.positions ?? [])]
    .map((position) => createPosition(position, nav))
    .sort((left, right) => Math.abs(right.marketValue) - Math.abs(left.marketValue));

  return {
    available: true,
    initialEquity,
    nav,
    cash: normalizedFinite(portfolio.cash_usd),
    gross: normalizedFinite(portfolio.gross_exposure_usd),
    dailyPnl,
    totalPnl,
    realizedPnl,
    unrealizedPnl,
    maxDrawdownPct: normalizedFinite(portfolio.max_drawdown_pct),
    navLabel: money(portfolio.nav_usd),
    cashLabel: money(portfolio.cash_usd),
    exposureLabel: money(portfolio.gross_exposure_usd),
    pnlLabel: money(totalPnl),
    dailyPnlLabel: money(dailyPnl),
    realizedPnlLabel: money(realizedPnl),
    unrealizedPnlLabel: money(unrealizedPnl),
    drawdownLabel: percent(portfolio.max_drawdown_pct),
    benchmark: {
      symbol: portfolio.benchmark_symbol ?? 'SPY',
      label: portfolio.benchmark_label ?? 'S&P500',
      status: portfolio.benchmark_status ?? (
        Number.isFinite(portfolio.benchmark_price_usd) ? 'live' : 'unavailable'
      ),
      price: Number.isFinite(portfolio.benchmark_price_usd)
        ? portfolio.benchmark_price_usd
        : null,
    },
    positions,
  };
}

function createPosition(position, nav) {
  const quantity = normalizedFinite(position.quantity);
  const marketValue = normalizedFinite(position.market_value_usd);
  const avgPrice = normalizedFinite(position.avg_price);
  const price = Number.isFinite(position.current_price_usd)
    ? normalizedFinite(position.current_price_usd)
    : quantity === 0 ? avgPrice : marketValue / quantity;
  const costBasis = Number.isFinite(position.cost_basis_usd)
    ? normalizedFinite(position.cost_basis_usd)
    : quantity * avgPrice;
  const unrealizedPnl = Number.isFinite(position.unrealized_pnl_usd)
    ? normalizedFinite(position.unrealized_pnl_usd)
    : marketValue - costBasis;
  const positionPnlPct = Number.isFinite(position.unrealized_pnl_pct)
    ? normalizedFinite(position.unrealized_pnl_pct)
    : costBasis !== 0 ? unrealizedPnl / Math.abs(costBasis) : 0;
  const weightPct = normalizedWeightPct(position.weight_pct, marketValue, nav);
  return {
    symbol: position.symbol,
    quantity,
    qty: quantity,
    avgPrice,
    price,
    currentPrice: price,
    costBasis,
    unrealizedPnl,
    marketValue,
    market_value_usd: marketValue,
    weightPct,
    weight_pct: weightPct,
    dayPnlPct: positionPnlPct,
    pnlPct: positionPnlPct,
    marketValueLabel: money(marketValue),
    costBasisLabel: money(costBasis),
    unrealizedPnlLabel: money(unrealizedPnl),
    currentPriceLabel: money(price),
  };
}

function createCycleSummaryRows(summaries, nowMs) {
  return (summaries ?? []).map((summary, index) => {
    const actions = Array.isArray(summary.actions) ? summary.actions : [];
    const symbols = Array.isArray(summary.symbols) ? summary.symbols : [];
    const primaryAction = actions[0] === 'sell' ? 'sell_notional' : 'buy_notional';
    const status = summary.status === 'accepted' ? 'accepted' : 'rejected';
    const blockedGate = status === 'accepted'
      ? null
      : gateForRejectionReason(summary.reason) ?? 'pricing';
    const gateDetails = blockedGate ? { [blockedGate]: summary.reason } : {};
    const actualNotional = normalizedFinite(summary.accepted_notional_usd);
    const requestedNotional = normalizedFinite(summary.requested_notional_usd);
    const requiredNotional = normalizedFinite(summary.required_notional_usd);
    return {
      id: summary.cycle_id,
      seq: summaries.length - index,
      asOf: timestampToMs(summary.as_of, summary.timestampMs, summary.receivedAtMs, nowMs),
      session: summary.market_session ?? 'rth',
      sessionLabel: SESSION_LABELS[summary.market_session] ?? summary.market_session ?? 'מסחר רציף',
      status,
      statusLabel: CYCLE_STATUS_LABELS[status] ?? status,
      thesis: summary.rationale,
      model: 'GPT-6 Astra xhigh',
      commands: symbols.map((symbol, commandIndex) => ({
        symbol,
        action: actions[commandIndex] === 'buy' ? 'buy_notional' : actions[commandIndex] === 'sell' ? 'sell_notional' : primaryAction,
        actionLabel: ACTION_LABELS[actions[commandIndex] === 'buy' ? 'buy_notional' : actions[commandIndex] === 'sell' ? 'sell_notional' : primaryAction],
        actionClass: actionClass(actions[commandIndex] === 'buy' ? 'buy_notional' : actions[commandIndex] === 'sell' ? 'sell_notional' : primaryAction),
        confidence: 0,
        maxNotional: requestedNotional,
        maxNotionalLabel: moneyZero(requestedNotional),
        rationale: summary.rationale,
        refs: [],
        verdict: summary.status,
      })),
      evidence: [],
      risk: {
        summary: summary.reason,
        verdict: status === 'accepted' ? 'within_limits' : 'blocked',
        verdictLabel: status === 'accepted'
          ? RISK_VERDICT_LABELS.within_limits
          : RISK_VERDICT_LABELS.blocked,
        notes: summary.mandatory_trade_status,
      },
      blockedGate,
      gates: gatePath(blockedGate, gateDetails),
      orders: [],
      memory: {
        conclusion: summary.rationale,
        watch: [],
      },
      actualNotional,
      requestedNotional,
      requiredNotional,
      actualNotionalLabel: money(actualNotional),
      requestedNotionalLabel: money(requestedNotional),
      requiredNotionalLabel: money(requiredNotional),
      mandatoryTradeStatus: summary.mandatory_trade_status,
      reason: summary.reason,
      cycleKind: summary.cycle_kind,
      summaryOnly: true,
      fresh: false,
    };
  });
}

function createCycleRows({
  cycles,
  orders,
  snapshot,
  serviceStatus,
  nowMs,
}) {
  return cycles.map((cycle, index) => {
    const cycleOrders = orders.filter((order) => order.cycleId === cycle.cycle_id);
    const authorityBlock = inferAuthorityBlock(cycle, cycleOrders, snapshot);
    const blockedGate = authorityBlock?.gate ?? null;
    const gateDetails = blockedGate ? { [blockedGate]: authorityBlock.detail } : {};
    const status = inferCycleStatus(cycle, cycleOrders, serviceStatus);
    return {
      id: cycle.cycle_id,
      seq: Number.isFinite(cycle.sequence) ? cycle.sequence : cycles.length - index,
      asOf: timestampToMs(cycle.as_of, cycle.timestampMs, cycle.receivedAtMs, nowMs),
      session: cycle.market_session ?? snapshot?.market_session ?? 'closed',
      sessionLabel: SESSION_LABELS[cycle.market_session] ?? cycle.market_session ?? 'סגור',
      status,
      statusLabel: CYCLE_STATUS_LABELS[status] ?? status,
      thesis: cycleThesis(cycle),
      allocation: cycle.target_portfolio?.holdings ?? [],
      model: 'GPT-6 Astra xhigh',
      commands: (cycle.commands ?? []).map(createCommand),
      evidence: (cycle.evidence ?? []).map(createEvidence),
      risk: createRisk(cycle.risk_assessment, blockedGate),
      blockedGate,
      gates: gatePath(blockedGate, gateDetails),
      orders: cycleOrders,
      memory: createCycleMemory(cycle, serviceStatus),
      fresh: index === 0,
    };
  });
}

function serviceActivityFrames(state) {
  if (state.serviceActivities?.length) return state.serviceActivities;
  return isServiceActivityStatus(state.serviceStatus?.status) ? [state.serviceStatus] : [];
}

function createServiceActivityRows(activities, nowMs) {
  return activities.map((activity, index) => {
    const repeatCount = Math.max(1, Number(activity.repeatCount) || 1);
    return {
      id: `service-${activity.timestamp_ns ?? activity.receivedAtMs ?? index}`,
      seq: 'מחקר',
      asOf: timestampToMs(null, activity.timestampMs, activity.receivedAtMs, nowMs),
      session: activity.market_session ?? 'closed',
      sessionLabel: SESSION_LABELS[activity.market_session] ?? activity.market_session ?? 'סגור',
      status: 'held',
      statusLabel: 'מחקר בלבד',
      thesis: repeatCount > 1
        ? `${activity.detail} אותה הזדמנות נבדקה ${repeatCount} פעמים.`
        : activity.detail,
      model: 'ללא קריאת LLM',
      commands: [],
      evidence: [],
      risk: {
        summary: 'מסלול סמכות Rust לא הופעל במחזור מחקר בלבד.',
        verdict: 'not_evaluated',
        verdictLabel: RISK_VERDICT_LABELS.not_evaluated,
        notes: '',
      },
      blockedGate: null,
      gates: GATE_SEQUENCE.map((gate) => ({ ...gate, state: 'na', detail: 'לא נבדק' })),
      orders: [],
      memory: {
        conclusion: activity.detail,
        watch: [],
      },
      serviceActivity: true,
      researchOnly: true,
      activityStatus: activity.status,
      repeatCount,
      fresh: false,
    };
  });
}

function createCommand(command) {
  return {
    symbol: command.symbol,
    action: command.action,
    actionLabel: ACTION_LABELS[command.action] ?? command.action,
    actionClass: actionClass(command.action),
    confidence: normalizedFinite(command.confidence),
    maxNotional: normalizedFinite(command.max_notional_usd),
    maxNotionalLabel: moneyZero(command.max_notional_usd),
    rationale: command.rationale,
    refs: command.evidence_refs ?? [],
    verdict: command.verdict ?? 'pending',
  };
}

function createEvidence(evidence) {
  return {
    id: evidence.id,
    source: evidence.source,
    freshness: evidence.freshness,
    freshnessLabel: FRESHNESS_LABELS[evidence.freshness] ?? evidence.freshness,
    summary: evidence.summary,
    retrievedAt: evidence.retrieved_at ?? 'unknown',
  };
}

function createRisk(riskAssessment, blockedGate) {
  const rawVerdict = String(riskAssessment?.risk_limit_verdict ?? '').toLowerCase();
  let verdict = 'within_limits';
  if (blockedGate === 'risk' || /block|reject|fail|breach/.test(rawVerdict)) {
    verdict = 'blocked';
  } else if (/reduce|trim|downsize/.test(rawVerdict)) {
    verdict = 'reduced';
  }
  return {
    summary: riskAssessment?.summary ?? 'עדיין אין הערכת סיכון פעילה.',
    verdict,
    verdictLabel: RISK_VERDICT_LABELS[verdict] ?? verdict,
    notes: riskAssessment?.max_loss_scenario ?? '',
  };
}

function createCycleMemory(cycle, serviceStatus) {
  const update = cycle.memory_updates?.find((item) => item?.conclusion);
  return {
    conclusion: update?.conclusion
      ?? serviceStatus?.detail
      ?? 'לא נרשמה מסקנת זיכרון במחזור הזה.',
    watch: update?.next_cycle_watch_items ?? [],
  };
}

function inferCycleStatus(cycle, orders, serviceStatus) {
  if (serviceStatus?.last_cycle_id === cycle.cycle_id && serviceStatus.status === 'llm_review_in_progress') {
    return 'reviewing';
  }
  if (orders.some((order) => order.status === 'accepted')) return 'accepted';
  if (orders.some((order) => order.status === 'rejected')) return 'rejected';
  if (serviceStatus?.last_cycle_id === cycle.cycle_id && serviceStatus.status === 'cycle_rejected') {
    return 'rejected';
  }
  const riskVerdict = String(cycle.risk_assessment?.risk_limit_verdict ?? '').toLowerCase();
  if (/block|reject|fail|breach/.test(riskVerdict)) return 'rejected';
  if ((cycle.commands ?? []).length === 0) return 'held';
  return 'held';
}

function inferAuthorityBlock(cycle, orders, snapshot) {
  if (orders.some((order) => order.status === 'accepted')) return null;

  const rejectedReason = orders.find((order) => order.status === 'rejected')?.reason ?? '';
  const reasonGate = gateForRejectionReason(rejectedReason);
  if (reasonGate) return { gate: reasonGate, detail: rejectedReason };
  if (rejectedReason) return { gate: 'risk', detail: `סמכות Rust דחתה: ${rejectedReason}` };

  if (cycleHasStaleEvidence(cycle)) {
    return { gate: 'freshness', detail: 'ראיות המחזור מתחת ל-SLA הטריות' };
  }
  if (hasMatchingStaleSnapshot(cycle, snapshot)) {
    return { gate: 'freshness', detail: 'נתוני השוק מתחת ל-SLA הטריות' };
  }

  const riskVerdict = String(cycle.risk_assessment?.risk_limit_verdict ?? '').toLowerCase();
  if (/block|reject|fail|breach/.test(riskVerdict)) {
    return { gate: 'risk', detail: cycle.risk_assessment?.risk_limit_verdict ?? 'סיכון נדחה' };
  }
  return null;
}

function gateForRejectionReason(rawReason) {
  const reason = String(rawReason ?? '').toLowerCase();
  if (!reason) return null;
  if (/schema|contract|malformed|forbidden|json/.test(reason)) return 'schema';
  if (/(^|[^a-z])(session|rth|after_hours|after-hours|afterhours|premarket|pre_market|market_closed|market-closed|orders_allowed|orders-allowed|regular_session)([^a-z]|$)/.test(reason)) return 'session';
  if (/stale|freshness|snapshot_decision_time_mismatch|nav_unavailable|data_unavailable/.test(reason)) return 'freshness';
  if (/sequence_replay|replay/.test(reason)) return 'replay';
  if (/market_price_unavailable|invalid_symbol|symbol_price_unavailable|price_unavailable|no_position_delta/.test(reason)) return 'pricing';
  return null;
}

function cycleHasStaleEvidence(cycle) {
  return (cycle.evidence ?? []).some((row) => row.freshness && row.freshness !== 'fresh');
}

function hasMatchingStaleSnapshot(cycle, snapshot) {
  if (!cycle?.as_of || !snapshot?.as_of || cycle.as_of !== snapshot.as_of) return false;
  return hasStaleSnapshot(snapshot);
}

function hasStaleSnapshot(snapshot) {
  if (!snapshot) return false;
  if ((snapshot.unavailable_data_flags ?? []).length > 0) return true;
  return (snapshot.source_freshness ?? []).some((row) => row.freshness !== 'fresh');
}

export function gatePath(blockedGate, details = {}) {
  let blocked = false;
  return GATE_SEQUENCE.map((gate) => {
    if (blocked) return { ...gate, state: 'na', detail: 'לא נבדק' };
    if (gate.key === blockedGate) {
      blocked = true;
      return { ...gate, state: 'block', detail: details[gate.key] ?? defaultGateDetail(gate.key) };
    }
    return { ...gate, state: 'pass', detail: details[gate.key] ?? 'עבר' };
  });
}

function defaultGateDetail(gateKey) {
  if (gateKey === 'schema') return 'החלטה נכשלה באימות סכמה או חוזה';
  if (gateKey === 'freshness') return 'נתוני השוק מתחת ל-SLA הטריות';
  if (gateKey === 'session') return 'פקודות מותרות רק בזמן RTH';
  if (gateKey === 'pricing') return 'לסימבול אין מחיר שמיש בזמן השחרור';
  if (gateKey === 'replay') return 'החלטת Replay נדחתה';
  return 'חסום';
}

function createSweepSummary({
  service,
  cycle,
  orders,
  portfolio,
}) {
  if (!cycle) {
    return {
      state: service.state,
      lastSweep: 'אין סריקה שהושלמה',
      decision: service.label,
      orderEffect: 'ממתין להחלטת LLM ראשונה.',
      portfolioEffect: 'עדיין אין מילוי סימולציה שהתקבל.',
    };
  }

  if (!portfolio.available) {
    return {
      state: service.state,
      lastSweep: cycle.cycle_id,
      decision: 'ממתין לתמונת תיק',
      orderEffect: 'החלטת המחזור התקבלה; תמונת התיק עדיין לא זמינה.',
      portfolioEffect: 'לא התקבל frame אמיתי של מצב התיק.',
    };
  }

  const commands = cycle.commands ?? [];
  const orderRows = orders.filter((order) => order.cycleId === cycle.cycle_id);
  const acceptedOrders = orderRows.filter((order) => order.status === 'accepted');
  const rejectedOrders = orderRows.filter((order) => order.status === 'rejected');
  const primaryCommand = commands[0];
  const primaryAction = primaryCommand?.action ?? 'none';
  const primaryActionLabel = ACTION_LABELS[primaryAction] ?? primaryAction;
  const primarySymbol = primaryCommand?.symbol ?? 'ללא סימבול';
  const primaryNotional = money(primaryCommand?.max_notional_usd ?? 0);
  const portfolioState = portfolio.positions.length === 0
    ? 'מזומן בלבד'
    : `${portfolio.positions.length} פוזיציות`;

  let orderEffect = 'לא התקבלה תוצאת פקודת סימולציה.';
  if (acceptedOrders.length > 0) {
    orderEffect = `${acceptedOrders.length} פקודות סימולציה התקבלו.`;
  } else if (rejectedOrders.length > 0) {
    orderEffect = `${rejectedOrders.length} פקודות סימולציה נדחו.`;
  }

  return {
    state: acceptedOrders.length > 0 ? 'good' : rejectedOrders.length > 0 ? 'bad' : 'warn',
    lastSweep: cycle.cycle_id,
    decision: `${primarySymbol} ${primaryActionLabel} ${primaryNotional}`,
    orderEffect,
    portfolioEffect: `${portfolioState} - חשיפה ברוטו ${portfolio.exposureLabel} - P&L ${portfolio.pnlLabel}`,
  };
}

function cycleThesis(cycle) {
  return cycle.execution_policy === 'fully_invested_v1'
    ? cycle.risk_assessment?.summary ?? 'הקצאת מלוא התיק לאחזקות שנבחרו.'
    : firstCommandRationale(cycle.commands);
}

function createLatestReasoning(cycle, snapshot, memoryUpdates) {
  if (!cycle) {
    const staleReasons = staleSnapshotReasons(snapshot);
    return {
      title: 'ממתין לסריקה השעתית הראשונה',
      thesis: staleReasons.length > 0
        ? `עדיין לא התקבל מחזור. מילויים חסומים כי ${staleReasons.join(', ')}.`
        : 'החשבון נשאר ללא שינוי עד שסריקת GPT-6 Astra xhigh תפרסם החלטת סימולציה.',
      commands: [],
      evidence: [],
      risk: 'עדיין אין הערכת סיכון פעילה.',
      memory: latestMemoryConclusion(memoryUpdates) ?? 'עדיין לא נרשמה מסקנת זיכרון.',
      cycleId: 'none',
    };
  }

  return {
    title: `מחזור ${cycle.cycle_id}`,
    thesis: cycleThesis(cycle),
    allocation: cycle.target_portfolio?.holdings ?? [],
    commands: (cycle.commands ?? []).map((command) => ({
      symbol: command.symbol,
      action: command.action,
      confidenceLabel: percent(command.confidence),
      notionalLabel: money(command.max_notional_usd),
      rationale: command.rationale,
    })),
    evidence: (cycle.evidence ?? []).map((item) => ({
      id: item.id,
      source: item.source,
      freshness: item.freshness,
      summary: item.summary,
    })),
    risk: `${cycle.risk_assessment?.summary ?? 'אין סיכום סיכון.'} פסק דין: ${
      cycle.risk_assessment?.risk_limit_verdict ?? 'unknown'
    }.`,
    memory: latestMemoryConclusion(memoryUpdates)
      ?? firstMemoryConclusion(cycle.memory_updates)
      ?? 'לא נרשמה מסקנת זיכרון במחזור הזה.',
    cycleId: cycle.cycle_id,
  };
}

function createOrderRows(orders) {
  return orders.map((order) => ({
    id: order.order_id,
    cycleId: order.cycle_id,
    symbol: order.symbol,
    side: order.side,
    sideLabel: ORDER_SIDE_LABELS[order.side] ?? order.side,
    status: order.status,
    statusLabel: ORDER_STATUS_LABELS[order.status] ?? order.status,
    qty: normalizedFinite(order.qty),
    avgPrice: normalizedFinite(order.avg_price),
    notional: normalizedFinite(order.notional_usd),
    notionalLabel: money(order.notional_usd),
    quantityLabel: NUMBER_FORMAT.format(normalizedFinite(order.qty)),
    reason: order.reason,
    timestampMs: timestampMsFromFrame(order),
    receivedAtMs: Number.isFinite(order.receivedAtMs) ? order.receivedAtMs : null,
  }));
}

function createFreshnessRows(snapshot) {
  if (!snapshot) {
    return [{
      source: 'market data',
      freshness: 'waiting',
      freshnessLabel: FRESHNESS_LABELS.waiting,
      detail: 'עדיין לא התקבל market research snapshot.',
    }];
  }
  const rows = (snapshot.source_freshness ?? []).map((row) => ({
    source: row.source,
    freshness: row.freshness,
    freshnessLabel: FRESHNESS_LABELS[row.freshness] ?? row.freshness,
    detail: row.freshness === 'fresh' ? 'מעודכן' : 'חוסם מילויים עד רענון',
  }));
  for (const flag of snapshot.unavailable_data_flags ?? []) {
    rows.push({
      source: flag,
      freshness: 'unavailable',
      freshnessLabel: FRESHNESS_LABELS.unavailable,
      detail: 'חוסם מילויים עד שחזור',
    });
  }
  return rows.length > 0 ? rows : [{
    source: 'market data',
    freshness: 'fresh',
    freshnessLabel: FRESHNESS_LABELS.fresh,
    detail: 'ה-snapshot לא סיפק אזהרות טריות.',
  }];
}

function createMemoryRows(memoryUpdates) {
  return memoryUpdates.map((event) => ({
    id: event.event_id,
    cycleId: event.cycle_id,
    thesis: event.thesis ?? '',
    conclusion: event.conclusion,
    watchItems: event.next_cycle_watch_items ?? [],
    watch: event.next_cycle_watch_items ?? [],
  }));
}

function createEventRows(events) {
  return events.map((event) => ({
    id: event.event_id,
    severity: event.severity,
    category: event.category,
    message: event.message,
  }));
}

function createNavHistory(navSeries, portfolio) {
  const values = navSeries
    .map((point) => point.nav)
    .filter(Number.isFinite);
  if (values.length >= 2) return values;
  if (!portfolio.available) return [];
  return [portfolio.initialEquity, portfolio.nav];
}

function createNavSeries(history, rawPortfolio, portfolio, activity = {}) {
  if (!portfolio.available) return [];
  const baseline = {
    id: 'nav-start',
    label: 'תחילה',
    nav: portfolio.initialEquity,
    cash: portfolio.initialEquity,
    grossExposure: 0,
    dailyPnl: 0,
    maxDrawdownPct: 0,
    positionCount: 0,
    timestampMs: null,
    receivedAtMs: null,
    isBaseline: true,
    benchmarkSymbol: null,
    benchmarkLabel: null,
    benchmarkStatus: 'unavailable',
    benchmarkPrice: null,
    benchmarkNav: null,
    positions: [],
  };
  const framePoints = [];
  const seen = new Set();

  [...history].reverse().forEach((item, index) => {
    appendNavSeriesPoint(framePoints, seen, navPointFromFrame(item, `history-${index}`));
  });

  if (rawPortfolio) {
    appendNavSeriesPoint(framePoints, seen, navPointFromFrame(rawPortfolio, 'current'));
  }

  if (framePoints.length === 0) {
    appendNavSeriesPoint(framePoints, seen, {
      id: 'nav-current',
      label: 'נוכחי',
      nav: portfolio.nav,
      cash: portfolio.cash,
      grossExposure: portfolio.gross,
      dailyPnl: portfolio.dailyPnl,
      maxDrawdownPct: portfolio.maxDrawdownPct,
      positionCount: portfolio.positions.length,
      timestampMs: null,
      receivedAtMs: null,
      isBaseline: false,
      benchmarkSymbol: null,
      benchmarkLabel: null,
      benchmarkStatus: portfolio.benchmark?.status ?? 'unavailable',
      benchmarkPrice: null,
      benchmarkNav: null,
      positions: portfolio.positions,
    });
  }

  const points = [
    baseline,
    ...dailyNavSeriesPoints(withTradeMarkers(
      framePoints,
      createTradeMarkers(activity.cycles ?? [], activity.orders ?? []),
    )),
  ];
  return withBenchmarkNav(points, portfolio.initialEquity).map((point, index) => ({ ...point, index }));
}

function dailyNavSeriesPoints(points) {
  const daily = new Map();
  const marked = [];
  const untimed = [];
  points.forEach((point, rawIndex) => {
    const ms = navPointTimeMs(point);
    if (!Number.isFinite(ms)) {
      (hasTradeMarkers(point) ? marked : untimed).push({ point, rawIndex });
      return;
    }
    if (hasTradeMarkers(point)) marked.push({ point, rawIndex, ms });
    const dayKey = utcDayKey(ms);
    const existing = daily.get(dayKey);
    if (!existing || ms > existing.ms || (ms === existing.ms && rawIndex > existing.rawIndex)) {
      daily.set(dayKey, { point, rawIndex, ms });
    }
  });
  const selected = [];
  const selectedPoints = new Set();
  for (const entry of [...daily.values(), ...marked, ...untimed]) {
    if (selectedPoints.has(entry.point)) continue;
    selectedPoints.add(entry.point);
    selected.push(entry);
  }
  return selected
    .sort((left, right) => {
      const leftMs = left.ms ?? navPointTimeMs(left.point);
      const rightMs = right.ms ?? navPointTimeMs(right.point);
      if (Number.isFinite(leftMs) && Number.isFinite(rightMs) && leftMs !== rightMs) {
        return leftMs - rightMs;
      }
      if (Number.isFinite(leftMs) !== Number.isFinite(rightMs)) {
        return Number.isFinite(leftMs) ? -1 : 1;
      }
      return left.rawIndex - right.rawIndex;
    })
    .map((entry) => entry.point);
}

function navPointTimeMs(point) {
  return Number.isFinite(point?.timestampMs)
    ? point.timestampMs
    : Number.isFinite(point?.receivedAtMs)
      ? point.receivedAtMs
      : null;
}

function utcDayKey(ms) {
  const date = new Date(ms);
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}-${String(date.getUTCDate()).padStart(2, '0')}`;
}

function navPointFromFrame(item, fallbackId) {
  const nav = normalizedFinite(item?.nav_usd);
  if (!Number.isFinite(nav)) return null;
  const timestampMs = timestampMsFromFrame(item);
  const receivedAtMs = Number.isFinite(item?.receivedAtMs) ? item.receivedAtMs : null;
  const cash = normalizedFinite(item?.cash_usd);
  const grossExposure = normalizedFinite(item?.gross_exposure_usd);
  const dailyPnl = normalizedFinite(item?.daily_pnl_usd);
  const maxDrawdownPct = normalizedFinite(item?.max_drawdown_pct);
  const positionCount = Array.isArray(item?.positions) ? item.positions.length : 0;
  const positions = positionSummaries(item?.positions);
  const benchmarkPrice = positiveFiniteOrNull(item?.benchmark_price_usd);
  const benchmarkSymbol = typeof item?.benchmark_symbol === 'string' ? item.benchmark_symbol : null;
  const benchmarkLabel = typeof item?.benchmark_label === 'string' ? item.benchmark_label : null;
  const benchmarkStatus = typeof item?.benchmark_status === 'string' ? item.benchmark_status : null;
  return {
    id: [
      'nav',
      fallbackId,
      timestampMs ?? 'no-ts',
      receivedAtMs ?? 'no-receipt',
      nav,
      cash,
      grossExposure,
      dailyPnl,
      maxDrawdownPct,
      positionCount,
      benchmarkSymbol ?? 'no-benchmark-symbol',
      benchmarkLabel ?? 'no-benchmark-label',
      benchmarkStatus ?? 'no-benchmark-status',
      benchmarkPrice ?? 'no-benchmark-price',
    ].join('-'),
    label: null,
    nav,
    cash,
    grossExposure,
    dailyPnl,
    maxDrawdownPct,
    positionCount,
    timestampMs,
    receivedAtMs,
    isBaseline: false,
    benchmarkSymbol,
    benchmarkLabel,
    benchmarkStatus,
    benchmarkPrice,
    benchmarkNav: null,
    positions,
  };
}

function withBenchmarkNav(points, initialEquity) {
  const basePoint = points.find((point) => (
    !point.isBaseline && Number.isFinite(point.benchmarkPrice)
  ));
  const metadataPoint = basePoint ?? points.find((point) => (
    !point.isBaseline && (point.benchmarkSymbol || point.benchmarkLabel || point.benchmarkStatus)
  ));
  if (
    !metadataPoint ||
    !Number.isFinite(initialEquity) ||
    initialEquity <= 0
  ) {
    return points;
  }
  const basePrice = basePoint?.benchmarkPrice ?? null;
  const benchmarkSymbol = metadataPoint.benchmarkSymbol ?? 'SPY';
  const benchmarkLabel = metadataPoint.benchmarkLabel ?? 'S&P500';
  const benchmarkStatus = metadataPoint.benchmarkStatus ?? (
    Number.isFinite(basePrice) ? 'live' : 'unavailable'
  );
  return points.map((point) => {
    if (point.isBaseline) {
      return {
        ...point,
        benchmarkSymbol,
        benchmarkLabel,
        benchmarkStatus,
        benchmarkPrice: Number.isFinite(basePrice) ? basePrice : null,
        benchmarkNav: Number.isFinite(basePrice) ? initialEquity : null,
      };
    }
    if (!Number.isFinite(point.benchmarkPrice)) {
      return {
        ...point,
        benchmarkSymbol: point.benchmarkSymbol ?? benchmarkSymbol,
        benchmarkLabel: point.benchmarkLabel ?? benchmarkLabel,
        benchmarkStatus: point.benchmarkStatus ?? 'unavailable',
      };
    }
    return {
      ...point,
      benchmarkSymbol: point.benchmarkSymbol ?? benchmarkSymbol,
      benchmarkLabel: point.benchmarkLabel ?? benchmarkLabel,
      benchmarkStatus: point.benchmarkStatus ?? 'live',
      benchmarkNav: Number.isFinite(basePrice) ? initialEquity * (point.benchmarkPrice / basePrice) : null,
    };
  });
}

function appendNavSeriesPoint(points, seen, point) {
  if (!point) return;
  const key = [
    point.isBaseline ? 'baseline' : 'frame',
    point.timestampMs ?? '',
    point.receivedAtMs ?? '',
    point.nav,
    point.cash,
    point.grossExposure,
    point.dailyPnl,
    point.maxDrawdownPct,
    point.positionCount,
    point.benchmarkSymbol ?? '',
    point.benchmarkLabel ?? '',
    point.benchmarkStatus ?? '',
    point.benchmarkPrice ?? '',
  ].join(':');
  if (seen.has(key)) return;
  seen.add(key);
  points.push(point);
}

function createTradeMarkers(cycles, orders) {
  const cycleById = new Map((cycles ?? [])
    .filter((cycle) => cycle?.cycle_id)
    .map((cycle) => [cycle.cycle_id, cycle]));
  return (orders ?? [])
    .filter((order) => order.status === 'accepted')
    .map((order, index) => {
      const cycle = cycleById.get(order.cycleId);
      const command = commandForOrder(cycle, order);
      const timestampMs = firstFiniteNumber(
        order.timestampMs,
        cycleTimeMs(cycle),
        order.receivedAtMs,
      );
      const side = order.side === 'sell' || command?.action === 'sell_notional' ? 'sell' : 'buy';
      return {
        id: order.id ?? `trade-${order.cycleId}-${order.symbol}-${index}`,
        cycleId: order.cycleId,
        symbol: order.symbol,
        side,
        action: command?.action ?? (side === 'sell' ? 'sell_notional' : 'buy_notional'),
        confidence: normalizedFinite(command?.confidence),
        notional: normalizedFinite(order.notional || command?.max_notional_usd),
        timestampMs,
      };
    })
    .filter((marker) => marker.symbol && (marker.side === 'buy' || marker.side === 'sell'))
    .sort((left, right) => {
      if (Number.isFinite(left.timestampMs) && Number.isFinite(right.timestampMs)) {
        return left.timestampMs - right.timestampMs;
      }
      if (Number.isFinite(left.timestampMs) !== Number.isFinite(right.timestampMs)) {
        return Number.isFinite(left.timestampMs) ? -1 : 1;
      }
      return String(left.id).localeCompare(String(right.id));
    });
}

function commandForOrder(cycle, order) {
  const commands = cycle?.commands ?? [];
  const expectedAction = order.side === 'sell' ? 'sell_notional' : 'buy_notional';
  return commands.find((command) => command.symbol === order.symbol && command.action === expectedAction)
    ?? commands.find((command) => command.symbol === order.symbol)
    ?? null;
}

function withTradeMarkers(points, markers) {
  if (!points.length || !markers.length) return points;
  const buckets = new Map();
  for (const marker of markers) {
    const pointIndex = navPointIndexForMarker(points, marker);
    if (pointIndex === null) continue;
    const exposure = inferTradeExposure(marker, points, pointIndex);
    const bucket = buckets.get(pointIndex) ?? [];
    bucket.push({
      ...marker,
      exposure,
      label: tradeMarkerLabel(marker.side, exposure),
    });
    buckets.set(pointIndex, bucket);
  }
  return points.map((point, index) => {
    const markersForPoint = buckets.get(index);
    if (!markersForPoint?.length) return point;
    return {
      ...point,
      tradeMarkers: [...(point.tradeMarkers ?? []), ...markersForPoint],
    };
  });
}

function navPointIndexForMarker(points, marker) {
  if (Number.isFinite(marker.timestampMs)) {
    const firstAfter = points.findIndex((point) => {
      const pointMs = navPointTimeMs(point);
      return Number.isFinite(pointMs) && pointMs >= marker.timestampMs;
    });
    if (firstAfter >= 0) return firstAfter;
    for (let index = points.length - 1; index >= 0; index -= 1) {
      const pointMs = navPointTimeMs(points[index]);
      if (Number.isFinite(pointMs) && pointMs <= marker.timestampMs) return index;
    }
  }
  return points.length > 0 ? points.length - 1 : null;
}

function inferTradeExposure(marker, points, pointIndex) {
  const postQuantity = positionQuantity(points[pointIndex], marker.symbol);
  const previousQuantity = previousPositionQuantity(points, pointIndex, marker.symbol);
  if (marker.side === 'sell') {
    return postQuantity < 0 ? 'short' : 'long';
  }
  return previousQuantity < 0 || postQuantity < 0 ? 'short' : 'long';
}

function previousPositionQuantity(points, pointIndex, symbol) {
  for (let index = pointIndex - 1; index >= 0; index -= 1) {
    const quantity = positionQuantity(points[index], symbol);
    if (quantity !== 0) return quantity;
  }
  return 0;
}

function positionQuantity(point, symbol) {
  const position = (point?.positions ?? []).find((candidate) => candidate.symbol === symbol);
  return Number.isFinite(position?.quantity) ? position.quantity : 0;
}

function tradeMarkerLabel(side, exposure) {
  if (side === 'buy' && exposure === 'short') return 'כיסוי שורט';
  if (side === 'buy') return 'קניית לונג';
  if (exposure === 'short') return 'מכירת שורט';
  return 'מכירת לונג';
}

function hasTradeMarkers(point) {
  return Array.isArray(point?.tradeMarkers) && point.tradeMarkers.length > 0;
}

function positionSummaries(positions) {
  if (!Array.isArray(positions)) return [];
  return positions
    .filter((position) => typeof position?.symbol === 'string')
    .map((position) => ({
      symbol: position.symbol,
      quantity: normalizedFinite(position.quantity),
      marketValue: normalizedFinite(position.market_value_usd),
    }));
}

function cycleTimeMs(cycle) {
  return firstFiniteNumber(
    Date.parse(cycle?.as_of),
    cycle?.timestampMs,
    cycle?.receivedAtMs,
  );
}

function firstFiniteNumber(...values) {
  return values.find((value) => Number.isFinite(value)) ?? null;
}

function timestampMsFromFrame(item) {
  if (Number.isFinite(item?.timestampMs)) return item.timestampMs;
  if (typeof item?.timestamp_ns === 'string' && item.timestamp_ns.trim() === '') return null;
  try {
    const timestampMs = Number(BigInt(item?.timestamp_ns) / 1_000_000n);
    return Number.isFinite(timestampMs) ? timestampMs : null;
  } catch {
    return null;
  }
}

function firstCommandRationale(commands) {
  return commands?.[0]?.rationale
    ?? 'ה-LLM לא סיפק נימוק לפקודה.';
}

function latestMemoryConclusion(memoryUpdates) {
  return memoryUpdates?.find((event) => event.conclusion)?.conclusion ?? null;
}

function firstMemoryConclusion(memoryUpdates) {
  return memoryUpdates?.find((event) => event?.conclusion)?.conclusion ?? null;
}

function staleSnapshotReasons(snapshot) {
  if (!snapshot) return [];
  const reasons = [];
  for (const row of snapshot.source_freshness ?? []) {
    if (row.freshness !== 'fresh') {
      reasons.push(`${row.source} הוא ${FRESHNESS_LABELS[row.freshness] ?? row.freshness}`);
    }
  }
  for (const flag of snapshot.unavailable_data_flags ?? []) {
    reasons.push(`${flag} לא זמין`);
  }
  return reasons;
}

function timestampToMs(asOf, timestampMs, receivedAtMs, nowMs) {
  const parsed = Date.parse(asOf);
  if (Number.isFinite(parsed)) return parsed;
  if (Number.isFinite(timestampMs)) return timestampMs;
  if (Number.isFinite(receivedAtMs)) return receivedAtMs;
  return nowMs;
}

function actionClass(action) {
  if (action === 'buy_notional') return 'buy';
  if (action === 'sell_notional' || action === 'close_position') return 'sell';
  if (action === 'set_target_weight') return 'target';
  return 'hold';
}

function money(value) {
  return MONEY_FORMAT.format(normalizedFinite(value));
}

function moneyZero(value) {
  return MONEY_ZERO_FORMAT.format(normalizedFinite(value));
}

function percent(value) {
  return PCT_FORMAT.format(normalizedFinite(value));
}

function normalizedFinite(value) {
  const number = Number.isFinite(value) ? value : 0;
  return Object.is(number, -0) || Math.abs(number) < 0.005 ? 0 : number;
}

function positiveFiniteOrNull(value) {
  return Number.isFinite(value) && value > 0 ? value : null;
}

function normalizedWeightPct(value, marketValue, nav) {
  const fallback = nav > 0 ? marketValue / nav : 0;
  const raw = Number.isFinite(value) ? value : fallback;
  return Math.abs(raw) > 1 ? raw / 100 : raw;
}

function formatAge(ageMs) {
  if (ageMs < 1_000) return 'עכשיו';
  if (ageMs < 60_000) return `לפני ${Math.floor(ageMs / 1_000)} שנ׳`;
  if (ageMs < 3_600_000) return `לפני ${Math.floor(ageMs / 60_000)} דק׳`;
  return `לפני ${Math.floor(ageMs / 3_600_000)} שע׳`;
}
