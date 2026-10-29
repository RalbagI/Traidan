export const MESSAGE_TYPES = Object.freeze({
  LLM_EQUITY_SERVICE_STATUS: 'llm_equity_service_status',
  LLM_EQUITY_CYCLE: 'llm_equity_cycle',
  LLM_EQUITY_CYCLE_SUMMARY: 'llm_equity_cycle_summary',
  EQUITY_ORDER_SIMULATED: 'equity_order_simulated',
  EQUITY_PORTFOLIO_STATE: 'equity_portfolio_state',
  MARKET_RESEARCH_SNAPSHOT: 'market_research_snapshot',
  LLM_MEMORY_UPDATE: 'llm_memory_update',
  OPERATOR_EVENT: 'operator_event',
});

const SESSION_LABELS = new Set(['premarket', 'rth', 'after_hours', 'closed']);
const COMMAND_ACTIONS = new Set(['buy_notional', 'sell_notional']);
const COMMAND_HORIZONS = new Set(['30m', '60m', 'close', '1d', '5d']);
const COMMAND_KEYS = new Set([
  'action',
  'symbol',
  'rationale',
  'confidence',
  'max_notional_usd',
  'horizon',
  'evidence_refs',
]);
const ORDER_SIDES = new Set(['buy', 'sell']);
const ORDER_STATUSES = new Set(['accepted', 'rejected']);
const CYCLE_SUMMARY_STATUSES = new Set(['accepted', 'blocked', 'rejected']);
const MANDATORY_TRADE_STATUSES = new Set(['accepted', 'blocked', 'recovering', 'not_applicable']);
const SUMMARY_ACTIONS = new Set(['buy', 'sell']);
const FRESHNESS = new Set(['fresh', 'stale', 'unavailable']);
const BENCHMARK_STATUSES = new Set(['live', 'stale', 'unavailable']);
const MIN_LLM_ORDER_NOTIONAL_EXCLUSIVE_USD = 0;
const MAX_TEXT = 1_200;
const MAX_COMMANDS = 16;
const MAX_EVIDENCE = 64;
const MAX_MEMORY_UPDATES = 16;
const MAX_POSITIONS = 128;
const MAX_MARKET_SYMBOLS = 128;
const MAX_SOURCE_FRESHNESS = 32;
const MAX_MACRO_NEWS = 32;
const MAX_UNAVAILABLE_FLAGS = 128;
const MAX_BENCHMARK_LABEL = 80;

export function parseEngineMessage(rawMessage, { logger = console } = {}) {
  let message;
  try {
    message = typeof rawMessage === 'string' ? JSON.parse(rawMessage) : rawMessage;
  } catch (error) {
    logger.error?.(`[WS] parse error: ${error.message}`);
    return null;
  }

  if (!isObject(message) || typeof message.type !== 'string') {
    logger.warn?.('[WS] malformed message ignored');
    return null;
  }

  switch (message.type) {
    case MESSAGE_TYPES.LLM_EQUITY_SERVICE_STATUS:
      return validateServiceStatus(message, logger);
    case MESSAGE_TYPES.LLM_EQUITY_CYCLE:
      return validateEquityCycle(message, logger);
    case MESSAGE_TYPES.LLM_EQUITY_CYCLE_SUMMARY:
      return validateCycleSummary(message, logger);
    case MESSAGE_TYPES.EQUITY_ORDER_SIMULATED:
      return validateSimulatedOrder(message, logger);
    case MESSAGE_TYPES.EQUITY_PORTFOLIO_STATE:
      return validatePortfolio(message, logger);
    case MESSAGE_TYPES.MARKET_RESEARCH_SNAPSHOT:
      return validateMarketSnapshot(message, logger);
    case MESSAGE_TYPES.LLM_MEMORY_UPDATE:
      return validateMemoryUpdate(message, logger);
    case MESSAGE_TYPES.OPERATOR_EVENT:
      return validateOperatorEvent(message, logger);
    default:
      logger.warn?.(`[WS] unknown message type: ${message.type}`);
      return null;
  }
}

function validateServiceStatus(message, logger) {
  const { data } = message;
  if (
    !isObject(data) ||
    !boundedText(data.status, 120) ||
    !boundedText(data.detail, MAX_TEXT) ||
    (data.market_session !== undefined && !SESSION_LABELS.has(data.market_session)) ||
    (data.has_market_research_snapshot !== undefined
      && typeof data.has_market_research_snapshot !== 'boolean') ||
    !optionalText(data.activity_key, 160) ||
    (data.activity_repeat_count !== undefined && (
      !Number.isInteger(data.activity_repeat_count) || data.activity_repeat_count < 0
    )) ||
    !optionalText(data.last_cycle_id, 160) ||
    !optionalText(data.last_error, MAX_TEXT) ||
    !timestampText(data.timestamp_ns)
  ) {
    logger.warn?.('[WS] invalid llm_equity_service_status payload ignored');
    return null;
  }
  return { type: message.type, data };
}

function validateEquityCycle(message, logger) {
  const { data } = message;
  if (
    !isObject(data) ||
    !boundedText(data.cycle_id, 160) ||
    !boundedText(data.as_of, 80) ||
    !SESSION_LABELS.has(data.market_session) ||
    !Number.isFinite(data.portfolio_nav_usd) ||
    data.portfolio_nav_usd <= 0 ||
    !Array.isArray(data.commands) ||
    (data.commands.length === 0 && !isTargetPortfolio(data)) ||
    data.commands.length > MAX_COMMANDS ||
    !data.commands.every(isCommand) ||
    !Array.isArray(data.evidence) ||
    data.evidence.length === 0 ||
    data.evidence.length > MAX_EVIDENCE ||
    !data.evidence.every(isEvidence) ||
    !isObject(data.risk_assessment) ||
    !boundedText(data.risk_assessment.summary, MAX_TEXT) ||
    !Array.isArray(data.memory_updates) ||
    data.memory_updates.length > MAX_MEMORY_UPDATES ||
    !timestampText(data.timestamp_ns)
  ) {
    logger.warn?.('[WS] invalid llm_equity_cycle payload ignored');
    return null;
  }
  return { type: message.type, data };
}

function isTargetPortfolio(data) {
  const target = data.target_portfolio;
  return data.execution_policy === 'fully_invested_v1'
    && isObject(target) && target.schema_version === 1
    && Array.isArray(target.holdings) && target.holdings.length >= 20 && target.holdings.length <= 40
    && target.holdings.every((row) => isObject(row) && equitySymbol(row.symbol)
      && Number.isInteger(row.weight_bps) && row.weight_bps > 0 && row.weight_bps <= 500)
    && new Set(target.holdings.map((row) => row.symbol)).size === target.holdings.length
    && target.holdings.reduce((sum, row) => sum + row.weight_bps, 0) === 10000;
}

function validateSimulatedOrder(message, logger) {
  const { data } = message;
  if (
    !isObject(data) ||
    !boundedText(data.order_id, 160) ||
    !boundedText(data.cycle_id, 160) ||
    !equitySymbol(data.symbol) ||
    !ORDER_SIDES.has(data.side) ||
    !ORDER_STATUSES.has(data.status) ||
    !nonNegative(data.qty) ||
    !nonNegative(data.avg_price) ||
    !nonNegative(data.notional_usd) ||
    !boundedText(data.reason, 240) ||
    !timestampText(data.timestamp_ns)
  ) {
    logger.warn?.('[WS] invalid equity_order_simulated payload ignored');
    return null;
  }
  return { type: message.type, data };
}

function validateCycleSummary(message, logger) {
  const { data } = message;
  if (
    !isObject(data) ||
    !boundedText(data.cycle_id, 160) ||
    !boundedText(data.cycle_kind, 80) ||
    !boundedText(data.as_of, 80) ||
    !SESSION_LABELS.has(data.market_session) ||
    !CYCLE_SUMMARY_STATUSES.has(data.status) ||
    !MANDATORY_TRADE_STATUSES.has(data.mandatory_trade_status) ||
    !nonNegative(data.required_notional_usd) ||
    !nonNegative(data.requested_notional_usd) ||
    !nonNegative(data.accepted_notional_usd) ||
    !Array.isArray(data.actions) ||
    data.actions.length === 0 ||
    data.actions.length > 80 ||
    !data.actions.every((action) => SUMMARY_ACTIONS.has(action)) ||
    !Array.isArray(data.symbols) ||
    data.symbols.length === 0 ||
    data.symbols.length > 80 ||
    !data.symbols.every(equitySymbol) ||
    !boundedText(data.rationale, MAX_TEXT) ||
    !boundedText(data.reason, 240) ||
    !timestampText(data.timestamp_ns)
  ) {
    logger.warn?.('[WS] invalid llm_equity_cycle_summary payload ignored');
    return null;
  }
  return { type: message.type, data };
}

function validatePortfolio(message, logger) {
  const { data } = message;
  if (
    !isObject(data) ||
    !positive(data.initial_equity_usd) ||
    !Number.isFinite(data.cash_usd) ||
    !Number.isFinite(data.nav_usd) ||
    !nonNegative(data.gross_exposure_usd) ||
    !Number.isFinite(data.daily_pnl_usd) ||
    !optionalFinite(data.total_pnl_usd) ||
    !optionalFinite(data.total_pnl_pct) ||
    !optionalFinite(data.realized_pnl_usd) ||
    !optionalFinite(data.unrealized_pnl_usd) ||
    !nonNegative(data.max_drawdown_pct) ||
    !Array.isArray(data.positions) ||
    data.positions.length > MAX_POSITIONS ||
    !data.positions.every(isPosition) ||
    !isOptionalBenchmark(data) ||
    !timestampText(data.timestamp_ns)
  ) {
    logger.warn?.('[WS] invalid equity_portfolio_state payload ignored');
    return null;
  }
  return { type: message.type, data };
}

function validateMarketSnapshot(message, logger) {
  const { data } = message;
  if (
    !isObject(data) ||
    !boundedText(data.snapshot_id, 160) ||
    !boundedText(data.as_of, 80) ||
    !SESSION_LABELS.has(data.market_session) ||
    !Array.isArray(data.symbols) ||
    data.symbols.length > MAX_MARKET_SYMBOLS ||
    !data.symbols.every(equitySymbol) ||
    !isObject(data.latest_bars) ||
    !isObject(data.latest_quotes) ||
    !isCalendarState(data.market_calendar_state) ||
    !Array.isArray(data.source_freshness) ||
    data.source_freshness.length > MAX_SOURCE_FRESHNESS ||
    !data.source_freshness.every(isFreshnessRow) ||
    !Array.isArray(data.macro_news_summaries) ||
    data.macro_news_summaries.length > MAX_MACRO_NEWS ||
    !data.macro_news_summaries.every(isMacroSummary) ||
    !Array.isArray(data.unavailable_data_flags) ||
    data.unavailable_data_flags.length > MAX_UNAVAILABLE_FLAGS ||
    !data.unavailable_data_flags.every((flag) => boundedText(flag, 160)) ||
    !timestampText(data.timestamp_ns)
  ) {
    logger.warn?.('[WS] invalid market_research_snapshot payload ignored');
    return null;
  }
  return { type: message.type, data };
}

function validateMemoryUpdate(message, logger) {
  const { data } = message;
  if (
    !isObject(data) ||
    !boundedText(data.event_id, 160) ||
    !boundedText(data.cycle_id, 160) ||
    !optionalText(data.thesis, MAX_TEXT) ||
    !optionalText(data.outcome, MAX_TEXT) ||
    !boundedText(data.conclusion, MAX_TEXT) ||
    !Array.isArray(data.next_cycle_watch_items ?? []) ||
    !(data.next_cycle_watch_items ?? []).every((item) => boundedText(item, 240)) ||
    !timestampText(data.timestamp_ns)
  ) {
    logger.warn?.('[WS] invalid llm_memory_update payload ignored');
    return null;
  }
  return { type: message.type, data };
}

function validateOperatorEvent(message, logger) {
  const { data } = message;
  if (
    !isObject(data) ||
    !boundedText(data.event_id, 160) ||
    !boundedText(data.severity, 40) ||
    !boundedText(data.category, 80) ||
    !boundedText(data.message, MAX_TEXT) ||
    !timestampText(data.timestamp_ns)
  ) {
    logger.warn?.('[WS] invalid operator_event payload ignored');
    return null;
  }
  return { type: message.type, data };
}

function isCommand(command) {
  return (
    isObject(command) &&
    hasOnlyKeys(command, COMMAND_KEYS) &&
    COMMAND_ACTIONS.has(command.action) &&
    COMMAND_HORIZONS.has(command.horizon) &&
    equitySymbol(command.symbol) &&
    boundedText(command.rationale, MAX_TEXT) &&
    Number.isFinite(command.confidence) &&
    command.confidence >= 0 &&
    command.confidence <= 1 &&
    Number.isFinite(command.max_notional_usd) &&
    command.max_notional_usd > MIN_LLM_ORDER_NOTIONAL_EXCLUSIVE_USD &&
    Array.isArray(command.evidence_refs) &&
    command.evidence_refs.length > 0 &&
    command.evidence_refs.every((reference) => boundedText(reference, 120))
  );
}

function isEvidence(evidence) {
  return (
    isObject(evidence) &&
    boundedText(evidence.id, 160) &&
    boundedText(evidence.source, 240) &&
    boundedText(evidence.summary, MAX_TEXT) &&
    boundedText(evidence.retrieved_at, 80) &&
    FRESHNESS.has(evidence.freshness)
  );
}

function isPosition(position) {
  return (
    isObject(position) &&
    equitySymbol(position.symbol) &&
    Number.isFinite(position.quantity) &&
    positive(position.avg_price) &&
    optionalPositive(position.current_price_usd) &&
    optionalFinite(position.cost_basis_usd) &&
    Number.isFinite(position.market_value_usd) &&
    optionalFinite(position.unrealized_pnl_usd) &&
    optionalFinite(position.unrealized_pnl_pct) &&
    Number.isFinite(position.weight_pct)
  );
}

function isOptionalBenchmark(data) {
  const hasAnyBenchmarkField = data.benchmark_symbol !== undefined
    || data.benchmark_label !== undefined
    || data.benchmark_status !== undefined
    || data.benchmark_price_usd !== undefined;
  if (!hasAnyBenchmarkField) return true;
  const status = data.benchmark_status ?? (data.benchmark_price_usd === undefined ? 'unavailable' : 'live');
  return data.benchmark_symbol === 'SPY'
    && data.benchmark_label === 'S&P500'
    && boundedText(data.benchmark_label, MAX_BENCHMARK_LABEL)
    && BENCHMARK_STATUSES.has(status)
    && (data.benchmark_price_usd === undefined || positive(data.benchmark_price_usd))
    && (status !== 'live' || positive(data.benchmark_price_usd));
}

function isFreshnessRow(row) {
  return (
    isObject(row) &&
    boundedText(row.source, 240) &&
    FRESHNESS.has(row.freshness)
  );
}

function isMacroSummary(summary) {
  return (
    isObject(summary) &&
    boundedText(summary.source, 240) &&
    boundedText(summary.retrieved_at, 80) &&
    boundedText(summary.summary, MAX_TEXT) &&
    FRESHNESS.has(summary.freshness)
  );
}

function isCalendarState(state) {
  return (
    isObject(state) &&
    boundedText(state.source, 240) &&
    boundedText(state.retrieved_at, 80) &&
    typeof state.is_open === 'boolean' &&
    typeof state.next_open === 'string' &&
    typeof state.next_close === 'string' &&
    FRESHNESS.has(state.freshness)
  );
}

function isObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function hasOnlyKeys(value, allowedKeys) {
  return Object.keys(value).every((key) => allowedKeys.has(key));
}

function boundedText(value, maxLength) {
  return typeof value === 'string' && value.length > 0 && value.length <= maxLength;
}

function optionalText(value, maxLength) {
  return value === null || value === undefined || boundedText(value, maxLength);
}

function equitySymbol(value) {
  return typeof value === 'string' && /^[A-Z][A-Z0-9.-]{0,9}$/.test(value);
}

function positive(value) {
  return Number.isFinite(value) && value > 0;
}

function nonNegative(value) {
  return Number.isFinite(value) && value >= 0;
}

function optionalFinite(value) {
  return value === undefined || Number.isFinite(value);
}

function optionalPositive(value) {
  return value === undefined || positive(value);
}

function timestampText(value) {
  return typeof value === 'string' && /^\d+$/.test(value);
}
