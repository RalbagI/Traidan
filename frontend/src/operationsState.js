import { MESSAGE_TYPES } from './messages.js';

export const MAX_EQUITY_ORDERS = 80;
export const MAX_EQUITY_CYCLES = 24;
export const MAX_CYCLE_SUMMARIES = 20;
export const MAX_SERVICE_ACTIVITIES = 24;
export const MAX_PORTFOLIO_HISTORY = 20_000;
export const MAX_MEMORY_UPDATES = 80;
export const MAX_OPERATOR_EVENTS = 80;

const SERVICE_ACTIVITY_STATUSES = new Set([
  'llm_research_only',
  'llm_edge_gate_skipped',
  'llm_release_skipped_no_edge',
  'llm_trade_cycle_skipped_stale',
  'llm_release_blocked',
  'llm_release_prepare_blocked',
]);

export function createOperationsState() {
  return {
    serviceStatus: null,
    serviceActivities: [],
    equityCycle: null,
    equityCycles: [],
    cycleSummaries: [],
    equityOrders: [],
    equityPortfolio: null,
    equityPortfolioHistory: [],
    marketResearchSnapshot: null,
    llmMemoryUpdates: [],
    operatorEvents: [],
  };
}

export function applyEngineMessage(state, message, nowMs = Date.now(), streamGeneration = null) {
  switch (message.type) {
    case MESSAGE_TYPES.LLM_EQUITY_SERVICE_STATUS:
      {
        const serviceStatus = withReceipt(message.data, nowMs, streamGeneration);
        const existingActivities = state.serviceActivities ?? [];
        const activityId = serviceStatus.activity_key ?? serviceStatus.timestamp_ns;
        const existingActivity = existingActivities.find(
          (activity) => activity.activityId === activityId,
        );
        const authoritativeRepeatCount = Number.isInteger(serviceStatus.activity_repeat_count)
          && serviceStatus.activity_repeat_count > 0
          ? serviceStatus.activity_repeat_count
          : null;
        const existingRepeatCount = Math.max(1, Number(existingActivity?.repeatCount) || 1);
        const serviceActivity = {
          ...serviceStatus,
          activityId,
          repeatCount: authoritativeRepeatCount !== null
            ? existingActivity
              ? Math.max(authoritativeRepeatCount, existingRepeatCount)
              : authoritativeRepeatCount
            : existingActivity
              ? existingActivity.timestamp_ns === serviceStatus.timestamp_ns
                ? existingRepeatCount
                : existingRepeatCount + 1
              : 1,
        };
        return {
          ...state,
          serviceStatus,
          serviceActivities: isServiceActivityStatus(serviceStatus.status)
            ? uniqueById(
              [serviceActivity, ...existingActivities],
              'activityId',
            ).slice(0, MAX_SERVICE_ACTIVITIES)
            : existingActivities,
        };
      }
    case MESSAGE_TYPES.LLM_EQUITY_CYCLE:
      {
        const cycle = withReceipt(message.data, nowMs, streamGeneration);
        return {
          ...state,
          equityCycle: cycle,
          equityCycles: uniqueById(
            [cycle, ...(state.equityCycles ?? [])],
            'cycle_id',
          ).slice(0, MAX_EQUITY_CYCLES),
        };
      }
    case MESSAGE_TYPES.LLM_EQUITY_CYCLE_SUMMARY:
      return {
        ...state,
        cycleSummaries: uniqueById(
          [withReceipt(message.data, nowMs, streamGeneration), ...(state.cycleSummaries ?? [])],
          'cycle_id',
        ).slice(0, MAX_CYCLE_SUMMARIES),
      };
    case MESSAGE_TYPES.EQUITY_ORDER_SIMULATED:
      return {
        ...state,
        equityOrders: uniqueById(
          [withReceipt(message.data, nowMs, streamGeneration), ...(state.equityOrders ?? [])],
          'order_id',
        ).slice(0, MAX_EQUITY_ORDERS),
      };
    case MESSAGE_TYPES.EQUITY_PORTFOLIO_STATE:
      {
        const portfolio = withReceipt(message.data, nowMs, streamGeneration);
        return {
          ...state,
          equityPortfolio: portfolio,
          equityPortfolioHistory: uniquePortfolioFrames(
            [portfolio, ...(state.equityPortfolioHistory ?? [])],
          ).slice(0, MAX_PORTFOLIO_HISTORY),
        };
      }
    case MESSAGE_TYPES.MARKET_RESEARCH_SNAPSHOT:
      return {
        ...state,
        marketResearchSnapshot: withReceipt(message.data, nowMs, streamGeneration),
      };
    case MESSAGE_TYPES.LLM_MEMORY_UPDATE:
      return {
        ...state,
        llmMemoryUpdates: uniqueById(
          [withReceipt(message.data, nowMs, streamGeneration), ...(state.llmMemoryUpdates ?? [])],
          'event_id',
        ).slice(0, MAX_MEMORY_UPDATES),
      };
    case MESSAGE_TYPES.OPERATOR_EVENT:
      return {
        ...state,
        operatorEvents: uniqueById(
          [withReceipt(message.data, nowMs, streamGeneration), ...(state.operatorEvents ?? [])],
          'event_id',
        ).slice(0, MAX_OPERATOR_EVENTS),
      };
    default:
      return state;
  }
}

export function isServiceActivityStatus(status) {
  return SERVICE_ACTIVITY_STATUSES.has(status);
}

export function createStateFromMessages(messages, nowMs = Date.now()) {
  return messages.reduce((state, message) => applyEngineMessage(state, message, nowMs), createOperationsState());
}

export function timestampNsToEpochMs(timestampNs) {
  if (typeof timestampNs === 'string' && timestampNs.trim() === '') {
    return null;
  }
  try {
    return Number(BigInt(timestampNs) / 1_000_000n);
  } catch {
    return null;
  }
}

function withReceipt(data, nowMs, streamGeneration) {
  return {
    ...data,
    receivedAtMs: nowMs,
    timestampMs: timestampNsToEpochMs(data.timestamp_ns),
    ...(Number.isInteger(streamGeneration) ? { streamGeneration } : {}),
  };
}

function uniqueById(items, key) {
  const seen = new Set();
  const result = [];
  for (const item of items) {
    const id = item?.[key];
    if (!id || seen.has(id)) continue;
    seen.add(id);
    result.push(item);
  }
  return result;
}

function uniquePortfolioFrames(items) {
  const seen = new Set();
  const result = [];
  for (const item of items) {
    const id = [
      item?.timestamp_ns,
      item?.nav_usd,
      item?.cash_usd,
      item?.gross_exposure_usd,
      item?.daily_pnl_usd,
      item?.total_pnl_usd,
      item?.realized_pnl_usd,
      item?.unrealized_pnl_usd,
      item?.benchmark_symbol,
      item?.benchmark_label,
      item?.benchmark_status,
      item?.benchmark_price_usd,
    ].join(':');
    if (seen.has(id)) continue;
    seen.add(id);
    result.push(item);
  }
  return result;
}
