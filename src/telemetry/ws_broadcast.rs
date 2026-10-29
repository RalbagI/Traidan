use crate::llm_equity::{
    LlmEquityApplyResult, LlmEquityCycleSummary, LlmEquityOrderSide, LlmEquityPortfolio,
    LlmEquityPortfolioHistoryPoint, LlmEquityRuntimeSnapshot, LlmEquityRuntimeState,
    LLM_EQUITY_INITIAL_CASH_USD,
};
use crate::telemetry::metrics::ExecutionMetrics;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::env;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::interval;
use tokio_tungstenite::accept_async;
use tracing::{debug, warn};

pub const DEFAULT_WS_BIND_ADDR: &str = "127.0.0.1:8080";
pub const WS_BIND_ADDR_ENV: &str = "TRAIDAN_WS_BIND_ADDR";
const DEFAULT_EQUITY_SYMBOL: &str = "SPY";
const DEFAULT_BENCHMARK_LABEL: &str = "S&P500";

pub fn ws_bind_addr() -> String {
    env::var(WS_BIND_ADDR_ENV).unwrap_or_else(|_| DEFAULT_WS_BIND_ADDR.to_string())
}

#[derive(Debug, Clone)]
pub struct EngineStreamFeed {
    state: LlmEquityRuntimeState,
}

impl EngineStreamFeed {
    pub fn llm_equity(state: LlmEquityRuntimeState) -> Self {
        Self { state }
    }

    pub fn current_messages(&self) -> Vec<EngineStreamMessage> {
        messages_from_snapshot(&self.state.snapshot())
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum EngineStreamMessage {
    LlmEquityServiceStatus(LlmEquityServiceStatusPayload),
    LlmEquityCycle(Value),
    LlmEquityCycleSummary(LlmEquityCycleSummaryPayload),
    EquityOrderSimulated(EquityOrderSimulatedPayload),
    EquityPortfolioState(EquityPortfolioStatePayload),
    MarketResearchSnapshot(Value),
    LlmMemoryUpdate(Value),
    OperatorEvent(OperatorEventPayload),
}

impl EngineStreamMessage {
    pub fn message_type(&self) -> &'static str {
        match self {
            Self::LlmEquityServiceStatus(_) => "llm_equity_service_status",
            Self::LlmEquityCycle(_) => "llm_equity_cycle",
            Self::LlmEquityCycleSummary(_) => "llm_equity_cycle_summary",
            Self::EquityOrderSimulated(_) => "equity_order_simulated",
            Self::EquityPortfolioState(_) => "equity_portfolio_state",
            Self::MarketResearchSnapshot(_) => "market_research_snapshot",
            Self::LlmMemoryUpdate(_) => "llm_memory_update",
            Self::OperatorEvent(_) => "operator_event",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LlmEquityServiceStatusPayload {
    pub status: String,
    pub detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub market_session: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub activity_key: Option<String>,
    pub activity_repeat_count: u32,
    pub has_market_research_snapshot: bool,
    pub last_cycle_id: Option<String>,
    pub last_error: Option<String>,
    #[serde(serialize_with = "serialize_u64_as_string")]
    pub timestamp_ns: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct EquityOrderSimulatedPayload {
    pub order_id: String,
    pub cycle_id: String,
    pub symbol: String,
    pub side: String,
    pub status: String,
    pub qty: f64,
    pub avg_price: f64,
    pub notional_usd: f64,
    pub reason: String,
    #[serde(serialize_with = "serialize_u64_as_string")]
    pub timestamp_ns: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct EquityPositionPayload {
    pub symbol: String,
    pub quantity: f64,
    pub avg_price: f64,
    pub current_price_usd: f64,
    pub cost_basis_usd: f64,
    pub market_value_usd: f64,
    pub unrealized_pnl_usd: f64,
    pub unrealized_pnl_pct: f64,
    pub weight_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct EquityPortfolioStatePayload {
    pub initial_equity_usd: f64,
    pub cash_usd: f64,
    pub nav_usd: f64,
    pub gross_exposure_usd: f64,
    pub daily_pnl_usd: f64,
    pub total_pnl_usd: f64,
    pub total_pnl_pct: f64,
    pub realized_pnl_usd: f64,
    pub unrealized_pnl_usd: f64,
    pub max_drawdown_pct: f64,
    pub positions: Vec<EquityPositionPayload>,
    pub benchmark_symbol: Option<String>,
    pub benchmark_label: Option<String>,
    pub benchmark_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub benchmark_price_usd: Option<f64>,
    #[serde(serialize_with = "serialize_u64_as_string")]
    pub timestamp_ns: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct LlmEquityCycleSummaryPayload {
    pub cycle_id: String,
    pub cycle_kind: String,
    pub as_of: String,
    pub market_session: String,
    pub status: String,
    pub mandatory_trade_status: String,
    pub required_notional_usd: f64,
    pub requested_notional_usd: f64,
    pub accepted_notional_usd: f64,
    pub actions: Vec<String>,
    pub symbols: Vec<String>,
    pub rationale: String,
    pub reason: String,
    #[serde(serialize_with = "serialize_u64_as_string")]
    pub timestamp_ns: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct OperatorEventPayload {
    pub event_id: String,
    pub severity: String,
    pub category: String,
    pub message: String,
    #[serde(serialize_with = "serialize_u64_as_string")]
    pub timestamp_ns: u64,
}

pub async fn serve_ws_broadcast_with_feed(
    bind_addr: &str,
    feed: EngineStreamFeed,
    metrics: ExecutionMetrics,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let feed = feed.clone();
        let metrics = metrics.clone();
        tokio::spawn(async move {
            if let Err(error) = serve_client(stream, feed, metrics).await {
                debug!(%error, "equity dashboard WebSocket client closed");
            }
        });
    }
}

async fn serve_client(
    stream: TcpStream,
    feed: EngineStreamFeed,
    metrics: ExecutionMetrics,
) -> anyhow::Result<()> {
    let mut ws = accept_async(stream).await?;
    let _guard = DashboardClientGuard::new(&metrics);
    let mut ticker = interval(Duration::from_secs(5));

    send_messages(&mut ws, &feed, &metrics).await?;
    loop {
        tokio::select! {
            _ = ticker.tick() => send_messages(&mut ws, &feed, &metrics).await?,
            inbound = ws.next() => {
                match inbound {
                    Some(Ok(message)) if message.is_close() => break,
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(error.into()),
                    None => break,
                }
            }
        }
    }
    Ok(())
}

async fn send_messages<S>(
    ws: &mut tokio_tungstenite::WebSocketStream<S>,
    feed: &EngineStreamFeed,
    metrics: &ExecutionMetrics,
) -> anyhow::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    for message in feed.current_messages() {
        let encoded = serde_json::to_string(&message)?;
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            encoded.into(),
        ))
        .await?;
        metrics.record_dashboard_ws_message(message.message_type());
    }
    Ok(())
}

fn messages_from_snapshot(snapshot: &LlmEquityRuntimeSnapshot) -> Vec<EngineStreamMessage> {
    let snapshot_timestamp_ns = timestamp_ns_from_datetime(snapshot.updated_at);
    let mut messages = vec![EngineStreamMessage::LlmEquityServiceStatus(
        LlmEquityServiceStatusPayload {
            status: snapshot.status.clone(),
            detail: snapshot.detail.clone(),
            market_session: snapshot.market_session.clone(),
            activity_key: snapshot.service_activity_key.clone(),
            activity_repeat_count: snapshot.service_activity_repeat_count,
            has_market_research_snapshot: snapshot.service_status_has_market_snapshot,
            last_cycle_id: snapshot.last_cycle_id.clone(),
            last_error: snapshot.last_error.clone(),
            timestamp_ns: snapshot_timestamp_ns,
        },
    )];

    let mut current_portfolio_emitted = false;
    for history_point in &snapshot.portfolio_history {
        let history_timestamp_ns = timestamp_ns_from_datetime(history_point.updated_at);
        messages.push(EngineStreamMessage::EquityPortfolioState(
            portfolio_payload_from_parts(
                &history_point.portfolio,
                &history_point.prices,
                &HashSet::new(),
                history_timestamp_ns,
            ),
        ));
        if history_point_represents_current_portfolio(history_point, snapshot) {
            current_portfolio_emitted = true;
        }
    }
    if !current_portfolio_emitted {
        messages.push(EngineStreamMessage::EquityPortfolioState(
            portfolio_payload(snapshot, snapshot_timestamp_ns),
        ));
    }

    if let Some(market_snapshot) = snapshot.latest_market_snapshot.clone() {
        messages.push(EngineStreamMessage::MarketResearchSnapshot(with_timestamp(
            market_snapshot,
            snapshot_timestamp_ns,
        )));
    }
    if let Some(decision) = snapshot.latest_decision.clone() {
        messages.push(EngineStreamMessage::LlmEquityCycle(with_timestamp(
            decision,
            snapshot_timestamp_ns,
        )));
    }
    for summary in &snapshot.cycle_summaries {
        messages.push(EngineStreamMessage::LlmEquityCycleSummary(
            cycle_summary_payload(summary),
        ));
    }
    if let Some(result) = &snapshot.latest_result {
        messages.extend(order_messages(result, snapshot_timestamp_ns));
        messages.extend(memory_messages(result, snapshot, snapshot_timestamp_ns));
    }
    messages.push(EngineStreamMessage::OperatorEvent(OperatorEventPayload {
        event_id: format!("llm-equity-service-{}", snapshot.status),
        severity: if snapshot.last_error.is_some() {
            "warning".to_string()
        } else {
            "info".to_string()
        },
        category: "lifecycle".to_string(),
        message: snapshot.detail.clone(),
        timestamp_ns: snapshot_timestamp_ns,
    }));
    messages
}

fn portfolio_payload(
    snapshot: &LlmEquityRuntimeSnapshot,
    timestamp_ns: u64,
) -> EquityPortfolioStatePayload {
    portfolio_payload_from_parts(
        &snapshot.portfolio,
        &snapshot.prices,
        &snapshot.stale_price_symbols,
        timestamp_ns,
    )
}

fn portfolio_payload_from_parts(
    portfolio: &LlmEquityPortfolio,
    prices: &HashMap<String, f64>,
    stale_price_symbols: &HashSet<String>,
    timestamp_ns: u64,
) -> EquityPortfolioStatePayload {
    let nav_usd = portfolio.nav_usd(prices);
    let gross_exposure_usd = portfolio.gross_exposure_usd(prices);
    let benchmark_price_usd = benchmark_price_usd(prices);
    let total_pnl_usd = nav_usd - portfolio.initial_cash_usd;
    let total_pnl_pct = if portfolio.initial_cash_usd > 0.0 {
        total_pnl_usd / portfolio.initial_cash_usd
    } else {
        0.0
    };
    let daily_pnl_usd = nav_usd - portfolio.day_start_nav_usd;
    let unrealized_pnl_usd = portfolio
        .positions
        .values()
        .filter_map(|position| {
            prices
                .get(&position.symbol)
                .map(|price| (price - position.avg_price) * position.quantity)
        })
        .sum::<f64>();
    let mut positions = portfolio
        .positions
        .values()
        .filter_map(|position| {
            prices.get(&position.symbol).map(|price| {
                let market_value_usd = position.market_value(*price);
                let cost_basis_usd = position.quantity * position.avg_price;
                let unrealized_pnl_usd = market_value_usd - cost_basis_usd;
                EquityPositionPayload {
                    symbol: position.symbol.clone(),
                    quantity: position.quantity,
                    avg_price: position.avg_price,
                    current_price_usd: *price,
                    cost_basis_usd,
                    market_value_usd,
                    unrealized_pnl_usd,
                    unrealized_pnl_pct: if cost_basis_usd.abs() > 0.0 {
                        unrealized_pnl_usd / cost_basis_usd.abs()
                    } else {
                        0.0
                    },
                    weight_pct: if nav_usd > 0.0 {
                        market_value_usd / nav_usd
                    } else {
                        0.0
                    },
                }
            })
        })
        .collect::<Vec<_>>();
    positions.sort_by(|left, right| left.symbol.cmp(&right.symbol));
    EquityPortfolioStatePayload {
        initial_equity_usd: LLM_EQUITY_INITIAL_CASH_USD,
        cash_usd: portfolio.cash_usd,
        nav_usd,
        gross_exposure_usd,
        daily_pnl_usd,
        total_pnl_usd,
        total_pnl_pct,
        realized_pnl_usd: portfolio.realized_pnl_usd,
        unrealized_pnl_usd,
        max_drawdown_pct: portfolio.max_drawdown_pct(prices),
        positions,
        benchmark_symbol: Some(DEFAULT_EQUITY_SYMBOL.to_string()),
        benchmark_label: Some(DEFAULT_BENCHMARK_LABEL.to_string()),
        benchmark_status: if benchmark_price_usd.is_some()
            && stale_price_symbols.contains(DEFAULT_EQUITY_SYMBOL)
        {
            "stale".to_string()
        } else if benchmark_price_usd.is_some() {
            "live".to_string()
        } else {
            "unavailable".to_string()
        },
        benchmark_price_usd,
        timestamp_ns,
    }
}

fn cycle_summary_payload(summary: &LlmEquityCycleSummary) -> LlmEquityCycleSummaryPayload {
    LlmEquityCycleSummaryPayload {
        cycle_id: summary.cycle_id.clone(),
        cycle_kind: summary.cycle_kind.clone(),
        as_of: summary.as_of.clone(),
        market_session: summary.market_session.clone(),
        status: summary.status.clone(),
        mandatory_trade_status: summary.mandatory_trade_status.clone(),
        required_notional_usd: summary.required_notional_usd,
        requested_notional_usd: summary.requested_notional_usd,
        accepted_notional_usd: summary.accepted_notional_usd,
        actions: summary.actions.clone(),
        symbols: summary.symbols.clone(),
        rationale: summary.rationale.clone(),
        reason: summary.reason.clone(),
        timestamp_ns: summary.timestamp_ns,
    }
}

fn benchmark_price_usd(prices: &HashMap<String, f64>) -> Option<f64> {
    prices
        .get(DEFAULT_EQUITY_SYMBOL)
        .copied()
        .filter(|price| price.is_finite() && *price > 0.0)
}

fn timestamp_ns_from_datetime(timestamp: DateTime<Utc>) -> u64 {
    timestamp
        .timestamp_nanos_opt()
        .map(|value| value.max(0) as u64)
        .unwrap_or_default()
}

fn history_point_represents_current_portfolio(
    history_point: &LlmEquityPortfolioHistoryPoint,
    snapshot: &LlmEquityRuntimeSnapshot,
) -> bool {
    history_point.updated_at.date_naive() == snapshot.updated_at.date_naive()
        && history_point.portfolio == snapshot.portfolio
        && history_point.prices == snapshot.prices
}

fn order_messages(result: &LlmEquityApplyResult, timestamp_ns: u64) -> Vec<EngineStreamMessage> {
    result
        .accepted_orders
        .iter()
        .chain(result.rejected_orders.iter())
        .enumerate()
        .map(|(index, order)| {
            EngineStreamMessage::EquityOrderSimulated(EquityOrderSimulatedPayload {
                order_id: order.command_id.clone(),
                cycle_id: order.cycle_id.clone(),
                symbol: order.symbol.clone(),
                side: match order.side {
                    LlmEquityOrderSide::Buy => "buy",
                    LlmEquityOrderSide::Sell => "sell",
                }
                .to_string(),
                status: order.status.clone(),
                qty: order.qty,
                avg_price: order.avg_price,
                notional_usd: order.notional_usd,
                reason: order.reason.clone(),
                timestamp_ns: timestamp_ns + index as u64 + 1,
            })
        })
        .collect()
}

fn memory_messages(
    result: &LlmEquityApplyResult,
    snapshot: &LlmEquityRuntimeSnapshot,
    timestamp_ns: u64,
) -> Vec<EngineStreamMessage> {
    let Some(decision) = snapshot.latest_decision.as_ref() else {
        return Vec::new();
    };
    decision
        .get("memory_updates")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
        .map(|(index, event)| {
            let fallback = json!({
                "event_id": format!("{}-memory-{}", result.cycle_id, index + 1),
                "cycle_id": result.cycle_id,
                "conclusion": "Hourly memory update recorded.",
                "next_cycle_watch_items": [],
            });
            EngineStreamMessage::LlmMemoryUpdate(with_timestamp(
                merge_object_defaults(event.clone(), fallback),
                timestamp_ns + index as u64 + 16,
            ))
        })
        .collect()
}

fn merge_object_defaults(value: Value, fallback: Value) -> Value {
    let Some(mut object) = value.as_object().cloned() else {
        return fallback;
    };
    if let Some(defaults) = fallback.as_object() {
        for (key, default_value) in defaults {
            object
                .entry(key.clone())
                .or_insert_with(|| default_value.clone());
        }
    }
    Value::Object(object)
}

fn with_timestamp(mut value: Value, timestamp_ns: u64) -> Value {
    if let Some(object) = value.as_object_mut() {
        object
            .entry("timestamp_ns")
            .or_insert_with(|| Value::String(timestamp_ns.to_string()));
    }
    value
}

fn serialize_u64_as_string<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&value.to_string())
}

struct DashboardClientGuard {
    metrics: ExecutionMetrics,
}

impl DashboardClientGuard {
    fn new(metrics: &ExecutionMetrics) -> Self {
        metrics.record_dashboard_ws_client_connected();
        Self {
            metrics: metrics.clone(),
        }
    }
}

impl Drop for DashboardClientGuard {
    fn drop(&mut self) {
        self.metrics.record_dashboard_ws_client_disconnected();
    }
}

pub fn default_equity_symbol() -> &'static str {
    DEFAULT_EQUITY_SYMBOL
}

pub fn log_ws_send_error(error: &anyhow::Error) {
    warn!(%error, "failed to send equity dashboard WebSocket frame");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm_equity::{
        LlmEquityPortfolioHistoryPoint, LlmEquityPosition, LlmEquityServiceStatusMetadata,
    };

    #[test]
    fn initial_feed_is_equity_cash_only() {
        let feed = EngineStreamFeed::llm_equity(LlmEquityRuntimeState::new());
        let messages = feed.current_messages();
        let types = messages
            .iter()
            .map(EngineStreamMessage::message_type)
            .collect::<Vec<_>>();

        assert!(types.contains(&"llm_equity_service_status"));
        assert!(types.contains(&"equity_portfolio_state"));
        assert!(!types.contains(&"llm_equity_cycle"));
    }

    #[test]
    fn service_status_broadcasts_the_current_market_session() {
        let state = LlmEquityRuntimeState::new();
        state
            .update_service_status_with_metadata(
                "llm_research_only".to_string(),
                "Public-news research completed without a trade cycle.".to_string(),
                None,
                Some("no_public_event_score_above_threshold".to_string()),
                LlmEquityServiceStatusMetadata {
                    service_identity: None,
                    market_session: Some("rth".to_string()),
                    activity_key: Some("news-scan:stable-opportunity".to_string()),
                },
            )
            .expect("service session accepted");
        state
            .update_service_status_with_metadata(
                "llm_research_only".to_string(),
                "Public-news research completed without a trade cycle.".to_string(),
                None,
                Some("no_public_event_score_above_threshold".to_string()),
                LlmEquityServiceStatusMetadata {
                    service_identity: None,
                    market_session: Some("rth".to_string()),
                    activity_key: Some("news-scan:stable-opportunity".to_string()),
                },
            )
            .expect("repeated service opportunity accepted");

        let payload = state.snapshot();
        let status = messages_from_snapshot(&payload)
            .into_iter()
            .find_map(|message| match message {
                EngineStreamMessage::LlmEquityServiceStatus(payload) => Some(payload),
                _ => None,
            })
            .expect("service status frame");

        assert_eq!(status.market_session.as_deref(), Some("rth"));
        assert_eq!(
            status.activity_key.as_deref(),
            Some("news-scan:stable-opportunity")
        );
        assert_eq!(status.activity_repeat_count, 2);
        assert!(!status.has_market_research_snapshot);
    }

    #[test]
    fn current_messages_replays_daily_portfolio_history() {
        let first_at = DateTime::parse_from_rfc3339("2026-05-28T14:30:00Z")
            .expect("first timestamp")
            .with_timezone(&Utc);
        let second_at = DateTime::parse_from_rfc3339("2026-05-29T14:30:00Z")
            .expect("second timestamp")
            .with_timezone(&Utc);
        let current_at = DateTime::parse_from_rfc3339("2026-05-30T14:30:00Z")
            .expect("current timestamp")
            .with_timezone(&Utc);
        let mut first_portfolio = LlmEquityPortfolio::seeded();
        first_portfolio.cash_usd = 10_001.0;
        let mut second_portfolio = LlmEquityPortfolio::seeded();
        second_portfolio.cash_usd = 10_002.0;
        let mut current_portfolio = LlmEquityPortfolio::seeded();
        current_portfolio.cash_usd = 10_003.0;
        let mut first_prices = HashMap::new();
        first_prices.insert("SPY".to_string(), 500.0);
        let mut second_prices = HashMap::new();
        second_prices.insert("SPY".to_string(), 505.0);
        let mut current_prices = HashMap::new();
        current_prices.insert("SPY".to_string(), 510.0);
        let snapshot = LlmEquityRuntimeSnapshot {
            status: "cycle_applied".to_string(),
            detail: "הסריקה השעתית הושלמה.".to_string(),
            fill_mode: "paper".to_string(),
            market_session: Some("rth".to_string()),
            service_activity_key: None,
            service_activity_repeat_count: 0,
            service_status_has_market_snapshot: false,
            updated_at: current_at,
            last_cycle_id: None,
            last_error: None,
            portfolio: current_portfolio,
            prices: current_prices,
            stale_price_symbols: HashSet::new(),
            latest_decision: None,
            latest_market_snapshot: None,
            latest_result: None,
            service_identity: None,
            portfolio_history: vec![
                LlmEquityPortfolioHistoryPoint {
                    updated_at: first_at,
                    portfolio: first_portfolio,
                    prices: first_prices,
                },
                LlmEquityPortfolioHistoryPoint {
                    updated_at: second_at,
                    portfolio: second_portfolio,
                    prices: second_prices,
                },
            ],
            dry_run_started_at: Some(first_at),
            benchmark_started_at: Some(first_at),
            benchmark_start_price_usd: Some(500.0),
            cycle_summaries: Vec::new(),
        };

        let payloads = messages_from_snapshot(&snapshot)
            .into_iter()
            .filter_map(|message| match message {
                EngineStreamMessage::EquityPortfolioState(payload) => Some(payload),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(payloads.len(), 3);
        assert_eq!(
            payloads
                .iter()
                .map(|payload| payload.nav_usd)
                .collect::<Vec<_>>(),
            vec![10_001.0, 10_002.0, 10_003.0]
        );
        assert_eq!(
            payloads
                .iter()
                .map(|payload| payload.timestamp_ns)
                .collect::<Vec<_>>(),
            vec![
                timestamp_ns_from_datetime(first_at),
                timestamp_ns_from_datetime(second_at),
                timestamp_ns_from_datetime(current_at),
            ]
        );
        assert_eq!(
            payloads
                .iter()
                .map(|payload| payload.benchmark_symbol.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("SPY"), Some("SPY"), Some("SPY")]
        );
        assert_eq!(
            payloads
                .iter()
                .map(|payload| payload.benchmark_label.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("S&P500"), Some("S&P500"), Some("S&P500")]
        );
        assert_eq!(
            payloads
                .iter()
                .map(|payload| payload.benchmark_price_usd)
                .collect::<Vec<_>>(),
            vec![Some(500.0), Some(505.0), Some(510.0)]
        );
    }

    #[test]
    fn current_messages_dedupes_same_day_matching_current_portfolio() {
        let history_at = DateTime::parse_from_rfc3339("2026-05-28T14:30:00Z")
            .expect("history timestamp")
            .with_timezone(&Utc);
        let current_at = DateTime::parse_from_rfc3339("2026-05-28T15:30:00Z")
            .expect("current timestamp")
            .with_timezone(&Utc);
        let mut portfolio = LlmEquityPortfolio::seeded();
        portfolio.cash_usd = 10_001.0;
        let snapshot = LlmEquityRuntimeSnapshot {
            status: "llm_review_in_progress".to_string(),
            detail: "Later same-day status.".to_string(),
            fill_mode: "shadow".to_string(),
            market_session: Some("rth".to_string()),
            service_activity_key: None,
            service_activity_repeat_count: 0,
            service_status_has_market_snapshot: false,
            updated_at: current_at,
            last_cycle_id: None,
            last_error: None,
            portfolio: portfolio.clone(),
            prices: HashMap::new(),
            stale_price_symbols: HashSet::new(),
            latest_decision: None,
            latest_market_snapshot: None,
            latest_result: None,
            service_identity: None,
            portfolio_history: vec![LlmEquityPortfolioHistoryPoint {
                updated_at: history_at,
                portfolio,
                prices: HashMap::new(),
            }],
            dry_run_started_at: Some(history_at),
            benchmark_started_at: None,
            benchmark_start_price_usd: None,
            cycle_summaries: Vec::new(),
        };

        let payloads = messages_from_snapshot(&snapshot)
            .into_iter()
            .filter_map(|message| match message {
                EngineStreamMessage::EquityPortfolioState(payload) => Some(payload),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].nav_usd, 10_001.0);
        assert_eq!(
            payloads[0].timestamp_ns,
            timestamp_ns_from_datetime(history_at)
        );
    }

    #[test]
    fn portfolio_payload_preserves_non_positive_nav() {
        let mut prices = HashMap::new();
        prices.insert("SPY".to_string(), 1.0);
        let mut portfolio = LlmEquityPortfolio::seeded();
        portfolio.cash_usd = -50.0;
        portfolio.positions.insert(
            "SPY".to_string(),
            LlmEquityPosition {
                symbol: "SPY".to_string(),
                quantity: 25.0,
                avg_price: 500.0,
            },
        );

        let payload =
            portfolio_payload_from_parts(&portfolio, &prices, &HashSet::new(), 1_700_000_000_000);

        assert_eq!(payload.nav_usd, -25.0);
        assert_eq!(payload.cash_usd, -50.0);
        assert_eq!(payload.gross_exposure_usd, 25.0);
        assert_eq!(payload.positions[0].weight_pct, 0.0);
    }

    #[test]
    fn portfolio_payload_reports_total_run_pnl_and_position_worth() {
        let mut prices = HashMap::new();
        prices.insert("SPY".to_string(), 510.0);
        let mut portfolio = LlmEquityPortfolio::seeded();
        portfolio.cash_usd = 9_000.0;
        portfolio.day_start_nav_usd = 10_010.0;
        portfolio.realized_pnl_usd = 4.0;
        portfolio.positions.insert(
            "SPY".to_string(),
            LlmEquityPosition {
                symbol: "SPY".to_string(),
                quantity: 2.0,
                avg_price: 500.0,
            },
        );

        let payload =
            portfolio_payload_from_parts(&portfolio, &prices, &HashSet::new(), 1_700_000_000_000);

        assert_eq!(payload.nav_usd, 10_020.0);
        assert_eq!(payload.total_pnl_usd, 20.0);
        assert_eq!(payload.total_pnl_pct, 0.002);
        assert_eq!(payload.daily_pnl_usd, 10.0);
        assert_eq!(payload.realized_pnl_usd, 4.0);
        assert_eq!(payload.unrealized_pnl_usd, 20.0);
        assert_eq!(payload.positions[0].current_price_usd, 510.0);
        assert_eq!(payload.positions[0].cost_basis_usd, 1_000.0);
        assert_eq!(payload.positions[0].market_value_usd, 1_020.0);
        assert_eq!(payload.positions[0].unrealized_pnl_usd, 20.0);
        assert_eq!(payload.positions[0].unrealized_pnl_pct, 0.02);
    }

    #[test]
    fn portfolio_payload_keeps_benchmark_identity_when_spy_price_unavailable() {
        let mut prices = HashMap::new();
        prices.insert("SPY".to_string(), 0.0);
        let portfolio = LlmEquityPortfolio::seeded();

        let payload =
            portfolio_payload_from_parts(&portfolio, &prices, &HashSet::new(), 1_700_000_000_000);

        assert_eq!(payload.nav_usd, 10_000.0);
        assert_eq!(payload.benchmark_symbol.as_deref(), Some("SPY"));
        assert_eq!(payload.benchmark_label.as_deref(), Some("S&P500"));
        assert_eq!(payload.benchmark_status, "unavailable");
        assert_eq!(payload.benchmark_price_usd, None);
    }

    #[test]
    fn portfolio_payload_marks_retained_spy_benchmark_as_stale() {
        let mut prices = HashMap::new();
        prices.insert("SPY".to_string(), 505.0);
        let stale_price_symbols = HashSet::from(["SPY".to_string()]);
        let portfolio = LlmEquityPortfolio::seeded();

        let payload = portfolio_payload_from_parts(
            &portfolio,
            &prices,
            &stale_price_symbols,
            1_700_000_000_000,
        );

        assert_eq!(payload.benchmark_symbol.as_deref(), Some("SPY"));
        assert_eq!(payload.benchmark_label.as_deref(), Some("S&P500"));
        assert_eq!(payload.benchmark_status, "stale");
        assert_eq!(payload.benchmark_price_usd, Some(505.0));
    }
}
