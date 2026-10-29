use crate::llm_equity::{
    LlmEquityAuthorityError, LlmEquityCycleSummary, LlmEquityRuntimeState,
    LlmEquityServiceStatusMetadata, LLM_EQUITY_INITIAL_CASH_USD,
};
use crate::runtime::RunMode;
use crate::telemetry::metrics::ExecutionMetrics;
use chrono::Utc;
use ring::digest::{digest, SHA256};
use serde_json::{json, Value};
use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tracing::warn;

pub const DEFAULT_ADMIN_BIND_ADDR: &str = "127.0.0.1:8090";
pub const ADMIN_BIND_ADDR_ENV: &str = "TRAIDAN_ADMIN_BIND_ADDR";
pub const ADMIN_TOKEN_ENV: &str = "TRAIDAN_ADMIN_TOKEN";
pub const ADMIN_TOKEN_FILE_ENV: &str = "TRAIDAN_ADMIN_TOKEN_FILE";
pub const ADMIN_STATUS_PATH: &str = "/admin/status";
pub const LLM_EQUITY_CYCLE_PATH: &str = "/internal/llm-equity/cycle";
pub const LLM_EQUITY_CYCLE_SUMMARY_PATH: &str = "/internal/llm-equity/cycle-summary";
pub const LLM_EQUITY_SERVICE_STATUS_PATH: &str = "/internal/llm-equity/service-status";
// Dynamic LLM equity snapshots are larger than the historical 32 KiB cap; keep bounded headroom.
pub const ADMIN_MAX_REQUEST_BYTES: usize = 256 * 1024;
pub const ADMIN_READ_TIMEOUT: Duration = Duration::from_secs(5);

const MIN_OPERATOR_TOKEN_LEN: usize = 32;
const HTTP_DATE_FORMAT: &str = "%a, %d %b %Y %H:%M:%S GMT";
const DASHBOARD_ORIGIN_ENV: &str = "TRAIDAN_DASHBOARD_ORIGIN";
const LLM_LAST_CYCLE_COST_ENV: &str = "TRAIDAN_LLM_LAST_CYCLE_COST_USD";
const LLM_SWARM_MONTHLY_MODEL_BUDGET_ENV: &str = "TRAIDAN_LLM_SWARM_MONTHLY_MODEL_BUDGET_USD";
const DEFAULT_LLM_SWARM_MONTHLY_MODEL_BUDGET_USD: f64 = 750.0;
const LLM_ESTIMATED_MONTHLY_CYCLES_ENV: &str = "TRAIDAN_LLM_EQUITY_ESTIMATED_MONTHLY_CYCLES";
const DEFAULT_ESTIMATED_MONTHLY_LLM_CYCLES: f64 = 147.0;
const LLM_TOKEN_COST_ENVS: [&str; 3] = [
    "TRAIDAN_LLM_EQUITY_INPUT_TOKENS",
    "TRAIDAN_LLM_EQUITY_CACHED_INPUT_TOKENS",
    "TRAIDAN_LLM_EQUITY_OUTPUT_TOKENS",
];
const LLM_REQUIRED_ALPHA_COST_ENV_PAIRS: [(&str, &str); 5] = [
    (
        "TRAIDAN_LLM_EQUITY_MONTHLY_MODEL_COST_USD",
        "TRAIDAN_LLM_MONTHLY_MODEL_COST_USD",
    ),
    (
        "TRAIDAN_LLM_EQUITY_MONTHLY_DATA_COST_USD",
        "TRAIDAN_LLM_MONTHLY_DATA_COST_USD",
    ),
    (
        "TRAIDAN_LLM_EQUITY_MONTHLY_HOSTING_COST_USD",
        "TRAIDAN_LLM_MONTHLY_HOSTING_COST_USD",
    ),
    (
        "TRAIDAN_LLM_EQUITY_MONTHLY_EXECUTION_COST_USD",
        "TRAIDAN_LLM_MONTHLY_EXECUTION_COST_USD",
    ),
    (
        "TRAIDAN_LLM_EQUITY_MONTHLY_SAFETY_MARGIN_USD",
        "TRAIDAN_LLM_MONTHLY_SAFETY_MARGIN_USD",
    ),
];

#[derive(Debug, Clone)]
pub struct AdminApiConfig {
    bind_addr: String,
    operator_token: OperatorToken,
    run_mode: RunMode,
    live_order_capability_disabled: bool,
}

impl AdminApiConfig {
    pub fn new(
        bind_addr: impl Into<String>,
        operator_token: OperatorToken,
        run_mode: RunMode,
        live_order_capability_disabled: bool,
    ) -> Self {
        Self {
            bind_addr: bind_addr.into(),
            operator_token,
            run_mode,
            live_order_capability_disabled,
        }
    }

    pub fn from_env(
        run_mode: RunMode,
        live_order_capability_disabled: bool,
    ) -> Result<Option<Self>, AdminTokenError> {
        let env_token = env::var(ADMIN_TOKEN_ENV).ok();
        let token_file = env::var(ADMIN_TOKEN_FILE_ENV).ok();
        let Some(operator_token) = load_operator_token_from_sources(
            env_token.as_deref(),
            token_file.as_deref(),
            |path| fs::read_to_string(path),
        )?
        else {
            return Ok(None);
        };

        let bind_addr = admin_bind_addr();
        if !is_loopback_admin_bind_addr(&bind_addr) {
            return Err(AdminTokenError::UnsafeBindAddr { bind_addr });
        }

        Ok(Some(Self::new(
            bind_addr,
            operator_token,
            run_mode,
            live_order_capability_disabled,
        )))
    }

    pub fn bind_addr(&self) -> &str {
        &self.bind_addr
    }

    fn operator_token(&self) -> &OperatorToken {
        &self.operator_token
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorToken {
    digest: [u8; 32],
    source: TokenSource,
}

impl OperatorToken {
    pub fn new(token: &str, source: TokenSource) -> Result<Self, AdminTokenError> {
        let trimmed = token.trim();
        if trimmed.len() < MIN_OPERATOR_TOKEN_LEN || !trimmed.is_ascii() {
            return Err(AdminTokenError::InvalidToken);
        }
        Ok(Self {
            digest: digest_token(trimmed),
            source,
        })
    }

    fn matches(&self, candidate: &str) -> bool {
        constant_time_eq(&self.digest, &digest_token(candidate.trim()))
    }

    fn source_label(&self) -> &'static str {
        match self.source {
            TokenSource::Environment => "environment",
            TokenSource::File => "file",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenSource {
    Environment,
    File,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminTokenError {
    InvalidToken,
    TokenFileUnreadable { path: String },
    UnsafeBindAddr { bind_addr: String },
}

impl fmt::Display for AdminTokenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidToken => write!(
                f,
                "admin token must be at least {MIN_OPERATOR_TOKEN_LEN} visible ASCII characters"
            ),
            Self::TokenFileUnreadable { path } => {
                write!(f, "admin token file could not be read: {path}")
            }
            Self::UnsafeBindAddr { bind_addr } => {
                write!(f, "{ADMIN_BIND_ADDR_ENV} must be loopback, got {bind_addr}")
            }
        }
    }
}

impl Error for AdminTokenError {}

#[derive(Clone)]
pub struct AdminApiState {
    config: Arc<AdminApiConfig>,
    equity_state: LlmEquityRuntimeState,
    metrics: ExecutionMetrics,
}

impl AdminApiState {
    pub fn new(
        config: AdminApiConfig,
        equity_state: LlmEquityRuntimeState,
        metrics: ExecutionMetrics,
    ) -> Self {
        Self {
            config: Arc::new(config),
            equity_state,
            metrics,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AdminHttpResponse {
    status: &'static str,
    body: Vec<u8>,
    content_type: &'static str,
    cors_origin: Option<String>,
}

impl AdminHttpResponse {
    fn json(status: &'static str, body: Value) -> Self {
        Self {
            status,
            body: serde_json::to_vec(&body).expect("admin JSON response encodes"),
            content_type: "application/json",
            cors_origin: None,
        }
    }

    fn text(status: &'static str, body: &'static str) -> Self {
        Self {
            status,
            body: body.as_bytes().to_vec(),
            content_type: "text/plain; charset=utf-8",
            cors_origin: None,
        }
    }

    fn with_cors_origin(mut self, origin: Option<&str>) -> Self {
        self.cors_origin = origin.map(ToString::to_string);
        self
    }
}

#[derive(Debug)]
struct AdminHttpRequest {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

pub async fn serve_admin_api(bind_addr: &str, state: AdminApiState) -> io::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(error) = serve_admin_stream(stream, state).await {
                warn!(%error, "admin API request failed");
            }
        });
    }
}

pub async fn serve_admin_api_once(listener: TcpListener, state: AdminApiState) -> io::Result<()> {
    let (stream, _) = listener.accept().await?;
    serve_admin_stream(stream, state).await
}

async fn serve_admin_stream(mut stream: TcpStream, state: AdminApiState) -> io::Result<()> {
    let request = match timeout(ADMIN_READ_TIMEOUT, read_bounded_request(&mut stream)).await {
        Ok(Ok(request)) => request,
        Ok(Err(error)) if error.kind() == io::ErrorKind::InvalidData => {
            return write_admin_response(
                &mut stream,
                AdminHttpResponse::text("413 Payload Too Large", "request too large\n"),
            )
            .await;
        }
        Ok(Err(error)) => return Err(error),
        Err(_) => {
            return write_admin_response(
                &mut stream,
                AdminHttpResponse::text("408 Request Timeout", "request timeout\n"),
            )
            .await;
        }
    };
    let response = handle_admin_request(&request.bytes, &state);
    write_admin_response(&mut stream, response).await
}

pub fn handle_admin_request(request: &[u8], state: &AdminApiState) -> AdminHttpResponse {
    let request = match parse_admin_request(request) {
        Some(request) => request,
        None => return AdminHttpResponse::text("400 Bad Request", "bad request\n"),
    };
    handle_parsed_admin_request(&request, state)
}

fn handle_parsed_admin_request(
    request: &AdminHttpRequest,
    state: &AdminApiState,
) -> AdminHttpResponse {
    let origin = request
        .header("origin")
        .filter(|origin| is_allowed_admin_origin(origin));

    if request.method == "OPTIONS" {
        return AdminHttpResponse::text("204 No Content", "").with_cors_origin(origin);
    }

    let path = request
        .path
        .split('?')
        .next()
        .unwrap_or(request.path.as_str());
    if path != ADMIN_STATUS_PATH
        && path != LLM_EQUITY_CYCLE_PATH
        && path != LLM_EQUITY_CYCLE_SUMMARY_PATH
        && path != LLM_EQUITY_SERVICE_STATUS_PATH
    {
        return AdminHttpResponse::json(
            "404 Not Found",
            json!({"status": "failed", "reason": "not_found"}),
        )
        .with_cors_origin(origin);
    }

    if !authenticate_admin_request(request, state.config.operator_token()) {
        return AdminHttpResponse::json(
            "401 Unauthorized",
            json!({"status": "failed", "reason": "unauthorized"}),
        )
        .with_cors_origin(origin);
    }

    match (request.method.as_str(), path) {
        ("GET", ADMIN_STATUS_PATH) => {
            AdminHttpResponse::json("200 OK", status_payload(state)).with_cors_origin(origin)
        }
        ("POST", LLM_EQUITY_CYCLE_PATH) => {
            handle_llm_equity_cycle(request, state).with_cors_origin(origin)
        }
        ("POST", LLM_EQUITY_CYCLE_SUMMARY_PATH) => {
            handle_llm_equity_cycle_summary(request, state).with_cors_origin(origin)
        }
        ("POST", LLM_EQUITY_SERVICE_STATUS_PATH) => {
            handle_llm_equity_service_status(request, state).with_cors_origin(origin)
        }
        (
            _,
            ADMIN_STATUS_PATH
            | LLM_EQUITY_CYCLE_PATH
            | LLM_EQUITY_CYCLE_SUMMARY_PATH
            | LLM_EQUITY_SERVICE_STATUS_PATH,
        ) => AdminHttpResponse::json(
            "405 Method Not Allowed",
            json!({"status": "failed", "reason": "method_not_allowed"}),
        )
        .with_cors_origin(origin),
        _ => AdminHttpResponse::json(
            "404 Not Found",
            json!({"status": "failed", "reason": "not_found"}),
        )
        .with_cors_origin(origin),
    }
}

fn handle_llm_equity_service_status(
    request: &AdminHttpRequest,
    state: &AdminApiState,
) -> AdminHttpResponse {
    let payload = match serde_json::from_slice::<Value>(&request.body) {
        Ok(Value::Object(payload)) => payload,
        Ok(_) | Err(_) => {
            return AdminHttpResponse::json(
                "400 Bad Request",
                json!({"status": "failed", "reason": "invalid_json"}),
            );
        }
    };

    let Some(status) = payload
        .get("status")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return AdminHttpResponse::json(
            "400 Bad Request",
            json!({"status": "failed", "reason": "status_required"}),
        );
    };
    let Some(detail) = payload
        .get("detail")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return AdminHttpResponse::json(
            "400 Bad Request",
            json!({"status": "failed", "reason": "detail_required"}),
        );
    };
    let last_error = payload
        .get("last_error")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let market_snapshot = payload.get("market_research_snapshot").cloned();
    let market_session = match payload.get("market_session") {
        Some(Value::String(value)) => Some(value.clone()),
        Some(_) => {
            return AdminHttpResponse::json(
                "422 Unprocessable Entity",
                json!({"status": "rejected", "reason": "market_session_invalid"}),
            );
        }
        None => None,
    };
    let service_activity_key = match payload.get("activity_key") {
        Some(Value::String(value)) => Some(value.clone()),
        Some(_) => {
            return AdminHttpResponse::json(
                "422 Unprocessable Entity",
                json!({"status": "rejected", "reason": "service_activity_key_invalid"}),
            );
        }
        None => None,
    };
    let terminal_swarm_status = matches!(
        status,
        "llm_research_only" | "llm_edge_gate_skipped" | "llm_release_skipped_no_edge"
    );

    match state.equity_state.update_service_status_with_metadata(
        status.to_string(),
        detail.to_string(),
        market_snapshot,
        last_error,
        LlmEquityServiceStatusMetadata {
            service_identity: payload.get("runtime_identity").cloned(),
            market_session,
            activity_key: service_activity_key,
        },
    ) {
        Ok(()) => {
            record_llm_swarm_metrics(
                &state.metrics,
                payload.get("llm_swarm"),
                terminal_swarm_status,
            );
            AdminHttpResponse::json(
                "200 OK",
                json!({"status": "success", "service_status": status}),
            )
        }
        Err(error) => AdminHttpResponse::json(
            "422 Unprocessable Entity",
            json!({"status": "rejected", "reason": error.to_string()}),
        ),
    }
}

fn handle_llm_equity_cycle_summary(
    request: &AdminHttpRequest,
    state: &AdminApiState,
) -> AdminHttpResponse {
    let payload = match serde_json::from_slice::<Value>(&request.body) {
        Ok(Value::Object(payload)) => payload,
        Ok(_) | Err(_) => {
            return AdminHttpResponse::json(
                "400 Bad Request",
                json!({"status": "failed", "reason": "invalid_json"}),
            );
        }
    };
    let summary = match cycle_summary_from_payload(&Value::Object(payload)) {
        Ok(summary) => summary,
        Err(reason) => {
            return AdminHttpResponse::json(
                "422 Unprocessable Entity",
                json!({"status": "rejected", "reason": reason}),
            );
        }
    };
    match state.equity_state.record_cycle_summary(summary) {
        Ok(true) => AdminHttpResponse::json("200 OK", json!({"status": "success"})),
        Ok(false) => AdminHttpResponse::json(
            "200 OK",
            json!({"status": "ignored", "reason": "cycle_summary_not_trade_ledger"}),
        ),
        Err(error) => AdminHttpResponse::json(
            "422 Unprocessable Entity",
            json!({"status": "rejected", "reason": error.to_string()}),
        ),
    }
}

fn handle_llm_equity_cycle(request: &AdminHttpRequest, state: &AdminApiState) -> AdminHttpResponse {
    let payload = match serde_json::from_slice::<Value>(&request.body) {
        Ok(Value::Object(payload)) => payload,
        Ok(_) | Err(_) => {
            return AdminHttpResponse::json(
                "400 Bad Request",
                json!({"status": "failed", "reason": "invalid_json"}),
            );
        }
    };

    let Some(decision) = payload.get("llm_trading_decision").cloned() else {
        return AdminHttpResponse::json(
            "400 Bad Request",
            json!({"status": "failed", "reason": "llm_trading_decision_required"}),
        );
    };
    let Some(market_snapshot) = payload.get("market_research_snapshot").cloned() else {
        return AdminHttpResponse::json(
            "400 Bad Request",
            json!({"status": "failed", "reason": "market_research_snapshot_required"}),
        );
    };

    match state
        .equity_state
        .ingest_cycle(decision, market_snapshot.clone())
    {
        Ok(result) => {
            let nav = result.portfolio.nav_usd(&result.prices);
            let pnl_pct = (nav - LLM_EQUITY_INITIAL_CASH_USD) / LLM_EQUITY_INITIAL_CASH_USD;
            let drawdown = result.portfolio.max_drawdown_pct(&result.prices);
            let gross_exposure_usd = result.portfolio.gross_exposure_usd(&result.prices);
            let net_exposure_usd = result
                .portfolio
                .positions
                .values()
                .filter_map(|position| {
                    result
                        .prices
                        .get(&position.symbol)
                        .map(|price| position.market_value(*price))
                })
                .sum::<f64>();
            let short_gross_exposure_usd = result
                .portfolio
                .positions
                .values()
                .filter_map(|position| {
                    result
                        .prices
                        .get(&position.symbol)
                        .map(|price| (-position.market_value(*price)).max(0.0))
                })
                .sum::<f64>();
            let daily_loss_pct = if result.portfolio.day_start_nav_usd > 0.0 {
                ((result.portfolio.day_start_nav_usd - nav).max(0.0)
                    / result.portfolio.day_start_nav_usd)
                    .max(0.0)
            } else {
                0.0
            };
            let exposure_denominator = nav.max(f64::EPSILON);
            let profitability = llm_profitability_metrics(
                &market_snapshot,
                nav,
                result.portfolio.day_start_nav_usd,
            );
            for order in result
                .accepted_orders
                .iter()
                .chain(result.rejected_orders.iter())
            {
                state
                    .metrics
                    .record_paper_order(&order.symbol, &order.status, &order.reason);
            }
            state.metrics.set_paper_portfolio_metrics(
                "SPY",
                nav,
                pnl_pct,
                result.portfolio.turnover_this_hour_usd,
                drawdown,
            );
            state.metrics.set_llm_profitability_metrics(
                "SPY",
                profitability.cycle_cost_usd,
                profitability.required_alpha_usd,
                profitability.benchmark_nav_usd,
                profitability.net_alpha_usd,
                profitability.source_health_ratio,
            );
            record_llm_swarm_metrics(&state.metrics, payload.get("llm_swarm"), true);
            let order_results = result
                .accepted_orders
                .iter()
                .chain(result.rejected_orders.iter())
                .map(|order| {
                    json!({
                        "command_id": order.command_id,
                        "symbol": order.symbol,
                        "side": order.side.as_str(),
                        "status": order.status,
                        "reason": order.reason,
                        "quantity": order.qty,
                        "avg_price": order.avg_price,
                        "notional_usd": order.notional_usd
                    })
                })
                .collect::<Vec<_>>();
            AdminHttpResponse::json(
                "200 OK",
                json!({
                    "status": "success",
                    "cycle_id": result.cycle_id,
                    "accepted_orders": result.accepted_orders.len(),
                    "rejected_orders": result.rejected_orders.len(),
                    "order_results": order_results,
                    "portfolio_nav_usd": nav,
                    "total_pnl_usd": nav - LLM_EQUITY_INITIAL_CASH_USD,
                    "gross_exposure_pct": gross_exposure_usd / exposure_denominator,
                    "net_exposure_abs_pct": net_exposure_usd.abs() / exposure_denominator,
                    "short_gross_exposure_pct": short_gross_exposure_usd / exposure_denominator,
                    "daily_turnover_pct": result.turnover_today_usd / exposure_denominator,
                    "daily_loss_pct": daily_loss_pct,
                    "max_drawdown_pct": drawdown
                }),
            )
        }
        Err(error) => {
            // A durable allocation may already exist when its journal write
            // fails. Clients must reconcile the identical request on 500.
            let pending = matches!(error, LlmEquityAuthorityError::Persistence(_));
            AdminHttpResponse::json(
                if pending {
                    "500 Internal Server Error"
                } else {
                    "422 Unprocessable Entity"
                },
                json!({"status": if pending { "pending" } else { "rejected" }, "reason": error.to_string()}),
            )
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct LlmProfitabilityMetrics {
    cycle_cost_usd: f64,
    required_alpha_usd: f64,
    benchmark_nav_usd: f64,
    net_alpha_usd: f64,
    source_health_ratio: f64,
}

fn llm_profitability_metrics(
    market_snapshot: &Value,
    nav_usd: f64,
    day_start_nav_usd: f64,
) -> LlmProfitabilityMetrics {
    let cycle_cost_usd = cycle_cost_usd_from_env();
    let monthly_model_cost_override = env_optional_non_negative_f64_first(&[
        LLM_REQUIRED_ALPHA_COST_ENV_PAIRS[0].0,
        LLM_REQUIRED_ALPHA_COST_ENV_PAIRS[0].1,
    ]);
    let monthly_model_budget_usd =
        env_optional_non_negative_f64(LLM_SWARM_MONTHLY_MODEL_BUDGET_ENV)
            .unwrap_or(DEFAULT_LLM_SWARM_MONTHLY_MODEL_BUDGET_USD);
    let estimated_monthly_cycles = env_optional_non_negative_f64(LLM_ESTIMATED_MONTHLY_CYCLES_ENV)
        .unwrap_or(DEFAULT_ESTIMATED_MONTHLY_LLM_CYCLES);
    let modeled_monthly_model_cost_usd = cycle_cost_usd * estimated_monthly_cycles;
    let monthly_model_cost_usd = monthly_model_cost_override
        .unwrap_or_else(|| modeled_monthly_model_cost_usd.max(monthly_model_budget_usd));
    let required_alpha_usd = monthly_model_cost_usd
        + LLM_REQUIRED_ALPHA_COST_ENV_PAIRS
            .iter()
            .skip(1)
            .map(|(primary, legacy)| env_non_negative_f64_first(&[*primary, *legacy]))
            .sum::<f64>();
    let benchmark_base_nav = if day_start_nav_usd.is_finite() && day_start_nav_usd > 0.0 {
        day_start_nav_usd
    } else {
        LLM_EQUITY_INITIAL_CASH_USD
    };
    let benchmark_nav_usd = benchmark_nav_from_snapshot(market_snapshot, "SPY", benchmark_base_nav)
        .unwrap_or(benchmark_base_nav);
    LlmProfitabilityMetrics {
        cycle_cost_usd,
        required_alpha_usd,
        benchmark_nav_usd,
        net_alpha_usd: nav_usd - benchmark_nav_usd - cycle_cost_usd,
        source_health_ratio: source_health_ratio_from_snapshot(market_snapshot),
    }
}

fn benchmark_nav_from_snapshot(
    market_snapshot: &Value,
    symbol: &str,
    benchmark_base_nav: f64,
) -> Option<f64> {
    let quote = market_snapshot.get("latest_quotes")?.get(symbol)?;
    let bar = market_snapshot.get("latest_bars")?.get(symbol)?;
    let bid = quote.get("bid")?.as_f64()?;
    let ask = quote.get("ask")?.as_f64()?;
    let open = bar.get("open")?.as_f64()?;
    if !(bid.is_finite()
        && ask.is_finite()
        && open.is_finite()
        && bid > 0.0
        && ask > 0.0
        && open > 0.0)
    {
        return None;
    }
    let mid = (bid + ask) / 2.0;
    Some(benchmark_base_nav * (mid / open))
}

fn source_health_ratio_from_snapshot(market_snapshot: &Value) -> f64 {
    let Some(rows) = market_snapshot
        .get("source_freshness")
        .and_then(Value::as_array)
    else {
        return 0.0;
    };
    if rows.is_empty() {
        return 0.0;
    }
    let fresh = rows
        .iter()
        .filter(|row| {
            row.get("freshness")
                .and_then(Value::as_str)
                .is_some_and(|freshness| freshness == "fresh")
        })
        .count();
    fresh as f64 / rows.len() as f64
}

fn record_llm_swarm_metrics(
    metrics: &ExecutionMetrics,
    llm_swarm: Option<&Value>,
    record_terminal_counts: bool,
) {
    let Some(swarm) = llm_swarm.and_then(Value::as_object) else {
        return;
    };
    if let Some(consensus) = swarm.get("consensus_score").and_then(Value::as_f64) {
        metrics.set_llm_swarm_consensus(consensus);
    }
    if let Some(strategy_books) = swarm.get("strategy_books").and_then(Value::as_object) {
        for (book, row) in strategy_books {
            let Some(nav_usd) = row.get("nav_usd").and_then(Value::as_f64) else {
                continue;
            };
            let Some(alpha_usd) = row.get("alpha_usd").and_then(Value::as_f64) else {
                continue;
            };
            metrics.set_llm_strategy_book_metrics(book, nav_usd, alpha_usd);
        }
    }
    if !record_terminal_counts {
        return;
    }
    if let Some(count) = swarm.get("alpha_card_count").and_then(Value::as_u64) {
        metrics.record_llm_alpha_cards(count);
    }
    let status = swarm
        .get("edge_gate_status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let reason = swarm
        .get("no_trade_reason")
        .and_then(Value::as_str)
        .or_else(|| swarm.get("edge_gate_reason").and_then(Value::as_str))
        .unwrap_or("unknown");
    metrics.record_llm_edge_gate(status, reason);
}

fn env_non_negative_f64(name: &str) -> f64 {
    env_optional_non_negative_f64(name).unwrap_or(0.0)
}

fn env_non_negative_f64_first(names: &[&str]) -> f64 {
    env_optional_non_negative_f64_first(names).unwrap_or(0.0)
}

fn env_optional_non_negative_f64_first(names: &[&str]) -> Option<f64> {
    names
        .iter()
        .find_map(|name| env_optional_non_negative_f64(name))
}

fn cycle_cost_usd_from_env() -> f64 {
    let input_tokens = env_optional_non_negative_f64(LLM_TOKEN_COST_ENVS[0]);
    let cached_tokens = env_optional_non_negative_f64(LLM_TOKEN_COST_ENVS[1]);
    let output_tokens = env_optional_non_negative_f64(LLM_TOKEN_COST_ENVS[2]);
    if input_tokens.is_some() || cached_tokens.is_some() || output_tokens.is_some() {
        return input_tokens.unwrap_or(0.0) / 1_000_000.0 * 5.00
            + cached_tokens.unwrap_or(0.0) / 1_000_000.0 * 0.50
            + output_tokens.unwrap_or(0.0) / 1_000_000.0 * 30.00;
    }
    env_non_negative_f64(LLM_LAST_CYCLE_COST_ENV)
}

fn env_optional_non_negative_f64(name: &str) -> Option<f64> {
    env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0)
}

fn cycle_summary_from_payload(payload: &Value) -> Result<LlmEquityCycleSummary, &'static str> {
    let summary = LlmEquityCycleSummary {
        cycle_id: required_payload_string(payload, "cycle_id", 160)?,
        cycle_kind: required_payload_string(payload, "cycle_kind", 80)?,
        as_of: required_payload_string(payload, "as_of", 80)?,
        market_session: required_payload_string(payload, "market_session", 40)?,
        status: required_payload_string(payload, "status", 40)?,
        mandatory_trade_status: required_payload_string(payload, "mandatory_trade_status", 40)?,
        required_notional_usd: required_non_negative_payload_number(
            payload,
            "required_notional_usd",
        )?,
        requested_notional_usd: required_non_negative_payload_number(
            payload,
            "requested_notional_usd",
        )?,
        accepted_notional_usd: required_non_negative_payload_number(
            payload,
            "accepted_notional_usd",
        )?,
        actions: required_payload_string_array(payload, "actions", 80, 40)?,
        symbols: required_payload_string_array(payload, "symbols", 80, 20)?,
        rationale: required_payload_string(payload, "rationale", 1_200)?,
        reason: required_payload_string(payload, "reason", 240)?,
        timestamp_ns: required_payload_timestamp_ns(payload, "timestamp_ns")?,
    };
    crate::llm_equity::validate_cycle_summary(&summary).map_err(|_| "cycle_summary_invalid")?;
    Ok(summary)
}

fn required_payload_string(
    payload: &Value,
    key: &'static str,
    max_len: usize,
) -> Result<String, &'static str> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.len() <= max_len)
        .map(ToString::to_string)
        .ok_or("string_field_invalid")
}

fn required_payload_string_array(
    payload: &Value,
    key: &'static str,
    max_items: usize,
    max_len: usize,
) -> Result<Vec<String>, &'static str> {
    let values = payload
        .get(key)
        .and_then(Value::as_array)
        .filter(|items| !items.is_empty() && items.len() <= max_items)
        .ok_or("array_field_invalid")?;
    let mut result = Vec::new();
    for value in values {
        let text = value
            .as_str()
            .map(str::trim)
            .filter(|item| !item.is_empty() && item.len() <= max_len)
            .ok_or("array_field_invalid")?;
        result.push(text.to_string());
    }
    Ok(result)
}

fn required_non_negative_payload_number(
    payload: &Value,
    key: &'static str,
) -> Result<f64, &'static str> {
    payload
        .get(key)
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && *value >= 0.0)
        .ok_or("number_field_invalid")
}

fn required_payload_timestamp_ns(payload: &Value, key: &'static str) -> Result<u64, &'static str> {
    let Some(value) = payload.get(key) else {
        return Err("timestamp_field_invalid");
    };
    match value {
        Value::String(text) => text.parse::<u64>().map_err(|_| "timestamp_field_invalid"),
        Value::Number(number) => number.as_u64().ok_or("timestamp_field_invalid"),
        _ => Err("timestamp_field_invalid"),
    }
}

fn status_payload(state: &AdminApiState) -> Value {
    let snapshot = state.equity_state.snapshot();
    let nav_usd = snapshot.portfolio.nav_usd(&snapshot.prices);
    let gross_exposure_usd = snapshot.portfolio.gross_exposure_usd(&snapshot.prices);
    let mut positions = snapshot
        .portfolio
        .positions
        .values()
        .map(|position| {
            let market_value_usd = snapshot
                .prices
                .get(&position.symbol)
                .map(|price| position.market_value(*price))
                .unwrap_or(0.0);
            json!({
                "symbol": position.symbol.clone(),
                "quantity": position.quantity,
                "avg_price": position.avg_price,
                "market_value_usd": market_value_usd
            })
        })
        .collect::<Vec<_>>();
    positions.sort_by(|left, right| {
        left["symbol"]
            .as_str()
            .unwrap_or_default()
            .cmp(right["symbol"].as_str().unwrap_or_default())
    });
    json!({
        "status": "ok",
        "service": "llm_equity_dry_run",
        "run_mode": state.config.run_mode.as_str(),
        "live_order_capability_disabled": state.config.live_order_capability_disabled,
        "admin_api": {
            "auth": "bearer_token",
            "token_source": state.config.operator_token.source_label(),
            "bind_addr": state.config.bind_addr(),
            "localhost_only": true
        },
        "llm_equity": {
            "status": snapshot.status,
            "detail": snapshot.detail,
            "fill_mode": snapshot.fill_mode,
            "runtime_identity": {
                "authority_schema_version": 1,
                "rust_build_ref": option_env!("CI_COMMIT_SHA").unwrap_or(env!("CARGO_PKG_VERSION")),
                "rust_package_version": env!("CARGO_PKG_VERSION"),
                "service": snapshot.service_identity
            },
            "last_cycle_id": snapshot.last_cycle_id,
            "activity_key": snapshot.service_activity_key,
            "activity_repeat_count": snapshot.service_activity_repeat_count,
            "has_market_research_snapshot": snapshot.service_status_has_market_snapshot,
            "last_error": snapshot.last_error,
            "initial_cash_usd": LLM_EQUITY_INITIAL_CASH_USD,
            "cash_usd": snapshot.portfolio.cash_usd,
            "nav_usd": nav_usd,
            "portfolio": {
                "initial_cash_usd": snapshot.portfolio.initial_cash_usd,
                "cash_usd": snapshot.portfolio.cash_usd,
                "nav_usd": nav_usd,
                "gross_exposure_usd": gross_exposure_usd,
                "realized_pnl_usd": snapshot.portfolio.realized_pnl_usd,
                "turnover_this_hour_usd": snapshot.portfolio.turnover_this_hour_usd,
                "peak_nav_usd": snapshot.portfolio.peak_nav_usd,
                "day_start_nav_usd": snapshot.portfolio.day_start_nav_usd,
                "positions": positions
            }
        },
        "component_health": [
            {"name": "admin_api", "status": "ok"},
            {"name": "llm_equity_authority", "status": snapshot.status}
        ],
        "timestamp": Utc::now().to_rfc3339()
    })
}

fn load_operator_token_from_sources<F>(
    env_token: Option<&str>,
    token_file: Option<&str>,
    read_to_string: F,
) -> Result<Option<OperatorToken>, AdminTokenError>
where
    F: FnOnce(&str) -> io::Result<String>,
{
    if let Some(token) = env_token.filter(|token| !token.trim().is_empty()) {
        return OperatorToken::new(token, TokenSource::Environment).map(Some);
    }
    if let Some(path) = token_file.filter(|path| !path.trim().is_empty()) {
        let token = read_to_string(path).map_err(|_| AdminTokenError::TokenFileUnreadable {
            path: path.to_string(),
        })?;
        return OperatorToken::new(&token, TokenSource::File).map(Some);
    }
    Ok(None)
}

fn admin_bind_addr() -> String {
    env::var(ADMIN_BIND_ADDR_ENV).unwrap_or_else(|_| DEFAULT_ADMIN_BIND_ADDR.to_string())
}

pub fn is_loopback_admin_bind_addr(bind_addr: &str) -> bool {
    let trimmed = bind_addr.trim();
    if trimmed.starts_with("localhost:") {
        return true;
    }
    trimmed
        .parse::<SocketAddr>()
        .map(|addr| addr.ip().is_loopback())
        .unwrap_or(false)
}

pub fn is_allowed_admin_origin(origin: &str) -> bool {
    let configured = env::var(DASHBOARD_ORIGIN_ENV).ok();
    is_allowed_admin_origin_with_config(origin, configured.as_deref())
}

fn is_allowed_admin_origin_with_config(origin: &str, configured: Option<&str>) -> bool {
    let Some((scheme, authority)) = origin.split_once("://") else {
        return false;
    };
    if !matches!(scheme, "http" | "https")
        || authority.is_empty()
        || authority
            .chars()
            .any(|c| c.is_whitespace() || c.is_control())
        || authority.contains(['/', '?', '#', '@', '\\'])
    {
        return false;
    }
    let host = if let Some((host, port)) = authority.split_once(':') {
        if !matches!(port.parse::<u16>(), Ok(1..=u16::MAX)) {
            return false;
        }
        host
    } else {
        authority
    };
    matches!(host, "localhost" | "127.0.0.1") || (scheme == "https" && configured == Some(origin))
}

fn authenticate_admin_request(request: &AdminHttpRequest, expected_token: &OperatorToken) -> bool {
    let Some(header) = request.header("authorization") else {
        return false;
    };
    let Some(token) = header.strip_prefix("Bearer ") else {
        return false;
    };
    expected_token.matches(token)
}

impl AdminHttpRequest {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

fn parse_admin_request(request: &[u8]) -> Option<AdminHttpRequest> {
    let split = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")?;
    let headers = &request[..split];
    let body = request[split + 4..].to_vec();
    let header_text = std::str::from_utf8(headers).ok()?;
    let mut lines = header_text.split("\r\n");
    let mut request_line = lines.next()?.split_whitespace();
    let method = request_line.next()?.to_string();
    let path = request_line.next()?.to_string();
    let mut parsed_headers = Vec::new();
    for line in lines {
        let (name, value) = line.split_once(':')?;
        parsed_headers.push((name.trim().to_string(), value.trim().to_string()));
    }
    Some(AdminHttpRequest {
        method,
        path,
        headers: parsed_headers,
        body,
    })
}

async fn read_bounded_request(stream: &mut TcpStream) -> io::Result<BoundedRequest> {
    let mut request = Vec::with_capacity(4096);
    let mut buffer = [0_u8; 4096];
    let mut expected_len = None;
    loop {
        let bytes_read = stream.read(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..bytes_read]);
        if request.len() > ADMIN_MAX_REQUEST_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "admin request too large",
            ));
        }
        if expected_len.is_none() {
            if let Some(header_end) = request
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|index| index + 4)
            {
                expected_len = content_length(&request[..header_end]).map(|len| header_end + len);
            }
        }
        if expected_len.is_some_and(|len| request.len() >= len) {
            break;
        }
        if expected_len.is_none() && request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    Ok(BoundedRequest { bytes: request })
}

fn content_length(headers: &[u8]) -> Option<usize> {
    let header_text = std::str::from_utf8(headers).ok()?;
    header_text.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if name.eq_ignore_ascii_case("content-length") {
            value.trim().parse::<usize>().ok()
        } else {
            None
        }
    })
}

#[derive(Debug)]
struct BoundedRequest {
    bytes: Vec<u8>,
}

async fn write_admin_response(
    stream: &mut TcpStream,
    response: AdminHttpResponse,
) -> io::Result<()> {
    let mut headers = format!(
        "HTTP/1.1 {}\r\nDate: {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        response.status,
        Utc::now().format(HTTP_DATE_FORMAT),
        response.content_type,
        response.body.len()
    );
    if let Some(origin) = response.cors_origin {
        headers.push_str(&format!(
            "Access-Control-Allow-Origin: {origin}\r\nAccess-Control-Allow-Headers: Authorization, Content-Type\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\nVary: Origin\r\n"
        ));
    }
    headers.push_str("\r\n");
    stream.write_all(headers.as_bytes()).await?;
    stream.write_all(&response.body).await
}

fn digest_token(token: &str) -> [u8; 32] {
    let digest = digest(&SHA256, token.as_bytes());
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(digest.as_ref());
    bytes
}

fn constant_time_eq(left: &[u8; 32], right: &[u8; 32]) -> bool {
    left.iter()
        .zip(right.iter())
        .fold(0_u8, |acc, (left, right)| acc | (left ^ right))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    const TEST_TOKEN: &str = "12345678901234567890123456789012";
    const LEGACY_ADMIN_MAX_REQUEST_BYTES: usize = 32 * 1024;

    #[test]
    fn public_release_rejects_untrusted_origins() {
        for origin in [
            "http://localhost.attacker.example",
            "http://127.0.0.1.attacker.example",
            "http://localhost@attacker.example",
            "https://dashboard.example.com",
        ] {
            assert!(
                !is_allowed_admin_origin_with_config(origin, None),
                "unexpected origin: {origin}"
            );
        }
    }

    #[test]
    fn public_release_allows_only_explicit_https_dashboard_origin() {
        let configured = Some("https://dashboard.example.com");
        assert!(is_allowed_admin_origin_with_config(
            "https://dashboard.example.com",
            configured
        ));
        for origin in [
            "https://dashboard.example.com.attacker.example",
            "https://dashboard.example.com/path",
            "https://dashboard.example.com?query",
            "https://dashboard.example.com#fragment",
            "https://dashboard.example.com\r\nInjected: header",
            "http://dashboard.example.com",
            "null",
            "https://",
            "file://localhost",
            "http://localhost:invalid",
            "http://localhost:0",
            "http://localhost:00",
            "http://localhost:65536",
        ] {
            assert!(!is_allowed_admin_origin_with_config(origin, configured));
        }
        assert!(!is_allowed_admin_origin_with_config(
            "http://dashboard.example.com",
            Some("http://dashboard.example.com")
        ));
    }

    #[test]
    fn public_release_keeps_loopback_dashboard_access() {
        for origin in [
            "http://localhost",
            "https://localhost:5173",
            "http://127.0.0.1:5173",
            "https://127.0.0.1",
        ] {
            assert!(is_allowed_admin_origin_with_config(origin, None));
        }
    }

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env lock")
    }

    fn clear_llm_profitability_env() {
        env::remove_var(LLM_LAST_CYCLE_COST_ENV);
        env::remove_var(LLM_SWARM_MONTHLY_MODEL_BUDGET_ENV);
        env::remove_var(LLM_ESTIMATED_MONTHLY_CYCLES_ENV);
        for key in LLM_TOKEN_COST_ENVS {
            env::remove_var(key);
        }
        for (primary, legacy) in LLM_REQUIRED_ALPHA_COST_ENV_PAIRS {
            env::remove_var(primary);
            env::remove_var(legacy);
        }
    }

    fn state() -> AdminApiState {
        let config = AdminApiConfig::new(
            DEFAULT_ADMIN_BIND_ADDR,
            OperatorToken::new(TEST_TOKEN, TokenSource::Environment).expect("token"),
            RunMode::DryRun,
            true,
        );
        AdminApiState::new(
            config,
            LlmEquityRuntimeState::new_with_fills_enabled(true),
            ExecutionMetrics::new().expect("metrics"),
        )
    }

    fn request(method: &str, path: &str, body: Option<&str>) -> String {
        let body = body.unwrap_or("");
        format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {TEST_TOKEN}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
    }

    #[test]
    fn status_reports_equity_authority_only() {
        let response =
            handle_admin_request(request("GET", ADMIN_STATUS_PATH, None).as_bytes(), &state());
        assert_eq!(response.status, "200 OK");
        let body: Value = serde_json::from_slice(&response.body).expect("json");
        assert_eq!(body["service"], "llm_equity_dry_run");
        assert_eq!(body["llm_equity"]["initial_cash_usd"], 10_000.0);
        assert_eq!(body["llm_equity"]["portfolio"]["cash_usd"], 10_000.0);
        assert_eq!(body["llm_equity"]["portfolio"]["positions"], json!([]));
        assert_eq!(body["live_order_capability_disabled"], true);
    }

    #[test]
    fn internal_cycle_endpoint_ingests_authenticated_llm_decision() {
        let state = state();
        let as_of = Utc::now().to_rfc3339();
        let payload = json!({
            "market_research_snapshot": {
                "snapshot_id": "snap-1",
                "as_of": as_of,
                "market_session": "closed",
                "symbols": ["SPY"],
                "latest_bars": {
                    "SPY": {
                        "timestamp": as_of,
                        "open": 500.0,
                        "high": 501.0,
                        "low": 499.0,
                        "close": 500.0,
                        "volume": 1000,
                        "source": "alpaca_market_data"
                    }
                },
                "latest_quotes": {
                    "SPY": {
                        "timestamp": as_of,
                        "bid": 499.95,
                        "ask": 500.05,
                        "source": "alpaca_market_data"
                    }
                },
                "market_calendar_state": {
                    "source": "alpaca_market_clock",
                    "retrieved_at": as_of,
                    "is_open": false,
                    "next_open": "2026-05-29T13:30:00Z",
                    "next_close": "2026-05-28T20:00:00Z",
                    "freshness": "fresh"
                },
                "trend_summaries": {"SPY": "Regular-session fills are blocked."},
                "source_freshness": [
                    {"source": "alpaca_market_data", "freshness": "fresh"},
                    {"source": "alpaca_market_clock", "freshness": "fresh"},
                    {"source": "macro_calendar", "freshness": "fresh"},
                    {"source": "news_summary", "freshness": "fresh"}
                ],
                "macro_news_summaries": [
                    {
                        "source": "macro_calendar",
                        "url": "https://example.com/calendar",
                        "retrieved_at": as_of,
                        "summary": "No fresh regular-session macro event.",
                        "freshness": "fresh"
                    },
                    {
                        "source": "news_summary",
                        "url": "https://example.com/news",
                        "retrieved_at": as_of,
                        "summary": "No fresh regular-session market headline.",
                        "freshness": "fresh"
                    }
                ],
                "unavailable_data_flags": []
            },
            "llm_trading_decision": {
                "cycle_id": "cycle-1",
                "sequence": 1,
                "as_of": as_of,
                "market_session": "closed",
                "portfolio_nav_usd": 10000.0,
                "commands": [{
                    "action": "buy_notional",
                    "symbol": "SPY",
                    "rationale": "Mandatory minimum directional command outside regular hours.",
                    "confidence": 0.8,
                    "max_notional_usd": 25.0,
                    "evidence_refs": ["ev-1"]
                }],
                "evidence": [{
                    "id": "ev-1",
                    "source": "market_research_snapshot",
                    "url": "https://example.com/snapshot",
                    "retrieved_at": as_of,
                    "summary": "Closed session.",
                    "freshness": "fresh"
                }],
                "risk_assessment": {
                    "summary": "Minimum directional command; Rust session gate decides acceptance.",
                    "max_loss_scenario": "Small simulated position if accepted.",
                    "risk_limit_verdict": "pass"
                },
                "memory_updates": [{
                    "event_id": "memory-1",
                    "cycle_id": "cycle-1",
                    "created_at": as_of,
                    "thesis": "Mandatory minimum directional command outside regular hours.",
                    "outcome": "Rust rejects fills outside regular trading hours.",
                    "conclusion": "Keep session rejection explicit in the next cycle.",
                    "prompt_adjustment": "Emit a directional command and let Rust enforce session gates.",
                    "next_cycle_watch_items": ["next regular session"]
                }]
            },
            "llm_swarm": {
                "consensus_score": 0.71,
                "alpha_card_count": 8,
                "edge_gate_status": "pass",
                "edge_gate_reason": "edge_passed",
                "strategy_books": {
                    "small_consensus": {
                        "nav_usd": 10001.0,
                        "alpha_usd": 1.0
                    }
                }
            }
        });
        let progress_payload = json!({
            "status": "llm_review_in_progress",
            "detail": "GPT-5.6-Sol xhigh equity sweep is running.",
            "llm_swarm": payload["llm_swarm"].clone()
        });
        let progress_response = handle_admin_request(
            request(
                "POST",
                LLM_EQUITY_SERVICE_STATUS_PATH,
                Some(&progress_payload.to_string()),
            )
            .as_bytes(),
            &state,
        );
        assert_eq!(progress_response.status, "200 OK");
        let response = handle_admin_request(
            request("POST", LLM_EQUITY_CYCLE_PATH, Some(&payload.to_string())).as_bytes(),
            &state,
        );
        assert_eq!(response.status, "200 OK");
        let response_body =
            serde_json::from_slice::<Value>(&response.body).expect("cycle response json");
        assert_eq!(response_body["portfolio_nav_usd"], json!(10_000.0));
        assert_eq!(response_body["gross_exposure_pct"], json!(0.0));
        assert_eq!(response_body["net_exposure_abs_pct"], json!(0.0));
        assert_eq!(response_body["short_gross_exposure_pct"], json!(0.0));
        assert_eq!(response_body["daily_turnover_pct"], json!(0.0));
        assert_eq!(response_body["daily_loss_pct"], json!(0.0));
        assert_eq!(response_body["max_drawdown_pct"], json!(0.0));
        assert_eq!(
            state.equity_state.snapshot().last_cycle_id.as_deref(),
            Some("cycle-1")
        );
        assert_eq!(
            state.equity_state.snapshot().market_session.as_deref(),
            Some("closed")
        );
        let encoded = state.metrics.gather_text().expect("metrics encode");
        assert!(encoded.contains("traidan_llm_benchmark_nav_usd{scope=\"SPY\"} 10000"));
        assert!(encoded.contains("traidan_llm_source_health{scope=\"SPY\"} 1"));
        assert!(encoded.contains("traidan_llm_swarm_consensus 0.71"));
        assert!(encoded.contains("traidan_llm_alpha_cards_total 8"));
        assert!(encoded
            .contains("traidan_llm_edge_gate_total{reason=\"edge_passed\",status=\"pass\"} 1"));
        assert!(
            encoded.contains("traidan_llm_strategy_book_nav_usd{book=\"small_consensus\"} 10001")
        );
    }

    #[test]
    fn internal_cycle_summary_endpoint_records_trade_cycle_summaries() {
        let state = state();
        let payload = json!({
            "cycle_id": "llm-equity-2026-07-24T15:35:00Z",
            "cycle_kind": "news_scan",
            "as_of": "2026-07-24T15:35:00Z",
            "market_session": "rth",
            "status": "accepted",
            "mandatory_trade_status": "accepted",
            "required_notional_usd": 25.0,
            "requested_notional_usd": 30.0,
            "accepted_notional_usd": 30.0,
            "actions": ["buy"],
            "symbols": ["SPY"],
            "rationale": "Fresh breadth and macro context supported a minimum paper buy.",
            "reason": "simulated_fill",
            "timestamp_ns": "1784907300000000000"
        });

        let response = handle_admin_request(
            request(
                "POST",
                LLM_EQUITY_CYCLE_SUMMARY_PATH,
                Some(&payload.to_string()),
            )
            .as_bytes(),
            &state,
        );

        assert_eq!(response.status, "200 OK");
        let snapshot = state.equity_state.snapshot();
        assert_eq!(snapshot.cycle_summaries.len(), 1);
        assert_eq!(
            snapshot.cycle_summaries[0].cycle_id,
            "llm-equity-2026-07-24T15:35:00Z"
        );
        assert_eq!(snapshot.cycle_summaries[0].accepted_notional_usd, 30.0);
        assert_eq!(snapshot.cycle_summaries[0].required_notional_usd, 25.0);
    }

    #[test]
    fn internal_cycle_summary_endpoint_preserves_full_allocation_legs() {
        let state = state();
        let actions: Vec<_> = (0..80)
            .map(|index| if index < 40 { "sell" } else { "buy" })
            .collect();
        let symbols: Vec<_> = (0..80).map(|index| format!("EQ{}", index % 40)).collect();
        let payload = json!({
            "cycle_id": "full-allocation",
            "cycle_kind": "hourly_review",
            "as_of": "2026-10-22T16:00:00Z",
            "market_session": "rth",
            "status": "accepted",
            "mandatory_trade_status": "not_applicable",
            "required_notional_usd": 0.0,
            "requested_notional_usd": 20000.0,
            "accepted_notional_usd": 20000.0,
            "actions": actions,
            "symbols": symbols,
            "rationale": "Model approved a complete paper allocation.",
            "reason": "simulated_fill",
            "timestamp_ns": "1792684800000000000"
        });
        for _ in 0..2 {
            let response = handle_admin_request(
                request(
                    "POST",
                    LLM_EQUITY_CYCLE_SUMMARY_PATH,
                    Some(&payload.to_string()),
                )
                .as_bytes(),
                &state,
            );
            assert_eq!(response.status, "200 OK", "{:?}", response.body);
            let snapshot = state.equity_state.snapshot();
            assert_eq!(snapshot.cycle_summaries.len(), 1);
            assert_eq!(snapshot.cycle_summaries[0].actions, actions);
            assert_eq!(snapshot.cycle_summaries[0].symbols, symbols);
        }
        for field in ["actions", "symbols"] {
            let mut oversized = payload.clone();
            let values = oversized[field].as_array_mut().unwrap();
            values.push(values[0].clone());
            let response = handle_admin_request(
                request(
                    "POST",
                    LLM_EQUITY_CYCLE_SUMMARY_PATH,
                    Some(&oversized.to_string()),
                )
                .as_bytes(),
                &state,
            );
            assert_eq!(response.status, "422 Unprocessable Entity");
            let snapshot = state.equity_state.snapshot();
            assert_eq!(snapshot.cycle_summaries.len(), 1);
            assert_eq!(snapshot.cycle_summaries[0].actions, actions);
            assert_eq!(snapshot.cycle_summaries[0].symbols, symbols);
        }
    }

    #[test]
    fn internal_cycle_summary_endpoint_reports_ignored_non_trade_rows() {
        let state = state();
        let payload = json!({
            "cycle_id": "llm-equity-2026-07-24T15:35:00Z",
            "cycle_kind": "news_scan",
            "as_of": "2026-07-24T15:35:00Z",
            "market_session": "rth",
            "status": "blocked",
            "mandatory_trade_status": "blocked",
            "required_notional_usd": 25.0,
            "requested_notional_usd": 25.0,
            "accepted_notional_usd": 0.0,
            "actions": ["buy"],
            "symbols": ["SPY"],
            "rationale": "Mandatory paper cycle was blocked before an accepted trade.",
            "reason": "mandatory_order_notional_required",
            "timestamp_ns": "1784907300000000000"
        });

        let response = handle_admin_request(
            request(
                "POST",
                LLM_EQUITY_CYCLE_SUMMARY_PATH,
                Some(&payload.to_string()),
            )
            .as_bytes(),
            &state,
        );

        assert_eq!(response.status, "200 OK");
        assert_eq!(
            serde_json::from_slice::<Value>(&response.body).expect("json"),
            json!({"status": "ignored", "reason": "cycle_summary_not_trade_ledger"})
        );
        assert!(state.equity_state.snapshot().cycle_summaries.is_empty());
    }

    #[test]
    fn profitability_metrics_use_spy_benchmark_costs_and_source_health() {
        let _guard = env_lock();
        clear_llm_profitability_env();
        env::set_var(LLM_LAST_CYCLE_COST_ENV, "99.0");
        env::set_var("TRAIDAN_LLM_EQUITY_INPUT_TOKENS", "1000000");
        env::set_var("TRAIDAN_LLM_EQUITY_CACHED_INPUT_TOKENS", "500000");
        env::set_var("TRAIDAN_LLM_EQUITY_OUTPUT_TOKENS", "100000");
        env::set_var("TRAIDAN_LLM_EQUITY_MONTHLY_MODEL_COST_USD", "25");
        env::set_var("TRAIDAN_LLM_EQUITY_MONTHLY_DATA_COST_USD", "100");
        env::set_var("TRAIDAN_LLM_EQUITY_MONTHLY_HOSTING_COST_USD", "50");
        env::set_var("TRAIDAN_LLM_EQUITY_MONTHLY_EXECUTION_COST_USD", "10");
        env::set_var("TRAIDAN_LLM_EQUITY_MONTHLY_SAFETY_MARGIN_USD", "5");
        let snapshot = json!({
            "latest_bars": {
                "SPY": {"open": 500.0}
            },
            "latest_quotes": {
                "SPY": {"bid": 504.0, "ask": 506.0}
            },
            "source_freshness": [
                {"source": "alpaca_market_data", "freshness": "fresh"},
                {"source": "alpaca_market_clock", "freshness": "fresh"},
                {"source": "macro_calendar", "freshness": "stale"},
                {"source": "news_summary", "freshness": "unavailable"}
            ]
        });

        let metrics = llm_profitability_metrics(&snapshot, 10_300.0, 10_200.0);

        assert_eq!(metrics.cycle_cost_usd, 8.25);
        assert_eq!(metrics.required_alpha_usd, 190.0);
        assert_eq!(metrics.benchmark_nav_usd, 10_302.0);
        assert_eq!(metrics.net_alpha_usd, -10.25);
        assert_eq!(metrics.source_health_ratio, 0.5);
        clear_llm_profitability_env();
    }

    #[test]
    fn profitability_metrics_support_legacy_required_alpha_env_fallback() {
        let _guard = env_lock();
        clear_llm_profitability_env();
        env::set_var("TRAIDAN_LLM_MONTHLY_MODEL_COST_USD", "12");
        let snapshot = json!({});

        let metrics = llm_profitability_metrics(&snapshot, 10_000.0, 10_000.0);

        assert_eq!(metrics.required_alpha_usd, 12.0);
        assert_eq!(metrics.benchmark_nav_usd, 10_000.0);
        clear_llm_profitability_env();
    }

    #[test]
    fn profitability_metrics_use_modeled_cost_when_it_exceeds_default_swarm_budget() {
        let _guard = env_lock();
        clear_llm_profitability_env();
        env::set_var("TRAIDAN_LLM_EQUITY_INPUT_TOKENS", "1000000");
        env::set_var("TRAIDAN_LLM_EQUITY_CACHED_INPUT_TOKENS", "500000");
        env::set_var("TRAIDAN_LLM_EQUITY_OUTPUT_TOKENS", "100000");
        let snapshot = json!({});

        let metrics = llm_profitability_metrics(&snapshot, 10_000.0, 10_000.0);

        assert_eq!(metrics.cycle_cost_usd, 8.25);
        assert_eq!(metrics.required_alpha_usd, 1212.75);
        clear_llm_profitability_env();
    }

    #[test]
    fn internal_service_status_endpoint_updates_runtime_state() {
        let state = state();
        let payload = json!({
            "status": "llm_edge_gate_skipped",
            "detail": "startup GPT-5.6-Sol xhigh equity sweep failed; no simulated command was posted.",
            "last_error": "invalid strict decision JSON",
            "market_session": "rth",
            "activity_key": "news-scan:stable-opportunity",
            "runtime_identity": {
                "schema_version": 2,
                "model": "gpt-5.6-sol",
                "reasoning_effort": "xhigh",
                "fill_mode": "shadow"
            },
            "llm_swarm": {
                "consensus_score": 0.41,
                "alpha_card_count": 8,
                "edge_gate_status": "skipped_no_edge",
                "edge_gate_reason": "expected_alpha_does_not_clear_costs_plus_30_bps",
                "no_trade_reason": "expected_alpha_does_not_clear_costs_plus_30_bps",
                "strategy_books": {
                    "small_consensus": {
                        "nav_usd": 9999.0,
                        "alpha_usd": -1.0
                    }
                }
            }
        });

        let response = handle_admin_request(
            request(
                "POST",
                LLM_EQUITY_SERVICE_STATUS_PATH,
                Some(&payload.to_string()),
            )
            .as_bytes(),
            &state,
        );

        assert_eq!(response.status, "200 OK");
        let snapshot = state.equity_state.snapshot();
        assert_eq!(snapshot.status, "llm_edge_gate_skipped");
        assert_eq!(
            snapshot.last_error.as_deref(),
            Some("invalid strict decision JSON")
        );
        assert_eq!(snapshot.last_cycle_id, None);
        assert_eq!(snapshot.market_session.as_deref(), Some("rth"));
        assert_eq!(
            snapshot.service_activity_key.as_deref(),
            Some("news-scan:stable-opportunity")
        );
        assert_eq!(snapshot.service_activity_repeat_count, 1);
        assert_eq!(
            snapshot
                .service_identity
                .as_ref()
                .and_then(|value| value.get("model")),
            Some(&json!("gpt-5.6-sol"))
        );
        let encoded = state.metrics.gather_text().expect("metrics encode");
        assert!(encoded.contains("traidan_llm_swarm_consensus 0.41"));
        assert!(
            encoded.contains("traidan_llm_strategy_book_alpha_usd{book=\"small_consensus\"} -1")
        );
        assert!(encoded.contains(
            "traidan_llm_edge_gate_total{reason=\"expected_alpha_does_not_clear_costs_plus_30_bps\",status=\"skipped_no_edge\"} 1"
        ));
    }

    #[test]
    fn internal_service_status_rejects_invalid_market_session() {
        for invalid_session in [json!("weekend"), json!(42)] {
            let state = state();
            let payload = json!({
                "status": "llm_research_only",
                "detail": "Public-news research completed without a trade cycle.",
                "market_session": invalid_session
            });

            let response = handle_admin_request(
                request(
                    "POST",
                    LLM_EQUITY_SERVICE_STATUS_PATH,
                    Some(&payload.to_string()),
                )
                .as_bytes(),
                &state,
            );

            assert_eq!(response.status, "422 Unprocessable Entity");
            assert_eq!(state.equity_state.snapshot().market_session, None);
        }
    }

    #[test]
    fn internal_service_status_rejects_invalid_activity_key() {
        for invalid_key in [json!(42), json!("")] {
            let state = state();
            let payload = json!({
                "status": "llm_research_only",
                "detail": "Public-news research completed without a trade cycle.",
                "activity_key": invalid_key
            });

            let response = handle_admin_request(
                request(
                    "POST",
                    LLM_EQUITY_SERVICE_STATUS_PATH,
                    Some(&payload.to_string()),
                )
                .as_bytes(),
                &state,
            );

            assert_eq!(response.status, "422 Unprocessable Entity");
            assert_eq!(state.equity_state.snapshot().service_activity_key, None);
        }
    }

    #[test]
    fn rejected_internal_service_status_does_not_record_swarm_metrics() {
        let state = state();
        let payload = json!({
            "status": "llm_edge_gate_skipped",
            "detail": "startup GPT-5.6-Sol xhigh equity sweep failed; no simulated command was posted.",
            "market_research_snapshot": {
                "raw_prompt": "forbidden raw prompt text"
            },
            "llm_swarm": {
                "consensus_score": 0.41,
                "alpha_card_count": 8,
                "edge_gate_status": "skipped_no_edge",
                "edge_gate_reason": "expected_alpha_does_not_clear_costs_plus_30_bps",
                "strategy_books": {
                    "small_consensus": {
                        "nav_usd": 9999.0,
                        "alpha_usd": -1.0
                    }
                }
            }
        });

        let response = handle_admin_request(
            request(
                "POST",
                LLM_EQUITY_SERVICE_STATUS_PATH,
                Some(&payload.to_string()),
            )
            .as_bytes(),
            &state,
        );

        assert_eq!(response.status, "422 Unprocessable Entity");
        let encoded = state.metrics.gather_text().expect("metrics encode");
        assert!(!encoded.contains("traidan_llm_alpha_cards_total 8"));
        assert!(!encoded.contains("traidan_llm_edge_gate_total"));
        assert!(!encoded.contains("traidan_llm_strategy_book_alpha_usd"));
    }

    #[tokio::test]
    async fn admin_reader_accepts_dynamic_internal_payload_above_legacy_limit() {
        let payload = json!({
            "status": "llm_review_in_progress",
            "detail": format!(
                "dynamic research snapshot is included for dashboard visibility: {}",
                "x".repeat(40 * 1024)
            ),
        })
        .to_string();
        assert!(payload.len() > LEGACY_ADMIN_MAX_REQUEST_BYTES);
        assert!(payload.len() < ADMIN_MAX_REQUEST_BYTES);

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("listener addr");
        let server_state = state();
        let server = tokio::spawn(async move {
            serve_admin_api_once(listener, server_state)
                .await
                .expect("serve admin API once");
        });

        let mut client = TcpStream::connect(addr).await.expect("connect");
        let request = request("POST", LLM_EQUITY_SERVICE_STATUS_PATH, Some(&payload));
        client
            .write_all(request.as_bytes())
            .await
            .expect("write request");
        client.shutdown().await.expect("shutdown request");

        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("read response");
        server.await.expect("server task");
        let response = String::from_utf8(response).expect("utf8 response");
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    }

    #[tokio::test]
    async fn admin_reader_rejects_payloads_above_request_limit() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("listener addr");
        let server_state = state();
        let server = tokio::spawn(async move {
            serve_admin_api_once(listener, server_state)
                .await
                .expect("serve admin API once");
        });

        let mut client = TcpStream::connect(addr).await.expect("connect");
        let oversized_body = "x".repeat(ADMIN_MAX_REQUEST_BYTES);
        let request = request(
            "POST",
            LLM_EQUITY_SERVICE_STATUS_PATH,
            Some(&oversized_body),
        );
        assert!(request.len() > ADMIN_MAX_REQUEST_BYTES);
        client
            .write_all(request.as_bytes())
            .await
            .expect("write request");
        client.shutdown().await.expect("shutdown request");

        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("read response");
        server.await.expect("server task");
        let response = String::from_utf8(response).expect("utf8 response");
        assert!(
            response.starts_with("HTTP/1.1 413 Payload Too Large"),
            "{response}"
        );
    }

    #[test]
    fn unauthenticated_internal_cycle_rejects() {
        let raw = format!(
            "POST {LLM_EQUITY_CYCLE_PATH} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\n{{}}"
        );
        let response = handle_admin_request(raw.as_bytes(), &state());
        assert_eq!(response.status, "401 Unauthorized");
    }

    #[test]
    fn admin_bind_addr_must_be_loopback() {
        assert!(is_loopback_admin_bind_addr("127.0.0.1:8090"));
        assert!(is_loopback_admin_bind_addr("localhost:8090"));
        assert!(!is_loopback_admin_bind_addr("0.0.0.0:8090"));
    }
}
