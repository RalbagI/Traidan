mod fully_invested;
#[cfg(test)]
mod fully_invested_tests;
use chrono::{DateTime, Datelike, Duration, NaiveDate, Timelike, Utc, Weekday};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::error::Error;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub const LLM_EQUITY_INITIAL_CASH_USD: f64 = 10_000.0;
const PORTFOLIO_BENCHMARK_SYMBOL: &str = "SPY";
const LLM_EQUITY_MANDATORY_ORDER_NOTIONAL_USD: f64 = 25.0;
const LLM_EQUITY_MIN_GROSS_EXPOSURE_USD: f64 = 700.0;
const LLM_EQUITY_SUMMARY_NOTIONAL_EPSILON_USD: f64 = 0.000_001;
const RESEARCH_POSITION_DUST_QUANTITY_EPSILON: f64 = 0.000_000_01;
const RESEARCH_POSITION_DUST_NOTIONAL_EPSILON_USD: f64 = 0.01;
const LLM_EQUITY_MAX_DECISION_AGE_MINUTES: i64 = 90;
const LLM_EQUITY_MAX_FUTURE_SKEW_MINUTES: i64 = 5;
const LLM_EQUITY_MAX_SERVICE_ACTIVITY_KEYS: usize = 128;
const LLM_EQUITY_MAX_BAR_AGE_SECONDS: i64 = 15 * 60;
const LLM_EQUITY_MAX_QUOTE_AGE_SECONDS: i64 = 2 * 60;
const FLOAT_EPSILON: f64 = 1e-9;
const LLM_EQUITY_FILL_MODE_ENV: &str = "TRAIDAN_LLM_EQUITY_FILL_MODE";
const LLM_EQUITY_RISK_MODE_ENV: &str = "TRAIDAN_LLM_EQUITY_RISK_MODE";
const LLM_EQUITY_RISK_MODE_LLM_DISCRETION: &str = "llm_discretion";
const LLM_EQUITY_RISK_MODE_FORCED_AGGRESSIVE: &str = "forced_aggressive";
const LLM_EQUITY_STATE_FILE_ENV: &str = "TRAIDAN_LLM_EQUITY_AUTHORITY_STATE_FILE";
const DEFAULT_LLM_EQUITY_STATE_FILE: &str =
    ".agent/state/llm-equity-dry-run/rust-authority-state.json";
const LLM_EQUITY_DASHBOARD_STATE_FILE_ENV: &str = "TRAIDAN_LLM_EQUITY_DASHBOARD_STATE_FILE";
const DEFAULT_LLM_EQUITY_DASHBOARD_STATE_FILE: &str =
    ".agent/state/llm-equity-dry-run/rust-dashboard-state.json";
const LLM_EQUITY_CYCLE_AUDIT_LEDGER_NAME: &str = "cycle-audit-ledger.json";
const LLM_EQUITY_MAX_CYCLE_SUMMARIES: usize = 20;
const LLM_EQUITY_MAX_GROSS_NAV_PCT: f64 = 0.75;
const LLM_EQUITY_MAX_NET_NAV_PCT: f64 = 0.35;
const RESEARCH_LONG_MAX_NET_NAV_PCT: f64 = 0.75;
const LLM_EQUITY_MAX_SINGLE_NAME_NAV_PCT: f64 = 0.075;
const LLM_EQUITY_MAX_SHORT_GROSS_NAV_PCT: f64 = 0.30;
const LLM_EQUITY_MIN_CASH_RESERVE_NAV_PCT: f64 = 0.25;
const LLM_EQUITY_MAX_DAILY_TURNOVER_NAV_PCT: f64 = 1.50;
const LLM_EQUITY_MAX_DAILY_LOSS_NAV_PCT: f64 = 0.01;
const LLM_EQUITY_MAX_DRAWDOWN_PCT: f64 = 0.05;
const LLM_EQUITY_MAX_HIGH_CORRELATION_GROUP_NAV_PCT: f64 = 0.10;
const FORCED_AGGRESSIVE_MAX_GROSS_NAV_PCT: f64 = 1.25;
const FORCED_AGGRESSIVE_MAX_NET_NAV_PCT: f64 = 0.75;
const FORCED_AGGRESSIVE_MAX_SINGLE_NAME_NAV_PCT: f64 = 0.15;
const FORCED_AGGRESSIVE_MAX_SHORT_GROSS_NAV_PCT: f64 = 0.60;
const FORCED_AGGRESSIVE_MIN_CASH_RESERVE_NAV_PCT: f64 = 0.05;
const FORCED_AGGRESSIVE_MAX_DAILY_TURNOVER_NAV_PCT: f64 = 4.00;
const FORCED_AGGRESSIVE_MAX_DAILY_LOSS_NAV_PCT: f64 = 0.03;
const FORCED_AGGRESSIVE_MAX_DRAWDOWN_PCT: f64 = 0.10;
const LLM_EQUITY_MARKET_IMPACT_BPS: f64 = 2.0;
const FORBIDDEN_JSON_KEYS: [&str; 15] = [
    "raw_prompt",
    "raw_prompts",
    "prompt",
    "prompts",
    "raw_log",
    "raw_logs",
    "stdout",
    "stderr",
    "article_text",
    "full_text",
    "credentials",
    "api_key",
    "secret",
    "token",
    "password",
];
const REQUIRED_SNAPSHOT_FRESHNESS_SOURCES: [&str; 4] = [
    "alpaca_market_data",
    "alpaca_market_clock",
    "macro_calendar",
    "news_summary",
];
const REQUIRED_MACRO_NEWS_SOURCES: [&str; 2] = ["macro_calendar", "news_summary"];

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct LlmEquityPosition {
    pub symbol: String,
    pub quantity: f64,
    pub avg_price: f64,
}

impl LlmEquityPosition {
    pub fn market_value(&self, price: f64) -> f64 {
        self.quantity * price
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct LlmEquityPortfolio {
    pub initial_cash_usd: f64,
    pub cash_usd: f64,
    pub positions: HashMap<String, LlmEquityPosition>,
    pub realized_pnl_usd: f64,
    pub turnover_this_hour_usd: f64,
    pub peak_nav_usd: f64,
    pub day_start_nav_usd: f64,
}

impl Default for LlmEquityPortfolio {
    fn default() -> Self {
        Self::seeded()
    }
}

impl LlmEquityPortfolio {
    pub fn seeded() -> Self {
        Self {
            initial_cash_usd: LLM_EQUITY_INITIAL_CASH_USD,
            cash_usd: LLM_EQUITY_INITIAL_CASH_USD,
            positions: HashMap::new(),
            realized_pnl_usd: 0.0,
            turnover_this_hour_usd: 0.0,
            peak_nav_usd: LLM_EQUITY_INITIAL_CASH_USD,
            day_start_nav_usd: LLM_EQUITY_INITIAL_CASH_USD,
        }
    }

    pub fn nav_usd(&self, prices: &HashMap<String, f64>) -> f64 {
        self.cash_usd
            + self
                .positions
                .values()
                .filter_map(|position| {
                    prices
                        .get(&position.symbol)
                        .map(|price| position.market_value(*price))
                })
                .sum::<f64>()
    }

    pub fn gross_exposure_usd(&self, prices: &HashMap<String, f64>) -> f64 {
        self.positions
            .values()
            .filter_map(|position| {
                prices
                    .get(&position.symbol)
                    .map(|price| position.market_value(*price).abs())
            })
            .sum()
    }

    pub fn max_drawdown_pct(&self, prices: &HashMap<String, f64>) -> f64 {
        let nav = self.nav_usd(prices);
        if self.peak_nav_usd <= 0.0 {
            return 0.0;
        }
        ((self.peak_nav_usd - nav).max(0.0) / self.peak_nav_usd).max(0.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LlmEquityOrderSide {
    Buy,
    Sell,
}

impl LlmEquityOrderSide {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct LlmEquitySimulatedOrder {
    pub command_id: String,
    pub cycle_id: String,
    pub symbol: String,
    pub side: LlmEquityOrderSide,
    pub status: String,
    pub qty: f64,
    pub avg_price: f64,
    pub notional_usd: f64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct LlmEquityApplyResult {
    pub cycle_id: String,
    pub as_of: DateTime<Utc>,
    pub accepted_orders: Vec<LlmEquitySimulatedOrder>,
    pub rejected_orders: Vec<LlmEquitySimulatedOrder>,
    pub portfolio: LlmEquityPortfolio,
    pub prices: HashMap<String, f64>,
    pub turnover_today_usd: f64,
}

#[derive(Debug, Clone, PartialEq)]
struct ValidatedLlmEquityCycle {
    result: LlmEquityApplyResult,
    decision: Value,
    market_snapshot: Value,
}

#[derive(Debug)]
struct ValidatedLlmEquityServiceStatusUpdate {
    status: String,
    detail: String,
    market_snapshot: Option<(Value, HashMap<String, f64>)>,
    last_error: Option<String>,
    service_identity: Option<Value>,
    market_session: Option<String>,
    service_activity_key: Option<String>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Default)]
pub struct LlmEquityServiceStatusMetadata {
    pub service_identity: Option<Value>,
    pub market_session: Option<String>,
    pub activity_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LlmEquityRuntimeSnapshot {
    pub status: String,
    pub detail: String,
    pub fill_mode: String,
    pub market_session: Option<String>,
    pub service_activity_key: Option<String>,
    pub service_activity_repeat_count: u32,
    pub service_status_has_market_snapshot: bool,
    pub updated_at: DateTime<Utc>,
    pub last_cycle_id: Option<String>,
    pub last_error: Option<String>,
    pub portfolio: LlmEquityPortfolio,
    pub prices: HashMap<String, f64>,
    pub stale_price_symbols: HashSet<String>,
    pub latest_decision: Option<Value>,
    pub latest_market_snapshot: Option<Value>,
    pub latest_result: Option<LlmEquityApplyResult>,
    pub service_identity: Option<Value>,
    pub portfolio_history: Vec<LlmEquityPortfolioHistoryPoint>,
    pub dry_run_started_at: Option<DateTime<Utc>>,
    pub benchmark_started_at: Option<DateTime<Utc>>,
    pub benchmark_start_price_usd: Option<f64>,
    pub cycle_summaries: Vec<LlmEquityCycleSummary>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct LlmEquityPortfolioHistoryPoint {
    pub updated_at: DateTime<Utc>,
    pub portfolio: LlmEquityPortfolio,
    pub prices: HashMap<String, f64>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct LlmEquityCycleSummary {
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
    pub timestamp_ns: u64,
}

#[derive(Debug, Default, Clone)]
struct LlmEquityDashboardBootstrap {
    dry_run_started_at: Option<DateTime<Utc>>,
    benchmark_started_at: Option<DateTime<Utc>>,
    benchmark_start_price_usd: Option<f64>,
    portfolio_history: Vec<LlmEquityPortfolioHistoryPoint>,
    cycle_summaries: Vec<LlmEquityCycleSummary>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedLlmEquityDashboard {
    schema_version: u32,
    saved_at: DateTime<Utc>,
    dry_run_started_at: Option<DateTime<Utc>>,
    benchmark_started_at: Option<DateTime<Utc>>,
    benchmark_start_price_usd: Option<f64>,
    #[serde(default)]
    portfolio_history: Vec<LlmEquityPortfolioHistoryPoint>,
    #[serde(default)]
    cycle_summaries: Vec<LlmEquityCycleSummary>,
}

#[derive(Debug)]
struct LlmEquityRuntimeInner {
    authority: LlmEquityDryRunAuthority,
    snapshot: LlmEquityRuntimeSnapshot,
    state_file: Option<PathBuf>,
    dashboard_state_file: Option<PathBuf>,
    service_activity_counts: HashMap<String, u32>,
    service_activity_order: VecDeque<String>,
}

#[derive(Debug, Clone)]
pub struct LlmEquityRuntimeState {
    inner: Arc<Mutex<LlmEquityRuntimeInner>>,
}

impl Default for LlmEquityRuntimeState {
    fn default() -> Self {
        Self::new()
    }
}

impl LlmEquityRuntimeState {
    pub fn new() -> Self {
        let fills_enabled = env::var(LLM_EQUITY_FILL_MODE_ENV)
            .is_ok_and(|value| value.trim().eq_ignore_ascii_case("paper"));
        let llm_discretion = fills_enabled
            && env::var(LLM_EQUITY_RISK_MODE_ENV).is_ok_and(|value| {
                value
                    .trim()
                    .eq_ignore_ascii_case(LLM_EQUITY_RISK_MODE_LLM_DISCRETION)
            });
        let forced_aggressive = fills_enabled
            && env::var(LLM_EQUITY_RISK_MODE_ENV).is_ok_and(|value| {
                value
                    .trim()
                    .eq_ignore_ascii_case(LLM_EQUITY_RISK_MODE_FORCED_AGGRESSIVE)
            });
        let profit_driven = fills_enabled
            && env::var(LLM_EQUITY_RISK_MODE_ENV)
                .is_ok_and(|value| value.trim().eq_ignore_ascii_case("profit_driven"));
        let fully_invested = fills_enabled
            && env::var(LLM_EQUITY_RISK_MODE_ENV)
                .is_ok_and(|v| v.trim().eq_ignore_ascii_case("fully_invested"));
        let configured_authority = if fully_invested {
            LlmEquityDryRunAuthority::paper_fully_invested()
        } else if profit_driven {
            LlmEquityDryRunAuthority::paper_profit_driven()
        } else if llm_discretion {
            LlmEquityDryRunAuthority::paper_llm_discretion()
        } else if forced_aggressive {
            LlmEquityDryRunAuthority::paper_forced_aggressive()
        } else if fills_enabled {
            LlmEquityDryRunAuthority::paper()
        } else {
            LlmEquityDryRunAuthority::shadow()
        };
        let state_file = PathBuf::from(
            env::var(LLM_EQUITY_STATE_FILE_ENV)
                .unwrap_or_else(|_| DEFAULT_LLM_EQUITY_STATE_FILE.to_string()),
        );
        let dashboard_state_file = PathBuf::from(
            env::var(LLM_EQUITY_DASHBOARD_STATE_FILE_ENV)
                .unwrap_or_else(|_| DEFAULT_LLM_EQUITY_DASHBOARD_STATE_FILE.to_string()),
        );
        let (authority, recovery_error) = if state_file.exists() {
            match LlmEquityDryRunAuthority::restore_from_path(
                &state_file,
                fills_enabled,
                !llm_discretion,
                true,
            ) {
                Ok(mut restored) => {
                    restored.forced_aggressive_policy = forced_aggressive || profit_driven;
                    restored.profit_driven_policy = profit_driven;
                    restored.fully_invested_policy = fully_invested;
                    (restored, None)
                }
                Err(error) => (LlmEquityDryRunAuthority::shadow(), Some(error.to_string())),
            }
        } else {
            (configured_authority, None)
        };
        let dashboard_bootstrap = load_or_derive_dashboard_bootstrap(
            &dashboard_state_file,
            Some(&state_file),
            &authority,
        );
        Self::with_authority_and_dashboard(
            authority,
            Some(state_file),
            Some(dashboard_state_file),
            recovery_error,
            dashboard_bootstrap,
        )
    }

    pub fn new_with_fills_enabled(fills_enabled: bool) -> Self {
        let authority = if fills_enabled {
            LlmEquityDryRunAuthority::seeded()
        } else {
            LlmEquityDryRunAuthority::shadow()
        };
        Self::with_authority(authority, None, None)
    }

    fn with_authority(
        authority: LlmEquityDryRunAuthority,
        state_file: Option<PathBuf>,
        recovery_error: Option<String>,
    ) -> Self {
        Self::with_authority_and_dashboard(
            authority,
            state_file,
            None,
            recovery_error,
            LlmEquityDashboardBootstrap::default(),
        )
    }

    fn with_authority_and_dashboard(
        authority: LlmEquityDryRunAuthority,
        state_file: Option<PathBuf>,
        dashboard_state_file: Option<PathBuf>,
        recovery_error: Option<String>,
        dashboard_bootstrap: LlmEquityDashboardBootstrap,
    ) -> Self {
        let now = Utc::now();
        let portfolio = authority.portfolio.clone();
        let prices = authority.last_prices.clone();
        let stale_price_symbols = HashSet::new();
        let fills_enabled = authority.fills_enabled;
        let dry_run_started_at = dashboard_bootstrap.dry_run_started_at;
        let benchmark_started_at = dashboard_bootstrap.benchmark_started_at;
        let benchmark_start_price_usd = dashboard_bootstrap.benchmark_start_price_usd;
        let portfolio_history = dashboard_bootstrap.portfolio_history;
        let cycle_summaries = dashboard_bootstrap.cycle_summaries;
        Self {
            inner: Arc::new(Mutex::new(LlmEquityRuntimeInner {
                authority,
                snapshot: LlmEquityRuntimeSnapshot {
                    status: if recovery_error.is_some() {
                        "state_recovery_failed"
                    } else {
                        "waiting_for_hourly_cycle"
                    }
                    .to_string(),
                    detail: if recovery_error.is_some() {
                        "שחזור מצב Rust נכשל; המערכת נעולה במצב shadow."
                    } else {
                        "ממתין לסריקת GPT-6 Astra xhigh הראשונה למניות."
                    }
                    .to_string(),
                    fill_mode: if fills_enabled { "paper" } else { "shadow" }.to_string(),
                    market_session: None,
                    service_activity_key: None,
                    service_activity_repeat_count: 0,
                    service_status_has_market_snapshot: false,
                    updated_at: now,
                    last_cycle_id: None,
                    last_error: recovery_error,
                    portfolio,
                    prices,
                    stale_price_symbols,
                    latest_decision: None,
                    latest_market_snapshot: None,
                    latest_result: None,
                    service_identity: None,
                    portfolio_history,
                    dry_run_started_at,
                    benchmark_started_at,
                    benchmark_start_price_usd,
                    cycle_summaries,
                },
                state_file,
                dashboard_state_file,
                service_activity_counts: HashMap::new(),
                service_activity_order: VecDeque::new(),
            })),
        }
    }

    pub fn snapshot(&self) -> LlmEquityRuntimeSnapshot {
        self.inner
            .lock()
            .expect("LLM equity runtime state lock poisoned")
            .snapshot
            .clone()
    }

    pub fn record_cycle_summary(
        &self,
        summary: LlmEquityCycleSummary,
    ) -> Result<bool, LlmEquityAuthorityError> {
        validate_cycle_summary(&summary)?;
        if !is_trade_ledger_cycle_summary(&summary) {
            return Ok(false);
        }
        let mut inner = self
            .inner
            .lock()
            .expect("LLM equity runtime state lock poisoned");
        record_cycle_summary_in_list(&mut inner.snapshot.cycle_summaries, summary);
        persist_dashboard_state_best_effort(&inner);
        Ok(true)
    }

    pub fn ingest_cycle(
        &self,
        decision: Value,
        market_snapshot: Value,
    ) -> Result<LlmEquityApplyResult, LlmEquityAuthorityError> {
        let decision_json = serde_json::to_string(&decision)
            .map_err(|err| LlmEquityAuthorityError::MalformedDecision(err.to_string()))?;
        let snapshot_json = serde_json::to_string(&market_snapshot)
            .map_err(|err| LlmEquityAuthorityError::MalformedSnapshot(err.to_string()))?;
        let received_at = Utc::now();
        let mut inner = self
            .inner
            .lock()
            .expect("LLM equity runtime state lock poisoned");
        let previous_authority = inner.authority.clone();
        match inner.authority.apply_json_with_validated_payloads(
            &decision_json,
            &snapshot_json,
            received_at,
        ) {
            Ok(cycle) => {
                if let Some(path) = inner.state_file.as_deref() {
                    if let Err(error) = inner.authority.persist_to_path(path) {
                        let committed = inner.authority.fully_invested_policy
                            && inner.authority.allocation_commit_matches(path);
                        if !committed {
                            inner.authority = previous_authority;
                        }
                        inner.snapshot.status = "state_persistence_failed".to_string();
                        inner.snapshot.detail = if committed {
                            "ההקצאה נשמרה; אימות היומן ממתין לניסיון חוזר."
                        } else {
                            "מחזור Rust בוטל כי שמירת מצב סמכות המסחר נכשלה."
                        }
                        .to_string();
                        inner.snapshot.updated_at = received_at;
                        inner.snapshot.last_error = Some(error.to_string());
                        return Err(error);
                    }
                }
                let result = cycle.result;
                let market_session = cycle
                    .market_snapshot
                    .get("market_session")
                    .and_then(Value::as_str)
                    .map(ToString::to_string);
                let (prices, stale_price_symbols) = retain_portfolio_marks(
                    result.prices.clone(),
                    &inner.snapshot.prices,
                    &result.portfolio,
                );
                let mut snapshot = LlmEquityRuntimeSnapshot {
                    status: "cycle_applied".to_string(),
                    detail: if result.accepted_orders.is_empty() {
                        "הסריקה השעתית הושלמה; לא התקבלה פקודת סימולציה.".to_string()
                    } else {
                        "הסריקה השעתית הושלמה; פקודת מניות בסימולציה יושמה.".to_string()
                    },
                    fill_mode: inner.snapshot.fill_mode.clone(),
                    market_session,
                    service_activity_key: None,
                    service_activity_repeat_count: 0,
                    service_status_has_market_snapshot: true,
                    updated_at: received_at,
                    last_cycle_id: Some(result.cycle_id.clone()),
                    last_error: None,
                    portfolio: result.portfolio.clone(),
                    prices,
                    stale_price_symbols,
                    latest_decision: Some(cycle.decision),
                    latest_market_snapshot: Some(cycle.market_snapshot),
                    latest_result: Some(result.clone()),
                    service_identity: inner.snapshot.service_identity.clone(),
                    portfolio_history: inner.snapshot.portfolio_history.clone(),
                    dry_run_started_at: inner.snapshot.dry_run_started_at,
                    benchmark_started_at: inner.snapshot.benchmark_started_at,
                    benchmark_start_price_usd: inner.snapshot.benchmark_start_price_usd,
                    cycle_summaries: inner.snapshot.cycle_summaries.clone(),
                };
                update_dashboard_benchmark_from_prices(&mut snapshot);
                record_current_portfolio_history(&mut snapshot);
                inner.snapshot = snapshot;
                persist_dashboard_state_best_effort(&inner);
                Ok(result)
            }
            Err(error) => {
                inner.snapshot.status = "cycle_rejected".to_string();
                inner.snapshot.detail = "הסריקה השעתית נדחתה על ידי סמכות Rust.".to_string();
                inner.snapshot.updated_at = received_at;
                inner.snapshot.last_error = Some(error.to_string());
                Err(error)
            }
        }
    }

    pub fn update_service_status(
        &self,
        status: String,
        detail: String,
        market_snapshot: Option<Value>,
        last_error: Option<String>,
    ) -> Result<(), LlmEquityAuthorityError> {
        self.update_service_status_with_identity(status, detail, market_snapshot, last_error, None)
    }

    pub fn update_service_status_with_identity(
        &self,
        status: String,
        detail: String,
        market_snapshot: Option<Value>,
        last_error: Option<String>,
        service_identity: Option<Value>,
    ) -> Result<(), LlmEquityAuthorityError> {
        self.update_service_status_with_metadata(
            status,
            detail,
            market_snapshot,
            last_error,
            LlmEquityServiceStatusMetadata {
                service_identity,
                market_session: None,
                activity_key: None,
            },
        )
    }

    pub fn update_service_status_with_metadata(
        &self,
        status: String,
        detail: String,
        market_snapshot: Option<Value>,
        last_error: Option<String>,
        metadata: LlmEquityServiceStatusMetadata,
    ) -> Result<(), LlmEquityAuthorityError> {
        let LlmEquityServiceStatusMetadata {
            service_identity,
            market_session,
            activity_key: service_activity_key,
        } = metadata;
        if status.trim().is_empty() {
            return Err(LlmEquityAuthorityError::Contract("service_status_required"));
        }
        if detail.trim().is_empty() {
            return Err(LlmEquityAuthorityError::Contract("service_detail_required"));
        }
        if contains_credential_assignment(&detail)
            || last_error
                .as_deref()
                .is_some_and(contains_credential_assignment)
        {
            return Err(LlmEquityAuthorityError::Contract("credential_like_text"));
        }

        let validated_snapshot = match market_snapshot {
            Some(value) => {
                reject_forbidden_json(&value)?;
                let snapshot = serde_json::from_value::<SnapshotEnvelope>(value)
                    .map_err(|err| LlmEquityAuthorityError::MalformedSnapshot(err.to_string()))?;
                validate_snapshot(&snapshot)?;
                let prices = snapshot_mid_prices(&snapshot)?;
                let sanitized = serde_json::to_value(&snapshot)
                    .map_err(|err| LlmEquityAuthorityError::MalformedSnapshot(err.to_string()))?;
                Some((sanitized, prices))
            }
            None => None,
        };
        if let Some(identity) = service_identity.as_ref() {
            reject_forbidden_json(identity)?;
            if !identity.is_object() {
                return Err(LlmEquityAuthorityError::Contract(
                    "runtime_identity_must_be_object",
                ));
            }
        }
        if market_session
            .as_deref()
            .is_some_and(|value| !is_valid_market_session_label(value))
        {
            return Err(LlmEquityAuthorityError::Contract("market_session_invalid"));
        }
        if service_activity_key.as_deref().is_some_and(|value| {
            value.trim().is_empty() || value.len() > 160 || contains_credential_assignment(value)
        }) {
            return Err(LlmEquityAuthorityError::Contract(
                "service_activity_key_invalid",
            ));
        }
        if let (Some(status_session), Some((snapshot, _))) =
            (market_session.as_deref(), validated_snapshot.as_ref())
        {
            if snapshot.get("market_session").and_then(Value::as_str) != Some(status_session) {
                return Err(LlmEquityAuthorityError::Contract("market_session_mismatch"));
            }
        }

        self.update_service_status_at(ValidatedLlmEquityServiceStatusUpdate {
            status,
            detail,
            market_snapshot: validated_snapshot,
            last_error,
            service_identity,
            market_session,
            service_activity_key,
            updated_at: Utc::now(),
        })
    }

    fn update_service_status_at(
        &self,
        update: ValidatedLlmEquityServiceStatusUpdate,
    ) -> Result<(), LlmEquityAuthorityError> {
        let ValidatedLlmEquityServiceStatusUpdate {
            status,
            detail,
            market_snapshot,
            last_error,
            service_identity,
            market_session,
            service_activity_key,
            updated_at,
        } = update;
        let mut inner = self
            .inner
            .lock()
            .expect("LLM equity runtime state lock poisoned");
        let service_status_has_market_snapshot = market_snapshot.is_some();
        let service_activity_repeat_count = match service_activity_key.as_ref() {
            Some(key) => {
                let next_count = inner
                    .service_activity_counts
                    .get(key)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(1)
                    .max(1);
                if !inner.service_activity_counts.contains_key(key)
                    && inner.service_activity_counts.len() >= LLM_EQUITY_MAX_SERVICE_ACTIVITY_KEYS
                {
                    if let Some(expired) = inner.service_activity_order.pop_front() {
                        inner.service_activity_counts.remove(&expired);
                    }
                }
                inner
                    .service_activity_order
                    .retain(|existing| existing != key);
                inner.service_activity_order.push_back(key.clone());
                inner
                    .service_activity_counts
                    .insert(key.clone(), next_count);
                next_count
            }
            None => 0,
        };
        inner.snapshot.status = status;
        inner.snapshot.detail = detail;
        inner.snapshot.updated_at = updated_at;
        inner.snapshot.last_error = last_error;
        inner.snapshot.service_activity_key = service_activity_key;
        inner.snapshot.service_activity_repeat_count = service_activity_repeat_count;
        inner.snapshot.service_status_has_market_snapshot = service_status_has_market_snapshot;
        if service_identity.is_some() {
            inner.snapshot.service_identity = service_identity;
        }
        if market_session.is_some() {
            inner.snapshot.market_session = market_session;
        }
        if let Some((snapshot, prices)) = market_snapshot {
            if inner.authority.profit_driven_policy || inner.authority.fully_invested_policy {
                let previous = inner.authority.clone();
                if inner
                    .authority
                    .observe_research_snapshot(snapshot.clone(), updated_at)
                    .is_ok()
                {
                    if let Some(path) = inner.state_file.as_deref() {
                        if let Err(error) = inner.authority.persist_to_path(path) {
                            if !inner.authority.fully_invested_policy
                                || !inner.authority.allocation_commit_matches(path)
                            {
                                inner.authority = previous;
                            }
                            return Err(error);
                        }
                    }
                    inner.snapshot.portfolio = inner.authority.portfolio.clone();
                }
            }
            inner.snapshot.latest_market_snapshot = Some(snapshot);
            let (prices, stale_price_symbols) =
                retain_portfolio_marks(prices, &inner.snapshot.prices, &inner.snapshot.portfolio);
            inner.snapshot.prices = prices;
            inner.snapshot.stale_price_symbols = stale_price_symbols;
            update_dashboard_benchmark_from_prices(&mut inner.snapshot);
            record_current_portfolio_history(&mut inner.snapshot);
            persist_dashboard_state_best_effort(&inner);
        }
        Ok(())
    }

    #[cfg(test)]
    fn update_service_status_for_test(
        &self,
        status: String,
        detail: String,
        market_snapshot: Option<Value>,
        last_error: Option<String>,
        updated_at: DateTime<Utc>,
    ) -> Result<(), LlmEquityAuthorityError> {
        let validated_snapshot = match market_snapshot {
            Some(value) => {
                reject_forbidden_json(&value)?;
                let snapshot = serde_json::from_value::<SnapshotEnvelope>(value)
                    .map_err(|err| LlmEquityAuthorityError::MalformedSnapshot(err.to_string()))?;
                validate_snapshot(&snapshot)?;
                let prices = snapshot_mid_prices(&snapshot)?;
                let sanitized = serde_json::to_value(&snapshot)
                    .map_err(|err| LlmEquityAuthorityError::MalformedSnapshot(err.to_string()))?;
                Some((sanitized, prices))
            }
            None => None,
        };
        self.update_service_status_at(ValidatedLlmEquityServiceStatusUpdate {
            status,
            detail,
            market_snapshot: validated_snapshot,
            last_error,
            service_identity: None,
            market_session: None,
            service_activity_key: None,
            updated_at,
        })
    }
}

fn is_valid_market_session_label(value: &str) -> bool {
    matches!(value, "premarket" | "rth" | "after_hours" | "closed")
}

fn retain_portfolio_marks(
    mut prices: HashMap<String, f64>,
    previous_prices: &HashMap<String, f64>,
    portfolio: &LlmEquityPortfolio,
) -> (HashMap<String, f64>, HashSet<String>) {
    let mut stale_symbols = HashSet::new();
    let mut retained_symbols = portfolio.positions.keys().cloned().collect::<Vec<_>>();
    retained_symbols.push(PORTFOLIO_BENCHMARK_SYMBOL.to_string());
    retained_symbols.sort();
    retained_symbols.dedup();
    for symbol in retained_symbols {
        let has_current = prices
            .get(&symbol)
            .is_some_and(|price| price.is_finite() && *price > 0.0);
        if has_current {
            continue;
        }
        if let Some(price) = previous_prices
            .get(&symbol)
            .copied()
            .filter(|price| price.is_finite() && *price > 0.0)
        {
            stale_symbols.insert(symbol.clone());
            prices.insert(symbol, price);
        }
    }
    (prices, stale_symbols)
}

fn record_current_portfolio_history(snapshot: &mut LlmEquityRuntimeSnapshot) {
    let point = LlmEquityPortfolioHistoryPoint {
        updated_at: snapshot.updated_at,
        portfolio: snapshot.portfolio.clone(),
        prices: snapshot.prices.clone(),
    };
    record_portfolio_history_point(&mut snapshot.portfolio_history, point);
}

fn record_portfolio_history_point(
    history: &mut Vec<LlmEquityPortfolioHistoryPoint>,
    point: LlmEquityPortfolioHistoryPoint,
) {
    let day = point.updated_at.date_naive();
    if let Some(existing) = history
        .iter_mut()
        .find(|item| item.updated_at.date_naive() == day)
    {
        *existing = point;
    } else {
        history.push(point);
    }
    history.sort_by_key(|item| item.updated_at);
}

fn update_dashboard_benchmark_from_prices(snapshot: &mut LlmEquityRuntimeSnapshot) {
    let Some(price) = snapshot
        .prices
        .get(PORTFOLIO_BENCHMARK_SYMBOL)
        .copied()
        .filter(|price| price.is_finite() && *price > 0.0)
    else {
        return;
    };
    if snapshot.dry_run_started_at.is_none() {
        snapshot.dry_run_started_at = Some(snapshot.updated_at);
    }
    if snapshot.benchmark_start_price_usd.is_none() {
        snapshot.benchmark_start_price_usd = Some(price);
        snapshot.benchmark_started_at = Some(snapshot.updated_at);
    }
}

fn record_cycle_summary_in_list(
    summaries: &mut Vec<LlmEquityCycleSummary>,
    summary: LlmEquityCycleSummary,
) {
    summaries.retain(|existing| existing.cycle_id != summary.cycle_id);
    summaries.insert(0, summary);
    summaries.sort_by_key(|item| std::cmp::Reverse(item.timestamp_ns));
    summaries.truncate(LLM_EQUITY_MAX_CYCLE_SUMMARIES);
}

fn persist_dashboard_state(inner: &LlmEquityRuntimeInner) -> Result<(), LlmEquityAuthorityError> {
    let Some(path) = inner.dashboard_state_file.as_deref() else {
        return Ok(());
    };
    let state = PersistedLlmEquityDashboard {
        schema_version: 1,
        saved_at: Utc::now(),
        dry_run_started_at: inner.snapshot.dry_run_started_at,
        benchmark_started_at: inner.snapshot.benchmark_started_at,
        benchmark_start_price_usd: inner.snapshot.benchmark_start_price_usd,
        portfolio_history: inner.snapshot.portfolio_history.clone(),
        cycle_summaries: inner.snapshot.cycle_summaries.clone(),
    };
    let encoded = serde_json::to_vec_pretty(&state)
        .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;
    if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;
    }
    let temp_path = path.with_extension(format!(
        "json.{}.{}.tmp",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let persist_result = (|| -> Result<(), LlmEquityAuthorityError> {
        let mut temp = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;
        temp.write_all(&encoded)
            .and_then(|()| temp.sync_all())
            .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;
        fs::rename(&temp_path, path)
            .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;
        if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
            fs::File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;
        }
        Ok(())
    })();
    if persist_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    persist_result
}

fn persist_dashboard_state_best_effort(inner: &LlmEquityRuntimeInner) {
    let _ = persist_dashboard_state(inner);
}

fn load_or_derive_dashboard_bootstrap(
    dashboard_state_file: &Path,
    authority_state_file: Option<&Path>,
    authority: &LlmEquityDryRunAuthority,
) -> LlmEquityDashboardBootstrap {
    if let Ok(encoded) = fs::read(dashboard_state_file) {
        if let Ok(state) = serde_json::from_slice::<PersistedLlmEquityDashboard>(&encoded) {
            if state.schema_version == 1
                && state
                    .benchmark_start_price_usd
                    .is_none_or(|price| price.is_finite() && price > 0.0)
            {
                return LlmEquityDashboardBootstrap {
                    dry_run_started_at: state.dry_run_started_at,
                    benchmark_started_at: state.benchmark_started_at,
                    benchmark_start_price_usd: state.benchmark_start_price_usd,
                    portfolio_history: state.portfolio_history,
                    cycle_summaries: bounded_cycle_summaries(state.cycle_summaries),
                };
            }
        }
    }
    derive_dashboard_bootstrap(authority_state_file, authority)
}

fn derive_dashboard_bootstrap(
    authority_state_file: Option<&Path>,
    authority: &LlmEquityDryRunAuthority,
) -> LlmEquityDashboardBootstrap {
    let mut bootstrap = LlmEquityDashboardBootstrap::default();
    if let Some(path) = authority_state_file {
        derive_dashboard_from_authority_journal(path, &mut bootstrap);
        derive_dashboard_cycle_summaries(path, &mut bootstrap);
    }
    if bootstrap.dry_run_started_at.is_none() {
        bootstrap.dry_run_started_at = Some(Utc::now());
    }
    if bootstrap.benchmark_start_price_usd.is_none() {
        bootstrap.benchmark_start_price_usd = authority
            .last_prices
            .get(PORTFOLIO_BENCHMARK_SYMBOL)
            .copied()
            .filter(|price| price.is_finite() && *price > 0.0);
        if bootstrap.benchmark_start_price_usd.is_some() {
            bootstrap.benchmark_started_at = bootstrap.dry_run_started_at;
        }
    }
    if bootstrap.portfolio_history.is_empty() && !authority.last_prices.is_empty() {
        let updated_at = bootstrap.dry_run_started_at.unwrap_or_else(Utc::now);
        record_portfolio_history_point(
            &mut bootstrap.portfolio_history,
            LlmEquityPortfolioHistoryPoint {
                updated_at,
                portfolio: authority.portfolio.clone(),
                prices: authority.last_prices.clone(),
            },
        );
    }
    bootstrap
}

fn derive_dashboard_from_authority_journal(
    authority_state_file: &Path,
    bootstrap: &mut LlmEquityDashboardBootstrap,
) {
    let journal_path = authority_state_file.with_extension("jsonl");
    let Ok(encoded) = fs::read_to_string(journal_path) else {
        return;
    };
    for line in encoded.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(state) = serde_json::from_str::<PersistedLlmEquityAuthority>(line) else {
            continue;
        };
        if validate_persisted_authority(&state).is_err() {
            continue;
        }
        bootstrap.dry_run_started_at.get_or_insert(state.saved_at);
        if bootstrap.benchmark_start_price_usd.is_none() {
            if let Some(price) = state
                .last_prices
                .get(PORTFOLIO_BENCHMARK_SYMBOL)
                .copied()
                .filter(|price| price.is_finite() && *price > 0.0)
            {
                bootstrap.benchmark_start_price_usd = Some(price);
                bootstrap.benchmark_started_at = Some(state.saved_at);
            }
        }
        record_portfolio_history_point(
            &mut bootstrap.portfolio_history,
            LlmEquityPortfolioHistoryPoint {
                updated_at: state.saved_at,
                portfolio: state.portfolio,
                prices: state.last_prices,
            },
        );
    }
}

fn derive_dashboard_cycle_summaries(
    authority_state_file: &Path,
    bootstrap: &mut LlmEquityDashboardBootstrap,
) {
    let Some(parent) = authority_state_file.parent() else {
        return;
    };
    let path = parent.join(LLM_EQUITY_CYCLE_AUDIT_LEDGER_NAME);
    let Ok(encoded) = fs::read_to_string(path) else {
        return;
    };
    let Ok(Value::Array(rows)) = serde_json::from_str::<Value>(&encoded) else {
        return;
    };
    for row in rows.iter().rev() {
        if let Some(summary) = cycle_summary_from_audit_row(row) {
            record_cycle_summary_in_list(&mut bootstrap.cycle_summaries, summary);
        }
    }
}

fn bounded_cycle_summaries(summaries: Vec<LlmEquityCycleSummary>) -> Vec<LlmEquityCycleSummary> {
    let mut result = Vec::new();
    for summary in summaries {
        if validate_cycle_summary(&summary).is_ok() && is_trade_ledger_cycle_summary(&summary) {
            record_cycle_summary_in_list(&mut result, summary);
        }
    }
    result
}

fn cycle_summary_from_audit_row(row: &Value) -> Option<LlmEquityCycleSummary> {
    let cycle_kind = row.get("cycle_kind")?.as_str()?.trim();
    if !matches!(
        cycle_kind,
        "run_once" | "news_scan" | "discovery_scan" | "release"
    ) {
        return None;
    }
    let market_session = row.get("market_session")?.as_str()?.trim();
    if market_session != "rth" {
        return None;
    }
    let mandatory_trade_status = row
        .get("mandatory_trade_status")
        .and_then(Value::as_str)
        .unwrap_or("not_applicable");
    let profit_driven = matches!(
        row.get("risk_mode").and_then(Value::as_str),
        Some("profit_driven" | "fully_invested")
    );
    if mandatory_trade_status != "accepted" && !profit_driven {
        return None;
    }
    let cycle_id = row.get("cycle_id")?.as_str()?.to_string();
    let as_of = row
        .get("as_of")
        .and_then(Value::as_str)
        .or_else(|| row.get("created_at").and_then(Value::as_str))?
        .to_string();
    let rust_response = row.get("rust_response").unwrap_or(&Value::Null);
    let order_results = rust_response
        .get("order_results")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let accepted_orders = order_results
        .iter()
        .filter(|order| order.get("status").and_then(Value::as_str) == Some("accepted"))
        .collect::<Vec<_>>();
    let accepted_trades = row
        .get("accepted_trades")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|trade| {
            let action = trade
                .get("action")
                .and_then(Value::as_str)
                .and_then(normalized_summary_action)?;
            let symbol = trade
                .get("symbol")
                .and_then(Value::as_str)
                .filter(|symbol| is_valid_llm_equity_symbol(symbol))?
                .to_string();
            let notional = trade
                .get("accepted_notional_usd")
                .and_then(Value::as_f64)
                .filter(|value| value.is_finite() && *value > 0.0)?;
            Some((action, symbol, notional))
        })
        .collect::<Vec<_>>();
    let review_attestation = row.get("review_attestation").unwrap_or(&Value::Null);
    let requested_notional_usd = review_attestation
        .get("order_notional_usd")
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && *value >= 0.0)
        .unwrap_or_else(|| {
            order_results
                .iter()
                .filter_map(|order| order.get("notional_usd").and_then(Value::as_f64))
                .filter(|value| value.is_finite() && *value > 0.0)
                .sum()
        });
    let accepted_notional_usd = if accepted_trades.is_empty() {
        row.get("mandatory_accepted_total_notional_usd")
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite() && *value >= 0.0)
            .unwrap_or_else(|| {
                accepted_orders
                    .iter()
                    .filter_map(|order| order.get("notional_usd").and_then(Value::as_f64))
                    .filter(|value| value.is_finite() && *value > 0.0)
                    .sum()
            })
    } else {
        accepted_trades
            .iter()
            .map(|(_action, _symbol, notional)| *notional)
            .sum()
    };
    let required_notional_usd = if profit_driven {
        0.0
    } else {
        row.get("mandatory_required_total_notional_usd")
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite() && *value >= 0.0)
            .unwrap_or_else(|| mandatory_notional_for_cycle_kind(cycle_kind))
            .max(LLM_EQUITY_MANDATORY_ORDER_NOTIONAL_USD)
    };
    if accepted_notional_usd + LLM_EQUITY_SUMMARY_NOTIONAL_EPSILON_USD < required_notional_usd {
        return None;
    }
    let pairs = if accepted_trades.is_empty() {
        accepted_orders
            .iter()
            .filter_map(|order| {
                let action = order
                    .get("side")
                    .and_then(Value::as_str)
                    .and_then(normalized_summary_action)?;
                let symbol = order
                    .get("symbol")
                    .and_then(Value::as_str)
                    .filter(|symbol| is_valid_llm_equity_symbol(symbol))?
                    .to_string();
                Some((action, symbol))
            })
            .collect::<Vec<_>>()
    } else {
        accepted_trades
            .iter()
            .map(|(action, symbol, _notional)| (action.clone(), symbol.clone()))
            .collect::<Vec<_>>()
    };
    let pairs = dedupe_summary_pairs(
        pairs,
        if row.get("risk_mode").and_then(Value::as_str) == Some("fully_invested") {
            80
        } else {
            8
        },
    );
    let actions = pairs
        .iter()
        .map(|(action, _symbol)| action.clone())
        .collect::<Vec<_>>();
    let symbols = pairs
        .iter()
        .map(|(_action, symbol)| symbol.clone())
        .collect::<Vec<_>>();
    let rationale = row
        .get("dashboard_summary_thesis")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("Legacy cycle-audit row does not retain the original LLM rationale.");
    let reason = accepted_orders
        .first()
        .and_then(|order| order.get("reason").and_then(Value::as_str))
        .or_else(|| rust_response.get("reason").and_then(Value::as_str))
        .or_else(|| row.get("no_trade_reason").and_then(Value::as_str))
        .or_else(|| row.get("edge_gate_status").and_then(Value::as_str))
        .unwrap_or("legacy_cycle_audit");
    let status = "accepted";
    let timestamp = row
        .get("created_at")
        .and_then(Value::as_str)
        .and_then(|value| parse_utc(value).ok())
        .or_else(|| {
            row.get("as_of")
                .and_then(Value::as_str)
                .and_then(|value| parse_utc(value).ok())
        })
        .unwrap_or_else(Utc::now);
    let summary = LlmEquityCycleSummary {
        cycle_id,
        cycle_kind: cycle_kind.to_string(),
        as_of,
        market_session: market_session.to_string(),
        status: status.to_string(),
        mandatory_trade_status: mandatory_trade_status.to_string(),
        required_notional_usd,
        requested_notional_usd,
        accepted_notional_usd,
        actions,
        symbols,
        rationale: rationale.to_string(),
        reason: reason.to_string(),
        timestamp_ns: timestamp_ns_from_datetime(timestamp),
    };
    validate_cycle_summary(&summary).ok()?;
    Some(summary)
}

fn mandatory_notional_for_cycle_kind(cycle_kind: &str) -> f64 {
    if matches!(cycle_kind, "release" | "discovery_scan") {
        100.0
    } else {
        LLM_EQUITY_MANDATORY_ORDER_NOTIONAL_USD
    }
}

fn normalized_summary_action(value: &str) -> Option<String> {
    match value.trim() {
        "buy" | "buy_notional" => Some("buy".to_string()),
        "sell" | "sell_notional" => Some("sell".to_string()),
        _ => None,
    }
}

fn dedupe_summary_pairs(values: Vec<(String, String)>, limit: usize) -> Vec<(String, String)> {
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    for value in values {
        if value.0.trim().is_empty() || value.1.trim().is_empty() || !seen.insert(value.clone()) {
            continue;
        }
        result.push(value);
        if result.len() >= limit {
            break;
        }
    }
    result
}

pub fn validate_cycle_summary(
    summary: &LlmEquityCycleSummary,
) -> Result<(), LlmEquityAuthorityError> {
    if summary.cycle_id.trim().is_empty() || summary.cycle_id.len() > 160 {
        return Err(LlmEquityAuthorityError::Contract(
            "cycle_summary_id_invalid",
        ));
    }
    if summary.cycle_kind.trim().is_empty() || summary.cycle_kind.len() > 80 {
        return Err(LlmEquityAuthorityError::Contract(
            "cycle_summary_kind_invalid",
        ));
    }
    if summary.as_of.trim().is_empty() || summary.as_of.len() > 80 {
        return Err(LlmEquityAuthorityError::Contract(
            "cycle_summary_as_of_invalid",
        ));
    }
    if !is_valid_market_session_label(&summary.market_session) {
        return Err(LlmEquityAuthorityError::Contract(
            "cycle_summary_market_session_invalid",
        ));
    }
    if !matches!(summary.status.as_str(), "accepted" | "blocked" | "rejected") {
        return Err(LlmEquityAuthorityError::Contract(
            "cycle_summary_status_invalid",
        ));
    }
    if !matches!(
        summary.mandatory_trade_status.as_str(),
        "accepted" | "blocked" | "recovering" | "not_applicable"
    ) {
        return Err(LlmEquityAuthorityError::Contract(
            "cycle_summary_mandatory_status_invalid",
        ));
    }
    if !summary.required_notional_usd.is_finite()
        || summary.required_notional_usd < 0.0
        || !summary.requested_notional_usd.is_finite()
        || summary.requested_notional_usd < 0.0
        || !summary.accepted_notional_usd.is_finite()
        || summary.accepted_notional_usd < 0.0
    {
        return Err(LlmEquityAuthorityError::Contract(
            "cycle_summary_notional_invalid",
        ));
    }
    if summary.actions.is_empty()
        || summary.actions.len() > 80
        || summary
            .actions
            .iter()
            .any(|value| !matches!(value.as_str(), "buy" | "sell"))
    {
        return Err(LlmEquityAuthorityError::Contract(
            "cycle_summary_actions_invalid",
        ));
    }
    if summary.symbols.is_empty()
        || summary.symbols.len() > 80
        || summary
            .symbols
            .iter()
            .any(|value| !is_valid_llm_equity_symbol(value))
    {
        return Err(LlmEquityAuthorityError::Contract(
            "cycle_summary_symbols_invalid",
        ));
    }
    if summary.rationale.trim().is_empty()
        || summary.rationale.len() > 1_200
        || contains_credential_assignment(&summary.rationale)
        || contains_credential_assignment(&summary.reason)
        || summary.reason.trim().is_empty()
        || summary.reason.len() > 240
    {
        return Err(LlmEquityAuthorityError::Contract(
            "cycle_summary_text_invalid",
        ));
    }
    Ok(())
}

fn is_trade_ledger_cycle_summary(summary: &LlmEquityCycleSummary) -> bool {
    let quota_free =
        summary.mandatory_trade_status == "not_applicable" && summary.required_notional_usd == 0.0;
    let required_floor = if quota_free {
        0.0
    } else {
        summary
            .required_notional_usd
            .max(LLM_EQUITY_MANDATORY_ORDER_NOTIONAL_USD)
    };
    // Blocked mandatory attempts remain in audit/status history; this compact
    // list powers the dashboard's accepted-trade ledger only.
    summary.status == "accepted"
        && (summary.mandatory_trade_status == "accepted" || quota_free)
        && summary.accepted_notional_usd > 0.0
        && summary.accepted_notional_usd + LLM_EQUITY_SUMMARY_NOTIONAL_EPSILON_USD >= required_floor
}

pub fn timestamp_ns_from_datetime(timestamp: DateTime<Utc>) -> u64 {
    timestamp
        .timestamp_nanos_opt()
        .map(|value| value.max(0) as u64)
        .unwrap_or_default()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LlmEquityAuthorityError {
    MalformedDecision(String),
    MalformedSnapshot(String),
    Contract(&'static str),
    Replay(&'static str),
    Persistence(String),
}

impl fmt::Display for LlmEquityAuthorityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedDecision(reason) => write!(f, "malformed LLM equity decision: {reason}"),
            Self::MalformedSnapshot(reason) => {
                write!(f, "malformed market research snapshot: {reason}")
            }
            Self::Contract(reason) => write!(f, "LLM equity contract rejected: {reason}"),
            Self::Replay(reason) => write!(f, "LLM equity replay rejected: {reason}"),
            Self::Persistence(reason) => write!(f, "LLM equity state persistence failed: {reason}"),
        }
    }
}

impl Error for LlmEquityAuthorityError {}

#[derive(Debug, Clone)]
pub struct LlmEquityDryRunAuthority {
    pub portfolio: LlmEquityPortfolio,
    last_prices: HashMap<String, f64>,
    seen_cycle_ids: HashSet<String>,
    highest_sequence: u64,
    fills_enabled: bool,
    risk_limits_enabled: bool,
    forced_aggressive_policy: bool,
    profit_driven_policy: bool,
    fully_invested_policy: bool,
    allocation_receipt: Option<fully_invested::AllocationReceipt>,
    drawdown_halted: bool,
    daily_loss_halted: bool,
    long_risk_adjusted_policy: bool,
    realistic_execution: bool,
    turnover_window: Option<(NaiveDate, u32)>,
    portfolio_day: Option<NaiveDate>,
    turnover_today_usd: f64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedLlmEquityAuthority {
    schema_version: u32,
    saved_at: DateTime<Utc>,
    portfolio: LlmEquityPortfolio,
    #[serde(default)]
    last_prices: HashMap<String, f64>,
    seen_cycle_ids: HashSet<String>,
    highest_sequence: u64,
    turnover_window: Option<(NaiveDate, u32)>,
    portfolio_day: Option<NaiveDate>,
    #[serde(default)]
    turnover_today_usd: f64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    drawdown_halted: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    daily_loss_halted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    allocation_receipt: Option<fully_invested::AllocationReceipt>,
}

impl Default for LlmEquityDryRunAuthority {
    fn default() -> Self {
        Self::seeded()
    }
}

impl LlmEquityDryRunAuthority {
    pub fn seeded() -> Self {
        Self {
            portfolio: LlmEquityPortfolio::seeded(),
            last_prices: HashMap::new(),
            seen_cycle_ids: HashSet::new(),
            highest_sequence: 0,
            fills_enabled: true,
            risk_limits_enabled: false,
            forced_aggressive_policy: false,
            profit_driven_policy: false,
            fully_invested_policy: false,
            allocation_receipt: None,
            drawdown_halted: false,
            daily_loss_halted: false,
            long_risk_adjusted_policy: false,
            realistic_execution: false,
            turnover_window: None,
            portfolio_day: None,
            turnover_today_usd: 0.0,
        }
    }

    pub fn paper() -> Self {
        Self {
            portfolio: LlmEquityPortfolio::seeded(),
            last_prices: HashMap::new(),
            seen_cycle_ids: HashSet::new(),
            highest_sequence: 0,
            fills_enabled: true,
            risk_limits_enabled: true,
            forced_aggressive_policy: false,
            profit_driven_policy: false,
            fully_invested_policy: false,
            allocation_receipt: None,
            drawdown_halted: false,
            daily_loss_halted: false,
            long_risk_adjusted_policy: false,
            realistic_execution: true,
            turnover_window: None,
            portfolio_day: None,
            turnover_today_usd: 0.0,
        }
    }

    pub fn paper_llm_discretion() -> Self {
        Self {
            portfolio: LlmEquityPortfolio::seeded(),
            last_prices: HashMap::new(),
            seen_cycle_ids: HashSet::new(),
            highest_sequence: 0,
            fills_enabled: true,
            risk_limits_enabled: false,
            forced_aggressive_policy: false,
            profit_driven_policy: false,
            fully_invested_policy: false,
            allocation_receipt: None,
            drawdown_halted: false,
            daily_loss_halted: false,
            long_risk_adjusted_policy: false,
            realistic_execution: true,
            turnover_window: None,
            portfolio_day: None,
            turnover_today_usd: 0.0,
        }
    }

    pub fn paper_forced_aggressive() -> Self {
        Self {
            portfolio: LlmEquityPortfolio::seeded(),
            last_prices: HashMap::new(),
            seen_cycle_ids: HashSet::new(),
            highest_sequence: 0,
            fills_enabled: true,
            risk_limits_enabled: true,
            forced_aggressive_policy: true,
            profit_driven_policy: false,
            fully_invested_policy: false,
            allocation_receipt: None,
            drawdown_halted: false,
            daily_loss_halted: false,
            long_risk_adjusted_policy: false,
            realistic_execution: true,
            turnover_window: None,
            portfolio_day: None,
            turnover_today_usd: 0.0,
        }
    }

    pub fn paper_profit_driven() -> Self {
        Self {
            profit_driven_policy: true,
            ..Self::paper_forced_aggressive()
        }
    }

    /// Isolated research-only long policy with the production 7.5% name,
    /// 75% gross, 25% cash, 150% turnover, 1% loss, and 5% drawdown limits.
    pub fn paper_long_risk_adjusted() -> Self {
        Self {
            portfolio: LlmEquityPortfolio::seeded(),
            last_prices: HashMap::new(),
            seen_cycle_ids: HashSet::new(),
            highest_sequence: 0,
            fills_enabled: true,
            risk_limits_enabled: true,
            forced_aggressive_policy: false,
            profit_driven_policy: false,
            fully_invested_policy: false,
            allocation_receipt: None,
            drawdown_halted: false,
            daily_loss_halted: false,
            long_risk_adjusted_policy: true,
            realistic_execution: true,
            turnover_window: None,
            portfolio_day: None,
            turnover_today_usd: 0.0,
        }
    }

    pub fn shadow() -> Self {
        Self {
            portfolio: LlmEquityPortfolio::seeded(),
            last_prices: HashMap::new(),
            seen_cycle_ids: HashSet::new(),
            highest_sequence: 0,
            fills_enabled: false,
            risk_limits_enabled: true,
            forced_aggressive_policy: false,
            profit_driven_policy: false,
            fully_invested_policy: false,
            allocation_receipt: None,
            drawdown_halted: false,
            daily_loss_halted: false,
            long_risk_adjusted_policy: false,
            realistic_execution: true,
            turnover_window: None,
            portfolio_day: None,
            turnover_today_usd: 0.0,
        }
    }

    fn persisted_state(&self) -> PersistedLlmEquityAuthority {
        PersistedLlmEquityAuthority {
            schema_version: 1,
            saved_at: Utc::now(),
            portfolio: self.portfolio.clone(),
            last_prices: self.last_prices.clone(),
            seen_cycle_ids: self.seen_cycle_ids.clone(),
            highest_sequence: self.highest_sequence,
            turnover_window: self.turnover_window,
            portfolio_day: self.portfolio_day,
            turnover_today_usd: self.turnover_today_usd,
            drawdown_halted: self.drawdown_halted,
            daily_loss_halted: self.daily_loss_halted,
            allocation_receipt: self.allocation_receipt.clone(),
        }
    }

    /// Confirm that an allocation reached its durable snapshot commit point.
    pub fn allocation_commit_matches(&self, path: &Path) -> bool {
        if !self.fully_invested_policy {
            return false;
        }
        fs::read(path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<PersistedLlmEquityAuthority>(&bytes).ok())
            .is_some_and(|state| {
                state.highest_sequence == self.highest_sequence
                    && state.allocation_receipt.as_ref().map(|r| &r.decision_hash)
                        == self.allocation_receipt.as_ref().map(|r| &r.decision_hash)
                    && state.portfolio == self.portfolio
            })
    }

    fn persist_to_path(&self, path: &Path) -> Result<(), LlmEquityAuthorityError> {
        let state = self.persisted_state();
        let encoded = serde_json::to_vec_pretty(&state)
            .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;
        if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
            fs::create_dir_all(parent)
                .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;
        }
        let temp_path = path.with_extension(format!(
            "json.{}.{}.tmp",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let persist_result = (|| -> Result<(), LlmEquityAuthorityError> {
            let mut temp = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temp_path)
                .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;
            temp.write_all(&encoded)
                .and_then(|()| temp.sync_all())
                .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;

            // For atomic allocations the snapshot (including the retry receipt)
            // is the commit record. Never journal an uncommitted rebalance.
            if self.fully_invested_policy {
                fs::rename(&temp_path, path)
                    .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;
                if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                    fs::File::open(parent)
                        .and_then(|dir| dir.sync_all())
                        .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;
                }
            }
            let line = serde_json::to_vec(&state)
                .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;
            let journal_path = path.with_extension("jsonl");
            let mut journal = OpenOptions::new()
                .create(true)
                .append(true)
                .open(journal_path)
                .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;
            journal
                .write_all(&line)
                .and_then(|()| journal.write_all(b"\n"))
                .and_then(|()| journal.sync_data())
                .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;

            if !self.fully_invested_policy {
                fs::rename(&temp_path, path)
                    .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;
            }
            if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
                fs::File::open(parent)
                    .and_then(|directory| directory.sync_all())
                    .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;
            }
            Ok(())
        })();
        if persist_result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }
        persist_result?;
        Ok(())
    }

    fn restore_from_path(
        path: &Path,
        fills_enabled: bool,
        risk_limits_enabled: bool,
        realistic_execution: bool,
    ) -> Result<Self, LlmEquityAuthorityError> {
        let metadata = fs::metadata(path)
            .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;
        if metadata.len() > 16 * 1024 * 1024 {
            return Err(LlmEquityAuthorityError::Persistence(
                "persisted authority state exceeds size limit".to_string(),
            ));
        }
        let encoded = fs::read(path)
            .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;
        let state: PersistedLlmEquityAuthority = serde_json::from_slice(&encoded)
            .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;
        Self::from_persisted_state(
            state,
            fills_enabled,
            risk_limits_enabled,
            realistic_execution,
        )
    }

    fn from_persisted_state(
        state: PersistedLlmEquityAuthority,
        fills_enabled: bool,
        risk_limits_enabled: bool,
        realistic_execution: bool,
    ) -> Result<Self, LlmEquityAuthorityError> {
        validate_persisted_authority(&state)?;
        Ok(Self {
            portfolio: state.portfolio,
            last_prices: state.last_prices,
            seen_cycle_ids: state.seen_cycle_ids,
            highest_sequence: state.highest_sequence,
            fills_enabled,
            risk_limits_enabled,
            forced_aggressive_policy: false,
            profit_driven_policy: false,
            fully_invested_policy: false,
            allocation_receipt: state.allocation_receipt,
            drawdown_halted: state.drawdown_halted,
            daily_loss_halted: state.daily_loss_halted,
            long_risk_adjusted_policy: false,
            realistic_execution,
            turnover_window: state.turnover_window,
            portfolio_day: state.portfolio_day,
            turnover_today_usd: state.turnover_today_usd,
        })
    }

    pub fn apply_json(
        &mut self,
        decision_json: &str,
        snapshot_json: &str,
        received_at: DateTime<Utc>,
    ) -> Result<LlmEquityApplyResult, LlmEquityAuthorityError> {
        self.apply_json_with_validated_payloads(decision_json, snapshot_json, received_at)
            .map(|cycle| cycle.result)
    }

    /// Apply one paper cycle at an injected research clock and return a
    /// serialization-friendly result. The live runtime continues to call the
    /// same authority with `Utc::now()` and is not routed through this helper.
    pub fn apply_research_json(
        &mut self,
        decision_json: &str,
        snapshot_json: &str,
        simulated_received_at: DateTime<Utc>,
    ) -> Result<Value, LlmEquityAuthorityError> {
        let mut result = self.apply_json(decision_json, snapshot_json, simulated_received_at)?;
        self.prune_research_position_dust();
        result.portfolio = self.portfolio.clone();
        serde_json::to_value(result)
            .map_err(|error| LlmEquityAuthorityError::MalformedDecision(error.to_string()))
    }

    fn prune_research_position_dust(&mut self) {
        let last_prices = &self.last_prices;
        self.portfolio.positions.retain(|symbol, position| {
            if position.quantity.abs() <= RESEARCH_POSITION_DUST_QUANTITY_EPSILON {
                return false;
            }
            last_prices.get(symbol).is_none_or(|price| {
                position.market_value(*price).abs() >= RESEARCH_POSITION_DUST_NOTIONAL_EPSILON_USD
            })
        });
    }

    /// Persist research authority state using the same atomic snapshot and
    /// journal format as the paper runtime.
    pub fn persist_research_to_path(&self, path: &Path) -> Result<(), LlmEquityAuthorityError> {
        self.persist_to_path(path)
    }

    /// Export the complete isolated authority state for coordinator-owned,
    /// content-addressed checkpointing.
    pub fn export_research_state(&self) -> Result<Value, LlmEquityAuthorityError> {
        serde_json::to_value(self.persisted_state())
            .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))
    }

    /// Restore an isolated research authority without consulting live runtime
    /// environment variables.
    pub fn restore_research_from_path(
        path: &Path,
        policy: &str,
    ) -> Result<Self, LlmEquityAuthorityError> {
        let (risk_limits_enabled, forced_aggressive_policy, long_risk_adjusted_policy) =
            match policy {
                "paper" => (true, false, false),
                "forced_aggressive" | "profit_driven" | "fully_invested" => (true, true, false),
                "llm_discretion" => (false, false, false),
                "long_risk_adjusted" => (true, false, true),
                _ => {
                    return Err(LlmEquityAuthorityError::Persistence(
                        "unsupported research policy".to_string(),
                    ));
                }
            };
        let mut restored = Self::restore_from_path(path, true, risk_limits_enabled, true)?;
        restored.forced_aggressive_policy = forced_aggressive_policy;
        restored.profit_driven_policy = policy == "profit_driven";
        restored.fully_invested_policy = policy == "fully_invested";
        restored.long_risk_adjusted_policy = long_risk_adjusted_policy;
        Ok(restored)
    }

    /// Restore a coordinator-owned research checkpoint without reading live
    /// runtime configuration or allowing the child process to own persistence.
    pub fn restore_research_from_value(
        value: Value,
        policy: &str,
    ) -> Result<Self, LlmEquityAuthorityError> {
        let state: PersistedLlmEquityAuthority = serde_json::from_value(value)
            .map_err(|error| LlmEquityAuthorityError::Persistence(error.to_string()))?;
        let (risk_limits_enabled, forced_aggressive_policy, long_risk_adjusted_policy) =
            match policy {
                "paper" => (true, false, false),
                "forced_aggressive" | "profit_driven" | "fully_invested" => (true, true, false),
                "llm_discretion" => (false, false, false),
                "long_risk_adjusted" => (true, false, true),
                _ => {
                    return Err(LlmEquityAuthorityError::Persistence(
                        "unsupported research policy".to_string(),
                    ));
                }
            };
        let mut restored = Self::from_persisted_state(state, true, risk_limits_enabled, true)?;
        restored.forced_aggressive_policy = forced_aggressive_policy;
        restored.profit_driven_policy = policy == "profit_driven";
        restored.fully_invested_policy = policy == "fully_invested";
        restored.long_risk_adjusted_policy = long_risk_adjusted_policy;
        Ok(restored)
    }

    /// Apply a point-in-time corporate action to the isolated research book.
    /// This method is not used by the resident runtime or HTTP endpoint.
    pub fn apply_research_corporate_action(
        &mut self,
        symbol: &str,
        action_type: &str,
        ratio: Option<f64>,
        cash_amount_usd: Option<f64>,
        new_symbol: Option<&str>,
    ) -> Result<(), LlmEquityAuthorityError> {
        if !is_valid_llm_equity_symbol(symbol) {
            return Err(LlmEquityAuthorityError::Contract("invalid_symbol"));
        }
        self.prune_research_position_dust();
        match action_type {
            "split" => {
                let ratio = ratio
                    .filter(|value| value.is_finite() && *value > 0.0)
                    .ok_or(LlmEquityAuthorityError::Contract(
                        "corporate_action_ratio_invalid",
                    ))?;
                if let Some(position) = self.portfolio.positions.get_mut(symbol) {
                    position.quantity *= ratio;
                    position.avg_price /= ratio;
                }
                if let Some(price) = self.last_prices.get_mut(symbol) {
                    *price /= ratio;
                }
            }
            "cash_dividend" => {
                let amount = cash_amount_usd
                    .filter(|value| value.is_finite() && *value >= 0.0)
                    .ok_or(LlmEquityAuthorityError::Contract(
                        "corporate_action_cash_invalid",
                    ))?;
                if let Some(position) = self.portfolio.positions.get(symbol) {
                    self.portfolio.cash_usd += position.quantity * amount;
                    self.portfolio.realized_pnl_usd += position.quantity * amount;
                }
            }
            "symbol_change" => {
                let new_symbol = new_symbol
                    .filter(|value| is_valid_llm_equity_symbol(value))
                    .ok_or(LlmEquityAuthorityError::Contract(
                        "corporate_action_new_symbol_invalid",
                    ))?;
                if new_symbol == symbol {
                    return Ok(());
                }
                if self.portfolio.positions.contains_key(new_symbol) {
                    return Err(LlmEquityAuthorityError::Contract(
                        "corporate_action_symbol_collision",
                    ));
                }
                if let Some(mut position) = self.portfolio.positions.remove(symbol) {
                    position.symbol = new_symbol.to_string();
                    self.portfolio
                        .positions
                        .insert(new_symbol.to_string(), position);
                }
                if let Some(price) = self.last_prices.remove(symbol) {
                    self.last_prices.insert(new_symbol.to_string(), price);
                }
            }
            _ => {
                return Err(LlmEquityAuthorityError::Contract(
                    "corporate_action_type_invalid",
                ));
            }
        }
        self.prune_research_position_dust();
        Ok(())
    }

    fn apply_json_with_validated_payloads(
        &mut self,
        decision_json: &str,
        snapshot_json: &str,
        received_at: DateTime<Utc>,
    ) -> Result<ValidatedLlmEquityCycle, LlmEquityAuthorityError> {
        let decision_value = serde_json::from_str::<Value>(decision_json)
            .map_err(|err| LlmEquityAuthorityError::MalformedDecision(err.to_string()))?;
        let snapshot_value = serde_json::from_str::<Value>(snapshot_json)
            .map_err(|err| LlmEquityAuthorityError::MalformedSnapshot(err.to_string()))?;
        if self.fully_invested_policy {
            if let Some(receipt) = &self.allocation_receipt {
                if receipt.decision_hash == fully_invested::request_digest(&decision_value)
                    && receipt.snapshot_hash == fully_invested::request_digest(&snapshot_value)
                {
                    return Ok(ValidatedLlmEquityCycle {
                        result: receipt.result.clone(),
                        decision: decision_value,
                        market_snapshot: snapshot_value,
                    });
                }
            }
        }
        let allocation_request = self
            .fully_invested_policy
            .then(|| (decision_value.clone(), snapshot_value.clone()));
        reject_forbidden_json(&decision_value)?;
        reject_forbidden_json(&snapshot_value)?;
        let decision = serde_json::from_value::<DecisionEnvelope>(decision_value)
            .map_err(|err| LlmEquityAuthorityError::MalformedDecision(err.to_string()))?;
        let snapshot = serde_json::from_value::<SnapshotEnvelope>(snapshot_value)
            .map_err(|err| LlmEquityAuthorityError::MalformedSnapshot(err.to_string()))?;
        let sanitized_decision = serde_json::to_value(&decision)
            .map_err(|err| LlmEquityAuthorityError::MalformedDecision(err.to_string()))?;
        let sanitized_snapshot = serde_json::to_value(&snapshot)
            .map_err(|err| LlmEquityAuthorityError::MalformedSnapshot(err.to_string()))?;
        let result = self.apply_decision(decision, snapshot, received_at)?;
        if let Some((decision, snapshot)) = allocation_request {
            self.allocation_receipt = Some(fully_invested::AllocationReceipt {
                decision_hash: fully_invested::request_digest(&decision),
                snapshot_hash: fully_invested::request_digest(&snapshot),
                result: result.clone(),
            });
        }
        Ok(ValidatedLlmEquityCycle {
            result,
            decision: sanitized_decision,
            market_snapshot: sanitized_snapshot,
        })
    }

    /// Mark and latch risk using validated current prices without creating orders.
    pub fn observe_research_snapshot(
        &mut self,
        value: Value,
        received_at: DateTime<Utc>,
    ) -> Result<(), LlmEquityAuthorityError> {
        reject_forbidden_json(&value)?;
        let snapshot: SnapshotEnvelope = serde_json::from_value(value)
            .map_err(|error| LlmEquityAuthorityError::MalformedSnapshot(error.to_string()))?;
        validate_snapshot(&snapshot)?;
        let prices = snapshot_mid_prices(&snapshot)?;
        if snapshot.market_session != MarketSessionLabel::Rth
            || classify_new_york_session(snapshot.as_of) != MarketSessionLabel::Rth
            || !execution_session_open(&snapshot, received_at)
            || !snapshot.market_calendar_state.is_open
            || snapshot_requires_hold(&snapshot)
            || snapshot_market_data_stale_at(&snapshot, &prices, received_at)
            || timestamp_stale(received_at, snapshot.as_of, 300)
            || self
                .portfolio
                .positions
                .keys()
                .any(|symbol| !prices.contains_key(symbol))
        {
            return Err(LlmEquityAuthorityError::Contract(
                "risk_mark_not_executable",
            ));
        }
        self.reset_period_counters(received_at, &prices);
        let nav = self.portfolio.nav_usd(&prices);
        self.portfolio.peak_nav_usd = self.portfolio.peak_nav_usd.max(nav);
        if self.profit_driven_policy {
            self.drawdown_halted |=
                self.portfolio.max_drawdown_pct(&prices) >= FORCED_AGGRESSIVE_MAX_DRAWDOWN_PCT;
            self.daily_loss_halted |= self.portfolio.day_start_nav_usd - nav
                >= self.portfolio.day_start_nav_usd * FORCED_AGGRESSIVE_MAX_DAILY_LOSS_NAV_PCT;
        }
        self.last_prices = prices;
        Ok(())
    }

    fn apply_decision(
        &mut self,
        decision: DecisionEnvelope,
        snapshot: SnapshotEnvelope,
        received_at: DateTime<Utc>,
    ) -> Result<LlmEquityApplyResult, LlmEquityAuthorityError> {
        if self.fully_invested_policy || decision.target_portfolio.is_some() {
            return self.apply_allocation(&decision, &snapshot, received_at);
        }
        validate_decision(&decision, self.profit_driven_policy)?;
        validate_snapshot(&snapshot)?;
        self.reject_replay_or_stale_decision(&decision, received_at)?;

        let prices = snapshot_mid_prices(&snapshot)?;
        let research_rejection =
            global_rejection_reason(&decision, &snapshot, &prices).or_else(|| {
                if self.profit_driven_policy && !execution_session_open(&snapshot, received_at) {
                    Some("execution_market_clock_closed")
                } else {
                    (self.profit_driven_policy
                        && snapshot_market_data_stale_at(&snapshot, &prices, received_at))
                    .then_some("stale_market_data")
                }
            });
        let held_position_price_missing = self
            .portfolio
            .positions
            .keys()
            .any(|symbol| !prices.contains_key(symbol));
        if research_rejection.is_none() && !held_position_price_missing {
            self.reset_period_counters(received_at, &prices);
            self.portfolio.peak_nav_usd = self
                .portfolio
                .peak_nav_usd
                .max(self.portfolio.nav_usd(&prices));
            self.last_prices = prices.clone();
            if self.profit_driven_policy {
                let nav = self.portfolio.nav_usd(&prices);
                self.drawdown_halted |=
                    self.portfolio.max_drawdown_pct(&prices) >= FORCED_AGGRESSIVE_MAX_DRAWDOWN_PCT;
                self.daily_loss_halted |= self.portfolio.day_start_nav_usd - nav
                    >= self.portfolio.day_start_nav_usd * FORCED_AGGRESSIVE_MAX_DAILY_LOSS_NAV_PCT;
            }
        }
        let global_rejection = if !self.fills_enabled {
            Some("fills_frozen_shadow_only")
        } else if let Some(reason) = research_rejection {
            Some(reason)
        } else if held_position_price_missing {
            Some("held_position_price_missing")
        } else {
            None
        };
        self.seen_cycle_ids.insert(decision.cycle_id.clone());
        self.highest_sequence = decision.sequence;

        let mut accepted_orders = Vec::new();
        let mut rejected_orders = Vec::new();
        for (index, command) in decision.commands.iter().enumerate() {
            let command_id = format!("{}-{}", decision.cycle_id, index + 1);
            if let Some(reason) = global_rejection {
                rejected_orders.push(rejected_order(
                    &decision.cycle_id,
                    &command_id,
                    command,
                    &prices,
                    reason,
                ));
                continue;
            }
            if let Some(reason) = self.command_rejection_reason(command, &snapshot, &prices) {
                rejected_orders.push(rejected_order(
                    &decision.cycle_id,
                    &command_id,
                    command,
                    &prices,
                    reason,
                ));
                continue;
            }
            accepted_orders.push(self.execute_command(
                &decision.cycle_id,
                &command_id,
                command,
                &snapshot,
                &prices,
            )?);
        }

        Ok(LlmEquityApplyResult {
            cycle_id: decision.cycle_id,
            as_of: decision.as_of,
            accepted_orders,
            rejected_orders,
            portfolio: self.portfolio.clone(),
            prices,
            turnover_today_usd: self.turnover_today_usd,
        })
    }

    fn reject_replay_or_stale_decision(
        &self,
        decision: &DecisionEnvelope,
        received_at: DateTime<Utc>,
    ) -> Result<(), LlmEquityAuthorityError> {
        if decision.sequence == 0 || decision.sequence <= self.highest_sequence {
            return Err(LlmEquityAuthorityError::Replay("decision_sequence_replay"));
        }
        if self.seen_cycle_ids.contains(&decision.cycle_id) {
            return Err(LlmEquityAuthorityError::Replay("duplicate_cycle_id"));
        }
        if decision.as_of > received_at + Duration::minutes(LLM_EQUITY_MAX_FUTURE_SKEW_MINUTES)
            || decision.as_of < received_at - Duration::minutes(LLM_EQUITY_MAX_DECISION_AGE_MINUTES)
        {
            return Err(LlmEquityAuthorityError::Contract(
                "decision_timestamp_out_of_window",
            ));
        }
        Ok(())
    }

    fn command_rejection_reason(
        &self,
        command: &DecisionCommand,
        snapshot: &SnapshotEnvelope,
        prices: &HashMap<String, f64>,
    ) -> Option<&'static str> {
        let Some(price) = prices.get(&command.symbol).copied() else {
            return Some("market_price_unavailable");
        };
        if !(price.is_finite() && price > 0.0) {
            return Some("market_price_unavailable");
        }
        let nav = self.portfolio.nav_usd(prices);
        if !(nav.is_finite() && nav > 0.0) {
            return Some("nav_unavailable");
        }
        if command.reduce_only {
            let held = self
                .portfolio
                .positions
                .get(&command.symbol)
                .map_or(0.0, |p| p.quantity);
            if held * command_delta_notional(command) >= 0.0 {
                return Some("reduce_only_no_opposing_position");
            }
        }
        let cash_delta = self.executable_notional(command, snapshot, price);
        let notional = cash_delta.abs();
        if notional <= FLOAT_EPSILON {
            return Some("no_position_delta");
        }
        let risk_delta = if self.realistic_execution {
            let Some(quote) = snapshot.latest_quotes.get(&command.symbol) else {
                return Some("executable_quote_unavailable");
            };
            if quote_source_unavailable(&quote.source)
                || !(quote.bid.is_finite()
                    && quote.bid > 0.0
                    && quote.ask.is_finite()
                    && quote.ask >= quote.bid)
            {
                return Some("executable_quote_unavailable");
            }
            let impact = LLM_EQUITY_MARKET_IMPACT_BPS / 10_000.0;
            let execution_price = if cash_delta >= 0.0 {
                quote.ask * (1.0 + impact)
            } else {
                quote.bid * (1.0 - impact)
            };
            if !(execution_price.is_finite() && execution_price > 0.0) {
                return Some("executable_quote_unavailable");
            }
            cash_delta * price / execution_price
        } else {
            cash_delta
        };

        if self.risk_limits_enabled {
            return self.production_risk_rejection_reason(
                command, snapshot, prices, nav, risk_delta, cash_delta,
            );
        }

        None
    }

    fn production_risk_rejection_reason(
        &self,
        command: &DecisionCommand,
        snapshot: &SnapshotEnvelope,
        prices: &HashMap<String, f64>,
        nav: f64,
        risk_delta: f64,
        cash_delta: f64,
    ) -> Option<&'static str> {
        let current_symbol_value = self
            .portfolio
            .positions
            .get(&command.symbol)
            .map_or(0.0, |position| position.quantity * prices[&command.symbol]);
        let projected_symbol_value = current_symbol_value + risk_delta;
        let risk_reducing = current_symbol_value.abs() > FLOAT_EPSILON
            && projected_symbol_value.abs() <= current_symbol_value.abs() + FLOAT_EPSILON
            && (projected_symbol_value.abs() <= FLOAT_EPSILON
                || projected_symbol_value.signum() == current_symbol_value.signum());
        let Some(metadata) = snapshot.asset_metadata.get(&command.symbol) else {
            return Some("asset_metadata_unavailable");
        };
        if !metadata.tradable {
            return Some("asset_not_tradable");
        }
        if self.long_risk_adjusted_policy && projected_symbol_value < -FLOAT_EPSILON {
            return Some("research_long_only");
        }
        if projected_symbol_value < current_symbol_value.min(0.0) && !metadata.shortable {
            return Some("short_borrow_unavailable");
        }
        let (
            max_single_name_pct,
            max_gross_pct,
            max_net_pct,
            max_short_gross_pct,
            min_cash_reserve_pct,
            max_daily_turnover_pct,
            max_daily_loss_pct,
            max_drawdown_pct,
        ) = if self.forced_aggressive_policy {
            (
                FORCED_AGGRESSIVE_MAX_SINGLE_NAME_NAV_PCT,
                FORCED_AGGRESSIVE_MAX_GROSS_NAV_PCT,
                FORCED_AGGRESSIVE_MAX_NET_NAV_PCT,
                FORCED_AGGRESSIVE_MAX_SHORT_GROSS_NAV_PCT,
                FORCED_AGGRESSIVE_MIN_CASH_RESERVE_NAV_PCT,
                FORCED_AGGRESSIVE_MAX_DAILY_TURNOVER_NAV_PCT,
                FORCED_AGGRESSIVE_MAX_DAILY_LOSS_NAV_PCT,
                FORCED_AGGRESSIVE_MAX_DRAWDOWN_PCT,
            )
        } else if self.long_risk_adjusted_policy {
            (
                LLM_EQUITY_MAX_SINGLE_NAME_NAV_PCT,
                LLM_EQUITY_MAX_GROSS_NAV_PCT,
                RESEARCH_LONG_MAX_NET_NAV_PCT,
                0.0,
                LLM_EQUITY_MIN_CASH_RESERVE_NAV_PCT,
                LLM_EQUITY_MAX_DAILY_TURNOVER_NAV_PCT,
                LLM_EQUITY_MAX_DAILY_LOSS_NAV_PCT,
                LLM_EQUITY_MAX_DRAWDOWN_PCT,
            )
        } else {
            (
                LLM_EQUITY_MAX_SINGLE_NAME_NAV_PCT,
                LLM_EQUITY_MAX_GROSS_NAV_PCT,
                LLM_EQUITY_MAX_NET_NAV_PCT,
                LLM_EQUITY_MAX_SHORT_GROSS_NAV_PCT,
                LLM_EQUITY_MIN_CASH_RESERVE_NAV_PCT,
                LLM_EQUITY_MAX_DAILY_TURNOVER_NAV_PCT,
                LLM_EQUITY_MAX_DAILY_LOSS_NAV_PCT,
                LLM_EQUITY_MAX_DRAWDOWN_PCT,
            )
        };
        let current_gross = self.portfolio.gross_exposure_usd(prices);
        let mut gross = 0.0;
        let mut net = 0.0;
        let mut short_gross = 0.0;
        for position in self.portfolio.positions.values() {
            let Some(price) = prices.get(&position.symbol) else {
                continue;
            };
            let mut value = position.quantity * price;
            if position.symbol == command.symbol {
                value = projected_symbol_value;
            }
            gross += value.abs();
            net += value;
            short_gross += (-value).max(0.0);
        }
        if !self.portfolio.positions.contains_key(&command.symbol) {
            gross += projected_symbol_value.abs();
            net += projected_symbol_value;
            short_gross += (-projected_symbol_value).max(0.0);
        }
        if !self.profit_driven_policy
            && current_gross + FLOAT_EPSILON < LLM_EQUITY_MIN_GROSS_EXPOSURE_USD
        {
            if command.action != CommandAction::BuyNotional
                || gross <= current_gross + FLOAT_EPSILON
            {
                return Some("minimum_gross_exposure_floor");
            }
        } else if !self.profit_driven_policy
            && gross + FLOAT_EPSILON < LLM_EQUITY_MIN_GROSS_EXPOSURE_USD
        {
            return Some("minimum_gross_exposure_floor");
        }
        if risk_reducing {
            return None;
        }
        let daily_loss_reached = self.portfolio.day_start_nav_usd - nav
            >= self.portfolio.day_start_nav_usd * max_daily_loss_pct;
        let drawdown_reached = self.portfolio.max_drawdown_pct(prices) >= max_drawdown_pct;
        let flat_benchmark_probe = self.forced_aggressive_policy
            && self.portfolio.gross_exposure_usd(prices) <= FLOAT_EPSILON
            && matches!(command.symbol.as_str(), "SPY" | "QQQ")
            && ((command.max_notional_usd - 25.0).abs() <= FLOAT_EPSILON
                || (command.max_notional_usd - 100.0).abs() <= FLOAT_EPSILON);
        if (daily_loss_reached || self.daily_loss_halted)
            && (!flat_benchmark_probe || self.profit_driven_policy)
        {
            return Some("daily_loss_limit");
        }
        if (drawdown_reached || self.drawdown_halted)
            && (!flat_benchmark_probe || self.profit_driven_policy)
        {
            return Some("drawdown_limit");
        }
        if projected_symbol_value.abs() > nav * max_single_name_pct {
            return Some("single_name_limit");
        }
        if is_high_correlation_proxy(&command.symbol) {
            let projected_group_gross = self
                .portfolio
                .positions
                .values()
                .filter(|position| is_high_correlation_proxy(&position.symbol))
                .filter_map(|position| {
                    prices.get(&position.symbol).map(|price| {
                        if position.symbol == command.symbol {
                            projected_symbol_value.abs()
                        } else {
                            (position.quantity * price).abs()
                        }
                    })
                })
                .sum::<f64>()
                + if self.portfolio.positions.contains_key(&command.symbol) {
                    0.0
                } else {
                    projected_symbol_value.abs()
                };
            if projected_group_gross > nav * LLM_EQUITY_MAX_HIGH_CORRELATION_GROUP_NAV_PCT {
                return Some("high_correlation_group_exposure_limit");
            }
        }
        if self.portfolio.cash_usd - cash_delta < nav * min_cash_reserve_pct {
            return Some("cash_reserve_limit");
        }
        if gross > nav * max_gross_pct {
            return Some("gross_exposure_limit");
        }
        if net.abs() > nav * max_net_pct {
            return Some("net_exposure_limit");
        }
        if short_gross > nav * max_short_gross_pct {
            return Some("short_gross_limit");
        }
        if self.turnover_today_usd + cash_delta.abs() > nav * max_daily_turnover_pct {
            return Some("turnover_limit");
        }
        None
    }

    fn executable_notional(
        &self,
        command: &DecisionCommand,
        snapshot: &SnapshotEnvelope,
        mid: f64,
    ) -> f64 {
        let delta = command_delta_notional(command);
        if !command.reduce_only {
            return delta;
        }
        let held = self
            .portfolio
            .positions
            .get(&command.symbol)
            .map_or(0.0, |p| p.quantity);
        if held * delta >= 0.0 {
            return 0.0;
        }
        let price = if self.realistic_execution {
            snapshot
                .latest_quotes
                .get(&command.symbol)
                .map_or(0.0, |q| {
                    let impact = LLM_EQUITY_MARKET_IMPACT_BPS / 10_000.0;
                    if delta > 0.0 {
                        q.ask * (1.0 + impact)
                    } else {
                        q.bid * (1.0 - impact)
                    }
                })
        } else {
            mid
        };
        delta.signum() * delta.abs().min(held.abs() * price)
    }

    fn execute_command(
        &mut self,
        cycle_id: &str,
        command_id: &str,
        command: &DecisionCommand,
        snapshot: &SnapshotEnvelope,
        prices: &HashMap<String, f64>,
    ) -> Result<LlmEquitySimulatedOrder, LlmEquityAuthorityError> {
        let mid_price = *prices
            .get(&command.symbol)
            .ok_or(LlmEquityAuthorityError::Contract(
                "market_price_unavailable",
            ))?;
        let delta = self.executable_notional(command, snapshot, mid_price);
        let notional = delta.abs();
        let price = if self.realistic_execution {
            let quote = snapshot
                .latest_quotes
                .get(&command.symbol)
                .ok_or(LlmEquityAuthorityError::Contract("latest_quote_missing"))?;
            let impact = LLM_EQUITY_MARKET_IMPACT_BPS / 10_000.0;
            if delta >= 0.0 {
                quote.ask * (1.0 + impact)
            } else {
                quote.bid * (1.0 - impact)
            }
        } else {
            mid_price
        };
        if !(price.is_finite() && price > 0.0) {
            return Err(LlmEquityAuthorityError::Contract(
                "executable_quote_unavailable",
            ));
        }
        let qty = notional / price;
        let side = if delta >= 0.0 {
            self.apply_signed_trade(&command.symbol, qty, price);
            LlmEquityOrderSide::Buy
        } else {
            self.apply_signed_trade(&command.symbol, -qty, price);
            LlmEquityOrderSide::Sell
        };
        self.portfolio.turnover_this_hour_usd += notional;
        self.turnover_today_usd += notional;
        self.portfolio.peak_nav_usd = self
            .portfolio
            .peak_nav_usd
            .max(self.portfolio.nav_usd(prices));

        Ok(LlmEquitySimulatedOrder {
            command_id: command_id.to_string(),
            cycle_id: cycle_id.to_string(),
            symbol: command.symbol.clone(),
            side,
            status: "accepted".to_string(),
            qty,
            avg_price: price,
            notional_usd: notional,
            reason: "simulated_fill".to_string(),
        })
    }

    fn reset_period_counters(&mut self, received_at: DateTime<Utc>, prices: &HashMap<String, f64>) {
        let offset_hours = if new_york_dst_active(received_at) {
            -4
        } else {
            -5
        };
        let local = received_at + Duration::hours(offset_hours);
        let hour_window = (local.date_naive(), local.hour());
        if self.turnover_window != Some(hour_window) {
            self.portfolio.turnover_this_hour_usd = 0.0;
            self.turnover_window = Some(hour_window);
        }
        let day = local.date_naive();
        if self.portfolio_day != Some(day) {
            self.portfolio.day_start_nav_usd = self.portfolio.nav_usd(prices);
            self.portfolio_day = Some(day);
            self.daily_loss_halted = false;
            self.turnover_today_usd = 0.0;
        }
    }

    fn apply_signed_trade(&mut self, symbol: &str, signed_qty: f64, price: f64) {
        if signed_qty.abs() <= FLOAT_EPSILON {
            return;
        }
        self.portfolio.cash_usd -= signed_qty * price;

        let mut remove_position = false;
        match self.portfolio.positions.get_mut(symbol) {
            Some(position) => {
                let old_qty = position.quantity;
                let new_qty = old_qty + signed_qty;
                if old_qty.signum() == signed_qty.signum() {
                    let total_abs_qty = old_qty.abs() + signed_qty.abs();
                    position.avg_price = ((position.avg_price * old_qty.abs())
                        + (price * signed_qty.abs()))
                        / total_abs_qty;
                    position.quantity = new_qty;
                } else {
                    let closing_qty = old_qty.abs().min(signed_qty.abs());
                    if old_qty > 0.0 {
                        self.portfolio.realized_pnl_usd +=
                            (price - position.avg_price) * closing_qty;
                    } else {
                        self.portfolio.realized_pnl_usd +=
                            (position.avg_price - price) * closing_qty;
                    }
                    if new_qty.abs() <= FLOAT_EPSILON {
                        remove_position = true;
                    } else {
                        if old_qty.signum() != new_qty.signum() {
                            position.avg_price = price;
                        }
                        position.quantity = new_qty;
                    }
                }
            }
            None => {
                self.portfolio.positions.insert(
                    symbol.to_string(),
                    LlmEquityPosition {
                        symbol: symbol.to_string(),
                        quantity: signed_qty,
                        avg_price: price,
                    },
                );
            }
        }

        if remove_position {
            self.portfolio.positions.remove(symbol);
        }
    }
}

fn validate_persisted_authority(
    state: &PersistedLlmEquityAuthority,
) -> Result<(), LlmEquityAuthorityError> {
    let portfolio = &state.portfolio;
    let scalar_state_is_valid = state.schema_version == 1
        && (portfolio.initial_cash_usd - LLM_EQUITY_INITIAL_CASH_USD).abs() <= FLOAT_EPSILON
        && portfolio.cash_usd.is_finite()
        && portfolio.realized_pnl_usd.is_finite()
        && portfolio.turnover_this_hour_usd.is_finite()
        && portfolio.turnover_this_hour_usd >= 0.0
        && portfolio.peak_nav_usd.is_finite()
        && portfolio.peak_nav_usd > 0.0
        && portfolio.day_start_nav_usd.is_finite()
        && portfolio.day_start_nav_usd > 0.0
        && state.turnover_today_usd.is_finite()
        && state.turnover_today_usd >= 0.0
        && state.seen_cycle_ids.len() <= 100_000
        && state.last_prices.len() <= 100_000;
    if !scalar_state_is_valid {
        return Err(LlmEquityAuthorityError::Persistence(
            "invalid persisted authority state".to_string(),
        ));
    }
    if state
        .seen_cycle_ids
        .iter()
        .any(|cycle_id| cycle_id.is_empty() || cycle_id.len() > 120)
    {
        return Err(LlmEquityAuthorityError::Persistence(
            "invalid persisted replay state".to_string(),
        ));
    }
    if state.last_prices.iter().any(|(symbol, price)| {
        !is_valid_llm_equity_symbol(symbol) || !price.is_finite() || *price <= 0.0
    }) {
        return Err(LlmEquityAuthorityError::Persistence(
            "invalid persisted market marks".to_string(),
        ));
    }
    for (symbol, position) in &portfolio.positions {
        if symbol != &position.symbol
            || !is_valid_llm_equity_symbol(symbol)
            || !position.quantity.is_finite()
            || position.quantity.abs() <= FLOAT_EPSILON
            || !position.avg_price.is_finite()
            || position.avg_price <= 0.0
            || !state.last_prices.contains_key(symbol)
        {
            return Err(LlmEquityAuthorityError::Persistence(
                "invalid persisted position state".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn is_valid_llm_equity_symbol(symbol: &str) -> bool {
    let symbol = symbol.trim();
    let mut chars = symbol.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    first.is_ascii_uppercase()
        && symbol.len() <= 10
        && chars.all(|value| {
            value.is_ascii_uppercase() || value.is_ascii_digit() || value == '.' || value == '-'
        })
}

fn is_high_correlation_proxy(symbol: &str) -> bool {
    matches!(
        symbol,
        "ARKB"
            | "BITB"
            | "BITO"
            | "BRRR"
            | "CETH"
            | "ETHE"
            | "ETHA"
            | "ETHV"
            | "ETHW"
            | "FETH"
            | "HODL"
            | "IBIT"
            | "QETH"
    )
}

fn validate_decision(
    decision: &DecisionEnvelope,
    profit_driven: bool,
) -> Result<(), LlmEquityAuthorityError> {
    if decision
        .execution_policy
        .as_deref()
        .is_some_and(|policy| policy != "profit_driven_v1" || !profit_driven)
    {
        return Err(LlmEquityAuthorityError::Contract(
            "execution_policy_mismatch",
        ));
    }
    if decision.cycle_id.trim().is_empty() {
        return Err(LlmEquityAuthorityError::Contract("cycle_id_required"));
    }
    if decision.sequence == 0 {
        return Err(LlmEquityAuthorityError::Contract("sequence_required"));
    }
    if !(decision.portfolio_nav_usd.is_finite() && decision.portfolio_nav_usd > 0.0) {
        return Err(LlmEquityAuthorityError::Contract("portfolio_nav_invalid"));
    }
    if decision.commands.is_empty() {
        return Err(LlmEquityAuthorityError::Contract("commands_required"));
    }
    if decision.evidence.is_empty() {
        return Err(LlmEquityAuthorityError::Contract("evidence_required"));
    }
    if decision.memory_updates.is_empty() {
        return Err(LlmEquityAuthorityError::Contract("memory_updates_required"));
    }
    if decision.risk_assessment.risk_limit_verdict != RiskLimitVerdict::Pass {
        return Err(LlmEquityAuthorityError::Contract(
            "risk_limit_verdict_must_pass",
        ));
    }
    let evidence_ids = decision
        .evidence
        .iter()
        .map(|evidence| evidence.id.as_str())
        .collect::<HashSet<_>>();
    for command in &decision.commands {
        validate_command(command, &evidence_ids)?;
    }
    if !profit_driven
        && !decision
            .commands
            .iter()
            .any(|command| command.max_notional_usd >= LLM_EQUITY_MANDATORY_ORDER_NOTIONAL_USD)
    {
        return Err(LlmEquityAuthorityError::Contract(
            "mandatory_order_notional_required",
        ));
    }
    Ok(())
}

fn validate_command(
    command: &DecisionCommand,
    evidence_ids: &HashSet<&str>,
) -> Result<(), LlmEquityAuthorityError> {
    if !is_valid_llm_equity_symbol(&command.symbol) {
        return Err(LlmEquityAuthorityError::Contract("invalid_symbol"));
    }
    if !(command.confidence.is_finite() && (0.0..=1.0).contains(&command.confidence)) {
        return Err(LlmEquityAuthorityError::Contract("confidence_invalid"));
    }
    if !(command.max_notional_usd.is_finite() && command.max_notional_usd >= 0.0) {
        return Err(LlmEquityAuthorityError::Contract("max_notional_invalid"));
    }
    if !matches!(
        command.action,
        CommandAction::BuyNotional | CommandAction::SellNotional
    ) {
        return Err(LlmEquityAuthorityError::Contract("action_must_buy_or_sell"));
    }
    if command.max_notional_usd <= 0.0 {
        return Err(LlmEquityAuthorityError::Contract("max_notional_invalid"));
    }
    if command.evidence_refs.is_empty()
        || command
            .evidence_refs
            .iter()
            .any(|reference| !evidence_ids.contains(reference.as_str()))
    {
        return Err(LlmEquityAuthorityError::Contract("evidence_refs_invalid"));
    }
    if !matches!(
        command._horizon.as_str(),
        "30m" | "60m" | "close" | "1d" | "5d"
    ) {
        return Err(LlmEquityAuthorityError::Contract("horizon_invalid"));
    }
    if let Some(forecast) = &command.forecast {
        let returns = [
            forecast.expected_excess_return_bps,
            forecast.return_p10_bps,
            forecast.return_p50_bps,
            forecast.return_p90_bps,
        ];
        if returns
            .iter()
            .any(|value| !value.is_finite() || !(-5000.0..=5000.0).contains(value))
            || !forecast.probability_positive.is_finite()
            || !(0.0..=1.0).contains(&forecast.probability_positive)
        {
            return Err(LlmEquityAuthorityError::Contract("forecast_invalid"));
        }
        if !(forecast.return_p10_bps <= forecast.return_p50_bps
            && forecast.return_p50_bps <= forecast.return_p90_bps)
        {
            return Err(LlmEquityAuthorityError::Contract(
                "forecast_quantiles_invalid",
            ));
        }
    }
    Ok(())
}

fn validate_snapshot(snapshot: &SnapshotEnvelope) -> Result<(), LlmEquityAuthorityError> {
    if snapshot.symbols.is_empty() {
        return Err(LlmEquityAuthorityError::Contract(
            "snapshot_symbols_required",
        ));
    }
    for symbol in &snapshot.symbols {
        if !is_valid_llm_equity_symbol(symbol) {
            return Err(LlmEquityAuthorityError::Contract("invalid_symbol"));
        }
        if !snapshot.latest_bars.contains_key(symbol) {
            return Err(LlmEquityAuthorityError::Contract("latest_bar_missing"));
        }
        if !snapshot.latest_quotes.contains_key(symbol) {
            return Err(LlmEquityAuthorityError::Contract("latest_quote_missing"));
        }
    }
    Ok(())
}

fn execution_session_open(snapshot: &SnapshotEnvelope, received_at: DateTime<Utc>) -> bool {
    classify_new_york_session(received_at) == MarketSessionLabel::Rth
        && snapshot.market_calendar_state.is_open
        && DateTime::parse_from_rfc3339(&snapshot.market_calendar_state._next_close)
            .is_ok_and(|close| received_at < close.with_timezone(&Utc))
}

fn global_rejection_reason(
    decision: &DecisionEnvelope,
    snapshot: &SnapshotEnvelope,
    prices: &HashMap<String, f64>,
) -> Option<&'static str> {
    let derived = classify_new_york_session(decision.as_of);
    if derived != MarketSessionLabel::Rth {
        return Some("orders_allowed_only_during_rth");
    }
    if decision.market_session != derived || snapshot.market_session != derived {
        return Some("session_mismatch");
    }
    if snapshot.as_of != decision.as_of {
        return Some("snapshot_decision_time_mismatch");
    }
    if !snapshot.market_calendar_state.is_open {
        return Some("market_clock_closed");
    }
    if snapshot_requires_hold(snapshot) {
        return Some("stale_research_requires_hold");
    }
    if snapshot_market_data_stale(snapshot, prices) {
        return Some("stale_market_data");
    }
    None
}

fn snapshot_requires_hold(snapshot: &SnapshotEnvelope) -> bool {
    !snapshot.unavailable_data_flags.is_empty()
        || !has_required_freshness_sources(snapshot)
        || !has_required_macro_news_sources(snapshot)
        || snapshot.market_calendar_state.source != "alpaca_market_clock"
        || snapshot
            .source_freshness
            .iter()
            .any(|source| source.freshness != Freshness::Fresh)
        || snapshot.market_calendar_state.freshness != Freshness::Fresh
        || snapshot
            .macro_news_summaries
            .iter()
            .any(|summary| summary.freshness != Freshness::Fresh)
}

fn has_required_freshness_sources(snapshot: &SnapshotEnvelope) -> bool {
    let present = snapshot
        .source_freshness
        .iter()
        .map(|source| source.source.as_str())
        .collect::<HashSet<_>>();
    REQUIRED_SNAPSHOT_FRESHNESS_SOURCES
        .iter()
        .all(|required| present.contains(required))
}

fn has_required_macro_news_sources(snapshot: &SnapshotEnvelope) -> bool {
    let present = snapshot
        .macro_news_summaries
        .iter()
        .map(|summary| summary.source.as_str())
        .collect::<HashSet<_>>();
    REQUIRED_MACRO_NEWS_SOURCES
        .iter()
        .all(|required| present.contains(required))
}

fn snapshot_market_data_stale(snapshot: &SnapshotEnvelope, prices: &HashMap<String, f64>) -> bool {
    snapshot_market_data_stale_at(snapshot, prices, snapshot.as_of)
}

fn snapshot_market_data_stale_at(
    snapshot: &SnapshotEnvelope,
    prices: &HashMap<String, f64>,
    as_of: DateTime<Utc>,
) -> bool {
    snapshot.symbols.iter().any(|symbol| {
        !prices.contains_key(symbol)
            || snapshot.latest_bars.get(symbol).is_none_or(|bar| {
                timestamp_stale(as_of, bar.timestamp, LLM_EQUITY_MAX_BAR_AGE_SECONDS)
            })
            || snapshot.latest_quotes.get(symbol).is_none_or(|quote| {
                timestamp_stale(as_of, quote.timestamp, LLM_EQUITY_MAX_QUOTE_AGE_SECONDS)
            })
    })
}

fn timestamp_stale(as_of: DateTime<Utc>, timestamp: DateTime<Utc>, max_age_seconds: i64) -> bool {
    timestamp > as_of + Duration::seconds(5)
        || as_of.signed_duration_since(timestamp).num_seconds() > max_age_seconds
}

fn snapshot_mid_prices(
    snapshot: &SnapshotEnvelope,
) -> Result<HashMap<String, f64>, LlmEquityAuthorityError> {
    let mut prices = HashMap::new();
    for symbol in &snapshot.symbols {
        let Some(quote) = snapshot.latest_quotes.get(symbol) else {
            return Err(LlmEquityAuthorityError::Contract("latest_quote_missing"));
        };
        if quote_source_unavailable(&quote.source) {
            if let Some(price) = snapshot_bar_close_price(snapshot, symbol) {
                prices.insert(symbol.clone(), price);
            }
            continue;
        }
        if !(quote.bid.is_finite()
            && quote.bid > 0.0
            && quote.ask.is_finite()
            && quote.ask >= quote.bid)
        {
            return Err(LlmEquityAuthorityError::Contract("quote_invalid"));
        }
        prices.insert(symbol.clone(), (quote.bid + quote.ask) / 2.0);
    }
    Ok(prices)
}

fn snapshot_bar_close_price(snapshot: &SnapshotEnvelope, symbol: &str) -> Option<f64> {
    let bar = snapshot.latest_bars.get(symbol)?;
    if quote_source_unavailable(&bar.source) || !(bar.close.is_finite() && bar.close > 0.0) {
        return None;
    }
    Some(bar.close)
}

fn quote_source_unavailable(source: &str) -> bool {
    source.contains("unavailable")
}

fn command_delta_notional(command: &DecisionCommand) -> f64 {
    match command.action {
        CommandAction::Hold => 0.0,
        CommandAction::BuyNotional => command.max_notional_usd,
        CommandAction::SellNotional => -command.max_notional_usd,
        CommandAction::ClosePosition | CommandAction::SetTargetWeight => 0.0,
    }
}

fn rejected_order(
    cycle_id: &str,
    command_id: &str,
    command: &DecisionCommand,
    prices: &HashMap<String, f64>,
    reason: &str,
) -> LlmEquitySimulatedOrder {
    let price = prices.get(&command.symbol).copied().unwrap_or(0.0);
    let side = match command.action {
        CommandAction::SellNotional | CommandAction::ClosePosition => LlmEquityOrderSide::Sell,
        _ => LlmEquityOrderSide::Buy,
    };
    LlmEquitySimulatedOrder {
        command_id: command_id.to_string(),
        cycle_id: cycle_id.to_string(),
        symbol: command.symbol.clone(),
        side,
        status: "rejected".to_string(),
        qty: if price > 0.0 {
            command.max_notional_usd / price
        } else {
            0.0
        },
        avg_price: price,
        notional_usd: command.max_notional_usd,
        reason: reason.to_string(),
    }
}

fn classify_new_york_session(as_of: DateTime<Utc>) -> MarketSessionLabel {
    let offset_hours = if new_york_dst_active(as_of) { -4 } else { -5 };
    let local = as_of + Duration::hours(offset_hours);
    if local.weekday() == Weekday::Sat || local.weekday() == Weekday::Sun {
        return MarketSessionLabel::Closed;
    }
    let minutes = local.hour() * 60 + local.minute();
    match minutes {
        240..=569 => MarketSessionLabel::Premarket,
        570..=959 => MarketSessionLabel::Rth,
        960..=1199 => MarketSessionLabel::AfterHours,
        _ => MarketSessionLabel::Closed,
    }
}

fn new_york_dst_active(as_of: DateTime<Utc>) -> bool {
    let year = as_of.year();
    let dst_start = nth_weekday_utc(year, 3, Weekday::Sun, 2, 7);
    let dst_end = nth_weekday_utc(year, 11, Weekday::Sun, 1, 6);
    as_of >= dst_start && as_of < dst_end
}

fn nth_weekday_utc(
    year: i32,
    month: u32,
    weekday: Weekday,
    occurrence: u32,
    utc_hour: u32,
) -> DateTime<Utc> {
    let first = NaiveDate::from_ymd_opt(year, month, 1).expect("valid month");
    let first_weekday = first.weekday().num_days_from_monday() as i64;
    let target_weekday = weekday.num_days_from_monday() as i64;
    let first_delta = (target_weekday - first_weekday).rem_euclid(7);
    let day = 1 + first_delta as u32 + (occurrence - 1) * 7;
    DateTime::<Utc>::from_naive_utc_and_offset(
        NaiveDate::from_ymd_opt(year, month, day)
            .expect("valid nth weekday")
            .and_hms_opt(utc_hour, 0, 0)
            .expect("valid UTC hour"),
        Utc,
    )
}

fn parse_utc(value: &str) -> Result<DateTime<Utc>, LlmEquityAuthorityError> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|_| LlmEquityAuthorityError::Contract("timestamp_invalid"))
}

fn reject_forbidden_json(value: &Value) -> Result<(), LlmEquityAuthorityError> {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                if FORBIDDEN_JSON_KEYS
                    .iter()
                    .any(|forbidden| key.eq_ignore_ascii_case(forbidden))
                {
                    return Err(LlmEquityAuthorityError::Contract(
                        "forbidden_raw_or_credential_field",
                    ));
                }
                reject_forbidden_json(child)?;
            }
        }
        Value::Array(items) => {
            for child in items {
                reject_forbidden_json(child)?;
            }
        }
        Value::String(text) if contains_credential_assignment(text) => {
            return Err(LlmEquityAuthorityError::Contract("credential_like_text"));
        }
        _ => {}
    }
    Ok(())
}

fn contains_credential_assignment(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    [
        "api_key", "api-key", "apikey", "secret", "token", "password",
    ]
    .iter()
    .any(|keyword| contains_keyword_assignment(&lower, keyword))
}

fn contains_keyword_assignment(lower: &str, keyword: &str) -> bool {
    let mut offset = 0;
    while let Some(relative_start) = lower[offset..].find(keyword) {
        let start = offset + relative_start;
        let end = start + keyword.len();
        let previous = lower[..start].chars().next_back();
        if previous.is_some_and(|character| character.is_ascii_alphanumeric() || character == '_') {
            offset = end;
            continue;
        }
        let rest = lower[end..].trim_start();
        let Some(value) = rest.strip_prefix('=').or_else(|| rest.strip_prefix(':')) else {
            offset = end;
            continue;
        };
        if value.trim_start().chars().next().is_some_and(|character| {
            !character.is_whitespace() && character != ',' && character != ';'
        }) {
            return true;
        }
        offset = end;
    }
    false
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DecisionEnvelope {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    execution_policy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target_portfolio: Option<fully_invested::TargetPortfolio>,
    cycle_id: String,
    sequence: u64,
    as_of: DateTime<Utc>,
    market_session: MarketSessionLabel,
    portfolio_nav_usd: f64,
    commands: Vec<DecisionCommand>,
    evidence: Vec<DecisionEvidence>,
    #[serde(rename = "risk_assessment")]
    risk_assessment: DecisionRiskAssessment,
    memory_updates: Vec<MemoryEvent>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DecisionEvidence {
    id: String,
    #[serde(rename = "source")]
    _source: String,
    #[serde(default, rename = "url")]
    _url: Option<String>,
    #[serde(rename = "summary")]
    _summary: String,
    #[serde(rename = "retrieved_at")]
    _retrieved_at: DateTime<Utc>,
    #[serde(rename = "freshness")]
    _freshness: Freshness,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DecisionCommand {
    action: CommandAction,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    reduce_only: bool,
    symbol: String,
    #[serde(rename = "rationale")]
    _rationale: String,
    confidence: f64,
    max_notional_usd: f64,
    evidence_refs: Vec<String>,
    #[serde(default = "default_decision_horizon", rename = "horizon")]
    _horizon: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    forecast: Option<DecisionForecast>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DecisionForecast {
    #[serde(rename = "edge_class")]
    _edge_class: ForecastEdgeClass,
    expected_excess_return_bps: f64,
    probability_positive: f64,
    return_p10_bps: f64,
    return_p50_bps: f64,
    return_p90_bps: f64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ForecastEdgeClass {
    PositiveEdge,
    LeastBadForced,
    RiskReduction,
}

fn default_decision_horizon() -> String {
    "60m".to_string()
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DecisionRiskAssessment {
    #[serde(rename = "summary")]
    _summary: String,
    #[serde(rename = "max_loss_scenario")]
    _max_loss_scenario: String,
    #[serde(rename = "risk_limit_verdict")]
    risk_limit_verdict: RiskLimitVerdict,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct MemoryEvent {
    #[serde(rename = "event_id")]
    _event_id: String,
    #[serde(rename = "cycle_id")]
    _cycle_id: String,
    #[serde(rename = "created_at")]
    _created_at: DateTime<Utc>,
    #[serde(rename = "thesis")]
    _thesis: String,
    #[serde(rename = "outcome")]
    _outcome: String,
    #[serde(rename = "conclusion")]
    _conclusion: String,
    #[serde(rename = "prompt_adjustment")]
    _prompt_adjustment: String,
    #[serde(rename = "next_cycle_watch_items")]
    _next_cycle_watch_items: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SnapshotEnvelope {
    #[serde(rename = "snapshot_id")]
    _snapshot_id: String,
    as_of: DateTime<Utc>,
    market_session: MarketSessionLabel,
    symbols: Vec<String>,
    latest_bars: HashMap<String, SnapshotBar>,
    latest_quotes: HashMap<String, SnapshotQuote>,
    #[serde(
        default,
        rename = "session_bars",
        skip_serializing_if = "HashMap::is_empty"
    )]
    _session_bars: HashMap<String, SnapshotBar>,
    #[serde(
        default,
        rename = "previous_daily_bars",
        skip_serializing_if = "HashMap::is_empty"
    )]
    _previous_daily_bars: HashMap<String, SnapshotBar>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    asset_metadata: HashMap<String, SnapshotAssetMetadata>,
    #[serde(
        default,
        rename = "discovery_health",
        skip_serializing_if = "HashMap::is_empty"
    )]
    _discovery_health: HashMap<String, Value>,
    market_calendar_state: SnapshotMarketCalendarState,
    #[serde(rename = "trend_summaries")]
    _trend_summaries: HashMap<String, String>,
    source_freshness: Vec<SnapshotSourceFreshness>,
    macro_news_summaries: Vec<SnapshotMacroNewsSummary>,
    unavailable_data_flags: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SnapshotBar {
    timestamp: DateTime<Utc>,
    #[serde(rename = "open")]
    _open: f64,
    #[serde(rename = "high")]
    _high: f64,
    #[serde(rename = "low")]
    _low: f64,
    #[serde(rename = "close")]
    close: f64,
    #[serde(rename = "volume")]
    _volume: f64,
    #[serde(rename = "source")]
    source: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SnapshotQuote {
    timestamp: DateTime<Utc>,
    bid: f64,
    ask: f64,
    source: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SnapshotAssetMetadata {
    tradable: bool,
    shortable: bool,
    #[serde(rename = "fractionable")]
    _fractionable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    investment_kind: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SnapshotMarketCalendarState {
    source: String,
    #[serde(rename = "retrieved_at")]
    _retrieved_at: DateTime<Utc>,
    is_open: bool,
    #[serde(rename = "next_open")]
    _next_open: String,
    #[serde(rename = "next_close")]
    _next_close: String,
    freshness: Freshness,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SnapshotSourceFreshness {
    source: String,
    freshness: Freshness,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SnapshotMacroNewsSourceRecord {
    #[serde(rename = "url")]
    _url: String,
    #[serde(rename = "published_at")]
    _published_at: DateTime<Utc>,
    #[serde(rename = "content_hash")]
    _content_hash: String,
    #[serde(rename = "summary")]
    _summary: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SnapshotMacroNewsSummary {
    source: String,
    #[serde(rename = "url")]
    _url: String,
    #[serde(rename = "retrieved_at")]
    _retrieved_at: DateTime<Utc>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "published_at"
    )]
    _published_at: Option<DateTime<Utc>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "content_hash"
    )]
    _content_hash: Option<String>,
    #[serde(rename = "summary")]
    _summary: String,
    freshness: Freshness,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    source_records: Vec<SnapshotMacroNewsSourceRecord>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MarketSessionLabel {
    Premarket,
    Rth,
    AfterHours,
    Closed,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CommandAction {
    Hold,
    BuyNotional,
    SellNotional,
    ClosePosition,
    SetTargetWeight,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Freshness {
    Fresh,
    Stale,
    Unavailable,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RiskLimitVerdict {
    Pass,
    Hold,
    Reject,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    pub(super) fn snapshot_value() -> Value {
        json!({
            "snapshot_id": "snap-1",
            "as_of": "2026-05-28T14:30:00Z",
            "market_session": "rth",
            "symbols": ["SPY", "AAPL"],
            "latest_bars": {
                "SPY": {
                    "timestamp": "2026-05-28T14:29:00Z",
                    "open": 500.0,
                    "high": 501.0,
                    "low": 499.0,
                    "close": 500.0,
                    "volume": 1000000,
                    "source": "alpaca_market_data"
                },
                "AAPL": {
                    "timestamp": "2026-05-28T14:29:00Z",
                    "open": 200.0,
                    "high": 201.0,
                    "low": 199.0,
                    "close": 200.0,
                    "volume": 1000000,
                    "source": "alpaca_market_data"
                }
            },
            "latest_quotes": {
                "SPY": {
                    "timestamp": "2026-05-28T14:29:59Z",
                    "bid": 499.95,
                    "ask": 500.05,
                    "source": "alpaca_market_data"
                },
                "AAPL": {
                    "timestamp": "2026-05-28T14:29:59Z",
                    "bid": 199.95,
                    "ask": 200.05,
                    "source": "alpaca_market_data"
                }
            },
            "market_calendar_state": {
                "source": "alpaca_market_clock",
                "retrieved_at": "2026-05-28T14:29:59Z",
                "is_open": true,
                "next_open": "2026-05-29T13:30:00Z",
                "next_close": "2026-05-28T20:00:00Z",
                "freshness": "fresh"
            },
            "trend_summaries": {
                "SPY": "Higher highs with improving volume.",
                "AAPL": "Flat relative strength."
            },
            "source_freshness": [
                {"source": "alpaca_market_data", "freshness": "fresh"},
                {"source": "macro_calendar", "freshness": "fresh"},
                {"source": "alpaca_market_clock", "freshness": "fresh"},
                {"source": "news_summary", "freshness": "fresh"}
            ],
            "macro_news_summaries": [
                {
                    "source": "macro_calendar",
                    "url": "https://example.com/calendar",
                    "retrieved_at": "2026-05-28T14:20:00Z",
                "summary": "אין אירוע מתוכנן משמעותי בשעה הקרובה.",
                    "freshness": "fresh"
                },
                {
                    "source": "news_summary",
                    "url": "https://example.com/news",
                    "retrieved_at": "2026-05-28T14:20:00Z",
                    "summary": "כותרות שוק טריות זמינות.",
                    "freshness": "fresh"
                }
            ],
            "unavailable_data_flags": []
        })
    }

    fn snapshot_with_qqq() -> Value {
        let mut snapshot = snapshot_value();
        snapshot["symbols"] = json!(["SPY", "AAPL", "QQQ"]);
        snapshot["latest_bars"]["QQQ"] = json!({
            "timestamp": "2026-05-28T14:29:00Z",
            "open": 300.0,
            "high": 301.0,
            "low": 299.0,
            "close": 300.0,
            "volume": 1000000,
            "source": "alpaca_market_data"
        });
        snapshot["latest_quotes"]["QQQ"] = json!({
            "timestamp": "2026-05-28T14:29:59Z",
            "bid": 299.95,
            "ask": 300.05,
            "source": "alpaca_market_data"
        });
        snapshot["asset_metadata"] = json!({
            "SPY": {"tradable": true, "shortable": true, "fractionable": true},
            "AAPL": {"tradable": true, "shortable": true, "fractionable": true},
            "QQQ": {"tradable": true, "shortable": true, "fractionable": true}
        });
        snapshot
    }

    fn snapshot_with_symbol(symbol: &str, price: f64) -> Value {
        let mut snapshot = snapshot_value();
        snapshot["symbols"]
            .as_array_mut()
            .expect("symbols array")
            .push(json!(symbol));
        snapshot["latest_bars"][symbol] = json!({
            "timestamp": "2026-05-28T14:29:00Z",
            "open": price,
            "high": price + 1.0,
            "low": price - 1.0,
            "close": price,
            "volume": 1000000,
            "source": "alpaca_market_data"
        });
        snapshot["latest_quotes"][symbol] = json!({
            "timestamp": "2026-05-28T14:29:59Z",
            "bid": price - 0.05,
            "ask": price + 0.05,
            "source": "alpaca_market_data"
        });
        snapshot["trend_summaries"][symbol] = json!("סימבול מניות דינמי שנחקר.");
        snapshot
    }

    fn set_spy_mid(snapshot: &mut Value, price: f64) {
        snapshot["latest_quotes"]["SPY"]["bid"] = json!(price - 0.05);
        snapshot["latest_quotes"]["SPY"]["ask"] = json!(price + 0.05);
        snapshot["latest_bars"]["SPY"]["close"] = json!(price);
    }

    fn set_aapl_mid(snapshot: &mut Value, price: f64) {
        snapshot["latest_quotes"]["AAPL"]["bid"] = json!(price - 0.05);
        snapshot["latest_quotes"]["AAPL"]["ask"] = json!(price + 0.05);
        snapshot["latest_bars"]["AAPL"]["close"] = json!(price);
    }

    pub(super) fn decision_value(sequence: u64, action: &str, symbol: &str) -> Value {
        json!({
            "cycle_id": format!("cycle-{sequence}"),
            "sequence": sequence,
            "as_of": "2026-05-28T14:30:00Z",
            "market_session": "rth",
            "portfolio_nav_usd": LLM_EQUITY_INITIAL_CASH_USD,
            "commands": [{
                "action": action,
                "symbol": symbol,
                "rationale": "ה-LLM מזהה מגמה מאושרת והקשר מאקרו טרי.",
                "confidence": 0.72,
                "max_notional_usd": 1000.0,
                "evidence_refs": ["ev-1"]
            }],
            "evidence": [{
                "id": "ev-1",
                "source": "market_research_snapshot",
                "url": "https://example.com/snapshot",
                "retrieved_at": "2026-05-28T14:29:00Z",
                "summary": "נתוני מגמה וציטוט טריים.",
                "freshness": "fresh"
            }],
            "risk_assessment": {
                "summary": "בתוך מגבלות dry-run.",
                "max_loss_scenario": "גאפ נגד הכניסה מוגבל כי אין מינוף.",
                "risk_limit_verdict": "pass"
            },
            "memory_updates": [{
                "event_id": format!("memory-{sequence}"),
                "cycle_id": format!("cycle-{sequence}"),
                "created_at": "2026-05-28T14:30:00Z",
                "thesis": "ה-LLM מזהה מגמה מאושרת והקשר מאקרו טרי.",
                "outcome": "ממתין לבדיקת מילוי סימולציה.",
                "conclusion": "לשמור על כניסות קטנות עד שההמשכיות מתבססת.",
                "prompt_adjustment": "לבדוק מחדש ראיות מיושנות לפני כל פקודה.",
                "next_cycle_watch_items": ["ריביות", "רוחב שוק"]
            }]
        })
    }

    fn apply(
        authority: &mut LlmEquityDryRunAuthority,
        decision: Value,
        snapshot: Value,
    ) -> Result<LlmEquityApplyResult, LlmEquityAuthorityError> {
        authority.apply_json(
            &decision.to_string(),
            &snapshot.to_string(),
            parse_utc("2026-05-28T14:30:01Z").expect("fixture timestamp"),
        )
    }

    #[test]
    fn research_injected_clock_harness_matches_direct_paper_authority() {
        let decision = decision_value(1, "buy_notional", "SPY");
        let snapshot = snapshot_value();
        let received_at = parse_utc("2026-05-28T14:30:01Z").expect("fixture timestamp");
        let mut direct = LlmEquityDryRunAuthority::paper();
        let mut research = LlmEquityDryRunAuthority::paper();

        let direct_result = direct
            .apply_json(&decision.to_string(), &snapshot.to_string(), received_at)
            .expect("direct authority result");
        let research_result = research
            .apply_research_json(&decision.to_string(), &snapshot.to_string(), received_at)
            .expect("research harness result");

        assert_eq!(
            research_result,
            serde_json::to_value(direct_result).expect("direct result serialization")
        );
        assert_eq!(research.portfolio, direct.portfolio);
    }

    #[test]
    fn research_corporate_actions_adjust_only_the_isolated_paper_book() {
        let mut authority = LlmEquityDryRunAuthority::paper();
        authority.portfolio.positions.insert(
            "AAPL".to_string(),
            LlmEquityPosition {
                symbol: "AAPL".to_string(),
                quantity: 10.0,
                avg_price: 200.0,
            },
        );
        authority.last_prices.insert("AAPL".to_string(), 220.0);

        authority
            .apply_research_corporate_action("AAPL", "split", Some(2.0), None, None)
            .expect("split applies");
        authority
            .apply_research_corporate_action("AAPL", "cash_dividend", None, Some(1.5), None)
            .expect("dividend applies");
        authority
            .apply_research_corporate_action("AAPL", "symbol_change", None, None, Some("APPL"))
            .expect("symbol change applies");

        assert_eq!(authority.portfolio.positions["APPL"].quantity, 20.0);
        assert_eq!(authority.portfolio.positions["APPL"].avg_price, 100.0);
        assert_eq!(authority.last_prices["APPL"], 110.0);
        assert_eq!(authority.portfolio.cash_usd, 10_030.0);
        assert_eq!(authority.portfolio.realized_pnl_usd, 30.0);
        assert!(!authority.portfolio.positions.contains_key("AAPL"));
    }

    #[test]
    fn research_corporate_action_self_symbol_change_is_idempotent() {
        let mut authority = LlmEquityDryRunAuthority::paper();
        authority.portfolio.positions.insert(
            "XOM".to_string(),
            LlmEquityPosition {
                symbol: "XOM".to_string(),
                quantity: 2.0,
                avg_price: 100.0,
            },
        );
        authority.last_prices.insert("XOM".to_string(), 110.0);

        authority
            .apply_research_corporate_action("XOM", "symbol_change", None, None, Some("XOM"))
            .expect("an exact self-symbol change is a successful no-op");

        assert_eq!(authority.portfolio.positions["XOM"].quantity, 2.0);
        assert_eq!(authority.last_prices["XOM"], 110.0);
    }

    #[test]
    fn research_corporate_action_ignores_economic_dust_collision() {
        let mut authority = LlmEquityDryRunAuthority::paper();
        authority.portfolio.positions.insert(
            "AAPL".to_string(),
            LlmEquityPosition {
                symbol: "AAPL".to_string(),
                quantity: 2.0,
                avg_price: 100.0,
            },
        );
        authority.portfolio.positions.insert(
            "APPL".to_string(),
            LlmEquityPosition {
                symbol: "APPL".to_string(),
                quantity: 0.000_000_001,
                avg_price: 100.0,
            },
        );
        authority.last_prices.insert("AAPL".to_string(), 110.0);
        authority.last_prices.insert("APPL".to_string(), 100.0);

        authority
            .apply_research_corporate_action("AAPL", "symbol_change", None, None, Some("APPL"))
            .expect("economic dust must not create a symbol collision");

        assert_eq!(authority.portfolio.positions["APPL"].quantity, 2.0);
        assert!(!authority.portfolio.positions.contains_key("AAPL"));
    }

    #[test]
    fn rust_authority_accepts_rth_buy_and_accounts_portfolio() {
        let mut authority = LlmEquityDryRunAuthority::seeded();

        let result = apply(
            &mut authority,
            decision_value(1, "buy_notional", "SPY"),
            snapshot_value(),
        )
        .expect("fresh RTH decision accepted");

        assert_eq!(result.accepted_orders.len(), 1);
        assert_eq!(result.accepted_orders[0].side, LlmEquityOrderSide::Buy);
        assert_eq!(result.accepted_orders[0].notional_usd, 1000.0);
        assert_eq!(result.portfolio.cash_usd, 9000.0);
        assert_eq!(
            result.portfolio.positions["SPY"].quantity, 2.0,
            "SPY fixture mid price is 500"
        );
        assert_eq!(result.portfolio.nav_usd(&result.prices), 10_000.0);
    }

    #[test]
    fn runtime_ingest_cycle_does_not_emit_provisional_cycle_summary() {
        let state =
            LlmEquityRuntimeState::with_authority(LlmEquityDryRunAuthority::paper(), None, None);
        let now = Utc::now().to_rfc3339();
        let mut decision = decision_value(1, "buy_notional", "SPY");
        decision["as_of"] = json!(now);
        decision["evidence"][0]["retrieved_at"] = decision["as_of"].clone();
        decision["memory_updates"][0]["created_at"] = decision["as_of"].clone();
        let mut snapshot = snapshot_value();
        snapshot["as_of"] = decision["as_of"].clone();
        snapshot["latest_bars"]["SPY"]["timestamp"] = decision["as_of"].clone();
        snapshot["latest_quotes"]["SPY"]["timestamp"] = decision["as_of"].clone();
        snapshot["latest_bars"]["AAPL"]["timestamp"] = decision["as_of"].clone();
        snapshot["latest_quotes"]["AAPL"]["timestamp"] = decision["as_of"].clone();
        snapshot["market_calendar_state"]["retrieved_at"] = decision["as_of"].clone();
        snapshot["macro_news_summaries"][0]["retrieved_at"] = decision["as_of"].clone();
        snapshot["macro_news_summaries"][1]["retrieved_at"] = decision["as_of"].clone();

        state
            .ingest_cycle(decision, snapshot)
            .expect("fresh cycle applied");

        let snapshot = state.snapshot();
        assert_eq!(snapshot.last_cycle_id.as_deref(), Some("cycle-1"));
        assert!(snapshot.latest_result.is_some());
        assert!(
            snapshot.cycle_summaries.is_empty(),
            "the permanent dashboard ledger is populated by audited cycle summaries only"
        );
    }

    #[test]
    fn runtime_records_only_accepted_trade_ledger_summaries() {
        let state =
            LlmEquityRuntimeState::with_authority(LlmEquityDryRunAuthority::paper(), None, None);
        let mut summary = LlmEquityCycleSummary {
            cycle_id: "summary-1".to_string(),
            cycle_kind: "news_scan".to_string(),
            as_of: "2026-07-24T15:35:00Z".to_string(),
            market_session: "rth".to_string(),
            status: "blocked".to_string(),
            mandatory_trade_status: "blocked".to_string(),
            required_notional_usd: 25.0,
            requested_notional_usd: 25.0,
            accepted_notional_usd: 0.0,
            actions: vec!["buy".to_string()],
            symbols: vec!["SPY".to_string()],
            rationale: "Blocked mandatory cycle is audit data, not a trade ledger row.".to_string(),
            reason: "mandatory_order_notional_required".to_string(),
            timestamp_ns: 1,
        };

        let recorded = state
            .record_cycle_summary(summary.clone())
            .expect("blocked summaries remain contract-valid but are not recorded");
        assert!(!recorded);
        assert!(state.snapshot().cycle_summaries.is_empty());

        summary.status = "accepted".to_string();
        summary.mandatory_trade_status = "accepted".to_string();
        summary.accepted_notional_usd = 24.99;
        let recorded = state
            .record_cycle_summary(summary.clone())
            .expect("under-floor summaries are ignored");
        assert!(!recorded);
        assert!(state.snapshot().cycle_summaries.is_empty());

        summary.accepted_notional_usd = 25.0;
        let recorded = state
            .record_cycle_summary(summary)
            .expect("accepted floor-satisfying summary is recorded");
        assert!(recorded);
        assert_eq!(state.snapshot().cycle_summaries.len(), 1);
    }

    #[test]
    fn bounded_cycle_summaries_keeps_only_accepted_floor_satisfying_rows() {
        let mut summary = LlmEquityCycleSummary {
            cycle_id: "summary-1".to_string(),
            cycle_kind: "news_scan".to_string(),
            as_of: "2026-07-24T15:35:00Z".to_string(),
            market_session: "rth".to_string(),
            status: "blocked".to_string(),
            mandatory_trade_status: "blocked".to_string(),
            required_notional_usd: 25.0,
            requested_notional_usd: 25.0,
            accepted_notional_usd: 0.0,
            actions: vec!["buy".to_string()],
            symbols: vec!["SPY".to_string()],
            rationale: "Blocked mandatory cycle is audit data, not a trade ledger row.".to_string(),
            reason: "mandatory_order_notional_required".to_string(),
            timestamp_ns: 1,
        };
        let blocked = summary.clone();
        summary.cycle_id = "summary-2".to_string();
        summary.status = "accepted".to_string();
        summary.mandatory_trade_status = "accepted".to_string();
        summary.accepted_notional_usd = 24.99;
        let under_floor = summary.clone();
        summary.cycle_id = "summary-3".to_string();
        summary.accepted_notional_usd = LLM_EQUITY_MANDATORY_ORDER_NOTIONAL_USD;

        let retained = bounded_cycle_summaries(vec![blocked, under_floor, summary]);

        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].cycle_id, "summary-3");
    }

    #[test]
    fn mandatory_summary_floor_constant_remains_twenty_five_usd() {
        assert_eq!(LLM_EQUITY_MANDATORY_ORDER_NOTIONAL_USD, 25.0);
    }

    #[test]
    fn retain_portfolio_marks_flags_retained_benchmark_as_stale() {
        let mut previous_prices = HashMap::new();
        previous_prices.insert("SPY".to_string(), 505.0);
        let portfolio = LlmEquityPortfolio::seeded();

        let (prices, stale_symbols) =
            retain_portfolio_marks(HashMap::new(), &previous_prices, &portfolio);

        assert_eq!(prices.get("SPY"), Some(&505.0));
        assert!(stale_symbols.contains("SPY"));
    }

    #[test]
    fn cycle_summary_from_audit_row_preserves_action_symbol_pairs_and_thesis() {
        let row = json!({
            "cycle_kind": "news_scan",
            "status": "posted",
            "cycle_id": "llm-equity-2026-07-24T17:35:00Z",
            "created_at": "2026-07-24T17:35:15Z",
            "as_of": "2026-07-24T17:35:00Z",
            "market_session": "rth",
            "symbols_sample": ["SPY", "QQQ", "IWM"],
            "dashboard_summary_thesis": "Buy relative strength while trimming weaker small-cap exposure.",
            "mandatory_trade_status": "accepted",
            "mandatory_required_total_notional_usd": 25.0,
            "mandatory_accepted_total_notional_usd": 100.0,
            "review_attestation": {
                "required": true,
                "status": "compliant",
                "order_action": "buy_notional",
                "order_notional_usd": 100.0,
                "accepted_side": "buy",
                "accepted_notional_usd": 100.0
            },
            "rust_response": {
                "status": "success",
                "order_results": [
                    {
                        "symbol": "SPY",
                        "side": "buy",
                        "status": "accepted",
                        "notional_usd": 25.0,
                        "reason": "spy_fill"
                    },
                    {
                        "symbol": "QQQ",
                        "side": "buy",
                        "status": "accepted",
                        "notional_usd": 25.0,
                        "reason": "qqq_fill"
                    },
                    {
                        "symbol": "IWM",
                        "side": "sell",
                        "status": "accepted",
                        "notional_usd": 50.0,
                        "reason": "iwm_trim"
                    },
                    {
                        "symbol": "SPY",
                        "side": "buy",
                        "status": "accepted",
                        "notional_usd": 25.0,
                        "reason": "duplicate_fill"
                    }
                ]
            }
        });

        let summary = cycle_summary_from_audit_row(&row).expect("audit summary recovered");

        assert_eq!(summary.actions, vec!["buy", "buy", "sell"]);
        assert_eq!(summary.symbols, vec!["SPY", "QQQ", "IWM"]);
        assert_eq!(
            summary.rationale,
            "Buy relative strength while trimming weaker small-cap exposure."
        );
    }

    #[test]
    fn cycle_summary_from_audit_row_prefers_cumulative_accepted_trades() {
        let row = json!({
            "cycle_kind": "release",
            "status": "posted",
            "cycle_id": "llm-equity-2026-07-24T17:35:00Z",
            "created_at": "2026-07-24T17:35:15Z",
            "as_of": "2026-07-24T17:35:00Z",
            "market_session": "rth",
            "symbols_sample": ["SPY", "QQQ", "IWM"],
            "mandatory_trade_status": "accepted",
            "mandatory_required_total_notional_usd": 100.0,
            "mandatory_accepted_total_notional_usd": 100.0,
            "accepted_trades": [
                {
                    "symbol": "SPY",
                    "action": "buy",
                    "accepted_notional_usd": 40.0,
                    "horizon": "60m",
                    "outcome_status": "pending",
                    "exposure_effect": "add"
                },
                {
                    "symbol": "IWM",
                    "action": "sell",
                    "accepted_notional_usd": 60.0,
                    "horizon": "60m",
                    "outcome_status": "pending",
                    "exposure_effect": "reduce"
                }
            ],
            "review_attestation": {
                "order_notional_usd": 60.0
            },
            "rust_response": {
                "status": "success",
                "order_results": [
                    {
                        "symbol": "IWM",
                        "side": "sell",
                        "status": "accepted",
                        "notional_usd": 60.0,
                        "reason": "final_recovery_fill"
                    }
                ]
            }
        });

        let summary = cycle_summary_from_audit_row(&row).expect("audit summary recovered");

        assert_eq!(summary.accepted_notional_usd, 100.0);
        assert_eq!(summary.actions, vec!["buy", "sell"]);
        assert_eq!(summary.symbols, vec!["SPY", "IWM"]);
    }

    #[test]
    fn cycle_summary_from_audit_row_excludes_blocked_or_under_floor_rows() {
        let mut row = json!({
            "cycle_kind": "news_scan",
            "status": "blocked",
            "cycle_id": "llm-equity-2026-07-24T16:35:00Z",
            "created_at": "2026-07-24T16:35:15Z",
            "as_of": "2026-07-24T16:35:00Z",
            "market_session": "rth",
            "symbols_sample": ["SPY"],
            "dashboard_summary_thesis": "Blocked mandatory paper cycle.",
            "mandatory_trade_status": "blocked",
            "mandatory_required_total_notional_usd": 25.0,
            "mandatory_accepted_total_notional_usd": 0.0,
            "review_attestation": {
                "required": true,
                "status": "compliant",
                "order_action": "buy_notional",
                "order_notional_usd": 25.0
            },
            "rust_response": {"status": "success", "order_results": []}
        });

        assert!(cycle_summary_from_audit_row(&row).is_none());

        row["status"] = json!("posted");
        row["mandatory_trade_status"] = json!("accepted");
        row["mandatory_accepted_total_notional_usd"] = json!(24.99);
        row["rust_response"] = json!({
            "status": "success",
            "order_results": [{
                "symbol": "SPY",
                "side": "buy",
                "status": "accepted",
                "notional_usd": 24.99,
                "reason": "simulated_fill"
            }]
        });

        assert!(cycle_summary_from_audit_row(&row).is_none());
    }

    #[test]
    fn shadow_authority_records_decision_without_mutating_portfolio() {
        let mut authority = LlmEquityDryRunAuthority::shadow();

        let result = apply(
            &mut authority,
            decision_value(1, "buy_notional", "SPY"),
            snapshot_value(),
        )
        .expect("valid shadow decision is recorded");

        assert!(result.accepted_orders.is_empty());
        assert_eq!(result.rejected_orders[0].reason, "fills_frozen_shadow_only");
        assert_eq!(result.portfolio.cash_usd, LLM_EQUITY_INITIAL_CASH_USD);
        assert!(result.portfolio.positions.is_empty());
    }

    #[test]
    fn rust_authority_rejects_unknown_learning_horizon() {
        let mut decision = decision_value(1, "buy_notional", "SPY");
        decision["commands"][0]["horizon"] = json!("2h");

        let error = apply(
            &mut LlmEquityDryRunAuthority::shadow(),
            decision,
            snapshot_value(),
        )
        .expect_err("unknown horizon must fail at the authority boundary");

        assert_eq!(error, LlmEquityAuthorityError::Contract("horizon_invalid"));
    }

    #[test]
    fn paper_authority_enforces_single_name_limit() {
        let mut snapshot = snapshot_value();
        snapshot["asset_metadata"] = json!({
            "SPY": {"tradable": true, "shortable": true, "fractionable": true},
            "AAPL": {"tradable": true, "shortable": true, "fractionable": true}
        });
        let mut decision = decision_value(1, "buy_notional", "SPY");
        decision["commands"][0]["max_notional_usd"] = json!(800.0);

        let result = apply(&mut LlmEquityDryRunAuthority::paper(), decision, snapshot)
            .expect("risk violation is an order-level rejection");

        assert!(result.accepted_orders.is_empty());
        assert_eq!(result.rejected_orders[0].reason, "single_name_limit");
    }

    #[test]
    fn paper_authority_caps_high_correlation_proxy_exposure() {
        let mut snapshot = snapshot_with_symbol("BITO", 100.0);
        snapshot["symbols"]
            .as_array_mut()
            .expect("symbols array")
            .push(json!("ETHA"));
        snapshot["latest_bars"]["ETHA"] = snapshot["latest_bars"]["BITO"].clone();
        snapshot["latest_quotes"]["ETHA"] = snapshot["latest_quotes"]["BITO"].clone();
        snapshot["trend_summaries"]["ETHA"] = json!("מתאם קריפטו גבוה.");
        snapshot["asset_metadata"] = json!({
            "SPY": {"tradable": true, "shortable": true, "fractionable": true},
            "AAPL": {"tradable": true, "shortable": true, "fractionable": true},
            "BITO": {"tradable": true, "shortable": true, "fractionable": true},
            "ETHA": {"tradable": true, "shortable": true, "fractionable": true}
        });
        let mut authority = LlmEquityDryRunAuthority::paper();
        let mut first = decision_value(1, "buy_notional", "BITO");
        first["commands"][0]["max_notional_usd"] = json!(600.0);
        let first_result = apply(&mut authority, first, snapshot.clone())
            .expect("first bounded proxy entry is accepted");
        assert_eq!(first_result.accepted_orders.len(), 1);

        let mut second = decision_value(2, "buy_notional", "ETHA");
        second["commands"][0]["max_notional_usd"] = json!(500.0);
        let second_result = apply(&mut authority, second, snapshot)
            .expect("correlated theme breach is an order-level rejection");

        assert!(second_result.accepted_orders.is_empty());
        assert_eq!(
            second_result.rejected_orders[0].reason,
            "high_correlation_group_exposure_limit"
        );
    }

    #[test]
    fn profit_runtime_configuration_is_restored_in_isolated_process() {
        if std::env::var("TRAIDAN_TEST_PROFIT_ENV").as_deref() != Ok("1") {
            let root = std::env::temp_dir().join(format!(
                "profit-env-{}-{}",
                std::process::id(),
                Utc::now().timestamp_nanos_opt().unwrap()
            ));
            std::fs::create_dir_all(&root).unwrap();
            let result=std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact","llm_equity::tests::profit_runtime_configuration_is_restored_in_isolated_process"])
                .env("TRAIDAN_TEST_PROFIT_ENV","1")
                .env(LLM_EQUITY_FILL_MODE_ENV,"paper").env(LLM_EQUITY_RISK_MODE_ENV,"profit_driven")
                .env(LLM_EQUITY_STATE_FILE_ENV,root.join("state.json"))
                .env(LLM_EQUITY_DASHBOARD_STATE_FILE_ENV,root.join("dashboard.json"))
                .output().unwrap();
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stdout)
            );
            std::fs::remove_dir_all(root).unwrap();
            return;
        }
        let state = LlmEquityRuntimeState::new();
        {
            let inner = state.inner.lock().unwrap();
            assert!(inner.authority.profit_driven_policy);
            inner
                .authority
                .persist_to_path(inner.state_file.as_ref().unwrap())
                .unwrap();
        }
        let restored = LlmEquityRuntimeState::new();
        assert!(
            restored
                .inner
                .lock()
                .unwrap()
                .authority
                .profit_driven_policy
        );
    }

    #[test]
    fn profit_runtime_marks_persist_and_rollback_on_write_failure() {
        let root = std::env::temp_dir().join(format!(
            "profit-runtime-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("state.json");
        let mut authority = LlmEquityDryRunAuthority::paper_profit_driven();
        authority.portfolio.peak_nav_usd = 12000.0;
        let update = || {
            let value = snapshot_with_qqq();
            let parsed: SnapshotEnvelope = serde_json::from_value(value.clone()).unwrap();
            ValidatedLlmEquityServiceStatusUpdate {
                status: "risk_monitoring".into(),
                detail: "holding check".into(),
                market_snapshot: Some((value, snapshot_mid_prices(&parsed).unwrap())),
                last_error: None,
                service_identity: None,
                market_session: None,
                service_activity_key: None,
                updated_at: DateTime::parse_from_rfc3339("2026-05-28T14:30:01Z")
                    .unwrap()
                    .with_timezone(&Utc),
            }
        };
        let state =
            LlmEquityRuntimeState::with_authority(authority.clone(), Some(path.clone()), None);
        state.update_service_status_at(update()).unwrap();
        assert!(
            LlmEquityDryRunAuthority::restore_research_from_path(&path, "profit_driven")
                .unwrap()
                .drawdown_halted
        );
        let failed =
            LlmEquityRuntimeState::with_authority(authority, Some(path.join("blocked.json")), None);
        assert!(failed.update_service_status_at(update()).is_err());
        assert!(!failed.inner.lock().unwrap().authority.drawdown_halted);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn profit_execution_boundary_and_short_cover_are_enforced() {
        let mut decision = decision_value(1, "buy_notional", "SPY");
        decision["execution_policy"] = json!("unknown");
        assert_eq!(
            apply(
                &mut LlmEquityDryRunAuthority::paper_profit_driven(),
                decision,
                snapshot_with_qqq()
            )
            .unwrap_err(),
            LlmEquityAuthorityError::Contract("execution_policy_mismatch")
        );
        let mut authority = LlmEquityDryRunAuthority::paper_profit_driven();
        let snapshot = snapshot_with_qqq();
        let parsed: SnapshotEnvelope = serde_json::from_value(snapshot.clone()).unwrap();
        let mut close = snapshot.clone();
        close["market_calendar_state"]["next_close"] = json!("2026-05-28T14:30:00Z");
        let result = apply(
            &mut authority,
            decision_value(1, "buy_notional", "SPY"),
            close,
        )
        .unwrap();
        assert_eq!(
            result.rejected_orders[0].reason,
            "execution_market_clock_closed"
        );
        let command:DecisionCommand=serde_json::from_value(json!({"action":"buy_notional","symbol":"SPY",
            "max_notional_usd":10000,"reduce_only":true,"confidence":1.0,"rationale":"cover", "evidence_refs":["e"]})).unwrap();
        assert_eq!(authority.executable_notional(&command, &parsed, 500.0), 0.0);
        authority.portfolio.positions.insert(
            "SPY".into(),
            LlmEquityPosition {
                symbol: "SPY".into(),
                quantity: -0.001,
                avg_price: 500.0,
            },
        );
        assert!(authority.executable_notional(&command, &parsed, 500.0) > 0.5);
        authority.realistic_execution = false;
        assert_eq!(authority.executable_notional(&command, &parsed, 500.0), 0.5);
    }

    #[test]
    fn profit_trade_summary_retains_dust_exit() {
        let row = json!({"risk_mode":"profit_driven","cycle_kind":"run_once","status":"posted", "cycle_id":"dust",
            "as_of":"2026-05-28T14:30:00Z","market_session":"rth","mandatory_trade_status":"not_applicable",
            "rust_response":{"order_results":[{"symbol":"SPY","side":"sell","status":"accepted","notional_usd":0.25,"reason":"fill"}]}});
        let summary = cycle_summary_from_audit_row(&row).unwrap();
        assert_eq!(summary.required_notional_usd, 0.0);
        assert!(is_trade_ledger_cycle_summary(&summary));
    }

    #[test]
    fn profit_driven_marks_latch_loss_without_a_model_decision() {
        let mut authority = LlmEquityDryRunAuthority::paper_profit_driven();
        authority.portfolio.peak_nav_usd = 12000.0;
        let at = DateTime::parse_from_rfc3339("2026-05-28T14:30:01Z")
            .unwrap()
            .with_timezone(&Utc);
        authority
            .observe_research_snapshot(snapshot_with_qqq(), at)
            .unwrap();
        assert!(authority.drawdown_halted);
        assert_eq!(authority.highest_sequence, 0);
        assert!(authority.seen_cycle_ids.is_empty());
        let mut stale = snapshot_with_qqq();
        stale["source_freshness"][0]["freshness"] = json!("stale");
        assert!(authority.observe_research_snapshot(stale, at).is_err());
    }

    #[test]
    fn profit_driven_closes_dust_without_floor_or_accidental_short() {
        let mut authority = LlmEquityDryRunAuthority::paper_profit_driven();
        let mut entry = decision_value(1, "buy_notional", "SPY");
        entry["commands"][0]["max_notional_usd"] = json!(1.0);
        let result = apply(&mut authority, entry, snapshot_with_qqq()).unwrap();
        assert_eq!(result.accepted_orders.len(), 1);
        let mut exit = decision_value(2, "sell_notional", "SPY");
        exit["commands"][0]["reduce_only"] = json!(true);
        let result = apply(&mut authority, exit, snapshot_with_qqq()).unwrap();
        assert_eq!(result.accepted_orders.len(), 1);
        assert!(authority.portfolio.positions.is_empty());
        assert!(result.accepted_orders[0].notional_usd < 1.0);
    }

    #[test]
    fn profit_driven_reduction_cannot_open_or_increase_position() {
        let mut authority = LlmEquityDryRunAuthority::paper_profit_driven();
        let mut decision = decision_value(1, "sell_notional", "SPY");
        decision["commands"][0]["reduce_only"] = json!(true);
        let result = apply(&mut authority, decision, snapshot_with_qqq()).unwrap();
        assert_eq!(
            result.rejected_orders[0].reason,
            "reduce_only_no_opposing_position"
        );
    }

    #[test]
    fn profit_driven_drawdown_halt_survives_recovery_and_restore() {
        let mut authority = LlmEquityDryRunAuthority::paper_profit_driven();
        authority.portfolio.peak_nav_usd = 12000.0;
        let result = apply(
            &mut authority,
            decision_value(1, "buy_notional", "SPY"),
            snapshot_with_qqq(),
        )
        .unwrap();
        assert_eq!(result.rejected_orders[0].reason, "drawdown_limit");
        authority.portfolio.cash_usd = 12000.0;
        let state = authority.export_research_state().unwrap();
        let mut restored =
            LlmEquityDryRunAuthority::restore_research_from_value(state, "profit_driven").unwrap();
        let result = apply(
            &mut restored,
            decision_value(2, "buy_notional", "SPY"),
            snapshot_with_qqq(),
        )
        .unwrap();
        assert_eq!(result.rejected_orders[0].reason, "drawdown_limit");
    }

    #[test]
    fn paper_authority_requires_borrow_for_new_short() {
        let mut snapshot = snapshot_value();
        snapshot["asset_metadata"] = json!({
            "SPY": {"tradable": true, "shortable": false, "fractionable": true},
            "AAPL": {"tradable": true, "shortable": true, "fractionable": true}
        });
        let mut decision = decision_value(1, "sell_notional", "SPY");
        decision["commands"][0]["max_notional_usd"] = json!(100.0);

        let result = apply(&mut LlmEquityDryRunAuthority::paper(), decision, snapshot)
            .expect("missing borrow is an order-level rejection");

        assert!(result.accepted_orders.is_empty());
        assert_eq!(result.rejected_orders[0].reason, "short_borrow_unavailable");
    }

    #[test]
    fn llm_discretion_paper_accepts_large_unborrowable_untradable_paper_intents() {
        let mut snapshot = snapshot_value();
        snapshot["asset_metadata"] = json!({
            "SPY": {"tradable": false, "shortable": false, "fractionable": false},
            "AAPL": {"tradable": true, "shortable": true, "fractionable": true}
        });
        let mut decision = decision_value(1, "sell_notional", "SPY");
        decision["commands"][0]["max_notional_usd"] = json!(100_000.0);

        let result = apply(
            &mut LlmEquityDryRunAuthority::paper_llm_discretion(),
            decision,
            snapshot,
        )
        .expect("paper discretion still returns an order-level result");

        assert_eq!(result.accepted_orders.len(), 1);
        assert!(result.rejected_orders.is_empty());
    }

    #[test]
    fn llm_discretion_paper_keeps_executable_quote_and_rth_hard_gates() {
        let mut unpriced_snapshot = snapshot_value();
        unpriced_snapshot["latest_quotes"]["SPY"]["bid"] = json!(0.0);
        unpriced_snapshot["latest_quotes"]["SPY"]["ask"] = json!(0.0);
        let unpriced = apply(
            &mut LlmEquityDryRunAuthority::paper_llm_discretion(),
            decision_value(1, "buy_notional", "SPY"),
            unpriced_snapshot,
        )
        .expect_err("unpriced paper discretion snapshot fails the hard quote contract");
        assert_eq!(unpriced, LlmEquityAuthorityError::Contract("quote_invalid"));

        let mut closed_snapshot = snapshot_value();
        closed_snapshot["market_calendar_state"]["is_open"] = json!(false);
        let closed = apply(
            &mut LlmEquityDryRunAuthority::paper_llm_discretion(),
            decision_value(1, "buy_notional", "SPY"),
            closed_snapshot,
        )
        .expect("closed-RTH paper discretion intent is rejected at order level");
        assert_eq!(closed.rejected_orders[0].reason, "market_clock_closed");
    }

    #[test]
    fn forced_aggressive_enforces_tradability_borrow_and_single_name_limit() {
        let mut untradable = snapshot_with_qqq();
        untradable["asset_metadata"]["QQQ"]["tradable"] = json!(false);
        let result = apply(
            &mut LlmEquityDryRunAuthority::paper_forced_aggressive(),
            decision_value(1, "buy_notional", "QQQ"),
            untradable,
        )
        .expect("untradable intent is an order rejection");
        assert_eq!(result.rejected_orders[0].reason, "asset_not_tradable");

        let mut unborrowable = snapshot_with_qqq();
        unborrowable["asset_metadata"]["QQQ"]["shortable"] = json!(false);
        let result = apply(
            &mut LlmEquityDryRunAuthority::paper_forced_aggressive(),
            decision_value(1, "sell_notional", "QQQ"),
            unborrowable,
        )
        .expect("unborrowable short is an order rejection");
        assert_eq!(result.rejected_orders[0].reason, "short_borrow_unavailable");

        let mut oversized = decision_value(1, "buy_notional", "QQQ");
        oversized["commands"][0]["max_notional_usd"] = json!(1_501.0);
        let result = apply(
            &mut LlmEquityDryRunAuthority::paper_forced_aggressive(),
            oversized,
            snapshot_with_qqq(),
        )
        .expect("single-name breach is an order rejection");
        assert_eq!(result.rejected_orders[0].reason, "single_name_limit");
    }

    #[test]
    fn research_long_risk_adjusted_rejects_shorts_and_uses_production_safety_limits() {
        let short = apply(
            &mut LlmEquityDryRunAuthority::paper_long_risk_adjusted(),
            decision_value(1, "sell_notional", "SPY"),
            snapshot_with_qqq(),
        )
        .expect("long-only intent is rejected at order level");
        assert_eq!(short.rejected_orders[0].reason, "research_long_only");

        let mut oversized = decision_value(1, "buy_notional", "QQQ");
        oversized["commands"][0]["max_notional_usd"] = json!(751.0);
        let result = apply(
            &mut LlmEquityDryRunAuthority::paper_long_risk_adjusted(),
            oversized,
            snapshot_with_qqq(),
        )
        .expect("single-name breach is rejected at order level");
        assert_eq!(result.rejected_orders[0].reason, "single_name_limit");
    }

    #[test]
    fn research_long_risk_adjusted_accepts_a_bounded_existing_long_reduction() {
        let mut authority = LlmEquityDryRunAuthority::paper_long_risk_adjusted();
        let mut entry = decision_value(1, "buy_notional", "SPY");
        entry["commands"][0]["max_notional_usd"] = json!(750.0);
        apply(&mut authority, entry, snapshot_with_qqq()).expect("long entry accepted");

        let mut reduction = decision_value(2, "sell_notional", "SPY");
        reduction["commands"][0]["max_notional_usd"] = json!(25.0);
        let result = apply(&mut authority, reduction, snapshot_with_qqq())
            .expect("bounded long reduction accepted");

        assert_eq!(result.accepted_orders.len(), 1);
        assert_eq!(result.accepted_orders[0].side, LlmEquityOrderSide::Sell);
        assert!(result.portfolio.positions["SPY"].quantity > 0.0);
    }

    #[test]
    fn forced_aggressive_prices_exact_boundary_shorts_at_executable_quantity() {
        let mut authority = LlmEquityDryRunAuthority::paper_forced_aggressive();
        authority.portfolio.cash_usd = 9_200.0;
        authority.portfolio.positions.insert(
            "SPY".to_string(),
            LlmEquityPosition {
                symbol: "SPY".to_string(),
                quantity: 1.6,
                avg_price: 500.0,
            },
        );
        let mut boundary = decision_value(1, "sell_notional", "AAPL");
        boundary["commands"][0]["max_notional_usd"] = json!(1_500.0);

        let result = apply(&mut authority, boundary, snapshot_with_qqq())
            .expect("boundary short is an order-level rejection");

        assert!(result.accepted_orders.is_empty());
        assert_eq!(result.rejected_orders[0].reason, "single_name_limit");

        let mut authority = LlmEquityDryRunAuthority::paper_forced_aggressive();
        authority.portfolio.cash_usd = 9_200.0;
        authority.portfolio.positions.insert(
            "SPY".to_string(),
            LlmEquityPosition {
                symbol: "SPY".to_string(),
                quantity: 1.6,
                avg_price: 500.0,
            },
        );
        let mut admissible = decision_value(1, "sell_notional", "AAPL");
        admissible["commands"][0]["max_notional_usd"] = json!(1_499.0);
        let result = apply(&mut authority, admissible, snapshot_with_qqq())
            .expect("short below executable-price headroom is accepted");

        assert_eq!(result.accepted_orders.len(), 1);
    }

    #[test]
    fn forced_aggressive_enforces_gross_net_short_cash_and_turnover_limits() {
        let mut gross = LlmEquityDryRunAuthority::paper_forced_aggressive();
        gross.portfolio.positions.insert(
            "SPY".to_string(),
            LlmEquityPosition {
                symbol: "SPY".to_string(),
                quantity: 12.0,
                avg_price: 500.0,
            },
        );
        gross.portfolio.positions.insert(
            "AAPL".to_string(),
            LlmEquityPosition {
                symbol: "AAPL".to_string(),
                quantity: -30.0,
                avg_price: 200.0,
            },
        );
        let mut decision = decision_value(1, "buy_notional", "QQQ");
        decision["commands"][0]["max_notional_usd"] = json!(600.0);
        let result = apply(&mut gross, decision, snapshot_with_qqq()).expect("gross rejection");
        assert_eq!(result.rejected_orders[0].reason, "gross_exposure_limit");

        let mut net = LlmEquityDryRunAuthority::paper_forced_aggressive();
        net.portfolio.cash_usd = 3_000.0;
        net.portfolio.positions.insert(
            "SPY".to_string(),
            LlmEquityPosition {
                symbol: "SPY".to_string(),
                quantity: 14.0,
                avg_price: 500.0,
            },
        );
        let mut decision = decision_value(1, "buy_notional", "QQQ");
        decision["commands"][0]["max_notional_usd"] = json!(600.0);
        let result = apply(&mut net, decision, snapshot_with_qqq()).expect("net rejection");
        assert_eq!(result.rejected_orders[0].reason, "net_exposure_limit");

        let mut short = LlmEquityDryRunAuthority::paper_forced_aggressive();
        short.portfolio.cash_usd = 15_500.0;
        short.portfolio.positions.insert(
            "SPY".to_string(),
            LlmEquityPosition {
                symbol: "SPY".to_string(),
                quantity: -11.0,
                avg_price: 500.0,
            },
        );
        let mut decision = decision_value(1, "sell_notional", "QQQ");
        decision["commands"][0]["max_notional_usd"] = json!(600.0);
        let result = apply(&mut short, decision, snapshot_with_qqq()).expect("short rejection");
        assert_eq!(result.rejected_orders[0].reason, "short_gross_limit");

        let mut cash = LlmEquityDryRunAuthority::paper_forced_aggressive();
        cash.portfolio.cash_usd = 600.0;
        cash.portfolio.positions.insert(
            "SPY".to_string(),
            LlmEquityPosition {
                symbol: "SPY".to_string(),
                quantity: 18.8,
                avg_price: 500.0,
            },
        );
        let mut decision = decision_value(1, "buy_notional", "QQQ");
        decision["commands"][0]["max_notional_usd"] = json!(200.0);
        let result = apply(&mut cash, decision, snapshot_with_qqq()).expect("cash rejection");
        assert_eq!(result.rejected_orders[0].reason, "cash_reserve_limit");

        let mut turnover = LlmEquityDryRunAuthority::paper_forced_aggressive();
        turnover.turnover_today_usd = 39_950.0;
        turnover.turnover_window = Some((NaiveDate::from_ymd_opt(2026, 5, 28).unwrap(), 10));
        turnover.portfolio_day = Some(NaiveDate::from_ymd_opt(2026, 5, 28).unwrap());
        let mut decision = decision_value(1, "buy_notional", "QQQ");
        decision["commands"][0]["max_notional_usd"] = json!(100.0);
        let result =
            apply(&mut turnover, decision, snapshot_with_qqq()).expect("turnover rejection");
        assert_eq!(result.rejected_orders[0].reason, "turnover_limit");
    }

    #[test]
    fn forced_aggressive_loss_and_drawdown_allow_only_reductions_or_flat_probe() {
        let trading_day = NaiveDate::from_ymd_opt(2026, 5, 28).expect("valid trading day");
        let mut loss = LlmEquityDryRunAuthority::paper_forced_aggressive();
        loss.portfolio.cash_usd = 9_700.0;
        loss.portfolio_day = Some(trading_day);
        let mut decision = decision_value(1, "buy_notional", "AAPL");
        decision["commands"][0]["max_notional_usd"] = json!(50.0);
        let result = apply(&mut loss, decision, snapshot_with_qqq()).expect("loss rejection");
        assert_eq!(result.rejected_orders[0].reason, "daily_loss_limit");

        let mut reduction = LlmEquityDryRunAuthority::paper_forced_aggressive();
        reduction.portfolio.cash_usd = 8_950.0;
        reduction.portfolio_day = Some(trading_day);
        reduction.portfolio.positions.insert(
            "SPY".to_string(),
            LlmEquityPosition {
                symbol: "SPY".to_string(),
                quantity: 1.5,
                avg_price: 500.0,
            },
        );
        let mut decision = decision_value(1, "sell_notional", "SPY");
        decision["commands"][0]["max_notional_usd"] = json!(25.0);
        let result =
            apply(&mut reduction, decision, snapshot_with_qqq()).expect("reduction accepted");
        assert_eq!(result.accepted_orders.len(), 1);

        let mut drawdown = LlmEquityDryRunAuthority::paper_forced_aggressive();
        drawdown.portfolio.cash_usd = 9_000.0;
        drawdown.portfolio.day_start_nav_usd = 9_000.0;
        drawdown.portfolio_day = Some(trading_day);
        let mut decision = decision_value(1, "buy_notional", "AAPL");
        decision["commands"][0]["max_notional_usd"] = json!(50.0);
        let result =
            apply(&mut drawdown, decision, snapshot_with_qqq()).expect("drawdown rejection");
        assert_eq!(result.rejected_orders[0].reason, "drawdown_limit");

        let mut probe = LlmEquityDryRunAuthority::paper_forced_aggressive();
        probe.portfolio.cash_usd = 9_000.0;
        probe.portfolio_day = Some(trading_day);
        let mut probe_decision = decision_value(1, "buy_notional", "SPY");
        probe_decision["commands"][0]["max_notional_usd"] = json!(100.0);
        let result = apply(&mut probe, probe_decision, snapshot_with_qqq())
            .expect("flat benchmark floor probe accepted");
        assert_eq!(result.accepted_orders.len(), 1);
    }

    #[test]
    fn risk_limited_paper_gross_floor_requires_buys_until_built() {
        let mut under_floor_sell = decision_value(1, "sell_notional", "SPY");
        under_floor_sell["commands"][0]["max_notional_usd"] = json!(25.0);
        let result = apply(
            &mut LlmEquityDryRunAuthority::paper_forced_aggressive(),
            under_floor_sell,
            snapshot_with_qqq(),
        )
        .expect("under-floor sell is an order-level rejection");
        assert_eq!(
            result.rejected_orders[0].reason,
            "minimum_gross_exposure_floor"
        );

        let mut under_floor_buy = decision_value(1, "buy_notional", "SPY");
        under_floor_buy["commands"][0]["max_notional_usd"] = json!(25.0);
        let result = apply(
            &mut LlmEquityDryRunAuthority::paper_forced_aggressive(),
            under_floor_buy,
            snapshot_with_qqq(),
        )
        .expect("minimum under-floor buy is accepted");
        assert_eq!(result.accepted_orders.len(), 1);
        assert_eq!(result.accepted_orders[0].side, LlmEquityOrderSide::Buy);
    }

    #[test]
    fn risk_limited_paper_gross_floor_blocks_reductions_below_floor() {
        let mut authority = LlmEquityDryRunAuthority::paper_forced_aggressive();
        authority.portfolio.cash_usd = 9_300.0;
        authority.portfolio.positions.insert(
            "SPY".to_string(),
            LlmEquityPosition {
                symbol: "SPY".to_string(),
                quantity: 1.4,
                avg_price: 500.0,
            },
        );
        let mut reduction = decision_value(1, "sell_notional", "SPY");
        reduction["commands"][0]["max_notional_usd"] = json!(25.0);

        let result = apply(&mut authority, reduction, snapshot_with_qqq())
            .expect("floor breach is an order-level rejection");

        assert!(result.accepted_orders.is_empty());
        assert_eq!(
            result.rejected_orders[0].reason,
            "minimum_gross_exposure_floor"
        );
        assert_eq!(result.portfolio.gross_exposure_usd(&result.prices), 700.0);
    }

    #[test]
    fn risk_limited_paper_gross_floor_allows_reductions_that_stay_above_floor() {
        let mut authority = LlmEquityDryRunAuthority::paper_forced_aggressive();
        authority.portfolio.cash_usd = 9_250.0;
        authority.portfolio.positions.insert(
            "SPY".to_string(),
            LlmEquityPosition {
                symbol: "SPY".to_string(),
                quantity: 1.5,
                avg_price: 500.0,
            },
        );
        let mut reduction = decision_value(1, "sell_notional", "SPY");
        reduction["commands"][0]["max_notional_usd"] = json!(25.0);

        let result = apply(&mut authority, reduction, snapshot_with_qqq())
            .expect("reduction remains above floor");

        assert_eq!(result.accepted_orders.len(), 1);
        assert_eq!(result.accepted_orders[0].side, LlmEquityOrderSide::Sell);
        assert!(result.portfolio.gross_exposure_usd(&result.prices) >= 700.0);
    }

    #[test]
    fn llm_discretion_paper_does_not_enforce_gross_floor() {
        let mut decision = decision_value(1, "sell_notional", "SPY");
        decision["commands"][0]["max_notional_usd"] = json!(25.0);

        let result = apply(
            &mut LlmEquityDryRunAuthority::paper_llm_discretion(),
            decision,
            snapshot_with_qqq(),
        )
        .expect("legacy discretion keeps risk limits disabled");

        assert_eq!(result.accepted_orders.len(), 1);
        assert_eq!(result.accepted_orders[0].side, LlmEquityOrderSide::Sell);
    }

    #[test]
    fn rust_authority_retains_optional_forecast_and_denies_unknown_forecast_fields() {
        let mut decision = decision_value(1, "buy_notional", "SPY");
        decision["commands"][0]["forecast"] = json!({
            "edge_class": "positive_edge",
            "expected_excess_return_bps": 25.0,
            "probability_positive": 0.6,
            "return_p10_bps": -30.0,
            "return_p50_bps": 20.0,
            "return_p90_bps": 80.0
        });
        let cycle = LlmEquityDryRunAuthority::shadow()
            .apply_json_with_validated_payloads(
                &decision.to_string(),
                &snapshot_value().to_string(),
                DateTime::parse_from_rfc3339("2026-05-28T14:30:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
            )
            .expect("forecast is retained");
        assert_eq!(
            cycle.decision["commands"][0]["forecast"]["edge_class"],
            "positive_edge"
        );

        decision["commands"][0]["forecast"]["unknown"] = json!(true);
        let error = apply(
            &mut LlmEquityDryRunAuthority::shadow(),
            decision,
            snapshot_value(),
        )
        .expect_err("unknown forecast field must fail closed");
        assert!(matches!(
            error,
            LlmEquityAuthorityError::MalformedDecision(_)
        ));
    }

    #[test]
    fn paper_authority_validates_long_to_short_sign_flip() {
        let mut authority = LlmEquityDryRunAuthority::paper();
        let mut entry_snapshot = snapshot_value();
        entry_snapshot["asset_metadata"] = json!({
            "SPY": {"tradable": true, "shortable": true, "fractionable": true},
            "AAPL": {"tradable": true, "shortable": true, "fractionable": true}
        });
        let mut entry = decision_value(1, "buy_notional", "SPY");
        entry["commands"][0]["max_notional_usd"] = json!(100.0);
        apply(&mut authority, entry, entry_snapshot).expect("long entry accepted");

        let mut flip_snapshot = snapshot_value();
        flip_snapshot["asset_metadata"] = json!({
            "SPY": {"tradable": true, "shortable": false, "fractionable": true},
            "AAPL": {"tradable": true, "shortable": true, "fractionable": true}
        });
        let mut flip = decision_value(2, "sell_notional", "SPY");
        flip["commands"][0]["max_notional_usd"] = json!(150.0);

        let result = apply(&mut authority, flip, flip_snapshot)
            .expect("sign-flip risk violation is an order-level rejection");

        assert!(result.accepted_orders.is_empty());
        assert_eq!(result.rejected_orders[0].reason, "short_borrow_unavailable");
        assert!(result.portfolio.positions["SPY"].quantity > 0.0);
    }

    #[test]
    fn paper_authority_validates_short_to_long_sign_flip() {
        let mut authority = LlmEquityDryRunAuthority::paper();
        authority.portfolio.cash_usd = 10_100.0;
        authority.portfolio.positions.insert(
            "SPY".to_string(),
            LlmEquityPosition {
                symbol: "SPY".to_string(),
                quantity: -0.2,
                avg_price: 500.0,
            },
        );

        let mut flip_snapshot = snapshot_value();
        flip_snapshot["asset_metadata"] = json!({
            "SPY": {"tradable": false, "shortable": true, "fractionable": true},
            "AAPL": {"tradable": true, "shortable": true, "fractionable": true}
        });
        let mut flip = decision_value(2, "buy_notional", "SPY");
        flip["commands"][0]["max_notional_usd"] = json!(150.0);

        let result = apply(&mut authority, flip, flip_snapshot)
            .expect("sign-flip risk violation is an order-level rejection");

        assert!(result.accepted_orders.is_empty());
        assert_eq!(result.rejected_orders[0].reason, "asset_not_tradable");
        assert!(result.portfolio.positions["SPY"].quantity < 0.0);
    }

    #[test]
    fn paper_authority_fills_across_spread_and_market_impact() {
        let mut snapshot = snapshot_value();
        snapshot["asset_metadata"] = json!({
            "SPY": {"tradable": true, "shortable": true, "fractionable": true},
            "AAPL": {"tradable": true, "shortable": true, "fractionable": true}
        });
        let mut decision = decision_value(1, "buy_notional", "SPY");
        decision["commands"][0]["max_notional_usd"] = json!(100.0);

        let result = apply(&mut LlmEquityDryRunAuthority::paper(), decision, snapshot)
            .expect("bounded paper order accepted");

        assert_eq!(result.accepted_orders.len(), 1);
        assert!(result.accepted_orders[0].avg_price > result.prices["SPY"]);
        assert!(result.portfolio.nav_usd(&result.prices) < LLM_EQUITY_INITIAL_CASH_USD);
    }

    #[test]
    fn paper_authority_enforces_daily_turnover_across_hour_boundaries() {
        let mut authority = LlmEquityDryRunAuthority::paper();
        authority.turnover_today_usd = 14_950.0;
        authority.turnover_window =
            Some((NaiveDate::from_ymd_opt(2026, 5, 28).expect("valid date"), 9));
        authority.portfolio_day = Some(NaiveDate::from_ymd_opt(2026, 5, 28).expect("valid date"));
        let mut snapshot = snapshot_value();
        snapshot["asset_metadata"] = json!({
            "SPY": {"tradable": true, "shortable": true, "fractionable": true},
            "AAPL": {"tradable": true, "shortable": true, "fractionable": true}
        });
        let mut decision = decision_value(1, "buy_notional", "SPY");
        decision["commands"][0]["max_notional_usd"] = json!(100.0);

        let result = apply(&mut authority, decision, snapshot)
            .expect("daily turnover breach is an order-level rejection");

        assert!(result.accepted_orders.is_empty());
        assert_eq!(result.rejected_orders[0].reason, "turnover_limit");
        assert_eq!(authority.portfolio.turnover_this_hour_usd, 0.0);
        assert_eq!(authority.turnover_today_usd, 14_950.0);
    }

    #[test]
    fn paper_authority_rejects_fill_when_only_a_fallback_bar_mark_exists() {
        let mut snapshot = snapshot_value();
        snapshot["asset_metadata"] = json!({
            "SPY": {"tradable": true, "shortable": true, "fractionable": true},
            "AAPL": {"tradable": true, "shortable": true, "fractionable": true}
        });
        snapshot["latest_quotes"]["SPY"]["bid"] = json!(0.0);
        snapshot["latest_quotes"]["SPY"]["ask"] = json!(0.0);
        snapshot["latest_quotes"]["SPY"]["source"] = json!("alpaca_market_data_unavailable");
        let mut decision = decision_value(1, "buy_notional", "SPY");
        decision["commands"][0]["max_notional_usd"] = json!(100.0);

        let result = apply(&mut LlmEquityDryRunAuthority::paper(), decision, snapshot)
            .expect("fallback marks may value a holding but may never price a fill");

        assert!(result.accepted_orders.is_empty());
        assert_eq!(
            result.rejected_orders[0].reason,
            "executable_quote_unavailable"
        );
    }

    #[test]
    fn paper_authority_requires_fresh_marks_for_every_existing_holding() {
        let mut authority = LlmEquityDryRunAuthority::paper();
        let mut first_snapshot = snapshot_value();
        first_snapshot["asset_metadata"] = json!({
            "SPY": {"tradable": true, "shortable": true, "fractionable": true},
            "AAPL": {"tradable": true, "shortable": true, "fractionable": true}
        });
        let mut first_decision = decision_value(1, "buy_notional", "SPY");
        first_decision["commands"][0]["max_notional_usd"] = json!(100.0);
        apply(&mut authority, first_decision, first_snapshot).expect("initial paper fill");

        let mut partial_snapshot = snapshot_value();
        partial_snapshot["symbols"] = json!(["AAPL"]);
        partial_snapshot["latest_bars"]
            .as_object_mut()
            .expect("bars object")
            .remove("SPY");
        partial_snapshot["latest_quotes"]
            .as_object_mut()
            .expect("quotes object")
            .remove("SPY");
        partial_snapshot["asset_metadata"] = json!({
            "AAPL": {"tradable": true, "shortable": true, "fractionable": true}
        });
        let mut second_decision = decision_value(2, "buy_notional", "AAPL");
        second_decision["commands"][0]["max_notional_usd"] = json!(100.0);

        let result = apply(&mut authority, second_decision, partial_snapshot)
            .expect("missing held mark is an order-level rejection");

        assert!(result.accepted_orders.is_empty());
        assert_eq!(
            result.rejected_orders[0].reason,
            "held_position_price_missing"
        );
    }

    #[test]
    fn authority_state_round_trip_preserves_portfolio_and_replay_guard() {
        let path = std::env::temp_dir().join(format!(
            "traidan-authority-{}-{}.json",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let mut snapshot = snapshot_value();
        snapshot["asset_metadata"] = json!({
            "SPY": {"tradable": true, "shortable": true, "fractionable": true},
            "AAPL": {"tradable": true, "shortable": true, "fractionable": true}
        });
        let decision = {
            let mut value = decision_value(1, "buy_notional", "SPY");
            value["commands"][0]["max_notional_usd"] = json!(100.0);
            value
        };
        let mut authority = LlmEquityDryRunAuthority::paper();
        apply(&mut authority, decision.clone(), snapshot.clone()).expect("paper fill");
        authority.persist_to_path(&path).expect("state persisted");

        let mut restored = LlmEquityDryRunAuthority::restore_from_path(&path, true, true, true)
            .expect("state restored");

        assert_eq!(restored.portfolio.cash_usd, authority.portfolio.cash_usd);
        assert_eq!(restored.last_prices, authority.last_prices);
        assert_eq!(restored.last_prices.get("SPY"), Some(&500.0));
        assert!(
            (restored.portfolio.positions["SPY"].quantity
                - authority.portfolio.positions["SPY"].quantity)
                .abs()
                < 1e-12
        );
        let replay = apply(&mut restored, decision, snapshot).expect_err("replay remains blocked");
        assert_eq!(
            replay,
            LlmEquityAuthorityError::Replay("decision_sequence_replay")
        );
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("jsonl"));
    }

    #[test]
    fn runtime_service_status_can_publish_snapshot_before_cycle() {
        let state = LlmEquityRuntimeState::new();
        assert_eq!(
            state.snapshot().detail,
            "ממתין לסריקת GPT-6 Astra xhigh הראשונה למניות."
        );

        state
            .update_service_status(
                "llm_review_in_progress".to_string(),
                "סריקת GPT-6 Astra xhigh למניות פועלת.".to_string(),
                Some(snapshot_value()),
                None,
            )
            .expect("service status accepted");

        let snapshot = state.snapshot();
        assert_eq!(snapshot.status, "llm_review_in_progress");
        assert_eq!(snapshot.last_cycle_id, None);
        assert!(snapshot.latest_market_snapshot.is_some());
        assert!(snapshot.service_status_has_market_snapshot);
        assert_eq!(snapshot.prices["SPY"], 500.0);

        state
            .update_service_status(
                "llm_research_only".to_string(),
                "Quiet scan completed without a new market snapshot.".to_string(),
                None,
                None,
            )
            .expect("status-only update accepted");

        let quiet_snapshot = state.snapshot();
        assert!(!quiet_snapshot.service_status_has_market_snapshot);
        assert!(quiet_snapshot.latest_market_snapshot.is_some());
    }

    #[test]
    fn runtime_service_status_rejects_a_mismatched_market_session() {
        let state = LlmEquityRuntimeState::new();

        let error = state
            .update_service_status_with_metadata(
                "llm_review_in_progress".to_string(),
                "Production-shaped snapshot pending review.".to_string(),
                Some(snapshot_value()),
                None,
                LlmEquityServiceStatusMetadata {
                    market_session: Some("closed".to_string()),
                    ..LlmEquityServiceStatusMetadata::default()
                },
            )
            .expect_err("status session must match the attached snapshot");

        assert_eq!(
            error,
            LlmEquityAuthorityError::Contract("market_session_mismatch")
        );
    }

    #[test]
    fn runtime_service_status_with_identity_preserves_legacy_signature() {
        let state = LlmEquityRuntimeState::new();

        state
            .update_service_status_with_identity(
                "llm_scheduler_waiting".to_string(),
                "Waiting for the next scheduled scan.".to_string(),
                None,
                None,
                Some(json!({"git_sha": "abc123"})),
            )
            .expect("legacy identity signature remains accepted");

        let snapshot = state.snapshot();
        assert_eq!(snapshot.status, "llm_scheduler_waiting");
        assert_eq!(
            snapshot.service_identity,
            Some(json!({"git_sha": "abc123"}))
        );
    }

    #[test]
    fn runtime_service_activity_counts_survive_intervening_statuses() {
        let state = LlmEquityRuntimeState::new();

        for key in [
            Some("news-scan:stable"),
            None,
            Some("edge-gate:other"),
            Some("news-scan:stable"),
        ] {
            state
                .update_service_status_with_metadata(
                    "llm_research_only".to_string(),
                    "Research activity completed.".to_string(),
                    None,
                    None,
                    LlmEquityServiceStatusMetadata {
                        activity_key: key.map(ToString::to_string),
                        ..LlmEquityServiceStatusMetadata::default()
                    },
                )
                .expect("activity status accepted");
        }

        let snapshot = state.snapshot();
        assert_eq!(
            snapshot.service_activity_key.as_deref(),
            Some("news-scan:stable")
        );
        assert_eq!(snapshot.service_activity_repeat_count, 2);
    }

    #[test]
    fn runtime_and_cycle_accept_production_snapshot_extension_keys() {
        let mut enriched = snapshot_value();
        enriched["session_bars"] = enriched["latest_bars"].clone();
        enriched["previous_daily_bars"] = enriched["latest_bars"].clone();
        enriched["discovery_health"] = json!({
            "assets": {"status": "fresh", "count": 2}
        });
        let state = LlmEquityRuntimeState::new();

        state
            .update_service_status(
                "llm_review_in_progress".to_string(),
                "Production-shaped snapshot accepted.".to_string(),
                Some(enriched.clone()),
                None,
            )
            .expect("service status accepts Python snapshot keys");

        let sanitized = state
            .snapshot()
            .latest_market_snapshot
            .expect("sanitized snapshot retained");
        assert!(sanitized.get("session_bars").is_some());
        assert!(sanitized.get("previous_daily_bars").is_some());
        assert!(sanitized.get("discovery_health").is_some());
        assert!(sanitized.get("_session_bars").is_none());
        apply(
            &mut LlmEquityDryRunAuthority::shadow(),
            decision_value(1, "buy_notional", "SPY"),
            enriched,
        )
        .expect("cycle accepts production-shaped snapshot keys");
    }

    #[test]
    fn runtime_portfolio_history_keeps_latest_snapshot_per_day() {
        let state =
            LlmEquityRuntimeState::with_authority(LlmEquityDryRunAuthority::seeded(), None, None);
        let first_at = parse_utc("2026-05-28T14:30:00Z").expect("first timestamp");
        let same_day_at = first_at + Duration::hours(1);
        let next_day_at = first_at + Duration::days(1);

        let mut first_snapshot = snapshot_value();
        set_spy_mid(&mut first_snapshot, 500.0);
        state
            .update_service_status_for_test(
                "llm_review_in_progress".to_string(),
                "First daily market mark.".to_string(),
                Some(first_snapshot),
                None,
                first_at,
            )
            .expect("first status accepted");

        let mut same_day_snapshot = snapshot_value();
        set_spy_mid(&mut same_day_snapshot, 501.0);
        state
            .update_service_status_for_test(
                "llm_review_in_progress".to_string(),
                "Later same-day market mark.".to_string(),
                Some(same_day_snapshot),
                None,
                same_day_at,
            )
            .expect("same-day status accepted");

        let mut next_day_snapshot = snapshot_value();
        set_spy_mid(&mut next_day_snapshot, 502.0);
        state
            .update_service_status_for_test(
                "llm_review_in_progress".to_string(),
                "Next-day market mark.".to_string(),
                Some(next_day_snapshot),
                None,
                next_day_at,
            )
            .expect("next-day status accepted");

        let snapshot = state.snapshot();
        assert_eq!(snapshot.portfolio_history.len(), 2);
        assert_eq!(snapshot.portfolio_history[0].updated_at, same_day_at);
        assert_eq!(snapshot.portfolio_history[0].prices["SPY"], 501.0);
        assert_eq!(snapshot.portfolio_history[1].updated_at, next_day_at);
        assert_eq!(snapshot.portfolio_history[1].prices["SPY"], 502.0);
    }

    #[test]
    fn runtime_service_status_preserves_spy_for_partial_market_snapshots() {
        let state =
            LlmEquityRuntimeState::with_authority(LlmEquityDryRunAuthority::seeded(), None, None);
        let first_at = parse_utc("2026-05-28T14:30:00Z").expect("first timestamp");
        let later_at = first_at + Duration::days(1);

        state
            .update_service_status_for_test(
                "llm_review_in_progress".to_string(),
                "Initial market mark with SPY.".to_string(),
                Some(snapshot_value()),
                None,
                first_at,
            )
            .expect("initial status accepted");

        let mut partial_snapshot = snapshot_value();
        partial_snapshot["symbols"] = json!(["AAPL"]);
        for field in ["latest_bars", "latest_quotes", "trend_summaries"] {
            partial_snapshot[field]
                .as_object_mut()
                .expect("snapshot object")
                .remove("SPY");
        }
        state
            .update_service_status_for_test(
                "llm_research_only".to_string(),
                "Partial market mark without SPY.".to_string(),
                Some(partial_snapshot),
                None,
                later_at,
            )
            .expect("partial status accepted");

        let snapshot = state.snapshot();
        assert_eq!(snapshot.prices["SPY"], 500.0);
        assert_eq!(snapshot.prices["AAPL"], 200.0);
        assert_eq!(snapshot.portfolio_history.len(), 2);
        assert_eq!(snapshot.portfolio_history[1].prices["SPY"], 500.0);
    }

    #[test]
    fn runtime_service_status_preserves_all_held_marks_for_partial_market_snapshots() {
        let mut authority = LlmEquityDryRunAuthority::seeded();
        authority.portfolio.cash_usd = 8_200.0;
        authority.portfolio.positions.insert(
            "SPY".to_string(),
            LlmEquityPosition {
                symbol: "SPY".to_string(),
                quantity: 2.0,
                avg_price: 500.0,
            },
        );
        authority.portfolio.positions.insert(
            "QQQ".to_string(),
            LlmEquityPosition {
                symbol: "QQQ".to_string(),
                quantity: 3.0,
                avg_price: 300.0,
            },
        );
        authority.portfolio.positions.insert(
            "AAPL".to_string(),
            LlmEquityPosition {
                symbol: "AAPL".to_string(),
                quantity: 1.0,
                avg_price: 200.0,
            },
        );
        authority.last_prices.insert("SPY".to_string(), 500.0);
        authority.last_prices.insert("QQQ".to_string(), 300.0);
        authority.last_prices.insert("AAPL".to_string(), 200.0);
        let state = LlmEquityRuntimeState::with_authority(authority, None, None);
        let later_at = parse_utc("2026-05-29T14:30:00Z").expect("later timestamp");

        let mut partial_snapshot = snapshot_value();
        partial_snapshot["symbols"] = json!(["AAPL"]);
        set_aapl_mid(&mut partial_snapshot, 210.0);
        for field in ["latest_bars", "latest_quotes", "trend_summaries"] {
            partial_snapshot[field]
                .as_object_mut()
                .expect("snapshot object")
                .remove("SPY");
        }

        state
            .update_service_status_for_test(
                "llm_research_only".to_string(),
                "Partial market mark for AAPL only.".to_string(),
                Some(partial_snapshot),
                None,
                later_at,
            )
            .expect("partial status accepted");

        let snapshot = state.snapshot();
        assert_eq!(snapshot.prices["AAPL"], 210.0);
        assert_eq!(snapshot.prices["SPY"], 500.0);
        assert_eq!(snapshot.prices["QQQ"], 300.0);
        assert_eq!(snapshot.portfolio.nav_usd(&snapshot.prices), 10_310.0);
        assert_eq!(snapshot.portfolio_history[0].prices["QQQ"], 300.0);
    }

    #[test]
    fn rust_authority_rejects_replayed_sequence_before_mutation() {
        let mut authority = LlmEquityDryRunAuthority::seeded();
        let first = apply(
            &mut authority,
            decision_value(1, "buy_notional", "SPY"),
            snapshot_value(),
        )
        .expect("first decision accepted");
        assert_eq!(first.portfolio.cash_usd, 9000.0);

        let replay = apply(
            &mut authority,
            decision_value(1, "buy_notional", "SPY"),
            snapshot_value(),
        )
        .expect_err("same sequence must be rejected");
        assert_eq!(
            replay,
            LlmEquityAuthorityError::Replay("decision_sequence_replay")
        );
        assert_eq!(authority.portfolio.cash_usd, 9000.0);
        assert_eq!(authority.portfolio.positions["SPY"].quantity, 2.0);
    }

    #[test]
    fn rust_authority_rejects_unknown_and_raw_payload_fields() {
        let mut unknown_decision = decision_value(1, "buy_notional", "SPY");
        unknown_decision["debug_extra"] = json!("not part of the contract");
        let rejected = apply(
            &mut LlmEquityDryRunAuthority::seeded(),
            unknown_decision,
            snapshot_value(),
        )
        .expect_err("unknown fields must fail before telemetry storage");
        assert!(matches!(
            rejected,
            LlmEquityAuthorityError::MalformedDecision(_)
        ));

        let mut raw_snapshot = snapshot_value();
        raw_snapshot["latest_quotes"]["SPY"]["full_text"] = json!("forbidden article text");
        let rejected = apply(
            &mut LlmEquityDryRunAuthority::seeded(),
            decision_value(1, "buy_notional", "SPY"),
            raw_snapshot,
        )
        .expect_err("raw fields must fail before telemetry storage");
        assert_eq!(
            rejected,
            LlmEquityAuthorityError::Contract("forbidden_raw_or_credential_field")
        );

        let mut credential_decision = decision_value(1, "buy_notional", "SPY");
        credential_decision["evidence"][0]["summary"] = json!("api_key = abc123");
        let rejected = apply(
            &mut LlmEquityDryRunAuthority::seeded(),
            credential_decision,
            snapshot_value(),
        )
        .expect_err("credential-like text must fail before telemetry storage");
        assert_eq!(
            rejected,
            LlmEquityAuthorityError::Contract("credential_like_text")
        );

        let mut benign_secret_incident = decision_value(1, "buy_notional", "SPY");
        benign_secret_incident["evidence"][0]["summary"] =
            json!("Tokenization risk: company rotated access controls after an incident.");
        apply(
            &mut LlmEquityDryRunAuthority::seeded(),
            benign_secret_incident,
            snapshot_value(),
        )
        .expect("benign incident prose should not match credential assignment");

        let mut password_snapshot = snapshot_value();
        password_snapshot["latest_quotes"]["SPY"]["password"] = json!("hidden");
        let rejected = apply(
            &mut LlmEquityDryRunAuthority::seeded(),
            decision_value(1, "buy_notional", "SPY"),
            password_snapshot,
        )
        .expect_err("password fields must fail before telemetry storage");
        assert_eq!(
            rejected,
            LlmEquityAuthorityError::Contract("forbidden_raw_or_credential_field")
        );
    }

    #[test]
    fn rust_authority_derives_session_and_staleness_from_timestamps() {
        let mut after_hours_decision = decision_value(1, "buy_notional", "SPY");
        after_hours_decision["as_of"] = json!("2026-05-28T13:01:00Z");
        after_hours_decision["market_session"] = json!("rth");
        let mut after_hours_snapshot = snapshot_value();
        after_hours_snapshot["as_of"] = json!("2026-05-28T13:01:00Z");
        after_hours_snapshot["market_session"] = json!("rth");
        after_hours_snapshot["latest_bars"]["SPY"]["timestamp"] = json!("2026-05-28T13:00:00Z");
        after_hours_snapshot["latest_quotes"]["SPY"]["timestamp"] = json!("2026-05-28T13:00:59Z");
        after_hours_snapshot["latest_bars"]["AAPL"]["timestamp"] = json!("2026-05-28T13:00:00Z");
        after_hours_snapshot["latest_quotes"]["AAPL"]["timestamp"] = json!("2026-05-28T13:00:59Z");

        let rejected = apply(
            &mut LlmEquityDryRunAuthority::seeded(),
            after_hours_decision,
            after_hours_snapshot,
        )
        .expect("session mismatch is a command rejection");
        assert_eq!(
            rejected.rejected_orders[0].reason,
            "orders_allowed_only_during_rth"
        );

        let mut stale_snapshot = snapshot_value();
        stale_snapshot["latest_quotes"]["SPY"]["timestamp"] = json!("2026-05-28T14:00:00Z");
        let stale = apply(
            &mut LlmEquityDryRunAuthority::seeded(),
            decision_value(1, "buy_notional", "SPY"),
            stale_snapshot,
        )
        .expect("stale data is a command rejection");
        assert_eq!(stale.rejected_orders[0].reason, "stale_market_data");
    }

    #[test]
    fn rust_authority_accepts_dynamic_symbols_and_unbounded_notional() {
        let mut authority = LlmEquityDryRunAuthority::seeded();
        let mut allowed = decision_value(1, "buy_notional", "SPY");
        allowed["commands"][0]["max_notional_usd"] = json!(50_000.0);

        let result = apply(&mut authority, allowed, snapshot_value())
            .expect("autonomous notional is accepted when operational gates pass");

        assert_eq!(result.accepted_orders[0].notional_usd, 50_000.0);
        assert_eq!(result.portfolio.positions["SPY"].quantity, 100.0);
        assert_eq!(result.portfolio.cash_usd, -40_000.0);

        let mut dynamic_symbol = decision_value(2, "sell_notional", "SPY");
        dynamic_symbol["commands"][0]["symbol"] = json!("ZZZZZ");
        dynamic_symbol["commands"][0]["max_notional_usd"] = json!(25_000.0);
        let result = apply(
            &mut authority,
            dynamic_symbol,
            snapshot_with_symbol("ZZZZZ", 50.0),
        )
        .expect("researched dynamic symbol is accepted");

        assert_eq!(result.accepted_orders[0].symbol, "ZZZZZ");
        assert_eq!(result.accepted_orders[0].notional_usd, 25_000.0);
        assert_eq!(result.portfolio.positions["ZZZZZ"].quantity, -500.0);
    }

    #[test]
    fn rust_authority_rejects_unpriced_dynamic_symbol_as_order_gate() {
        let mut decision = decision_value(1, "buy_notional", "SPY");
        decision["commands"][0]["symbol"] = json!("ZZZZZ");

        let result = apply(
            &mut LlmEquityDryRunAuthority::seeded(),
            decision,
            snapshot_value(),
        )
        .expect("missing dynamic symbol price is an order rejection");

        assert!(result.accepted_orders.is_empty());
        assert_eq!(result.rejected_orders.len(), 1);
        assert_eq!(result.rejected_orders[0].symbol, "ZZZZZ");
        assert_eq!(result.rejected_orders[0].reason, "market_price_unavailable");
    }

    #[test]
    fn rust_authority_forces_hold_when_required_context_rows_are_missing() {
        let mut missing_clock_freshness = snapshot_value();
        missing_clock_freshness["source_freshness"] = json!([
            {"source": "alpaca_market_data", "freshness": "fresh"},
            {"source": "macro_calendar", "freshness": "fresh"},
            {"source": "news_summary", "freshness": "fresh"}
        ]);
        let result = apply(
            &mut LlmEquityDryRunAuthority::seeded(),
            decision_value(1, "buy_notional", "SPY"),
            missing_clock_freshness,
        )
        .expect("missing clock freshness row is a command rejection");
        assert_eq!(
            result.rejected_orders[0].reason,
            "stale_research_requires_hold"
        );

        let mut missing_news_summary = snapshot_value();
        missing_news_summary["macro_news_summaries"] = json!([{
            "source": "macro_calendar",
            "url": "https://example.com/calendar",
            "retrieved_at": "2026-05-28T14:20:00Z",
            "summary": "אין אירוע מתוכנן משמעותי בשעה הקרובה.",
            "freshness": "fresh"
        }]);
        let result = apply(
            &mut LlmEquityDryRunAuthority::seeded(),
            decision_value(1, "buy_notional", "SPY"),
            missing_news_summary,
        )
        .expect("missing news summary row is a command rejection");
        assert_eq!(
            result.rejected_orders[0].reason,
            "stale_research_requires_hold"
        );
    }

    #[test]
    fn rust_authority_rejects_orders_when_authoritative_clock_is_closed() {
        let mut closed = snapshot_value();
        closed["market_calendar_state"]["is_open"] = json!(false);

        let result = apply(
            &mut LlmEquityDryRunAuthority::seeded(),
            decision_value(1, "buy_notional", "SPY"),
            closed,
        )
        .expect("closed authoritative clock is an order-level rejection");

        assert!(result.accepted_orders.is_empty());
        assert_eq!(result.rejected_orders[0].reason, "market_clock_closed");
    }

    #[test]
    fn rust_authority_does_not_mark_positions_with_unavailable_placeholder_quotes() {
        let mut authority = LlmEquityDryRunAuthority::seeded();
        let first = apply(
            &mut authority,
            decision_value(1, "buy_notional", "SPY"),
            snapshot_value(),
        )
        .expect("first buy accepted");
        assert_eq!(first.portfolio.positions["SPY"].quantity, 2.0);

        let mut unavailable_snapshot = snapshot_value();
        unavailable_snapshot["source_freshness"][0]["freshness"] = json!("unavailable");
        unavailable_snapshot["unavailable_data_flags"] = json!(["alpaca_market_data"]);
        unavailable_snapshot["latest_quotes"]["SPY"]["bid"] = json!(0.0);
        unavailable_snapshot["latest_quotes"]["SPY"]["ask"] = json!(0.0);
        unavailable_snapshot["latest_quotes"]["SPY"]["source"] =
            json!("alpaca_market_data_unavailable");
        unavailable_snapshot["latest_quotes"]["AAPL"]["bid"] = json!(0.0);
        unavailable_snapshot["latest_quotes"]["AAPL"]["ask"] = json!(0.0);
        unavailable_snapshot["latest_quotes"]["AAPL"]["source"] =
            json!("alpaca_market_data_unavailable");
        unavailable_snapshot["latest_bars"]["SPY"]["source"] =
            json!("alpaca_market_data_unavailable");
        unavailable_snapshot["latest_bars"]["AAPL"]["source"] =
            json!("alpaca_market_data_unavailable");

        let result = apply(
            &mut authority,
            decision_value(2, "buy_notional", "SPY"),
            unavailable_snapshot,
        )
        .expect("unavailable data rejects command without fabricating marks");

        assert_eq!(
            result.rejected_orders[0].reason,
            "stale_research_requires_hold"
        );
        assert!(!result.prices.contains_key("SPY"));
        assert_eq!(result.rejected_orders[0].avg_price, 0.0);
        assert_eq!(result.portfolio.nav_usd(&result.prices), 9_000.0);
    }

    #[test]
    fn rust_authority_uses_bar_close_when_quote_unavailable_for_marks() {
        let mut authority = LlmEquityDryRunAuthority::seeded();
        apply(
            &mut authority,
            decision_value(1, "buy_notional", "SPY"),
            snapshot_value(),
        )
        .expect("first buy accepted");

        let mut partial_snapshot = snapshot_value();
        partial_snapshot["source_freshness"][0]["freshness"] = json!("unavailable");
        partial_snapshot["unavailable_data_flags"] = json!(["alpaca_market_data"]);
        partial_snapshot["latest_quotes"]["SPY"]["bid"] = json!(0.0);
        partial_snapshot["latest_quotes"]["SPY"]["ask"] = json!(0.0);
        partial_snapshot["latest_quotes"]["SPY"]["source"] =
            json!("alpaca_market_data_unavailable");

        let result = apply(
            &mut authority,
            decision_value(2, "buy_notional", "SPY"),
            partial_snapshot,
        )
        .expect("unavailable quote rejects command but keeps bar mark");

        assert_eq!(
            result.rejected_orders[0].reason,
            "stale_research_requires_hold"
        );
        assert_eq!(result.prices["SPY"], 500.0);
        assert_eq!(result.rejected_orders[0].avg_price, 500.0);
        assert_eq!(result.portfolio.nav_usd(&result.prices), 10_000.0);
        assert_eq!(result.portfolio.gross_exposure_usd(&result.prices), 1_000.0);
    }

    #[test]
    fn sell_notional_opens_short_and_accounts_portfolio() {
        let mut authority = LlmEquityDryRunAuthority::seeded();

        let result = apply(
            &mut authority,
            decision_value(1, "sell_notional", "SPY"),
            snapshot_value(),
        )
        .expect("short sell accepted");

        assert_eq!(result.accepted_orders[0].side, LlmEquityOrderSide::Sell);
        assert_eq!(result.accepted_orders[0].notional_usd, 1000.0);
        assert_eq!(result.portfolio.cash_usd, 11_000.0);
        assert_eq!(result.portfolio.positions["SPY"].quantity, -2.0);
        assert_eq!(result.portfolio.positions["SPY"].avg_price, 500.0);
        assert_eq!(result.portfolio.nav_usd(&result.prices), 10_000.0);
        assert_eq!(result.portfolio.gross_exposure_usd(&result.prices), 1_000.0);
    }

    #[test]
    fn buy_notional_can_cover_existing_short() {
        let mut authority = LlmEquityDryRunAuthority::seeded();
        apply(
            &mut authority,
            decision_value(1, "sell_notional", "SPY"),
            snapshot_value(),
        )
        .expect("short sell accepted");

        let mut cover = decision_value(2, "buy_notional", "SPY");
        cover["commands"][0]["max_notional_usd"] = json!(500.0);
        let result = apply(&mut authority, cover, snapshot_value()).expect("short cover accepted");

        assert_eq!(result.accepted_orders[0].side, LlmEquityOrderSide::Buy);
        assert_eq!(result.accepted_orders[0].notional_usd, 500.0);
        assert_eq!(result.portfolio.cash_usd, 10_500.0);
        assert_eq!(result.portfolio.positions["SPY"].quantity, -1.0);
        assert_eq!(result.portfolio.nav_usd(&result.prices), 10_000.0);
    }

    #[test]
    fn rust_authority_rejects_non_directional_and_non_positive_commands() {
        let mut hold = decision_value(1, "hold", "SPY");
        hold["commands"][0]["max_notional_usd"] = json!(25.0);
        let rejected = apply(
            &mut LlmEquityDryRunAuthority::seeded(),
            hold,
            snapshot_value(),
        )
        .expect_err("hold is no longer a valid command");
        assert_eq!(
            rejected,
            LlmEquityAuthorityError::Contract("action_must_buy_or_sell")
        );

        let mut zero_notional = decision_value(1, "buy_notional", "SPY");
        zero_notional["commands"][0]["max_notional_usd"] = json!(0.0);
        let rejected = apply(
            &mut LlmEquityDryRunAuthority::seeded(),
            zero_notional,
            snapshot_value(),
        )
        .expect_err("zero notional is invalid");

        assert_eq!(
            rejected,
            LlmEquityAuthorityError::Contract("max_notional_invalid")
        );

        let mut too_small_buy = decision_value(1, "buy_notional", "SPY");
        too_small_buy["commands"][0]["max_notional_usd"] = json!(24.99);
        let rejected = apply(
            &mut LlmEquityDryRunAuthority::seeded(),
            too_small_buy,
            snapshot_value(),
        )
        .expect_err("buy_notional below the mandatory cycle minimum is invalid");

        assert_eq!(
            rejected,
            LlmEquityAuthorityError::Contract("mandatory_order_notional_required")
        );

        let mut too_small_sell = decision_value(1, "sell_notional", "SPY");
        too_small_sell["commands"][0]["max_notional_usd"] = json!(24.99);
        let rejected = apply(
            &mut LlmEquityDryRunAuthority::seeded(),
            too_small_sell,
            snapshot_value(),
        )
        .expect_err("sell_notional below the mandatory cycle minimum is invalid");

        assert_eq!(
            rejected,
            LlmEquityAuthorityError::Contract("mandatory_order_notional_required")
        );

        let mut split_too_small = decision_value(1, "buy_notional", "SPY");
        split_too_small["commands"][0]["max_notional_usd"] = json!(12.5);
        split_too_small["commands"]
            .as_array_mut()
            .expect("commands array")
            .push(json!({
                "action": "sell_notional",
                "symbol": "SPY",
                "rationale": "ה-LLM מזהה מגמה מאושרת והקשר מאקרו טרי.",
                "confidence": 0.72,
                "max_notional_usd": 12.5,
                "evidence_refs": ["ev-1"]
            }));
        let rejected = apply(
            &mut LlmEquityDryRunAuthority::seeded(),
            split_too_small,
            snapshot_value(),
        )
        .expect_err("split sub-minimum commands do not satisfy the cycle minimum");

        assert_eq!(
            rejected,
            LlmEquityAuthorityError::Contract("mandatory_order_notional_required")
        );

        let mut mixed_with_minimum = decision_value(1, "buy_notional", "SPY");
        mixed_with_minimum["commands"][0]["max_notional_usd"] = json!(12.5);
        mixed_with_minimum["commands"]
            .as_array_mut()
            .expect("commands array")
            .push(json!({
                "action": "sell_notional",
                "symbol": "SPY",
                "rationale": "ה-LLM מזהה מגמה מאושרת והקשר מאקרו טרי.",
                "confidence": 0.72,
                "max_notional_usd": 25.0,
                "evidence_refs": ["ev-1"]
            }));
        let result = apply(
            &mut LlmEquityDryRunAuthority::seeded(),
            mixed_with_minimum,
            snapshot_value(),
        )
        .expect("one command at the cycle minimum satisfies the mandatory order rule");

        assert_eq!(result.accepted_orders.len(), 2);
        assert_eq!(result.accepted_orders[1].side, LlmEquityOrderSide::Sell);
        assert_eq!(result.accepted_orders[1].notional_usd, 25.0);

        let mut target_weight = decision_value(1, "buy_notional", "SPY");
        target_weight["commands"][0]["target_weight"] = json!(0.1);
        let rejected = apply(
            &mut LlmEquityDryRunAuthority::seeded(),
            target_weight,
            snapshot_value(),
        )
        .expect_err("target_weight is outside the mandatory command contract");
        assert!(matches!(
            rejected,
            LlmEquityAuthorityError::MalformedDecision(_)
        ));
    }
}
