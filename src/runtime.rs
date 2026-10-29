use crate::llm_equity::{is_valid_llm_equity_symbol, LlmEquityRuntimeState};
use crate::telemetry::metrics::ExecutionMetrics;
use crate::telemetry::ws_broadcast::{default_equity_symbol, EngineStreamFeed};
use std::env;
use std::error::Error;
use std::fmt;

pub const RUN_MODE_ENV: &str = "TRAIDAN_RUN_MODE";
pub const MARKET_DATA_SOURCE_ENV: &str = "TRAIDAN_MARKET_DATA_SOURCE";
pub const MARKET_SYMBOL_ENV: &str = "TRAIDAN_MARKET_SYMBOL";
pub const LIVE_MODE_DISABLED_REASON: &str =
    "live mode is disabled; Traidan is an LLM-only equity dry-run system";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMode {
    DryRun,
    Live,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketDataSource {
    AlpacaEquity,
}

impl RunMode {
    pub fn from_env() -> Result<Self, RunModeError> {
        Self::from_env_value(env::var(RUN_MODE_ENV).ok().as_deref())
    }

    pub fn from_env_value(raw: Option<&str>) -> Result<Self, RunModeError> {
        match raw.map(str::trim) {
            None | Some("") | Some("dry_run") => Ok(Self::DryRun),
            Some("live") => Ok(Self::Live),
            Some(value) => Err(RunModeError {
                value: value.to_string(),
            }),
        }
    }

    pub const fn live_order_capability_disabled(self) -> bool {
        true
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DryRun => "dry_run",
            Self::Live => "live",
        }
    }
}

impl MarketDataSource {
    pub fn from_env() -> Result<Self, MarketDataSourceError> {
        Self::from_env_value(env::var(MARKET_DATA_SOURCE_ENV).ok().as_deref())
    }

    pub fn from_env_value(raw: Option<&str>) -> Result<Self, MarketDataSourceError> {
        match raw.map(str::trim) {
            None | Some("") | Some("alpaca_equity") => Ok(Self::AlpacaEquity),
            Some(value) => Err(MarketDataSourceError {
                value: value.to_string(),
            }),
        }
    }
}

impl fmt::Display for RunMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunModeError {
    pub value: String,
}

impl fmt::Display for RunModeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{RUN_MODE_ENV} must be dry_run, got {:?}", self.value)
    }
}

impl Error for RunModeError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketDataSourceError {
    pub value: String,
}

impl fmt::Display for MarketDataSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{MARKET_DATA_SOURCE_ENV} must be alpaca_equity, got {:?}",
            self.value
        )
    }
}

impl Error for MarketDataSourceError {}

pub trait LiveOrderGatewayFactory {
    fn open_live_order_gateway(&self) -> anyhow::Result<()>;
}

pub struct DisabledLiveOrderGatewayFactory;

impl LiveOrderGatewayFactory for DisabledLiveOrderGatewayFactory {
    fn open_live_order_gateway(&self) -> anyhow::Result<()> {
        Err(anyhow::anyhow!(LIVE_MODE_DISABLED_REASON))
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionStartup {
    pub run_mode: RunMode,
    live_order_capability_disabled: bool,
    websocket_feed: EngineStreamFeed,
    equity_state: LlmEquityRuntimeState,
}

impl ExecutionStartup {
    pub fn prepare(
        run_mode: RunMode,
        _gateway_factory: &impl LiveOrderGatewayFactory,
        metrics: &ExecutionMetrics,
    ) -> anyhow::Result<Self> {
        metrics.set_run_mode(run_mode.as_str(), run_mode.live_order_capability_disabled());
        if run_mode == RunMode::Live {
            anyhow::bail!(LIVE_MODE_DISABLED_REASON);
        }
        MarketDataSource::from_env()?;
        let symbol = market_symbol();
        validate_llm_equity_symbol(&symbol)?;
        let equity_state = LlmEquityRuntimeState::new();
        metrics.set_paper_portfolio_metrics(&symbol, 10_000.0, 0.0, 0.0, 0.0);
        Ok(Self {
            run_mode,
            live_order_capability_disabled: true,
            websocket_feed: EngineStreamFeed::llm_equity(equity_state.clone()),
            equity_state,
        })
    }

    pub fn live_order_capability_disabled(&self) -> bool {
        self.live_order_capability_disabled
    }

    pub fn websocket_feed(&self) -> EngineStreamFeed {
        self.websocket_feed.clone()
    }

    pub fn equity_state(&self) -> LlmEquityRuntimeState {
        self.equity_state.clone()
    }
}

pub fn market_symbol() -> String {
    env::var(MARKET_SYMBOL_ENV).unwrap_or_else(|_| default_equity_symbol().to_string())
}

pub fn validate_llm_equity_symbol(symbol: &str) -> anyhow::Result<()> {
    let trimmed = symbol.trim();
    if is_valid_llm_equity_symbol(trimmed) {
        return Ok(());
    }
    anyhow::bail!("{MARKET_SYMBOL_ENV} must be a valid LLM equity dry-run symbol");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_data_source_defaults_to_alpaca_equity_only() {
        assert_eq!(
            MarketDataSource::from_env_value(None).expect("default source"),
            MarketDataSource::AlpacaEquity
        );
        assert!(MarketDataSource::from_env_value(Some("synthetic")).is_err());
        assert!(MarketDataSource::from_env_value(Some("binance_public")).is_err());
    }

    #[test]
    fn valid_equity_symbols_are_required() {
        assert!(validate_llm_equity_symbol("SPY").is_ok());
        assert!(validate_llm_equity_symbol("ZZZZZ").is_ok());
        assert!(validate_llm_equity_symbol("BRK.B").is_ok());
        assert!(validate_llm_equity_symbol("BAD SYMBOL").is_err());
    }

    #[test]
    fn startup_is_cash_only_equity_feed() {
        let metrics = ExecutionMetrics::new().expect("metrics");
        let startup =
            ExecutionStartup::prepare(RunMode::DryRun, &DisabledLiveOrderGatewayFactory, &metrics)
                .expect("startup");
        let messages = startup.websocket_feed().current_messages();
        let message_types = messages
            .iter()
            .map(crate::telemetry::ws_broadcast::EngineStreamMessage::message_type)
            .collect::<Vec<_>>();

        assert!(message_types.contains(&"llm_equity_service_status"));
        assert!(message_types.contains(&"equity_portfolio_state"));
        assert!(!message_types.contains(&"llm_equity_cycle"));
    }
}
