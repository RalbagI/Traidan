// Hard Value-at-Risk enforcement. Breaching the drawdown limit triggers
// fail-safe: cancel open orders, publish breach events, halt signal consumption.
// See architecture.md §4 — Risk Engine.

use crate::disruptor::ring_buffer::{MarketDataReceiver, PublishError};
use crate::oms::state::PortfolioState;
use chrono::Utc;
use prometheus::{Encoder, Gauge, IntCounter, Registry, TextEncoder};
use std::env;
use std::error::Error;
use std::fmt;
use std::io;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub const MAX_PORTFOLIO_VAR_USD: f64 = 10_000.0;
pub const MAX_DRAWDOWN_PCT: f64 = 0.05;
pub const RISK_MAX_VAR_USD_ENV: &str = "RISK_MAX_VAR_USD";
pub const RISK_MAX_DRAWDOWN_PCT_ENV: &str = "RISK_MAX_DRAWDOWN_PCT";
pub const RISK_METRICS_PATH: &str = "/metrics";
pub const RISK_METRICS_MAX_REQUEST_BYTES: usize = 8192;
const HTTP_DATE_FORMAT: &str = "%a, %d %b %Y %H:%M:%S GMT";

pub fn check_var(current_var_usd: f64, portfolio_usd: f64) -> VarStatus {
    check_var_with_limits(current_var_usd, portfolio_usd, RiskLimits::default())
}

pub fn check_var_from_env(
    current_var_usd: f64,
    portfolio_usd: f64,
) -> Result<VarStatus, RiskConfigError> {
    Ok(check_var_with_limits(
        current_var_usd,
        portfolio_usd,
        RiskLimits::from_env()?,
    ))
}

pub fn check_var_with_limits(
    current_var_usd: f64,
    portfolio_usd: f64,
    limits: RiskLimits,
) -> VarStatus {
    if current_var_usd >= limits.max_portfolio_var_usd {
        return VarStatus::Breach;
    }
    if portfolio_usd <= 0.0 {
        return if current_var_usd > 0.0 {
            VarStatus::Breach
        } else {
            VarStatus::Ok
        };
    }

    let drawdown = current_var_usd / portfolio_usd;
    if drawdown >= limits.max_drawdown_pct {
        VarStatus::Breach
    } else {
        VarStatus::Ok
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RiskLimits {
    pub max_portfolio_var_usd: f64,
    pub max_drawdown_pct: f64,
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            max_portfolio_var_usd: MAX_PORTFOLIO_VAR_USD,
            max_drawdown_pct: MAX_DRAWDOWN_PCT,
        }
    }
}

impl RiskLimits {
    pub fn from_env() -> Result<Self, RiskConfigError> {
        let max_var = env::var(RISK_MAX_VAR_USD_ENV).ok();
        let max_drawdown = env::var(RISK_MAX_DRAWDOWN_PCT_ENV).ok();

        Self::from_env_values(max_var.as_deref(), max_drawdown.as_deref())
    }

    pub fn from_env_values(
        max_var_usd: Option<&str>,
        max_drawdown_pct: Option<&str>,
    ) -> Result<Self, RiskConfigError> {
        let defaults = Self::default();
        let limits = Self {
            max_portfolio_var_usd: parse_env_f64(
                RISK_MAX_VAR_USD_ENV,
                max_var_usd,
                defaults.max_portfolio_var_usd,
            )?,
            max_drawdown_pct: parse_env_f64(
                RISK_MAX_DRAWDOWN_PCT_ENV,
                max_drawdown_pct,
                defaults.max_drawdown_pct,
            )?,
        };
        limits.validate()?;
        Ok(limits)
    }

    fn validate(&self) -> Result<(), RiskConfigError> {
        if self.max_portfolio_var_usd <= 0.0 {
            return Err(RiskConfigError::OutOfRange {
                env: RISK_MAX_VAR_USD_ENV,
                value: self.max_portfolio_var_usd.to_string(),
                expected: "greater than 0",
            });
        }
        if !(self.max_drawdown_pct > 0.0 && self.max_drawdown_pct <= 1.0) {
            return Err(RiskConfigError::OutOfRange {
                env: RISK_MAX_DRAWDOWN_PCT_ENV,
                value: self.max_drawdown_pct.to_string(),
                expected: "greater than 0 and less than or equal to 1",
            });
        }
        Ok(())
    }
}

fn parse_env_f64(
    name: &'static str,
    raw: Option<&str>,
    default: f64,
) -> Result<f64, RiskConfigError> {
    let Some(raw) = raw else {
        return Ok(default);
    };
    let value = raw
        .parse::<f64>()
        .map_err(|_| RiskConfigError::InvalidFloat {
            env: name,
            value: raw.to_string(),
        })?;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(RiskConfigError::InvalidFloat {
            env: name,
            value: raw.to_string(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RiskConfigError {
    InvalidFloat {
        env: &'static str,
        value: String,
    },
    OutOfRange {
        env: &'static str,
        value: String,
        expected: &'static str,
    },
}

impl fmt::Display for RiskConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFloat { env, value } => {
                write!(f, "{env} must be a finite decimal, got {value:?}")
            }
            Self::OutOfRange {
                env,
                value,
                expected,
            } => {
                write!(f, "{env} must be {expected}, got {value}")
            }
        }
    }
}

impl Error for RiskConfigError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VarStatus {
    Ok,
    Breach,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VarBreachPublication {
    NotRequired,
    Published(u64),
    Backpressured(PublishError),
    AlreadyPublished,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RiskEnforcement {
    pub status: VarStatus,
    pub canceled_orders: usize,
    pub publication: VarBreachPublication,
}

pub struct RiskEngine {
    limits: RiskLimits,
    cooldown: Duration,
    suspended_until: Option<Instant>,
    breach_published: bool,
}

impl RiskEngine {
    pub fn new(limits: RiskLimits, cooldown: Duration) -> Self {
        Self {
            limits,
            cooldown,
            suspended_until: None,
            breach_published: false,
        }
    }

    pub fn from_env(cooldown: Duration) -> Result<Self, RiskConfigError> {
        Ok(Self::new(RiskLimits::from_env()?, cooldown))
    }

    pub fn check_var(&self, current_var_usd: f64, portfolio_usd: f64) -> VarStatus {
        check_var_with_limits(current_var_usd, portfolio_usd, self.limits)
    }

    pub fn check_and_enforce(
        &mut self,
        current_var_usd: f64,
        portfolio_usd: f64,
        portfolio: &mut PortfolioState,
        publisher: &MarketDataReceiver,
        metrics: &RiskMetrics,
        now: Instant,
    ) -> RiskEnforcement {
        metrics.observe_var_utilization(current_var_usd, self.limits.max_portfolio_var_usd);
        let status = self.check_var(current_var_usd, portfolio_usd);
        if status != VarStatus::Breach {
            return RiskEnforcement {
                status,
                canceled_orders: 0,
                publication: VarBreachPublication::NotRequired,
            };
        }

        let already_active = self.check_and_maybe_resume_signal_consumption(now);
        let canceled_orders = portfolio.cancel_all_open_orders();
        if !already_active {
            self.suspend_signal_consumption(now);
            self.breach_published = false;
            metrics.record_var_breach();
        }
        let publication = if self.breach_published {
            VarBreachPublication::AlreadyPublished
        } else {
            match publisher.publish_var_breach() {
                Ok(sequence) => {
                    self.breach_published = true;
                    VarBreachPublication::Published(sequence)
                }
                Err(error) => VarBreachPublication::Backpressured(error),
            }
        };

        RiskEnforcement {
            status,
            canceled_orders,
            publication,
        }
    }

    pub fn suspend_signal_consumption(&mut self, now: Instant) {
        self.suspended_until = Some(now + self.cooldown);
    }

    pub fn signal_consumption_suspended(&self) -> bool {
        self.suspended_until.is_some()
    }

    pub fn check_and_maybe_resume_signal_consumption(&mut self, now: Instant) -> bool {
        if self.suspended_until.is_some_and(|until| now >= until) {
            self.suspended_until = None;
            self.breach_published = false;
        }
        self.suspended_until.is_some()
    }

    pub fn manual_reset(&mut self) {
        self.suspended_until = None;
        self.breach_published = false;
    }
}

#[derive(Clone)]
pub struct RiskMetrics {
    registry: Registry,
    var_breach_total: IntCounter,
    var_utilization_ratio: Gauge,
}

impl RiskMetrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        Self::new_in_registry(Registry::new())
    }

    pub fn new_in_registry(registry: Registry) -> Result<Self, prometheus::Error> {
        let var_breach_total = IntCounter::new(
            "traidan_var_breach_total",
            "Total number of VaR breaches that triggered fail-safe handling.",
        )?;
        let var_utilization_ratio = Gauge::new(
            "traidan_var_utilization_ratio",
            "Current Value-at-Risk utilization ratio relative to the configured hard limit.",
        )?;
        registry.register(Box::new(var_breach_total.clone()))?;
        registry.register(Box::new(var_utilization_ratio.clone()))?;

        Ok(Self {
            registry,
            var_breach_total,
            var_utilization_ratio,
        })
    }

    pub fn record_var_breach(&self) {
        self.var_breach_total.inc();
    }

    pub fn observe_var_utilization(&self, current_var_usd: f64, max_var_usd: f64) {
        if current_var_usd.is_finite() && max_var_usd.is_finite() && max_var_usd > 0.0 {
            self.var_utilization_ratio
                .set(current_var_usd.max(0.0) / max_var_usd);
        }
    }

    pub fn encode(&self) -> String {
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&metric_families, &mut buffer)
            .expect("Prometheus text encoding should not fail for in-memory metrics");
        String::from_utf8(buffer).expect("Prometheus text encoder emits UTF-8")
    }
}

/// Handles one `/metrics` HTTP request for tests and embedding.
///
/// Production callers should wrap this in an accept loop with process
/// supervision. Request headers are bounded so a malformed client cannot force
/// unbounded buffering.
pub async fn serve_risk_metrics_once(
    listener: TcpListener,
    metrics: RiskMetrics,
) -> io::Result<()> {
    let (mut stream, _) = listener.accept().await?;
    let mut request = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    let mut request_too_large = false;
    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..read]);
        if request.len() > RISK_METRICS_MAX_REQUEST_BYTES {
            request_too_large = true;
            break;
        }
        if request_headers_complete(&request) {
            break;
        }
    }

    let request_line = request
        .split(|byte| *byte == b'\n')
        .next()
        .unwrap_or_default();
    let metrics_prefix = format!("GET {RISK_METRICS_PATH} ");
    let is_metrics_request = request_line.starts_with(metrics_prefix.as_bytes());
    let (status, content_type, body) = if request_too_large {
        (
            "431 Request Header Fields Too Large",
            "text/plain",
            "request headers too large\n".to_string(),
        )
    } else if is_metrics_request {
        ("200 OK", "text/plain; version=0.0.4", metrics.encode())
    } else {
        ("404 Not Found", "text/plain", "not found\n".to_string())
    };
    let date = http_date_header();
    let response = format!(
        "HTTP/1.1 {status}\r\nDate: {date}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

fn request_headers_complete(request: &[u8]) -> bool {
    request.windows(4).any(|window| window == b"\r\n\r\n")
}

fn http_date_header() -> String {
    Utc::now().format(HTTP_DATE_FORMAT).to_string()
}

#[cfg(test)]
mod tests;
