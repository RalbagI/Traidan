use super::*;
use crate::disruptor::ring_buffer::{MarketEvent, PublishError, RingBuffer, BUFFER_SIZE};
use crate::market_context::MarketContext;
use crate::oms::state::{Order, PortfolioState, Side};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn pending_order(cl_ord_id: &str) -> Order {
    Order {
        cl_ord_id: cl_ord_id.to_string(),
        symbol: "SPY".to_string(),
        side: Side::Buy,
        qty: 0.1,
        price: Some(50_000.0),
        market_context: MarketContext::default(),
    }
}

async fn request_metrics_once(metrics: RiskMetrics, request: String) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test metrics listener binds");
    let address = listener.local_addr().expect("test listener has address");
    let server = tokio::spawn(async move { serve_risk_metrics_once(listener, metrics).await });

    let mut client = TcpStream::connect(address)
        .await
        .expect("test client connects to metrics endpoint");
    client
        .write_all(request.as_bytes())
        .await
        .expect("test client writes request");
    client
        .shutdown()
        .await
        .expect("test client flushes request");
    let mut response = String::new();
    client
        .read_to_string(&mut response)
        .await
        .expect("test client reads response");

    server
        .await
        .expect("server task joins")
        .expect("server handles one request");

    response
}

#[test]
fn check_var_allows_normal_and_near_threshold_values() {
    let limits = RiskLimits {
        max_portfolio_var_usd: 10_000.0,
        max_drawdown_pct: 0.05,
    };

    assert_eq!(
        check_var_with_limits(4_000.0, 100_000.0, limits),
        VarStatus::Ok
    );
    assert_eq!(
        check_var_with_limits(4_999.0, 100_000.0, limits),
        VarStatus::Ok
    );
}

#[test]
fn check_var_breaches_on_portfolio_var_or_drawdown() {
    let limits = RiskLimits {
        max_portfolio_var_usd: 10_000.0,
        max_drawdown_pct: 0.05,
    };

    assert_eq!(
        check_var_with_limits(10_000.0, 1_000_000.0, limits),
        VarStatus::Breach
    );
    assert_eq!(
        check_var_with_limits(5_000.0, 100_000.0, limits),
        VarStatus::Breach
    );
}

#[test]
fn risk_limits_read_env_overrides() {
    let limits =
        RiskLimits::from_env_values(Some("25000.5"), Some("0.075")).expect("env values parsed");

    assert_eq!(limits.max_portfolio_var_usd, 25_000.5);
    assert_eq!(limits.max_drawdown_pct, 0.075);
}

#[test]
fn risk_limits_reject_non_positive_var_and_out_of_range_drawdown() {
    assert!(matches!(
        RiskLimits::from_env_values(Some("0"), Some("0.05")),
        Err(RiskConfigError::OutOfRange {
            env: RISK_MAX_VAR_USD_ENV,
            ..
        })
    ));
    assert!(matches!(
        RiskLimits::from_env_values(Some("-1"), Some("0.05")),
        Err(RiskConfigError::OutOfRange {
            env: RISK_MAX_VAR_USD_ENV,
            ..
        })
    ));
    assert!(matches!(
        RiskLimits::from_env_values(Some("10000"), Some("0")),
        Err(RiskConfigError::OutOfRange {
            env: RISK_MAX_DRAWDOWN_PCT_ENV,
            ..
        })
    ));
    assert!(matches!(
        RiskLimits::from_env_values(Some("10000"), Some("1.5")),
        Err(RiskConfigError::OutOfRange {
            env: RISK_MAX_DRAWDOWN_PCT_ENV,
            ..
        })
    ));
}

#[test]
fn no_breach_enforcement_leaves_orders_metrics_signals_and_ring_unchanged() {
    let (publisher, dispatcher) = RingBuffer::new().split();
    let mut portfolio = PortfolioState::default();
    portfolio
        .pending_orders
        .insert("order-1".to_string(), pending_order("order-1"));
    let metrics = RiskMetrics::new().expect("risk metrics registered");
    let mut engine = RiskEngine::new(
        RiskLimits {
            max_portfolio_var_usd: 10_000.0,
            max_drawdown_pct: 0.05,
        },
        Duration::from_secs(60),
    );

    let enforcement = engine.check_and_enforce(
        1_000.0,
        100_000.0,
        &mut portfolio,
        &publisher,
        &metrics,
        Instant::now(),
    );

    assert_eq!(
        enforcement,
        RiskEnforcement {
            status: VarStatus::Ok,
            canceled_orders: 0,
            publication: VarBreachPublication::NotRequired,
        }
    );
    assert_eq!(portfolio.pending_orders.len(), 1);
    assert!(!engine.signal_consumption_suspended());
    assert!(dispatcher.try_dispatch().is_none());
    let encoded = metrics.encode();
    assert!(encoded.contains("traidan_var_breach_total 0"));
    assert!(encoded.contains("traidan_var_utilization_ratio 0.1"));
}

#[test]
fn var_breach_cancels_orders_publishes_event_suspends_signals_and_counts_metric() {
    let (publisher, dispatcher) = RingBuffer::new().split();
    let mut portfolio = PortfolioState::default();
    portfolio
        .pending_orders
        .insert("order-1".to_string(), pending_order("order-1"));
    portfolio
        .pending_orders
        .insert("order-2".to_string(), pending_order("order-2"));
    let metrics = RiskMetrics::new().expect("risk metrics registered");
    let mut engine = RiskEngine::new(
        RiskLimits {
            max_portfolio_var_usd: 10_000.0,
            max_drawdown_pct: 0.05,
        },
        Duration::from_secs(60),
    );
    let now = Instant::now();

    let enforcement = engine.check_and_enforce(
        10_500.0,
        1_000_000.0,
        &mut portfolio,
        &publisher,
        &metrics,
        now,
    );

    assert_eq!(enforcement.status, VarStatus::Breach);
    assert_eq!(enforcement.canceled_orders, 2);
    assert!(matches!(
        enforcement.publication,
        VarBreachPublication::Published(0)
    ));
    assert_eq!(portfolio.pending_orders.len(), 0);
    assert!(engine.signal_consumption_suspended());

    let event = dispatcher.try_dispatch().expect("var breach dispatched");
    assert_eq!(event.event, MarketEvent::VarBreach);

    let encoded = metrics.encode();
    assert!(encoded.contains("traidan_var_breach_total 1"));
    assert!(encoded.contains("traidan_var_utilization_ratio 1.05"));
}

#[test]
fn var_breach_ring_backpressure_is_reported_without_double_counting_fail_safe() {
    let (publisher, dispatcher) = RingBuffer::new().split();
    for seq in 0..BUFFER_SIZE as u64 {
        publisher
            .publish_market_depth_update(7, seq)
            .expect("fills ring to simulate backpressure");
    }
    let mut portfolio = PortfolioState::default();
    portfolio
        .pending_orders
        .insert("order-1".to_string(), pending_order("order-1"));
    let metrics = RiskMetrics::new().expect("risk metrics registered");
    let mut engine = RiskEngine::new(
        RiskLimits {
            max_portfolio_var_usd: 10_000.0,
            max_drawdown_pct: 0.05,
        },
        Duration::from_secs(60),
    );
    let now = Instant::now();

    let first = engine.check_and_enforce(
        20_000.0,
        1_000_000.0,
        &mut portfolio,
        &publisher,
        &metrics,
        now,
    );
    let second = engine.check_and_enforce(
        20_000.0,
        1_000_000.0,
        &mut portfolio,
        &publisher,
        &metrics,
        now + Duration::from_secs(1),
    );

    assert_eq!(first.canceled_orders, 1);
    assert_eq!(portfolio.pending_orders.len(), 0);
    assert_eq!(
        first.publication,
        VarBreachPublication::Backpressured(PublishError::Full)
    );
    assert_eq!(second.canceled_orders, 0);
    assert_eq!(
        second.publication,
        VarBreachPublication::Backpressured(PublishError::Full)
    );
    assert!(engine.check_and_maybe_resume_signal_consumption(now + Duration::from_secs(1)));
    assert!(metrics.encode().contains("traidan_var_breach_total 1"));
    assert!(dispatcher.try_dispatch().is_some());
}

#[test]
fn signal_suspension_can_be_manually_reset_or_expire_after_cooldown() {
    let mut engine = RiskEngine::new(
        RiskLimits {
            max_portfolio_var_usd: 10_000.0,
            max_drawdown_pct: 0.05,
        },
        Duration::from_secs(30),
    );
    let now = Instant::now();

    engine.suspend_signal_consumption(now);
    assert!(engine.signal_consumption_suspended());
    assert!(engine.check_and_maybe_resume_signal_consumption(now + Duration::from_secs(29)));
    assert!(!engine.check_and_maybe_resume_signal_consumption(now + Duration::from_secs(30)));

    engine.suspend_signal_consumption(now);
    engine.manual_reset();
    assert!(!engine.signal_consumption_suspended());
}

#[tokio::test]
async fn metrics_endpoint_serves_var_breach_counter() {
    let metrics = RiskMetrics::new().expect("risk metrics registered");
    metrics.record_var_breach();
    let response = request_metrics_once(
        metrics,
        "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n".to_string(),
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("\r\nDate: "));
    assert!(response.contains("Content-Type: text/plain; version=0.0.4"));
    assert!(response.contains("traidan_var_breach_total 1"));
}

#[tokio::test]
async fn metrics_endpoint_accepts_headers_larger_than_one_read_chunk() {
    let metrics = RiskMetrics::new().expect("risk metrics registered");
    metrics.record_var_breach();
    let request = format!(
        "GET /metrics HTTP/1.1\r\nHost: localhost\r\nX-Pad: {}\r\n\r\n",
        "x".repeat(1500)
    );

    let response = request_metrics_once(metrics, request).await;

    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("traidan_var_breach_total 1"));
}

#[tokio::test]
async fn metrics_endpoint_rejects_oversized_request_headers() {
    let metrics = RiskMetrics::new().expect("risk metrics registered");
    let request = format!(
        "GET /metrics HTTP/1.1\r\nHost: localhost\r\nX-Pad: {}\r\n\r\n",
        "x".repeat(RISK_METRICS_MAX_REQUEST_BYTES)
    );

    let response = request_metrics_once(metrics, request).await;

    assert!(response.starts_with("HTTP/1.1 431 Request Header Fields Too Large"));
    assert!(response.contains("request headers too large"));
}
