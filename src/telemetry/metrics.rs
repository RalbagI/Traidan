use crate::oms::state::OmsMetrics;
use crate::risk::var_limits::RiskMetrics;
use chrono::Utc;
#[cfg(target_os = "linux")]
use prometheus::process_collector::ProcessCollector;
use prometheus::{
    Encoder, Gauge, GaugeVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
    TextEncoder,
};
use std::env;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

pub const DEFAULT_METRICS_BIND_ADDR: &str = "127.0.0.1:9100";
pub const METRICS_BIND_ADDR_ENV: &str = "TRAIDAN_METRICS_BIND_ADDR";
pub const METRICS_PATH: &str = "/metrics";
pub const METRICS_MAX_REQUEST_BYTES: usize = 8192;
pub const METRICS_READ_TIMEOUT: Duration = Duration::from_secs(5);
const HTTP_DATE_FORMAT: &str = "%a, %d %b %Y %H:%M:%S GMT";

pub const DASHBOARD_WS_MESSAGE_TYPES: [&str; 7] = [
    "llm_equity_service_status",
    "llm_equity_cycle",
    "equity_order_simulated",
    "equity_portfolio_state",
    "market_research_snapshot",
    "llm_memory_update",
    "operator_event",
];

#[derive(Clone)]
pub struct ExecutionMetrics {
    registry: Registry,
    oms_metrics: OmsMetrics,
    risk_metrics: RiskMetrics,
    run_mode: IntGaugeVec,
    live_order_capability_disabled: IntGauge,
    dashboard_ws_connected_clients: IntGauge,
    dashboard_ws_connected_clients_value: Arc<Mutex<i64>>,
    dashboard_ws_messages_total: IntCounterVec,
    dashboard_ws_last_message_timestamp_seconds: Gauge,
    paper_equity_usd: GaugeVec,
    paper_pnl_pct: GaugeVec,
    paper_turnover_usd: GaugeVec,
    paper_max_drawdown_pct: GaugeVec,
    paper_order_total: IntCounterVec,
    llm_cycle_cost_usd: GaugeVec,
    llm_required_alpha_usd: GaugeVec,
    llm_benchmark_nav_usd: GaugeVec,
    llm_net_alpha_usd: GaugeVec,
    llm_source_health: GaugeVec,
    llm_swarm_consensus: Gauge,
    llm_alpha_cards_total: IntCounter,
    llm_edge_gate_total: IntCounterVec,
    llm_strategy_book_nav_usd: GaugeVec,
    llm_strategy_book_alpha_usd: GaugeVec,
}

impl ExecutionMetrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new();
        #[cfg(target_os = "linux")]
        registry.register(Box::new(ProcessCollector::for_self()))?;
        let oms_metrics = OmsMetrics::new_in_registry(registry.clone())?;
        let risk_metrics = RiskMetrics::new_in_registry(registry.clone())?;
        let run_mode = IntGaugeVec::new(
            Opts::new(
                "traidan_run_mode",
                "Execution run mode, 1 for the selected mode and 0 for inactive modes.",
            ),
            &["mode"],
        )?;
        let live_order_capability_disabled = IntGauge::new(
            "traidan_live_order_capability_disabled",
            "Live order capability state, 1 when live order routing is disabled.",
        )?;
        let dashboard_ws_connected_clients = IntGauge::new(
            "traidan_dashboard_ws_connected_clients",
            "Currently connected dashboard WebSocket clients on the JSON stream endpoint.",
        )?;
        let dashboard_ws_connected_clients_value = Arc::new(Mutex::new(0));
        let dashboard_ws_messages_total = IntCounterVec::new(
            Opts::new(
                "traidan_dashboard_ws_messages_total",
                "Total dashboard WebSocket JSON messages sent by message type.",
            ),
            &["message_type"],
        )?;
        let dashboard_ws_last_message_timestamp_seconds = Gauge::new(
            "traidan_dashboard_ws_last_message_timestamp_seconds",
            "Unix timestamp in seconds of the last dashboard WebSocket JSON message sent.",
        )?;
        let paper_equity_usd = GaugeVec::new(
            Opts::new(
                "traidan_paper_equity_usd",
                "Authoritative LLM equity dry-run account equity in USD.",
            ),
            &["symbol"],
        )?;
        let paper_pnl_pct = GaugeVec::new(
            Opts::new(
                "traidan_paper_pnl_pct",
                "Authoritative LLM equity dry-run PnL as a fraction of initial equity.",
            ),
            &["symbol"],
        )?;
        let paper_turnover_usd = GaugeVec::new(
            Opts::new(
                "traidan_paper_turnover_usd",
                "Authoritative LLM equity dry-run notional turnover in USD.",
            ),
            &["symbol"],
        )?;
        let paper_max_drawdown_pct = GaugeVec::new(
            Opts::new(
                "traidan_paper_max_drawdown_pct",
                "Authoritative LLM equity dry-run max drawdown as a fraction of peak equity.",
            ),
            &["symbol"],
        )?;
        let paper_order_total = IntCounterVec::new(
            Opts::new(
                "traidan_paper_order_total",
                "LLM equity dry-run simulated orders by symbol, outcome, and reason.",
            ),
            &["symbol", "outcome", "reason"],
        )?;
        let llm_cycle_cost_usd = GaugeVec::new(
            Opts::new(
                "traidan_llm_cycle_cost_usd",
                "Estimated LLM equity cycle model cost in USD.",
            ),
            &["scope"],
        )?;
        let llm_required_alpha_usd = GaugeVec::new(
            Opts::new(
                "traidan_llm_required_alpha_usd",
                "Required monthly alpha in USD to cover model, data, hosting, execution, and safety-margin costs.",
            ),
            &["scope"],
        )?;
        let llm_benchmark_nav_usd = GaugeVec::new(
            Opts::new(
                "traidan_llm_benchmark_nav_usd",
                "Benchmark NAV in USD for LLM equity profitability comparison.",
            ),
            &["scope"],
        )?;
        let llm_net_alpha_usd = GaugeVec::new(
            Opts::new(
                "traidan_llm_net_alpha_usd",
                "Net alpha in USD after estimated LLM cycle cost versus benchmark.",
            ),
            &["scope"],
        )?;
        let llm_source_health = GaugeVec::new(
            Opts::new(
                "traidan_llm_source_health",
                "Fresh-source ratio for the LLM equity cycle context, from 0 to 1.",
            ),
            &["scope"],
        )?;
        let llm_swarm_consensus = Gauge::new(
            "traidan_llm_swarm_consensus",
            "Latest LLM alpha swarm consensus score, from 0 to 1.",
        )?;
        let llm_alpha_cards_total = IntCounter::new(
            "traidan_llm_alpha_cards_total",
            "Total scrubbed LLM alpha cards evaluated by terminal paper cycles.",
        )?;
        let llm_edge_gate_total = IntCounterVec::new(
            Opts::new(
                "traidan_llm_edge_gate_total",
                "LLM alpha swarm edge gate terminal outcomes by status and reason.",
            ),
            &["status", "reason"],
        )?;
        let llm_strategy_book_nav_usd = GaugeVec::new(
            Opts::new(
                "traidan_llm_strategy_book_nav_usd",
                "Sidecar LLM paper strategy lab virtual book NAV in USD.",
            ),
            &["book"],
        )?;
        let llm_strategy_book_alpha_usd = GaugeVec::new(
            Opts::new(
                "traidan_llm_strategy_book_alpha_usd",
                "Sidecar LLM paper strategy lab virtual book alpha in USD.",
            ),
            &["book"],
        )?;

        for collector in [
            Box::new(run_mode.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(live_order_capability_disabled.clone()),
            Box::new(dashboard_ws_connected_clients.clone()),
            Box::new(dashboard_ws_messages_total.clone()),
            Box::new(dashboard_ws_last_message_timestamp_seconds.clone()),
            Box::new(paper_equity_usd.clone()),
            Box::new(paper_pnl_pct.clone()),
            Box::new(paper_turnover_usd.clone()),
            Box::new(paper_max_drawdown_pct.clone()),
            Box::new(paper_order_total.clone()),
            Box::new(llm_cycle_cost_usd.clone()),
            Box::new(llm_required_alpha_usd.clone()),
            Box::new(llm_benchmark_nav_usd.clone()),
            Box::new(llm_net_alpha_usd.clone()),
            Box::new(llm_source_health.clone()),
            Box::new(llm_swarm_consensus.clone()),
            Box::new(llm_alpha_cards_total.clone()),
            Box::new(llm_edge_gate_total.clone()),
            Box::new(llm_strategy_book_nav_usd.clone()),
            Box::new(llm_strategy_book_alpha_usd.clone()),
        ] {
            registry.register(collector)?;
        }

        for message_type in DASHBOARD_WS_MESSAGE_TYPES {
            dashboard_ws_messages_total
                .with_label_values(&[message_type])
                .reset();
        }

        Ok(Self {
            registry,
            oms_metrics,
            risk_metrics,
            run_mode,
            live_order_capability_disabled,
            dashboard_ws_connected_clients,
            dashboard_ws_connected_clients_value,
            dashboard_ws_messages_total,
            dashboard_ws_last_message_timestamp_seconds,
            paper_equity_usd,
            paper_pnl_pct,
            paper_turnover_usd,
            paper_max_drawdown_pct,
            paper_order_total,
            llm_cycle_cost_usd,
            llm_required_alpha_usd,
            llm_benchmark_nav_usd,
            llm_net_alpha_usd,
            llm_source_health,
            llm_swarm_consensus,
            llm_alpha_cards_total,
            llm_edge_gate_total,
            llm_strategy_book_nav_usd,
            llm_strategy_book_alpha_usd,
        })
    }

    pub fn set_run_mode(&self, mode: &str, live_order_capability_disabled: bool) {
        self.run_mode.with_label_values(&["dry_run"]).set(0);
        self.run_mode.with_label_values(&["live"]).set(0);
        self.run_mode.with_label_values(&[mode]).set(1);
        self.live_order_capability_disabled
            .set(i64::from(live_order_capability_disabled));
    }

    pub fn oms_metrics(&self) -> &OmsMetrics {
        &self.oms_metrics
    }

    pub fn risk_metrics(&self) -> &RiskMetrics {
        &self.risk_metrics
    }

    pub fn record_dashboard_ws_client_connected(&self) {
        let mut current = self
            .dashboard_ws_connected_clients_value
            .lock()
            .expect("dashboard client gauge lock poisoned");
        *current += 1;
        self.dashboard_ws_connected_clients.set(*current);
    }

    pub fn record_dashboard_ws_client_disconnected(&self) {
        let mut current = self
            .dashboard_ws_connected_clients_value
            .lock()
            .expect("dashboard client gauge lock poisoned");
        *current = (*current - 1).max(0);
        self.dashboard_ws_connected_clients.set(*current);
    }

    pub fn record_dashboard_ws_message(&self, message_type: &str) {
        let label = if DASHBOARD_WS_MESSAGE_TYPES.contains(&message_type) {
            message_type
        } else {
            "operator_event"
        };
        self.dashboard_ws_messages_total
            .with_label_values(&[label])
            .inc();
        self.dashboard_ws_last_message_timestamp_seconds
            .set(Utc::now().timestamp() as f64);
    }

    pub fn set_paper_portfolio_metrics(
        &self,
        symbol: &str,
        equity_usd: f64,
        pnl_pct: f64,
        turnover_usd: f64,
        max_drawdown_pct: f64,
    ) {
        self.paper_equity_usd
            .with_label_values(&[symbol])
            .set(equity_usd);
        self.paper_pnl_pct.with_label_values(&[symbol]).set(pnl_pct);
        self.paper_turnover_usd
            .with_label_values(&[symbol])
            .set(turnover_usd);
        self.paper_max_drawdown_pct
            .with_label_values(&[symbol])
            .set(max_drawdown_pct);
    }

    pub fn record_paper_order(&self, symbol: &str, outcome: &str, reason: &str) {
        self.paper_order_total
            .with_label_values(&[symbol, outcome, reason])
            .inc();
    }

    pub fn set_llm_profitability_metrics(
        &self,
        scope: &str,
        cycle_cost_usd: f64,
        required_alpha_usd: f64,
        benchmark_nav_usd: f64,
        net_alpha_usd: f64,
        source_health_ratio: f64,
    ) {
        self.llm_cycle_cost_usd
            .with_label_values(&[scope])
            .set(cycle_cost_usd);
        self.llm_required_alpha_usd
            .with_label_values(&[scope])
            .set(required_alpha_usd);
        self.llm_benchmark_nav_usd
            .with_label_values(&[scope])
            .set(benchmark_nav_usd);
        self.llm_net_alpha_usd
            .with_label_values(&[scope])
            .set(net_alpha_usd);
        self.llm_source_health
            .with_label_values(&[scope])
            .set(source_health_ratio);
    }

    pub fn set_llm_swarm_consensus(&self, consensus: f64) {
        self.llm_swarm_consensus.set(consensus.clamp(0.0, 1.0));
    }

    pub fn record_llm_alpha_cards(&self, count: u64) {
        self.llm_alpha_cards_total.inc_by(count);
    }

    pub fn record_llm_edge_gate(&self, status: &str, reason: &str) {
        self.llm_edge_gate_total
            .with_label_values(&[status, reason])
            .inc();
    }

    pub fn set_llm_strategy_book_metrics(&self, book: &str, nav_usd: f64, alpha_usd: f64) {
        self.llm_strategy_book_nav_usd
            .with_label_values(&[book])
            .set(nav_usd);
        self.llm_strategy_book_alpha_usd
            .with_label_values(&[book])
            .set(alpha_usd);
    }

    pub fn gather_text(&self) -> Result<String, prometheus::Error> {
        let families = self.registry.gather();
        let mut encoded = Vec::new();
        TextEncoder::new().encode(&families, &mut encoded)?;
        Ok(String::from_utf8_lossy(&encoded).into_owned())
    }
}

impl Default for ExecutionMetrics {
    fn default() -> Self {
        Self::new().expect("execution metrics registry should initialize")
    }
}

pub fn metrics_bind_addr() -> String {
    env::var(METRICS_BIND_ADDR_ENV).unwrap_or_else(|_| DEFAULT_METRICS_BIND_ADDR.to_string())
}

pub async fn serve_metrics(bind_addr: &str, metrics: ExecutionMetrics) -> io::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let metrics = metrics.clone();
        tokio::spawn(async move {
            let _ = serve_metrics_stream(stream, metrics).await;
        });
    }
}

async fn serve_metrics_stream(mut stream: TcpStream, metrics: ExecutionMetrics) -> io::Result<()> {
    let mut request = vec![0_u8; METRICS_MAX_REQUEST_BYTES];
    let bytes_read = timeout(METRICS_READ_TIMEOUT, stream.read(&mut request)).await??;
    let request_text = String::from_utf8_lossy(&request[..bytes_read]);
    let body = if request_text.starts_with("GET /metrics ") {
        metrics
            .gather_text()
            .unwrap_or_else(|error| format!("metrics encode error: {error}\n"))
    } else {
        "not found\n".to_string()
    };
    let status = if request_text.starts_with("GET /metrics ") {
        "200 OK"
    } else {
        "404 Not Found"
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nDate: {}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        Utc::now().format(HTTP_DATE_FORMAT),
        body.len(),
        body
    );
    stream.write_all(response.as_bytes()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_record_equity_dashboard_frames() {
        let metrics = ExecutionMetrics::new().expect("metrics registry");

        metrics.set_run_mode("dry_run", true);
        metrics.record_dashboard_ws_message("llm_equity_service_status");
        metrics.record_dashboard_ws_message("llm_equity_cycle");
        metrics.set_paper_portfolio_metrics("SPY", 10_000.0, 0.0, 0.0, 0.0);
        metrics.record_paper_order("SPY", "accepted", "within_limits");
        metrics.set_llm_profitability_metrics("SPY", 1.25, 250.0, 10_050.0, -51.25, 0.75);
        metrics.set_llm_swarm_consensus(0.72);
        metrics.record_llm_alpha_cards(8);
        metrics.record_llm_edge_gate("pass", "edge_passed");
        metrics.set_llm_strategy_book_metrics("small_consensus", 10_005.0, 5.0);

        let encoded = metrics.gather_text().expect("metrics encode");
        assert!(encoded.contains("traidan_run_mode{mode=\"dry_run\"} 1"));
        assert!(encoded
            .contains("traidan_dashboard_ws_messages_total{message_type=\"llm_equity_cycle\"} 1"));
        assert!(encoded.contains("traidan_paper_equity_usd{symbol=\"SPY\"} 10000"));
        assert!(encoded.contains("traidan_llm_cycle_cost_usd{scope=\"SPY\"} 1.25"));
        assert!(encoded.contains("traidan_llm_required_alpha_usd{scope=\"SPY\"} 250"));
        assert!(encoded.contains("traidan_llm_benchmark_nav_usd{scope=\"SPY\"} 10050"));
        assert!(encoded.contains("traidan_llm_net_alpha_usd{scope=\"SPY\"} -51.25"));
        assert!(encoded.contains("traidan_llm_source_health{scope=\"SPY\"} 0.75"));
        assert!(encoded.contains("traidan_llm_swarm_consensus 0.72"));
        assert!(encoded.contains("traidan_llm_alpha_cards_total 8"));
        assert!(encoded
            .contains("traidan_llm_edge_gate_total{reason=\"edge_passed\",status=\"pass\"} 1"));
        assert!(
            encoded.contains("traidan_llm_strategy_book_nav_usd{book=\"small_consensus\"} 10005")
        );
        assert!(encoded.contains("traidan_llm_strategy_book_alpha_usd{book=\"small_consensus\"} 5"));
    }
}
