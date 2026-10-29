use _rust::admin_api::{serve_admin_api, AdminApiConfig, AdminApiState};
use _rust::llm_equity::LlmEquityRuntimeState;
use _rust::runtime::{
    DisabledLiveOrderGatewayFactory, ExecutionStartup, RunMode, LIVE_MODE_DISABLED_REASON,
};
use _rust::telemetry::metrics::{metrics_bind_addr, serve_metrics, ExecutionMetrics};
use _rust::telemetry::ws_broadcast::{
    serve_ws_broadcast_with_feed, ws_bind_addr, EngineStreamFeed,
};
use tokio::task::JoinHandle;
use tracing::{error, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Traidan execution engine starting");

    let run_mode = RunMode::from_env()?;
    let metrics = ExecutionMetrics::new()?;
    let startup_result =
        ExecutionStartup::prepare(run_mode, &DisabledLiveOrderGatewayFactory, &metrics);
    if startup_result.is_err() && run_mode == RunMode::Live {
        error!(
            run_mode = %run_mode,
            live_order_capability_disabled = run_mode.live_order_capability_disabled(),
            reason = LIVE_MODE_DISABLED_REASON,
            "Traidan live execution startup blocked"
        );
    }
    let startup = startup_result?;
    info!(
        run_mode = %startup.run_mode,
        live_order_capability_disabled = startup.live_order_capability_disabled(),
        "Traidan execution engine run mode selected"
    );

    let metrics_bind_addr = metrics_bind_addr();
    let ws_bind_addr = ws_bind_addr();
    let admin_api_config =
        AdminApiConfig::from_env(startup.run_mode, startup.live_order_capability_disabled())?;
    info!(
        bind_addr = %metrics_bind_addr,
        "starting Prometheus metrics exporter"
    );
    info!(
        bind_addr = %ws_bind_addr,
        "starting WebSocket JSON broadcast endpoint"
    );
    match admin_api_config.as_ref() {
        Some(config) => info!(
            bind_addr = %config.bind_addr(),
            "starting localhost admin API"
        ),
        None => info!(
            token_env = "TRAIDAN_ADMIN_TOKEN",
            token_file_env = "TRAIDAN_ADMIN_TOKEN_FILE",
            "localhost admin API disabled until an operator token is configured"
        ),
    }

    // The only active trading path is hourly LLM equity dry-run ingestion.
    let metrics_task = spawn_metrics_server(metrics_bind_addr, metrics.clone());
    let websocket_task =
        spawn_websocket_server(ws_bind_addr, startup.websocket_feed(), metrics.clone());
    let admin_api_task = wait_for_optional_admin_api_server(
        admin_api_config
            .map(|config| spawn_admin_api_server(config, startup.equity_state(), metrics.clone())),
    );
    tokio::select! {
        result = metrics_task => {
            result??;
        }
        result = websocket_task => {
            result??;
        }
        result = admin_api_task => {
            result?;
        }
        shutdown = tokio::signal::ctrl_c() => {
            shutdown?;
            info!("shutdown requested");
        }
    }

    Ok(())
}

fn spawn_metrics_server(
    bind_addr: String,
    metrics: ExecutionMetrics,
) -> JoinHandle<std::io::Result<()>> {
    tokio::spawn(async move { serve_metrics(&bind_addr, metrics).await })
}

fn spawn_websocket_server(
    bind_addr: String,
    feed: EngineStreamFeed,
    metrics: ExecutionMetrics,
) -> JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move { serve_ws_broadcast_with_feed(&bind_addr, feed, metrics).await })
}

fn spawn_admin_api_server(
    config: AdminApiConfig,
    equity_state: LlmEquityRuntimeState,
    metrics: ExecutionMetrics,
) -> JoinHandle<anyhow::Result<()>> {
    let bind_addr = config.bind_addr().to_string();
    let state = AdminApiState::new(config, equity_state, metrics);
    tokio::spawn(async move {
        serve_admin_api(&bind_addr, state)
            .await
            .map_err(anyhow::Error::from)
    })
}

async fn wait_for_optional_admin_api_server(
    task: Option<JoinHandle<anyhow::Result<()>>>,
) -> anyhow::Result<()> {
    match task {
        Some(task) => task.await?,
        None => std::future::pending::<anyhow::Result<()>>().await,
    }
}
