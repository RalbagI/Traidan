mod disruptor;
mod exchange;
mod ipc;
mod oms;
mod risk;

use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Traderalbag execution engine starting");

    // TODO: wire up disruptor ring buffer, exchange handlers, IPC channels
    // See architecture.md §4 for startup sequence

    Ok(())
}
