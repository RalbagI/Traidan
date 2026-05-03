// Binance SBE WebSocket decoder.
// Subscribes to the Diff Depth Stream (25 ms interval) and maintains a local
// L2 order book using the spot_2_0.xml FIX SBE schema.
// See architecture.md §4 — Binance SBE.

/// Represents one level in the L2 order book (price + volume).
#[derive(Debug, Clone, Copy)]
pub struct BookLevel {
    pub price: f64,
    pub qty: f64,
}

/// Local L2 order book state maintained from incremental diff updates.
#[derive(Debug, Default)]
pub struct LocalOrderBook {
    pub last_update_id: u64,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
}

impl LocalOrderBook {
    /// Apply a diff depth update. Call after verifying sequence continuity.
    pub fn apply_diff(&mut self, _update_id: u64, _bids: &[BookLevel], _asks: &[BookLevel]) {
        // TODO: implement sorted merge of diff into bids/asks vectors
    }
}

/// Entry point: connect to Binance SBE WebSocket and stream diff events.
pub async fn run_market_data_stream(
    _symbol: &str,
    _book: &mut LocalOrderBook,
) -> anyhow::Result<()> {
    // TODO: open WS connection, decode SBE binary frames via spot_2_0.xml schema,
    //       dispatch DepthDiffStreamEvent to apply_diff
    Ok(())
}
