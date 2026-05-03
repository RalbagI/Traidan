// Smart order routing: selects venue and execution algorithm based on spread
// and VaR headroom. See architecture.md §4 — OMS routing.

#[derive(Debug, Clone)]
pub enum Venue {
    Binance,
    Coinbase,
}

#[derive(Debug, Clone)]
pub enum ExecAlgo {
    AggressiveIoc,
    PassiveLimit,
}

pub struct RoutingDecision {
    pub venue: Venue,
    pub algo: ExecAlgo,
}

pub fn route(_symbol: &str, _spread_bps: f64, _var_headroom: f64) -> RoutingDecision {
    // TODO: implement venue + algo selection logic
    RoutingDecision {
        venue: Venue::Binance,
        algo: ExecAlgo::AggressiveIoc,
    }
}
