// Real-time gross/net position exposure calculations.
// See architecture.md §4 — Risk Engine.

use std::collections::HashMap;

pub struct ExposureReport {
    pub gross_usd: f64,
    pub net_usd: f64,
    pub per_asset: HashMap<String, f64>,
}

pub fn calculate_exposure(
    positions: &HashMap<String, f64>,
    mid_prices: &HashMap<String, f64>,
) -> ExposureReport {
    let mut per_asset = HashMap::new();
    let mut gross = 0.0f64;
    let mut net = 0.0f64;

    for (symbol, qty) in positions {
        if let Some(price) = mid_prices.get(symbol) {
            let notional = qty * price;
            per_asset.insert(symbol.clone(), notional);
            gross += notional.abs();
            net += notional;
        }
    }

    ExposureReport { gross_usd: gross, net_usd: net, per_asset }
}
