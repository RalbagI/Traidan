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
            // Exposure reports mirror current OMS string-keyed state and are
            // not published on the LMAX ring hot path.
            per_asset.insert(symbol.clone(), notional);
            gross += notional.abs();
            net += notional;
        }
    }

    ExposureReport {
        gross_usd: gross,
        net_usd: net,
        per_asset,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculate_exposure_reports_gross_net_and_per_asset_notional() {
        let positions = HashMap::from([("SPY".to_string(), 10.0), ("QQQ".to_string(), -4.0)]);
        let mid_prices = HashMap::from([("SPY".to_string(), 500.0), ("QQQ".to_string(), 400.0)]);

        let report = calculate_exposure(&positions, &mid_prices);

        assert_eq!(report.per_asset["SPY"], 5_000.0);
        assert_eq!(report.per_asset["QQQ"], -1_600.0);
        assert_eq!(report.gross_usd, 6_600.0);
        assert_eq!(report.net_usd, 3_400.0);
    }

    #[test]
    fn calculate_exposure_ignores_positions_without_mid_price() {
        let positions = HashMap::from([("SPY".to_string(), 10.0), ("QQQ".to_string(), 4.0)]);
        let mid_prices = HashMap::from([("SPY".to_string(), 500.0)]);

        let report = calculate_exposure(&positions, &mid_prices);

        assert_eq!(report.per_asset.len(), 1);
        assert_eq!(report.per_asset["SPY"], 5_000.0);
        assert_eq!(report.gross_usd, 5_000.0);
        assert_eq!(report.net_usd, 5_000.0);
    }
}
