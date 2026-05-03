// Hard Value-at-Risk enforcement. Breaching the drawdown limit triggers
// fail-safe: cancel all orders, flatten positions, halt signal consumption.
// See architecture.md §4 — Risk Engine.

pub const MAX_PORTFOLIO_VAR_USD: f64 = 10_000.0;
pub const MAX_SINGLE_POSITION_VAR_USD: f64 = 3_000.0;
pub const MAX_DRAWDOWN_PCT: f64 = 0.05;

pub fn check_var(current_var_usd: f64, portfolio_usd: f64) -> VarStatus {
    let drawdown = current_var_usd / portfolio_usd;
    if drawdown >= MAX_DRAWDOWN_PCT || current_var_usd >= MAX_PORTFOLIO_VAR_USD {
        VarStatus::Breach
    } else {
        VarStatus::Ok
    }
}

#[derive(Debug, PartialEq)]
pub enum VarStatus {
    Ok,
    Breach,
}
