// Live portfolio state: open positions, pending orders, realized/unrealized PnL.
// See architecture.md §4 — OMS state machine.

use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct PortfolioState {
    pub positions: HashMap<String, f64>,
    pub pending_orders: HashMap<String, Order>,
    pub realized_pnl: f64,
}

#[derive(Debug, Clone)]
pub struct Order {
    pub cl_ord_id: String,
    pub symbol: String,
    pub side: Side,
    pub qty: f64,
    pub price: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Side {
    Buy,
    Sell,
}
