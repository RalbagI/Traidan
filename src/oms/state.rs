// Live portfolio state: open positions, pending orders, realized/unrealized PnL.
// See architecture.md §4 — OMS state machine.

use super::routing::Venue;
use crate::market_context::{AssetClass, MarketContext, NO_STRATEGY_ID};
use prometheus::{Encoder, Gauge, GaugeVec, IntCounter, IntGauge, Opts, Registry, TextEncoder};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::{Arc, RwLock};

const FLOAT_EPSILON: f64 = 1e-9;

#[derive(Debug, Default, Clone)]
pub struct PortfolioState {
    pub positions: HashMap<String, f64>,
    pub pending_orders: HashMap<String, Order>,
    pub realized_pnl: f64,
    pub order_lifecycle: HashMap<String, OrderLifecycle>,
    average_entry_price: HashMap<String, f64>,
    position_context: HashMap<String, MarketContext>,
    realized_pnl_by_asset_class: HashMap<AssetClass, f64>,
    realized_pnl_by_strategy: HashMap<u64, f64>,
}

impl PortfolioState {
    pub fn submit_order(&mut self, order: Order) -> Result<(), OmsStateError> {
        let cl_ord_id = order.cl_ord_id.clone();
        validate_order(&order)?;
        if self.order_lifecycle.contains_key(&cl_ord_id) {
            return Err(OmsStateError::DuplicateOrder(cl_ord_id));
        }
        self.order_lifecycle.insert(
            cl_ord_id.clone(),
            OrderLifecycle {
                status: OrderStatus::Pending,
                filled_qty: 0.0,
                average_fill_price: None,
                last_fill_venue: None,
                last_fill_market_context: None,
            },
        );
        self.pending_orders.insert(cl_ord_id, order);
        Ok(())
    }

    pub fn set_position(
        &mut self,
        symbol: impl Into<String>,
        qty: f64,
        average_entry_price: Option<f64>,
    ) -> Result<(), OmsStateError> {
        self.set_position_with_context(symbol, qty, average_entry_price, MarketContext::default())
    }

    pub fn set_position_with_context(
        &mut self,
        symbol: impl Into<String>,
        qty: f64,
        average_entry_price: Option<f64>,
        market_context: MarketContext,
    ) -> Result<(), OmsStateError> {
        let symbol = symbol.into();
        if symbol.is_empty() {
            return Err(OmsStateError::InvalidPosition {
                symbol,
                reason: "position symbol must not be empty",
            });
        }
        if !qty.is_finite() {
            return Err(OmsStateError::InvalidPosition {
                symbol,
                reason: "position quantity must be finite",
            });
        }

        if qty.abs() <= FLOAT_EPSILON {
            self.positions.remove(&symbol);
            self.average_entry_price.remove(&symbol);
            self.position_context.remove(&symbol);
            return Ok(());
        }

        let Some(average_entry_price) = average_entry_price else {
            return Err(OmsStateError::MissingCostBasis { symbol });
        };
        if !(average_entry_price.is_finite() && average_entry_price > 0.0) {
            return Err(OmsStateError::InvalidPosition {
                symbol,
                reason: "average entry price must be finite and greater than 0",
            });
        }

        self.positions.insert(symbol.clone(), qty);
        self.average_entry_price
            .insert(symbol.clone(), average_entry_price);
        self.position_context.insert(symbol, market_context);
        Ok(())
    }

    pub fn position_average_entry_price(&self, symbol: &str) -> Option<f64> {
        self.average_entry_price.get(symbol).copied()
    }

    pub fn position_market_context(&self, symbol: &str) -> Option<MarketContext> {
        self.position_context.get(symbol).copied()
    }

    pub fn realized_pnl_by_asset_class(&self) -> HashMap<AssetClass, f64> {
        self.realized_pnl_by_asset_class.clone()
    }

    pub fn realized_pnl_by_strategy(&self) -> HashMap<u64, f64> {
        self.realized_pnl_by_strategy.clone()
    }

    pub fn acknowledge_order(&mut self, cl_ord_id: &str) -> Result<(), OmsStateError> {
        let lifecycle = self
            .order_lifecycle
            .get_mut(cl_ord_id)
            .ok_or_else(|| OmsStateError::UnknownOrder(cl_ord_id.to_string()))?;
        match lifecycle.status {
            OrderStatus::Pending => {
                lifecycle.status = OrderStatus::Acknowledged;
                Ok(())
            }
            status => Err(OmsStateError::InvalidTransition {
                cl_ord_id: cl_ord_id.to_string(),
                from: status,
                to: OrderStatus::Acknowledged,
            }),
        }
    }

    pub fn cancel_order(&mut self, cl_ord_id: &str) -> Result<(), OmsStateError> {
        let lifecycle = self
            .order_lifecycle
            .get_mut(cl_ord_id)
            .ok_or_else(|| OmsStateError::UnknownOrder(cl_ord_id.to_string()))?;
        match lifecycle.status {
            OrderStatus::Pending | OrderStatus::Acknowledged | OrderStatus::PartialFill => {
                lifecycle.status = OrderStatus::Cancelled;
                self.pending_orders.remove(cl_ord_id);
                Ok(())
            }
            status => Err(OmsStateError::InvalidTransition {
                cl_ord_id: cl_ord_id.to_string(),
                from: status,
                to: OrderStatus::Cancelled,
            }),
        }
    }

    pub fn apply_fill(&mut self, report: FillReport) -> Result<OrderLifecycle, OmsStateError> {
        if !(report.qty.is_finite() && report.qty > 0.0) {
            return Err(OmsStateError::InvalidFill {
                cl_ord_id: report.cl_ord_id,
                reason: "fill quantity must be finite and greater than 0",
            });
        }
        if !(report.price.is_finite() && report.price > 0.0) {
            return Err(OmsStateError::InvalidFill {
                cl_ord_id: report.cl_ord_id,
                reason: "fill price must be finite and greater than 0",
            });
        }

        let order = self
            .pending_orders
            .get(&report.cl_ord_id)
            .cloned()
            .ok_or_else(|| OmsStateError::UnknownOrder(report.cl_ord_id.clone()))?;
        let lifecycle = self
            .order_lifecycle
            .get(&report.cl_ord_id)
            .ok_or_else(|| OmsStateError::UnknownOrder(report.cl_ord_id.clone()))?;
        if matches!(
            lifecycle.status,
            OrderStatus::Filled | OrderStatus::Cancelled
        ) {
            return Err(OmsStateError::InvalidTransition {
                cl_ord_id: report.cl_ord_id,
                from: lifecycle.status,
                to: OrderStatus::PartialFill,
            });
        }

        let new_filled_qty = lifecycle.filled_qty + report.qty;
        if new_filled_qty > order.qty + FLOAT_EPSILON {
            return Err(OmsStateError::Overfill {
                cl_ord_id: report.cl_ord_id,
                order_qty: order.qty,
                attempted_filled_qty: new_filled_qty,
            });
        }

        let inherited_market_context = self.position_context.get(&order.symbol).copied();
        let market_context = effective_market_context(&order, &report, inherited_market_context);
        let lifecycle_snapshot = OrderLifecycle {
            status: if new_filled_qty + FLOAT_EPSILON >= order.qty {
                OrderStatus::Filled
            } else {
                OrderStatus::PartialFill
            },
            filled_qty: new_filled_qty,
            average_fill_price: Some(weighted_average_fill_price(
                lifecycle.filled_qty,
                lifecycle.average_fill_price,
                report.qty,
                report.price,
            )),
            last_fill_venue: Some(report.venue),
            last_fill_market_context: Some(market_context),
        };

        let realized_before = self.realized_pnl;
        self.apply_position_fill(&order.symbol, order.side, report.qty, report.price)?;
        if self.positions.contains_key(&order.symbol) {
            self.position_context
                .insert(order.symbol.clone(), market_context);
        } else {
            self.position_context.remove(&order.symbol);
        }
        self.record_realized_pnl_delta(market_context, self.realized_pnl - realized_before);

        let lifecycle = self
            .order_lifecycle
            .get_mut(&report.cl_ord_id)
            .ok_or_else(|| OmsStateError::UnknownOrder(report.cl_ord_id.clone()))?;
        *lifecycle = lifecycle_snapshot.clone();

        if lifecycle_snapshot.status == OrderStatus::Filled {
            self.pending_orders.remove(&report.cl_ord_id);
        }

        Ok(lifecycle_snapshot)
    }

    pub fn open_orders_count(&self) -> usize {
        self.pending_orders.len()
    }

    pub fn order_status(&self, cl_ord_id: &str) -> Option<OrderStatus> {
        self.order_lifecycle
            .get(cl_ord_id)
            .map(|lifecycle| lifecycle.status)
    }

    /// Clears local pending-order state after fail-safe risk handling starts.
    ///
    /// This is not an exchange cancel transport. Production OMS integration
    /// must send venue cancel requests before or around this local state
    /// transition.
    pub fn cancel_all_open_orders(&mut self) -> usize {
        let canceled = self.pending_orders.len();
        for cl_ord_id in self.pending_orders.keys() {
            if let Some(lifecycle) = self.order_lifecycle.get_mut(cl_ord_id) {
                lifecycle.status = OrderStatus::Cancelled;
            }
        }
        self.pending_orders.clear();
        canceled
    }

    fn apply_position_fill(
        &mut self,
        symbol: &str,
        side: Side,
        qty: f64,
        price: f64,
    ) -> Result<(), OmsStateError> {
        match side {
            Side::Buy => self.apply_buy_fill(symbol, qty, price),
            Side::Sell => self.apply_sell_fill(symbol, qty, price),
        }
    }

    fn apply_buy_fill(&mut self, symbol: &str, qty: f64, price: f64) -> Result<(), OmsStateError> {
        let current_qty = *self.positions.get(symbol).unwrap_or(&0.0);
        let current_avg = self.current_average_or_fill_price(symbol, current_qty, price)?;
        let new_qty = current_qty + qty;

        if current_qty >= 0.0 {
            self.update_long_average(symbol, current_qty, current_avg, qty, price, new_qty);
        } else {
            let closing_qty = qty.min(-current_qty);
            self.realized_pnl += (current_avg - price) * closing_qty;
            let next_average = if new_qty < -FLOAT_EPSILON {
                current_avg
            } else {
                price
            };
            self.update_position_after_cross(symbol, new_qty, next_average);
        }

        Ok(())
    }

    fn apply_sell_fill(&mut self, symbol: &str, qty: f64, price: f64) -> Result<(), OmsStateError> {
        let current_qty = *self.positions.get(symbol).unwrap_or(&0.0);
        let current_avg = self.current_average_or_fill_price(symbol, current_qty, price)?;
        let new_qty = current_qty - qty;

        if current_qty <= 0.0 {
            self.update_short_average(symbol, current_qty, current_avg, qty, price, new_qty);
        } else {
            let closing_qty = qty.min(current_qty);
            self.realized_pnl += (price - current_avg) * closing_qty;
            let next_average = if new_qty > FLOAT_EPSILON {
                current_avg
            } else {
                price
            };
            self.update_position_after_cross(symbol, new_qty, next_average);
        }

        Ok(())
    }

    fn current_average_or_fill_price(
        &self,
        symbol: &str,
        current_qty: f64,
        fill_price: f64,
    ) -> Result<f64, OmsStateError> {
        if current_qty.abs() <= FLOAT_EPSILON {
            return Ok(fill_price);
        }

        self.average_entry_price
            .get(symbol)
            .copied()
            .filter(|average| average.is_finite() && *average > 0.0)
            .ok_or_else(|| OmsStateError::MissingCostBasis {
                symbol: symbol.to_string(),
            })
    }

    fn update_long_average(
        &mut self,
        symbol: &str,
        current_qty: f64,
        current_avg: f64,
        qty: f64,
        price: f64,
        new_qty: f64,
    ) {
        self.positions.insert(symbol.to_string(), new_qty);
        if new_qty.abs() <= FLOAT_EPSILON {
            self.average_entry_price.remove(symbol);
        } else {
            let average = ((current_qty * current_avg) + (qty * price)) / new_qty;
            self.average_entry_price.insert(symbol.to_string(), average);
        }
    }

    fn update_short_average(
        &mut self,
        symbol: &str,
        current_qty: f64,
        current_avg: f64,
        qty: f64,
        price: f64,
        new_qty: f64,
    ) {
        self.positions.insert(symbol.to_string(), new_qty);
        if new_qty.abs() <= FLOAT_EPSILON {
            self.average_entry_price.remove(symbol);
        } else {
            let average = (((-current_qty) * current_avg) + (qty * price)) / (-new_qty);
            self.average_entry_price.insert(symbol.to_string(), average);
        }
    }

    fn update_position_after_cross(&mut self, symbol: &str, new_qty: f64, next_average: f64) {
        if new_qty.abs() <= FLOAT_EPSILON {
            self.positions.remove(symbol);
            self.average_entry_price.remove(symbol);
            self.position_context.remove(symbol);
        } else {
            self.positions.insert(symbol.to_string(), new_qty);
            if new_qty.signum() != 0.0 {
                self.average_entry_price
                    .insert(symbol.to_string(), next_average);
            }
        }
    }

    fn record_realized_pnl_delta(&mut self, market_context: MarketContext, delta: f64) {
        if delta.abs() <= FLOAT_EPSILON {
            return;
        }
        *self
            .realized_pnl_by_asset_class
            .entry(market_context.asset_class)
            .or_insert(0.0) += delta;
        if market_context.strategy_id != NO_STRATEGY_ID {
            *self
                .realized_pnl_by_strategy
                .entry(market_context.strategy_id)
                .or_insert(0.0) += delta;
        }
    }
}

fn validate_order(order: &Order) -> Result<(), OmsStateError> {
    if order.symbol.is_empty() {
        return Err(OmsStateError::InvalidOrder {
            cl_ord_id: order.cl_ord_id.clone(),
            reason: "order symbol must not be empty",
        });
    }
    if !(order.qty.is_finite() && order.qty > 0.0) {
        return Err(OmsStateError::InvalidOrder {
            cl_ord_id: order.cl_ord_id.clone(),
            reason: "order quantity must be finite and greater than 0",
        });
    }
    if let Some(price) = order.price {
        if !(price.is_finite() && price > 0.0) {
            return Err(OmsStateError::InvalidOrder {
                cl_ord_id: order.cl_ord_id.clone(),
                reason: "limit price must be finite and greater than 0",
            });
        }
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct Order {
    pub cl_ord_id: String,
    pub symbol: String,
    pub side: Side,
    pub qty: f64,
    pub price: Option<f64>,
    pub market_context: MarketContext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    Pending,
    Acknowledged,
    PartialFill,
    Filled,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderLifecycle {
    pub status: OrderStatus,
    pub filled_qty: f64,
    pub average_fill_price: Option<f64>,
    pub last_fill_venue: Option<Venue>,
    pub last_fill_market_context: Option<MarketContext>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FillReport {
    pub cl_ord_id: String,
    pub venue: Venue,
    pub qty: f64,
    pub price: f64,
    pub market_context: MarketContext,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OmsStateError {
    DuplicateOrder(String),
    UnknownOrder(String),
    InvalidOrder {
        cl_ord_id: String,
        reason: &'static str,
    },
    InvalidPosition {
        symbol: String,
        reason: &'static str,
    },
    InvalidFill {
        cl_ord_id: String,
        reason: &'static str,
    },
    MissingCostBasis {
        symbol: String,
    },
    InvalidTransition {
        cl_ord_id: String,
        from: OrderStatus,
        to: OrderStatus,
    },
    Overfill {
        cl_ord_id: String,
        order_qty: f64,
        attempted_filled_qty: f64,
    },
}

impl fmt::Display for OmsStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateOrder(cl_ord_id) => write!(f, "duplicate order {cl_ord_id}"),
            Self::UnknownOrder(cl_ord_id) => write!(f, "unknown order {cl_ord_id}"),
            Self::InvalidOrder { cl_ord_id, reason } => {
                write!(f, "invalid order {cl_ord_id}: {reason}")
            }
            Self::InvalidPosition { symbol, reason } => {
                write!(f, "invalid position {symbol}: {reason}")
            }
            Self::InvalidFill { cl_ord_id, reason } => {
                write!(f, "invalid fill for {cl_ord_id}: {reason}")
            }
            Self::MissingCostBasis { symbol } => {
                write!(f, "missing average entry price for open position {symbol}")
            }
            Self::InvalidTransition {
                cl_ord_id,
                from,
                to,
            } => write!(
                f,
                "invalid order transition for {cl_ord_id}: {from:?} -> {to:?}"
            ),
            Self::Overfill {
                cl_ord_id,
                order_qty,
                attempted_filled_qty,
            } => write!(
                f,
                "order {cl_ord_id} overfilled: order qty {order_qty}, attempted filled qty {attempted_filled_qty}"
            ),
        }
    }
}

impl Error for OmsStateError {}

#[derive(Clone, Default)]
pub struct SharedPortfolioState {
    inner: Arc<RwLock<PortfolioState>>,
}

impl SharedPortfolioState {
    pub fn new(portfolio: PortfolioState) -> Self {
        Self {
            inner: Arc::new(RwLock::new(portfolio)),
        }
    }

    pub fn update<T>(&self, update: impl FnOnce(&mut PortfolioState) -> T) -> T {
        let mut portfolio = self.inner.write().expect("portfolio state lock poisoned");
        update(&mut portfolio)
    }

    pub fn snapshot(&self) -> PortfolioState {
        self.inner
            .read()
            .expect("portfolio state lock poisoned")
            .clone()
    }
}

#[derive(Clone)]
pub struct OmsMetrics {
    registry: Registry,
    open_orders: IntGauge,
    fill_total: IntCounter,
    partial_fill_total: IntCounter,
    partial_fill_ratio: Gauge,
    realized_pnl: GaugeVec,
}

impl OmsMetrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        Self::new_in_registry(Registry::new())
    }

    pub fn new_in_registry(registry: Registry) -> Result<Self, prometheus::Error> {
        let open_orders = IntGauge::new(
            "traidan_oms_open_orders",
            "Current number of open OMS orders.",
        )?;
        let fill_total = IntCounter::new(
            "traidan_oms_fill_total",
            "Total number of fill reports applied to OMS state.",
        )?;
        let partial_fill_total = IntCounter::new(
            "traidan_oms_partial_fill_total",
            "Total number of fill reports that left the order partially filled.",
        )?;
        let partial_fill_ratio = Gauge::new(
            "traidan_oms_partial_fill_ratio",
            "Ratio of partial-fill reports to all fill reports.",
        )?;
        let realized_pnl = GaugeVec::new(
            Opts::new(
                "traidan_realized_pnl_usd",
                "Current realized PnL in USD by symbol.",
            ),
            &["symbol"],
        )?;

        registry.register(Box::new(open_orders.clone()))?;
        registry.register(Box::new(fill_total.clone()))?;
        registry.register(Box::new(partial_fill_total.clone()))?;
        registry.register(Box::new(partial_fill_ratio.clone()))?;
        registry.register(Box::new(realized_pnl.clone()))?;

        Ok(Self {
            registry,
            open_orders,
            fill_total,
            partial_fill_total,
            partial_fill_ratio,
            realized_pnl,
        })
    }

    pub fn observe_portfolio(&self, portfolio: &PortfolioState) {
        self.open_orders.set(portfolio.open_orders_count() as i64);
    }

    pub fn set_realized_pnl(&self, symbol: &str, pnl_usd: f64) {
        if !symbol.is_empty() && pnl_usd.is_finite() {
            self.realized_pnl.with_label_values(&[symbol]).set(pnl_usd);
        }
    }

    pub fn record_fill(&self, status: OrderStatus) {
        self.fill_total.inc();
        if status == OrderStatus::PartialFill {
            self.partial_fill_total.inc();
        }

        let fill_total = self.fill_total.get();
        if fill_total > 0 {
            self.partial_fill_ratio
                .set(self.partial_fill_total.get() as f64 / fill_total as f64);
        }
    }

    pub fn encode(&self) -> String {
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&metric_families, &mut buffer)
            .expect("Prometheus text encoding should not fail for in-memory metrics");
        String::from_utf8(buffer).expect("Prometheus text encoder emits UTF-8")
    }
}

impl Default for OmsMetrics {
    fn default() -> Self {
        Self::new().expect("OMS metric descriptors are static and valid")
    }
}

fn weighted_average_fill_price(
    current_qty: f64,
    current_average: Option<f64>,
    fill_qty: f64,
    fill_price: f64,
) -> f64 {
    let Some(current_average) = current_average else {
        return fill_price;
    };
    ((current_qty * current_average) + (fill_qty * fill_price)) / (current_qty + fill_qty)
}

fn effective_market_context(
    order: &Order,
    report: &FillReport,
    inherited_context: Option<MarketContext>,
) -> MarketContext {
    if report.market_context != MarketContext::default() {
        report.market_context
    } else if order.market_context != MarketContext::default() {
        order.market_context
    } else {
        inherited_context.unwrap_or_default()
    }
}

#[cfg(test)]
mod tests;
