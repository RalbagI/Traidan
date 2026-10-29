use super::*;
use crate::market_context::{strategy_id_hash, AssetClass, MarketContext};
use crate::oms::routing::Venue;
use std::thread;

fn order(id: &str, side: Side, qty: f64, price: Option<f64>) -> Order {
    Order {
        cl_ord_id: id.to_string(),
        symbol: "SPY".to_string(),
        side,
        qty,
        price,
        market_context: MarketContext::default(),
    }
}

fn fill_report(cl_ord_id: &str, venue: Venue, qty: f64, price: f64) -> FillReport {
    FillReport {
        cl_ord_id: cl_ord_id.to_string(),
        venue,
        qty,
        price,
        market_context: MarketContext::default(),
    }
}

#[test]
fn side_is_copy_for_order_construction() {
    fn assert_copy<T: Copy>() {}
    assert_copy::<Side>();

    let side = Side::Sell;
    let first = order("copy-side-1", side, 0.25, Some(500.0));
    let second = order("copy-side-2", side, 0.5, Some(501.0));

    assert_eq!(first.side, Side::Sell);
    assert_eq!(second.side, Side::Sell);
}

#[test]
fn oms_metrics_default_registers_metrics() {
    let metrics = OmsMetrics::default();
    let encoded = metrics.encode();

    assert!(encoded.contains("traidan_oms_open_orders"));
    assert!(encoded.contains("traidan_oms_fill_total"));
}

#[test]
fn submit_order_rejects_invalid_quantity_and_limit_price() {
    let mut portfolio = PortfolioState::default();

    assert!(matches!(
        portfolio.submit_order(order("zero-qty", Side::Buy, 0.0, Some(500.0))),
        Err(OmsStateError::InvalidOrder {
            cl_ord_id,
            reason: "order quantity must be finite and greater than 0",
        }) if cl_ord_id == "zero-qty"
    ));
    assert!(matches!(
        portfolio.submit_order(order("bad-price", Side::Buy, 1.0, Some(-1.0))),
        Err(OmsStateError::InvalidOrder {
            cl_ord_id,
            reason: "limit price must be finite and greater than 0",
        }) if cl_ord_id == "bad-price"
    ));
    assert!(portfolio.pending_orders.is_empty());
}

#[test]
fn order_lifecycle_tracks_acknowledge_cancel_and_fail_safe_cancel() {
    let mut portfolio = PortfolioState::default();

    portfolio
        .submit_order(order("order-1", Side::Buy, 0.5, Some(500.0)))
        .expect("order submitted");
    assert_eq!(
        portfolio.order_status("order-1"),
        Some(OrderStatus::Pending)
    );

    portfolio
        .acknowledge_order("order-1")
        .expect("order acknowledged");
    assert_eq!(
        portfolio.order_status("order-1"),
        Some(OrderStatus::Acknowledged)
    );

    portfolio
        .submit_order(order("order-2", Side::Sell, 0.25, Some(510.0)))
        .expect("second order submitted");
    assert_eq!(portfolio.open_orders_count(), 2);

    assert_eq!(portfolio.cancel_all_open_orders(), 2);
    assert_eq!(portfolio.open_orders_count(), 0);
    assert_eq!(
        portfolio.order_status("order-1"),
        Some(OrderStatus::Cancelled)
    );
    assert_eq!(
        portfolio.order_status("order-2"),
        Some(OrderStatus::Cancelled)
    );
}

#[test]
fn initialized_position_cost_basis_drives_realized_pnl() {
    let mut portfolio = PortfolioState::default();
    portfolio
        .set_position("SPY", 2.0, Some(490.0))
        .expect("position initialized with cost basis");

    portfolio
        .submit_order(order("sell-preloaded", Side::Sell, 0.5, Some(500.0)))
        .expect("sell submitted");
    portfolio
        .apply_fill(fill_report(
            "sell-preloaded",
            Venue::UsEquitiesPaper,
            0.5,
            500.0,
        ))
        .expect("sell fill applied");

    assert_eq!(portfolio.positions["SPY"], 1.5);
    assert_eq!(portfolio.realized_pnl, 5.0);
    assert_eq!(portfolio.position_average_entry_price("SPY"), Some(490.0));
}

#[test]
fn direct_position_without_cost_basis_rejects_closing_fill() {
    let mut portfolio = PortfolioState::default();
    portfolio.positions.insert("SPY".to_string(), 1.0);
    portfolio
        .submit_order(order("sell-missing-basis", Side::Sell, 0.5, Some(500.0)))
        .expect("sell submitted");

    assert!(matches!(
        portfolio.apply_fill(FillReport {
            cl_ord_id: "sell-missing-basis".to_string(),
            venue: Venue::UsEquitiesPaper,
            qty: 0.5,
            price: 500.0,
            market_context: MarketContext::default(),
        }),
        Err(OmsStateError::MissingCostBasis { symbol }) if symbol == "SPY"
    ));
    assert_eq!(portfolio.positions["SPY"], 1.0);
    assert_eq!(portfolio.realized_pnl, 0.0);
}

#[test]
fn fills_update_positions_realized_pnl_lifecycle_and_metrics() {
    let mut portfolio = PortfolioState::default();
    let metrics = OmsMetrics::new().expect("oms metrics registered");

    portfolio
        .submit_order(order("buy-1", Side::Buy, 1.0, Some(500.0)))
        .expect("buy submitted");
    let partial = portfolio
        .apply_fill(FillReport {
            cl_ord_id: "buy-1".to_string(),
            venue: Venue::UsEquitiesPaper,
            qty: 0.4,
            price: 500.0,
            market_context: MarketContext::default(),
        })
        .expect("partial fill applied");
    metrics.record_fill(partial.status);

    assert_eq!(partial.status, OrderStatus::PartialFill);
    assert_eq!(portfolio.positions["SPY"], 0.4);
    assert_eq!(portfolio.open_orders_count(), 1);

    let filled = portfolio
        .apply_fill(FillReport {
            cl_ord_id: "buy-1".to_string(),
            venue: Venue::UsEquitiesPaper,
            qty: 0.6,
            price: 500.0,
            market_context: MarketContext::default(),
        })
        .expect("full fill applied");
    metrics.record_fill(filled.status);

    assert_eq!(filled.status, OrderStatus::Filled);
    assert_eq!(portfolio.positions["SPY"], 1.0);
    assert_eq!(portfolio.open_orders_count(), 0);

    portfolio
        .submit_order(order("sell-1", Side::Sell, 0.25, Some(510.0)))
        .expect("sell submitted");
    let sell = portfolio
        .apply_fill(FillReport {
            cl_ord_id: "sell-1".to_string(),
            venue: Venue::UsEquitiesPaper,
            qty: 0.25,
            price: 510.0,
            market_context: MarketContext::default(),
        })
        .expect("sell fill applied");
    metrics.record_fill(sell.status);
    metrics.observe_portfolio(&portfolio);
    metrics.set_realized_pnl("SPY", portfolio.realized_pnl);

    assert_eq!(portfolio.positions["SPY"], 0.75);
    assert_eq!(portfolio.realized_pnl, 2.5);
    assert_eq!(portfolio.open_orders_count(), 0);

    let encoded = metrics.encode();
    assert!(encoded.contains("traidan_oms_open_orders 0"));
    assert!(encoded.contains("traidan_oms_fill_total 3"));
    assert!(encoded.contains("traidan_oms_partial_fill_ratio"));
    assert!(encoded.contains("traidan_realized_pnl_usd{symbol=\"SPY\"} 2.5"));
}

#[test]
fn shared_portfolio_state_serializes_updates_across_threads() {
    let shared = SharedPortfolioState::default();

    thread::scope(|scope| {
        for index in 0..4 {
            let shared = shared.clone();
            scope.spawn(move || {
                let order_id = format!("order-{index}");
                shared.update(|portfolio| {
                    portfolio
                        .submit_order(order(&order_id, Side::Buy, 0.25, Some(10.0)))
                        .expect("thread submits order");
                    portfolio
                        .apply_fill(FillReport {
                            cl_ord_id: order_id,
                            venue: Venue::UsEquitiesPaper,
                            qty: 0.25,
                            price: 10.0,
                            market_context: MarketContext::default(),
                        })
                        .expect("thread fills order");
                });
            });
        }
    });

    let snapshot = shared.snapshot();
    assert_eq!(snapshot.positions["SPY"], 1.0);
    assert_eq!(snapshot.open_orders_count(), 0);
}

#[test]
fn fills_record_market_context_and_aggregate_realized_pnl_by_asset_class_and_strategy() {
    let mut portfolio = PortfolioState::default();
    let strategy_id = strategy_id_hash("index_trend_following");
    let market_context = MarketContext::equity_paper(AssetClass::EquityEtf, strategy_id);
    portfolio
        .set_position_with_context("SPY", 10.0, Some(500.0), market_context)
        .expect("position initialized with market context");
    let mut sell = order("sell-spy", Side::Sell, 4.0, Some(505.0));
    sell.symbol = "SPY".to_string();
    sell.market_context = market_context;
    portfolio.submit_order(sell).expect("sell submitted");

    let lifecycle = portfolio
        .apply_fill(FillReport {
            cl_ord_id: "sell-spy".to_string(),
            venue: Venue::UsEquitiesPaper,
            qty: 4.0,
            price: 505.0,
            market_context,
        })
        .expect("fill applied");

    assert_eq!(lifecycle.last_fill_market_context, Some(market_context));
    assert_eq!(
        portfolio.position_market_context("SPY"),
        Some(market_context)
    );
    assert_eq!(portfolio.realized_pnl, 20.0);
    assert_eq!(
        portfolio.realized_pnl_by_asset_class()[&AssetClass::EquityEtf],
        20.0
    );
    assert_eq!(portfolio.realized_pnl_by_strategy()[&strategy_id], 20.0);
}

#[test]
fn closing_default_context_order_inherits_existing_position_context_for_realized_pnl() {
    let mut portfolio = PortfolioState::default();
    let strategy_id = strategy_id_hash("index_trend_following");
    let market_context = MarketContext::equity_paper(AssetClass::EquityEtf, strategy_id);
    portfolio
        .set_position_with_context("SPY", 10.0, Some(500.0), market_context)
        .expect("position initialized with market context");
    let mut sell = order("sell-spy-default-context", Side::Sell, 4.0, Some(505.0));
    sell.symbol = "SPY".to_string();
    portfolio.submit_order(sell).expect("sell submitted");

    portfolio
        .apply_fill(FillReport {
            cl_ord_id: "sell-spy-default-context".to_string(),
            venue: Venue::UsEquitiesPaper,
            qty: 4.0,
            price: 505.0,
            market_context: MarketContext::default(),
        })
        .expect("fill applied");

    assert_eq!(
        portfolio.realized_pnl_by_asset_class()[&AssetClass::EquityEtf],
        20.0
    );
    assert_eq!(portfolio.realized_pnl_by_strategy()[&strategy_id], 20.0);
    assert_eq!(portfolio.realized_pnl_by_asset_class().len(), 1);
}
