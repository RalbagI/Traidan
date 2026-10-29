use super::*;
use serde_json::json;

fn packet(sequence: u64, start: usize, authority: &LlmEquityDryRunAuthority) -> (Value, Value) {
    let mut snap = tests::snapshot_value();
    let bar = snap["latest_bars"]["AAPL"].clone();
    let quote = snap["latest_quotes"]["AAPL"].clone();
    let symbols: Vec<String> = (0..50).map(|i| format!("STK{i}")).collect();
    snap["symbols"] = json!(symbols);
    snap["latest_bars"] = json!({});
    snap["latest_quotes"] = json!({});
    snap["asset_metadata"] = json!({});
    for symbol in &symbols {
        snap["latest_bars"][symbol] = bar.clone();
        snap["latest_quotes"][symbol] = quote.clone();
        snap["asset_metadata"][symbol] = json!({"tradable":true,"shortable":false,
            "fractionable":true,"investment_kind":"equity_stock"});
    }
    let mut decision = tests::decision_value(sequence, "buy_notional", "STK0");
    decision["execution_policy"] = json!("fully_invested_v1");
    decision["commands"] = json!([]);
    decision["target_portfolio"] = json!({"schema_version":1,
        "account_cash_usd":authority.portfolio.cash_usd,
        "account_positions":authority.portfolio.positions.iter().map(|(s,p)|(s.clone(),p.quantity)).collect::<HashMap<_,_>>(),
        "holdings":(start..start+20).map(|i|json!({"symbol":format!("STK{i}"),"weight_bps":500})).collect::<Vec<_>>()});
    (decision, snap)
}

fn execute(
    a: &mut LlmEquityDryRunAuthority,
    d: &Value,
    s: &Value,
) -> Result<LlmEquityApplyResult, LlmEquityAuthorityError> {
    a.apply_json(
        &d.to_string(),
        &s.to_string(),
        "2026-05-28T14:30:00Z".parse().unwrap(),
    )
}

#[test]
fn fully_invested_bootstrap_and_rotation_are_complete_and_idempotent() {
    let mut a = LlmEquityDryRunAuthority::paper_fully_invested();
    let (d, s) = packet(1, 0, &a);
    let result = execute(&mut a, &d, &s).unwrap();
    assert!(a.portfolio.cash_usd >= 0.0 && a.portfolio.cash_usd <= 0.01);
    assert_eq!(a.portfolio.positions.len(), 20);
    let nav = a.portfolio.nav_usd(&result.prices);
    assert!(
        a.portfolio
            .positions
            .values()
            .all(|p| p.quantity > 0.0
                && p.market_value(result.prices[&p.symbol]) <= nav * 0.05 + 1e-6)
    );
    let before = a.portfolio.clone();
    assert_eq!(execute(&mut a, &d, &s).unwrap(), result);
    assert_eq!(a.portfolio, before);
    let (d, s) = packet(2, 20, &a);
    let result = execute(&mut a, &d, &s).unwrap();
    assert_eq!(result.accepted_orders.len(), 40);
    assert!(a
        .portfolio
        .positions
        .keys()
        .all(|s| s[3..].parse::<usize>().unwrap() >= 20));
    assert!(a.portfolio.cash_usd >= 0.0 && a.portfolio.cash_usd <= 0.01);
}

#[test]
fn fully_invested_covers_preserved_short_inventory_without_resetting_nav() {
    let mut a = LlmEquityDryRunAuthority::paper_fully_invested();
    a.portfolio.cash_usd = 11000.0;
    a.portfolio.positions.insert(
        "STK49".into(),
        LlmEquityPosition {
            symbol: "STK49".into(),
            quantity: -5.0,
            avg_price: 200.0,
        },
    );
    let (d, s) = packet(1, 0, &a);
    let before_nav = a
        .portfolio
        .nav_usd(&snapshot_mid_prices(&serde_json::from_value(s.clone()).unwrap()).unwrap());
    let result = execute(&mut a, &d, &s).unwrap();
    assert_eq!(result.accepted_orders.len(), 21);
    assert!(!a.portfolio.positions.contains_key("STK49"));
    assert!(a.portfolio.positions.values().all(|p| p.quantity > 0.0));
    assert!((0.0..=0.01).contains(&a.portfolio.cash_usd));
    assert!(a.portfolio.nav_usd(&result.prices) < before_nav);
    assert!(a.portfolio.nav_usd(&result.prices) > before_nav * 0.99);
}

#[test]
fn fully_invested_invalid_replacement_never_executes_a_sale() {
    let mut a = LlmEquityDryRunAuthority::paper_fully_invested();
    let (d, s) = packet(1, 0, &a);
    execute(&mut a, &d, &s).unwrap();
    let before = a.portfolio.clone();
    for reason in [
        "ineligible",
        "overweight",
        "stale",
        "base",
        "closed",
        "missing",
        "sum",
    ] {
        let (mut d, mut s) = packet(2, 20, &a);
        match reason {
            "ineligible" => s["asset_metadata"]["STK20"]["investment_kind"] = json!("excluded"),
            "overweight" => d["target_portfolio"]["holdings"][0]["weight_bps"] = json!(501),
            "stale" => s["latest_quotes"]["STK20"]["timestamp"] = json!("2026-05-28T12:00:00Z"),
            "base" => d["target_portfolio"]["account_cash_usd"] = json!(10000),
            "closed" => s["market_calendar_state"]["is_open"] = json!(false),
            "missing" => {
                s["latest_quotes"].as_object_mut().unwrap().remove("STK0");
            }
            _ => d["target_portfolio"]["holdings"][0]["weight_bps"] = json!(499),
        }
        assert!(execute(&mut a, &d, &s).is_err(), "{reason}");
        assert_eq!(a.portfolio, before, "{reason}");
        assert_eq!(a.highest_sequence, 1);
    }
}

#[test]
fn fully_invested_loss_flags_do_not_block_and_legacy_cannot_accept_allocation() {
    let mut a = LlmEquityDryRunAuthority::paper_fully_invested();
    a.drawdown_halted = true;
    a.daily_loss_halted = true;
    a.portfolio.peak_nav_usd = 20000.0;
    let (d, s) = packet(1, 0, &a);
    assert!(execute(&mut a, &d, &s).is_ok());
    assert!(execute(&mut LlmEquityDryRunAuthority::paper_profit_driven(), &d, &s).is_err());
}

#[test]
fn fully_invested_restart_recovers_receipt_even_after_journal_failure() {
    let root = std::env::temp_dir().join(format!(
        "allocation-recovery-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap()
    ));
    fs::create_dir_all(&root).unwrap();
    let path = root.join("state.json");
    let mut a = LlmEquityDryRunAuthority::paper_fully_invested();
    let (d, s) = packet(1, 0, &a);
    let result = execute(&mut a, &d, &s).unwrap();
    fs::create_dir(path.with_extension("jsonl")).unwrap();
    assert!(a.persist_to_path(&path).is_err());
    assert!(a.allocation_commit_matches(&path));
    let mut restored =
        LlmEquityDryRunAuthority::restore_research_from_path(&path, "fully_invested").unwrap();
    let replay = restored
        .apply_json(
            &d.to_string(),
            &s.to_string(),
            "2026-05-29T14:30:00Z".parse().unwrap(),
        )
        .unwrap();
    assert_eq!(replay, result);
    assert_eq!(restored.portfolio, a.portfolio);
    let mut changed = d.clone();
    changed["target_portfolio"]["account_cash_usd"] = json!(3.0);
    assert!(execute(&mut restored, &changed, &s).is_err());
    let mut changed_snapshot = s.clone();
    changed_snapshot["latest_quotes"]["STK0"]["ask"] = json!(1000.0);
    assert!(execute(&mut restored, &d, &changed_snapshot).is_err());
    fs::remove_dir(path.with_extension("jsonl")).unwrap();
    restored.persist_to_path(&path).unwrap();
    assert_eq!(
        fs::read_to_string(path.with_extension("jsonl"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn fully_invested_failure_before_commit_preserves_durable_account() {
    let root = std::env::temp_dir().join(format!(
        "allocation-uncommitted-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap()
    ));
    fs::create_dir_all(&root).unwrap();
    let path = root.join("state.json");
    let mut a = LlmEquityDryRunAuthority::paper_fully_invested();
    a.persist_to_path(&path).unwrap();
    let before = fs::read(&path).unwrap();
    let (d, s) = packet(1, 0, &a);
    execute(&mut a, &d, &s).unwrap();
    assert!(a.persist_to_path(&path.join("impossible.json")).is_err());
    assert!(!a.allocation_commit_matches(&path));
    assert_eq!(fs::read(&path).unwrap(), before);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn fully_invested_variable_prices_forty_holdings_and_dividend_reinvestment() {
    let mut a = LlmEquityDryRunAuthority::paper_fully_invested();
    a.portfolio.cash_usd = 10_016.865_4;
    let (mut d, mut s) = packet(1, 0, &a);
    d["target_portfolio"]["holdings"] = json!((0..40)
        .map(|i| json!({"symbol": format!("STK{i}"), "weight_bps": if i < 20 { 400 } else { 100 }}))
        .collect::<Vec<_>>());
    for i in 0..50 {
        let price = 3.0 + f64::from(i) * 37.0;
        let symbol = format!("STK{i}");
        s["latest_quotes"][&symbol]["bid"] = json!(price * 0.999);
        s["latest_quotes"][&symbol]["ask"] = json!(price * 1.001);
        for key in ["open", "high", "low", "close"] {
            s["latest_bars"][&symbol][key] = json!(price);
        }
    }
    let result = execute(&mut a, &d, &s).unwrap();
    assert_eq!(a.portfolio.positions.len(), 40);
    assert!((0.0..=0.01).contains(&a.portfolio.cash_usd));
    assert!(a.portfolio.nav_usd(&result.prices) < 10_016.865_4);
    a.apply_research_corporate_action("STK0", "cash_dividend", None, Some(0.25), None)
        .unwrap();
    assert!(a.portfolio.cash_usd > 0.01);
    let (mut refill, _) = packet(2, 0, &a);
    refill["target_portfolio"]["holdings"] = d["target_portfolio"]["holdings"].clone();
    let result = execute(&mut a, &refill, &s).unwrap();
    assert!((0.0..=0.01).contains(&a.portfolio.cash_usd));
    assert!(result
        .accepted_orders
        .iter()
        .all(|o| o.side == LlmEquityOrderSide::Buy));
}

#[test]
fn fully_invested_turnover_and_legacy_sell_attempts_leave_investments_intact() {
    let mut a = LlmEquityDryRunAuthority::paper_fully_invested();
    let (d, s) = packet(1, 0, &a);
    execute(&mut a, &d, &s).unwrap();
    a.turnover_today_usd = 40_000.0;
    let before = a.portfolio.clone();
    let (mut rotation, s) = packet(2, 20, &a);
    assert_eq!(
        execute(&mut a, &rotation, &s).unwrap_err(),
        LlmEquityAuthorityError::Contract("turnover_limit")
    );
    rotation["execution_policy"] = json!("profit_driven_v1");
    assert!(execute(&mut a, &rotation, &s).is_err());
    let sell = tests::decision_value(2, "sell_notional", "STK0");
    assert!(execute(&mut a, &sell, &s).is_err());
    assert_eq!(a.portfolio, before);
}
