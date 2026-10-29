//! Atomic target-weight rebalances for the fully invested paper policy.
use super::*;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TargetPortfolio {
    schema_version: u32,
    account_cash_usd: f64,
    account_positions: BTreeMap<String, f64>,
    holdings: Vec<TargetHolding>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TargetHolding {
    symbol: String,
    weight_bps: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AllocationReceipt {
    pub decision_hash: String,
    pub snapshot_hash: String,
    pub result: LlmEquityApplyResult,
}

pub(super) fn request_digest(value: &Value) -> String {
    ring::digest::digest(&ring::digest::SHA256, value.to_string().as_bytes())
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn reject(reason: &'static str) -> LlmEquityAuthorityError {
    LlmEquityAuthorityError::Contract(reason)
}

impl LlmEquityDryRunAuthority {
    pub fn paper_fully_invested() -> Self {
        Self {
            fully_invested_policy: true,
            ..Self::paper()
        }
    }

    pub(super) fn apply_allocation(
        &mut self,
        decision: &DecisionEnvelope,
        snapshot: &SnapshotEnvelope,
        received_at: DateTime<Utc>,
    ) -> Result<LlmEquityApplyResult, LlmEquityAuthorityError> {
        if !self.fills_enabled
            || !self.fully_invested_policy
            || decision.execution_policy.as_deref() != Some("fully_invested_v1")
            || !decision.commands.is_empty()
        {
            return Err(reject("allocation_policy_mismatch"));
        }
        let target = decision
            .target_portfolio
            .as_ref()
            .ok_or(reject("target_portfolio_required"))?;
        if target.schema_version != 1
            || !(20..=40).contains(&target.holdings.len())
            || decision.cycle_id.trim().is_empty()
            || decision.evidence.is_empty()
            || decision.memory_updates.is_empty()
            || decision.risk_assessment.risk_limit_verdict != RiskLimitVerdict::Pass
            || !decision.portfolio_nav_usd.is_finite()
            || decision.portfolio_nav_usd <= 0.0
        {
            return Err(reject("allocation_contract_invalid"));
        }
        self.reject_replay_or_stale_decision(decision, received_at)?;
        validate_snapshot(snapshot)?;
        let prices = snapshot_mid_prices(snapshot)?;
        if let Some(reason) = global_rejection_reason(decision, snapshot, &prices) {
            return Err(reject(reason));
        }
        if !execution_session_open(snapshot, received_at)
            || snapshot_market_data_stale_at(snapshot, &prices, received_at)
        {
            return Err(reject("allocation_market_not_executable"));
        }
        if !target.account_cash_usd.is_finite()
            || (self.portfolio.cash_usd - target.account_cash_usd).abs() > 1e-6
            || self.portfolio.positions.len() != target.account_positions.len()
            || self.portfolio.positions.iter().any(|(s, p)| {
                target
                    .account_positions
                    .get(s)
                    .is_none_or(|q| !q.is_finite() || (q - p.quantity).abs() > 1e-9)
            })
        {
            return Err(reject("allocation_account_changed"));
        }
        let mut weights = BTreeMap::new();
        for holding in &target.holdings {
            if !is_valid_llm_equity_symbol(&holding.symbol)
                || !(1..=500).contains(&holding.weight_bps)
                || weights
                    .insert(holding.symbol.clone(), holding.weight_bps)
                    .is_some()
            {
                return Err(reject("allocation_weights_invalid"));
            }
            let meta = snapshot
                .asset_metadata
                .get(&holding.symbol)
                .ok_or(reject("asset_metadata_unavailable"))?;
            if !meta.tradable
                || !meta._fractionable
                || !matches!(
                    meta.investment_kind.as_deref(),
                    Some("equity_stock" | "equity_etf")
                )
            {
                return Err(reject("allocation_asset_ineligible"));
            }
        }
        if weights.values().sum::<u32>() != 10000 {
            return Err(reject("allocation_weights_must_total_10000"));
        }
        let mut symbols: Vec<_> = weights
            .keys()
            .chain(self.portfolio.positions.keys())
            .cloned()
            .collect();
        symbols.sort();
        symbols.dedup();
        for symbol in &symbols {
            let quote = snapshot
                .latest_quotes
                .get(symbol)
                .ok_or(reject("latest_quote_missing"))?;
            if !(snapshot.symbols.contains(symbol)
                && prices.contains_key(symbol)
                && quote.bid.is_finite()
                && quote.ask.is_finite()
                && quote.bid > 0.0
                && quote.ask >= quote.bid
                && snapshot
                    .asset_metadata
                    .get(symbol)
                    .is_some_and(|m| m.tradable))
            {
                return Err(reject("allocation_quote_unavailable"));
            }
        }
        let initial_nav = self.portfolio.nav_usd(&prices);
        if !initial_nav.is_finite() || initial_nav <= 0.0 || self.portfolio.cash_usd < -1e-9 {
            return Err(reject("allocation_account_invalid"));
        }
        let impact = LLM_EQUITY_MARKET_IMPACT_BPS / 10000.0;
        let quantity = |symbol: &String, invested: f64| {
            invested * f64::from(*weights.get(symbol).unwrap_or(&0)) / 10000.0 / prices[symbol]
        };
        let execution_price = |symbol: &String, delta: f64| {
            let quote = &snapshot.latest_quotes[symbol];
            if delta > 0.0 {
                quote.ask * (1.0 + impact)
            } else {
                quote.bid * (1.0 - impact)
            }
        };
        let remaining_cash = |invested: f64| {
            self.portfolio.cash_usd
                - symbols
                    .iter()
                    .map(|symbol| {
                        let delta = quantity(symbol, invested)
                            - self
                                .portfolio
                                .positions
                                .get(symbol)
                                .map_or(0.0, |p| p.quantity);
                        delta * execution_price(symbol, delta)
                    })
                    .sum::<f64>()
        };
        // Solve post-cost invested NAV, not pre-cost ticket notionals. Choose the
        // lower endpoint so rounding cannot borrow even a fraction of a cent.
        let (mut low, mut high) = (0.0, initial_nav);
        for _ in 0..96 {
            let mid = (low + high) / 2.0;
            if remaining_cash(mid) >= 1e-8 {
                low = mid;
            } else {
                high = mid;
            }
        }
        let mut trades: Vec<_> = symbols
            .iter()
            .filter_map(|symbol| {
                let delta = quantity(symbol, low)
                    - self
                        .portfolio
                        .positions
                        .get(symbol)
                        .map_or(0.0, |p| p.quantity);
                (delta.abs() > FLOAT_EPSILON)
                    .then(|| (symbol.clone(), delta, execution_price(symbol, delta)))
            })
            .collect();
        trades.sort_by(|a, b| {
            a.1.is_sign_positive()
                .cmp(&b.1.is_sign_positive())
                .then(a.0.cmp(&b.0))
        });
        let mut staged = self.clone();
        staged.reset_period_counters(received_at, &prices);
        let turnover: f64 = trades.iter().map(|(_, q, p)| (q * p).abs()).sum();
        if staged.turnover_today_usd + turnover
            > initial_nav * FORCED_AGGRESSIVE_MAX_DAILY_TURNOVER_NAV_PCT
        {
            return Err(reject("turnover_limit"));
        }
        let mut orders = Vec::new();
        for (index, (symbol, qty, price)) in trades.into_iter().enumerate() {
            staged.apply_signed_trade(&symbol, qty, price);
            if staged.portfolio.cash_usd < -1e-7 {
                return Err(reject("allocation_would_borrow"));
            }
            orders.push(LlmEquitySimulatedOrder {
                command_id: format!("{}-{}", decision.cycle_id, index + 1),
                cycle_id: decision.cycle_id.clone(),
                symbol,
                side: if qty > 0.0 {
                    LlmEquityOrderSide::Buy
                } else {
                    LlmEquityOrderSide::Sell
                },
                status: "accepted".into(),
                qty: qty.abs(),
                avg_price: price,
                notional_usd: (qty * price).abs(),
                reason: "atomic_fully_invested_rebalance".into(),
            });
        }
        let nav = staged.portfolio.nav_usd(&prices);
        if !(0.0..=0.01).contains(&staged.portfolio.cash_usd)
            || staged
                .portfolio
                .positions
                .values()
                .any(|p| p.quantity <= 0.0 || p.market_value(prices[&p.symbol]) > nav * 0.05 + 1e-6)
            || staged.portfolio.positions.len() < 20
        {
            return Err(reject("allocation_postcondition_failed"));
        }
        staged.portfolio.turnover_this_hour_usd += turnover;
        staged.turnover_today_usd += turnover;
        staged.portfolio.peak_nav_usd = staged.portfolio.peak_nav_usd.max(nav);
        staged.last_prices = prices.clone();
        staged.seen_cycle_ids.insert(decision.cycle_id.clone());
        staged.highest_sequence = decision.sequence;
        let result = LlmEquityApplyResult {
            cycle_id: decision.cycle_id.clone(),
            as_of: decision.as_of,
            accepted_orders: orders,
            rejected_orders: Vec::new(),
            portfolio: staged.portfolio.clone(),
            prices,
            turnover_today_usd: staged.turnover_today_usd,
        };
        *self = staged;
        Ok(result)
    }
}
