use std::error::Error;
use std::fmt;

pub const MAX_ORDER_NOTIONAL_USD: f64 = 1_000.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Venue {
    UsEquitiesPaper,
}

impl Venue {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UsEquitiesPaper => "us_equities_paper",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecAlgo {
    HourlyLlmSimulated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoutingDecision {
    pub venue: Venue,
    pub algo: ExecAlgo,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RoutingRequest<'a> {
    pub symbol: &'a str,
    pub order_notional_usd: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RoutingError {
    EmptySymbol,
    InvalidNotional,
    OrderTooLarge { max_usd: f64, requested_usd: f64 },
}

impl fmt::Display for RoutingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptySymbol => write!(f, "symbol must not be empty"),
            Self::InvalidNotional => write!(f, "order notional must be finite and non-negative"),
            Self::OrderTooLarge {
                max_usd,
                requested_usd,
            } => write!(
                f,
                "order requested {requested_usd} USD but equity dry-run cap is {max_usd} USD"
            ),
        }
    }
}

impl Error for RoutingError {}

pub fn route(request: RoutingRequest<'_>) -> Result<RoutingDecision, RoutingError> {
    if request.symbol.trim().is_empty() {
        return Err(RoutingError::EmptySymbol);
    }
    if !(request.order_notional_usd.is_finite() && request.order_notional_usd >= 0.0) {
        return Err(RoutingError::InvalidNotional);
    }
    if request.order_notional_usd > MAX_ORDER_NOTIONAL_USD {
        return Err(RoutingError::OrderTooLarge {
            max_usd: MAX_ORDER_NOTIONAL_USD,
            requested_usd: request.order_notional_usd,
        });
    }
    Ok(RoutingDecision {
        venue: Venue::UsEquitiesPaper,
        algo: ExecAlgo::HourlyLlmSimulated,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_uses_equity_paper_venue_only() {
        let decision = route(RoutingRequest {
            symbol: "SPY",
            order_notional_usd: 750.0,
        })
        .expect("equity paper route accepted");

        assert_eq!(decision.venue, Venue::UsEquitiesPaper);
        assert_eq!(decision.algo, ExecAlgo::HourlyLlmSimulated);
    }

    #[test]
    fn route_rejects_oversized_orders() {
        assert!(matches!(
            route(RoutingRequest {
                symbol: "SPY",
                order_notional_usd: 1_001.0,
            }),
            Err(RoutingError::OrderTooLarge { .. })
        ));
    }
}
