use serde::{Deserialize, Serialize};
use std::fmt;

pub const NO_STRATEGY_ID: u64 = 0;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssetClass {
    #[default]
    EquityEtf,
    EquityStock,
}

impl AssetClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EquityEtf => "equity_etf",
            Self::EquityStock => "equity_stock",
        }
    }
}

impl fmt::Display for AssetClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketVenue {
    #[default]
    UsEquitiesPaper,
}

impl MarketVenue {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UsEquitiesPaper => "us_equities_paper",
        }
    }
}

impl fmt::Display for MarketVenue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketSession {
    #[default]
    Rth,
    Premarket,
    AfterHours,
    Closed,
}

impl MarketSession {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rth => "rth",
            Self::Premarket => "premarket",
            Self::AfterHours => "after_hours",
            Self::Closed => "closed",
        }
    }
}

impl fmt::Display for MarketSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MarketContext {
    pub asset_class: AssetClass,
    pub venue: MarketVenue,
    pub session: MarketSession,
    pub strategy_id: u64,
}

impl MarketContext {
    pub const fn equity_paper(asset_class: AssetClass, strategy_id: u64) -> Self {
        Self {
            asset_class,
            venue: MarketVenue::UsEquitiesPaper,
            session: MarketSession::Rth,
            strategy_id,
        }
    }
}

impl Default for MarketContext {
    fn default() -> Self {
        Self::equity_paper(AssetClass::EquityEtf, NO_STRATEGY_ID)
    }
}

pub fn strategy_id_hash(strategy_id: &str) -> u64 {
    let value = strategy_id.trim();
    if value.is_empty() {
        return NO_STRATEGY_ID;
    }

    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    if hash == NO_STRATEGY_ID {
        1
    } else {
        hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_market_context_is_equity_paper() {
        let context = MarketContext::default();

        assert_eq!(context.asset_class, AssetClass::EquityEtf);
        assert_eq!(context.venue, MarketVenue::UsEquitiesPaper);
        assert_eq!(context.session, MarketSession::Rth);
        assert_eq!(context.strategy_id, NO_STRATEGY_ID);
    }
}
