//! Mocked price engine for the Market Service.
//!
//! A static catalog of listed symbols with base prices; each quote applies a
//! small bounded-random jitter (±`band_bps` basis points) so prices move on
//! every request. A configurable set of symbols can be forced to "unavailable"
//! to exercise market-failure handling in the saga (env `MARKET_FAIL_SYMBOLS`,
//! comma-separated).

use std::collections::{HashMap, HashSet};

use chrono::Utc;
use rand::Rng;
use rust_decimal::Decimal;
use shared::domain::{Price, Symbol};

/// Outcome of a price lookup.
pub enum Quote {
    /// Symbol is listed and priced.
    Priced(Decimal),
    /// Symbol is not listed.
    Unknown,
    /// Symbol is listed but pricing is (simulated) unavailable.
    Unavailable,
}

pub struct PriceEngine {
    bases: HashMap<Symbol, Decimal>,
    failing: HashSet<Symbol>,
    band_bps: i64,
}

impl PriceEngine {
    /// Build from the default catalog, reading `MARKET_FAIL_SYMBOLS` for symbols
    /// that should simulate market unavailability.
    pub fn from_env() -> Self {
        let failing = std::env::var("MARKET_FAIL_SYMBOLS")
            .ok()
            .map(|s| {
                s.split(',')
                    .map(str::trim)
                    .filter(|x| !x.is_empty())
                    .map(Symbol::from)
                    .collect()
            })
            .unwrap_or_default();
        Self {
            bases: default_catalog(),
            failing,
            band_bps: 200, // ±2%
        }
    }

    /// All listed symbols, sorted.
    pub fn symbols(&self) -> Vec<Symbol> {
        let mut v: Vec<Symbol> = self.bases.keys().cloned().collect();
        v.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        v
    }

    /// Quote a single symbol.
    pub fn quote(&self, symbol: &Symbol) -> Quote {
        if self.failing.contains(symbol) {
            return Quote::Unavailable;
        }
        match self.bases.get(symbol) {
            None => Quote::Unknown,
            Some(base) => Quote::Priced(self.jitter(*base)),
        }
    }

    /// Build a [`Price`] for a symbol, stamped with the current time. `None`
    /// when the symbol is unknown or unavailable.
    pub fn price(&self, symbol: &Symbol) -> Option<Price> {
        match self.quote(symbol) {
            Quote::Priced(price) => Some(Price {
                symbol: symbol.clone(),
                price,
                as_of: Utc::now(),
            }),
            _ => None,
        }
    }

    /// Current prices for all listed (non-failing) symbols.
    pub fn all_prices(&self) -> Vec<Price> {
        self.symbols()
            .iter()
            .filter_map(|s| self.price(s))
            .collect()
    }

    /// Apply ±`band_bps` jitter to a base price, rounded to 2 dp.
    fn jitter(&self, base: Decimal) -> Decimal {
        let bps = rand::thread_rng().gen_range(-self.band_bps..=self.band_bps);
        // factor = (10000 + bps) / 10000, e.g. bps=123 -> 1.0123
        let factor = Decimal::new(10_000 + bps, 4);
        (base * factor).round_dp(2)
    }
}

/// The listed symbols and their base prices.
fn default_catalog() -> HashMap<Symbol, Decimal> {
    [
        ("BTC", Decimal::new(6_000_000, 2)), // 60000.00
        ("ETH", Decimal::new(300_000, 2)),   //  3000.00
        ("SOL", Decimal::new(15_000, 2)),    //   150.00
        ("ADA", Decimal::new(45, 2)),        //     0.45
        ("DOGE", Decimal::new(15, 2)),       //     0.15
    ]
    .into_iter()
    .map(|(s, p)| (Symbol::from(s), p))
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_symbol_is_priced_within_band() {
        let engine = PriceEngine::from_env();
        let base = Decimal::new(6_000_000, 2);
        for _ in 0..50 {
            match engine.quote(&Symbol::from("BTC")) {
                Quote::Priced(p) => {
                    let lo = base * Decimal::new(9_800, 4); // -2%
                    let hi = base * Decimal::new(10_200, 4); // +2%
                    assert!(p >= lo && p <= hi, "price {p} out of band");
                }
                _ => panic!("BTC should be priced"),
            }
        }
    }

    #[test]
    fn unknown_symbol_is_unknown() {
        let engine = PriceEngine::from_env();
        assert!(matches!(
            engine.quote(&Symbol::from("NOPE")),
            Quote::Unknown
        ));
    }
}
