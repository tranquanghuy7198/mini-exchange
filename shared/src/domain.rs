//! Core domain types shared across services.
//!
//! Money/quantities use [`rust_decimal::Decimal`] (never floats — exact decimal
//! arithmetic). Ids are UUIDs; timestamps are UTC. All types derive `serde` and
//! serialize to JSON.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{AppError, AppResult};

/// A user / account identifier.
pub type UserId = Uuid;
/// An order identifier — also the saga correlation id.
pub type OrderId = Uuid;

/// A tradable symbol, normalized to trimmed upper-case (e.g. `BTC`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Symbol(String);

impl Symbol {
    pub fn new(s: impl Into<String>) -> Self {
        Symbol(s.into().trim().to_uppercase())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for Symbol {
    fn from(s: &str) -> Self {
        Symbol::new(s)
    }
}

/// Side of a market order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

/// Lifecycle status of an order as it moves through the saga.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderStatus {
    /// Persisted, `OrderCreated` emitted; awaiting a price quote.
    Created,
    /// Price received; balance/asset effects being applied.
    Priced,
    /// Successfully executed; portfolio updated.
    Executed,
    /// Terminated without execution (see [`RejectionReason`]).
    Rejected,
}

/// Why an order was rejected. Some reasons originate in the Market Service,
/// others in the Portfolio Service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RejectionReason {
    /// Symbol not listed by the Market Service.
    UnknownSymbol,
    /// Market Service could not provide a price (down / timeout).
    MarketUnavailable,
    /// BUY rejected: not enough cash.
    InsufficientBalance,
    /// SELL rejected: not enough of the asset held.
    InsufficientAsset,
    /// Order failed validation.
    InvalidOrder,
}

/// A (mocked) price quote for a symbol.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Price {
    pub symbol: Symbol,
    pub price: Decimal,
    pub as_of: DateTime<Utc>,
}

/// A held quantity of a single asset.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Holding {
    pub symbol: Symbol,
    pub quantity: Decimal,
}

/// A user's portfolio: free cash plus asset holdings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Portfolio {
    pub user_id: UserId,
    pub cash_balance: Decimal,
    pub holdings: Vec<Holding>,
}

/// A persisted order and its current saga state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Order {
    pub id: OrderId,
    pub user_id: UserId,
    pub symbol: Symbol,
    pub side: Side,
    pub quantity: Decimal,
    pub status: OrderStatus,
    /// Execution price, set once the order is priced/executed.
    pub price: Option<Decimal>,
    /// Human-readable rejection detail, set when `status == Rejected`.
    pub reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// The `POST /orders` request body — a market order request from a client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewOrder {
    pub user_id: UserId,
    pub symbol: Symbol,
    pub side: Side,
    pub quantity: Decimal,
}

impl NewOrder {
    /// Validate the request. Returns [`AppError::BadRequest`] on invalid input.
    pub fn validate(&self) -> AppResult<()> {
        if self.symbol.as_str().is_empty() {
            return Err(AppError::BadRequest("symbol must not be empty".into()));
        }
        if self.quantity <= Decimal::ZERO {
            return Err(AppError::BadRequest("quantity must be positive".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_is_normalized() {
        assert_eq!(Symbol::new("  btc ").as_str(), "BTC");
        assert_eq!(Symbol::from("eth").to_string(), "ETH");
    }

    #[test]
    fn side_and_status_serialize_uppercase() {
        assert_eq!(serde_json::to_string(&Side::Buy).unwrap(), "\"BUY\"");
        assert_eq!(serde_json::to_string(&Side::Sell).unwrap(), "\"SELL\"");
        assert_eq!(
            serde_json::to_string(&OrderStatus::Executed).unwrap(),
            "\"EXECUTED\""
        );
    }

    #[test]
    fn rejection_reason_is_screaming_snake() {
        assert_eq!(
            serde_json::to_string(&RejectionReason::InsufficientBalance).unwrap(),
            "\"INSUFFICIENT_BALANCE\""
        );
    }

    #[test]
    fn new_order_validation() {
        let mut o = NewOrder {
            user_id: Uuid::nil(),
            symbol: Symbol::from("BTC"),
            side: Side::Buy,
            quantity: Decimal::new(5, 1), // 0.5
        };
        assert!(o.validate().is_ok());

        o.quantity = Decimal::ZERO;
        assert!(o.validate().is_err());

        o.quantity = Decimal::ONE;
        o.symbol = Symbol::from("");
        assert!(o.validate().is_err());
    }
}
