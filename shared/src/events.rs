//! Kafka event contracts for the SAGA choreography.
//!
//! Every event is wrapped in an [`EventEnvelope`] carrying a unique `event_id`
//! (used for idempotent consumption), the `order_id` saga correlation id, a
//! schema `version`, and `occurred_at`. Events are serialized as **JSON**.
//! Each saga event type maps 1:1 to a Kafka topic (see [`topics`]); messages are
//! keyed by `order_id` so all events for one order share a partition and stay
//! ordered.
//!
//! ## Producer / consumer map
//!
//! | Topic            | Payload         | Produced by         | Consumed by                  |
//! |------------------|-----------------|---------------------|------------------------------|
//! | `order-created`  | [`OrderCreated`]| Portfolio Service   | Market Service, Audit Service|
//! | `price-quoted`   | [`PriceQuoted`] | Market Service      | Portfolio Service, Audit     |
//! | `order-executed` | [`OrderExecuted`]| Portfolio Service  | Audit Service                |
//! | `order-rejected` | [`OrderRejected`]| Market or Portfolio | Portfolio Service, Audit     |
//!
//! Flow (happy path): Portfolio emits `OrderCreated` → Market prices it and emits
//! `PriceQuoted` → Portfolio applies balance/asset effects and emits
//! `OrderExecuted`. On any failure, the responsible service emits `OrderRejected`;
//! Portfolio consumes it to run compensation (e.g. release a cash reservation).

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{OrderId, RejectionReason, Side, Symbol, UserId};

/// Kafka topic names. Mirror the topics created by `kafka-init` (Task 1).
pub mod topics {
    pub const ORDER_CREATED: &str = "order-created";
    pub const PRICE_QUOTED: &str = "price-quoted";
    pub const ORDER_EXECUTED: &str = "order-executed";
    pub const ORDER_REJECTED: &str = "order-rejected";
}

/// Current schema version stamped onto newly produced events.
pub const SCHEMA_VERSION: u16 = 1;

/// Envelope wrapping every saga event with routing/idempotency metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope<T> {
    /// Globally unique id for this event instance (dedupe key for consumers).
    pub event_id: Uuid,
    /// Saga correlation id (the order this event concerns). Also the Kafka key.
    pub order_id: OrderId,
    /// Schema version of the payload.
    pub version: u16,
    /// When the event was produced.
    pub occurred_at: DateTime<Utc>,
    /// The event-specific data.
    pub payload: T,
}

impl<T: SagaEvent> EventEnvelope<T> {
    /// Wrap `payload` for `order_id`, generating a fresh `event_id`/timestamp
    /// and stamping the current [`SCHEMA_VERSION`].
    pub fn new(order_id: OrderId, payload: T) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            order_id,
            version: SCHEMA_VERSION,
            occurred_at: Utc::now(),
            payload,
        }
    }

    /// The Kafka topic this envelope's payload belongs to.
    pub fn topic(&self) -> &'static str {
        T::TOPIC
    }
}

/// Binds a saga payload type to its Kafka topic and a stable type name.
pub trait SagaEvent {
    /// Topic the event is published to / consumed from.
    const TOPIC: &'static str;
    /// Stable, human-readable type name (used in audit records / logs).
    const EVENT_TYPE: &'static str;
}

/// An order was accepted and persisted; awaiting pricing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderCreated {
    pub user_id: UserId,
    pub symbol: Symbol,
    pub side: Side,
    pub quantity: Decimal,
}

/// The Market Service quoted a price for the order's symbol.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PriceQuoted {
    pub symbol: Symbol,
    pub price: Decimal,
}

/// The order executed and the portfolio was updated.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderExecuted {
    pub user_id: UserId,
    pub symbol: Symbol,
    pub side: Side,
    pub quantity: Decimal,
    pub price: Decimal,
}

/// The order was terminated without executing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderRejected {
    pub reason: RejectionReason,
    /// Human-readable detail for diagnostics/audit.
    pub detail: String,
}

impl SagaEvent for OrderCreated {
    const TOPIC: &'static str = topics::ORDER_CREATED;
    const EVENT_TYPE: &'static str = "OrderCreated";
}
impl SagaEvent for PriceQuoted {
    const TOPIC: &'static str = topics::PRICE_QUOTED;
    const EVENT_TYPE: &'static str = "PriceQuoted";
}
impl SagaEvent for OrderExecuted {
    const TOPIC: &'static str = topics::ORDER_EXECUTED;
    const EVENT_TYPE: &'static str = "OrderExecuted";
}
impl SagaEvent for OrderRejected {
    const TOPIC: &'static str = topics::ORDER_REJECTED;
    const EVENT_TYPE: &'static str = "OrderRejected";
}

/// Convenience envelope aliases per event type.
pub type OrderCreatedEvent = EventEnvelope<OrderCreated>;
pub type PriceQuotedEvent = EventEnvelope<PriceQuoted>;
pub type OrderExecutedEvent = EventEnvelope<OrderExecuted>;
pub type OrderRejectedEvent = EventEnvelope<OrderRejected>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_roundtrips_as_json() {
        let order_id = Uuid::new_v4();
        let env = EventEnvelope::new(
            order_id,
            OrderCreated {
                user_id: Uuid::new_v4(),
                symbol: Symbol::from("BTC"),
                side: Side::Buy,
                quantity: Decimal::new(15, 1), // 1.5
            },
        );

        let json = serde_json::to_string(&env).unwrap();
        let back: OrderCreatedEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(env, back);
        assert_eq!(back.order_id, order_id);
        assert_eq!(back.version, SCHEMA_VERSION);
        assert_eq!(env.topic(), "order-created");
    }

    #[test]
    fn topic_mapping_is_stable() {
        assert_eq!(OrderCreated::TOPIC, "order-created");
        assert_eq!(PriceQuoted::TOPIC, "price-quoted");
        assert_eq!(OrderExecuted::TOPIC, "order-executed");
        assert_eq!(OrderRejected::TOPIC, "order-rejected");
    }

    #[test]
    fn rejected_event_serializes_reason() {
        let env = EventEnvelope::new(
            Uuid::new_v4(),
            OrderRejected {
                reason: RejectionReason::InsufficientBalance,
                detail: "needed 100, had 40".into(),
            },
        );
        let json = serde_json::to_string(&env).unwrap();
        assert!(json.contains("INSUFFICIENT_BALANCE"));
    }
}
