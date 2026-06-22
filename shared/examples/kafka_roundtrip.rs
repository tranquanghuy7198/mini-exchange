//! End-to-end check of the shared Kafka client against a running broker.
//!
//!   docker compose up -d kafka kafka-init
//!   cargo run -p shared --example kafka_roundtrip
//!
//! Produces an `OrderCreated` envelope and consumes it back, asserting the
//! round-trip preserves the event. Uses a fresh consumer group each run so it
//! reads from the beginning; filters by the just-produced `order_id`.

use rust_decimal::Decimal;
use shared::domain::{Side, Symbol};
use shared::events::{EventEnvelope, OrderCreated, OrderCreatedEvent, SagaEvent};
use shared::kafka::{brokers_from_env, ensure_saga_topics, EventConsumer, EventProducer};
use std::time::Duration;
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    shared::telemetry::init();
    let brokers = brokers_from_env();
    println!("brokers = {brokers}");

    ensure_saga_topics(&brokers).await?;

    let order_id = Uuid::new_v4();
    let sent = EventEnvelope::new(
        order_id,
        OrderCreated {
            user_id: Uuid::new_v4(),
            symbol: Symbol::from("BTC"),
            side: Side::Buy,
            quantity: Decimal::new(25, 1), // 2.5
        },
    );

    let producer = EventProducer::new(&brokers)?;
    producer.send(&sent).await?;
    println!("produced OrderCreated order_id={order_id}");

    // Unique group so we read history; scan until we find our event.
    let group = format!("roundtrip-{}", Uuid::new_v4());
    let consumer = EventConsumer::new(&brokers, &group, &[OrderCreated::TOPIC])?;

    let found = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let env: OrderCreatedEvent = consumer.recv_event().await?;
            if env.order_id == order_id {
                return anyhow::Ok(env);
            }
        }
    })
    .await??;

    assert_eq!(found, sent, "round-tripped event must match");
    println!(
        "consumed matching event: order_id={} symbol={} side={:?} qty={}",
        found.order_id, found.payload.symbol, found.payload.side, found.payload.quantity
    );
    println!("OK: Kafka round-trip succeeded");
    Ok(())
}
