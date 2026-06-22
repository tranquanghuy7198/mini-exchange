//! Reusable Kafka producer/consumer layer for the saga (Task 6).
//!
//! - [`EventProducer`] serializes an [`EventEnvelope`] to JSON and publishes it
//!   to the payload's topic ([`SagaEvent::TOPIC`]), keyed by `order_id` so all
//!   events for one order land on the same partition and stay ordered.
//! - [`EventConsumer`] wraps a `StreamConsumer` with a consumer group, **manual
//!   offset commits** (commit only after the handler succeeds → at-least-once),
//!   and graceful shutdown.
//! - [`ensure_saga_topics`] is an idempotent admin step mirroring the
//!   `kafka-init` compose service.
//!
//! Brokers come from `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9094` for
//! host dev; set to `kafka:9092` inside compose).

use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::events::{topics, EventEnvelope, SagaEvent};

/// Read the broker list from `KAFKA_BOOTSTRAP_SERVERS` (loading `.env` first),
/// defaulting to the host-facing listener.
pub fn brokers_from_env() -> String {
    dotenvy::dotenv().ok();
    std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9094".to_string())
}

/// Idempotent JSON producer for saga events.
#[derive(Clone)]
pub struct EventProducer {
    inner: FutureProducer,
}

impl EventProducer {
    /// Build a producer for the given brokers (idempotent, `acks=all`).
    pub fn new(brokers: &str) -> anyhow::Result<Self> {
        let inner = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .create()?;
        Ok(Self { inner })
    }

    /// Build a producer from `KAFKA_BOOTSTRAP_SERVERS`.
    pub fn from_env() -> anyhow::Result<Self> {
        Self::new(&brokers_from_env())
    }

    /// Publish an event envelope to its topic, keyed by `order_id`.
    pub async fn send<T>(&self, event: &EventEnvelope<T>) -> anyhow::Result<()>
    where
        T: SagaEvent + Serialize,
    {
        let payload = serde_json::to_vec(event)?;
        let key = event.order_id.to_string();
        let record = FutureRecord::to(T::TOPIC)
            .payload(payload.as_slice())
            .key(key.as_str());

        self.inner
            .send(record, Timeout::After(Duration::from_secs(5)))
            .await
            .map_err(|(e, _)| anyhow::anyhow!("kafka send to {} failed: {e}", T::TOPIC))?;

        tracing::debug!(topic = T::TOPIC, key = %key, event = T::EVENT_TYPE, "event produced");
        Ok(())
    }
}

/// A consumed message in raw form. Use [`RawEvent::deserialize`] to decode the
/// envelope for the topic's payload type.
#[derive(Debug, Clone)]
pub struct RawEvent {
    pub topic: String,
    pub key: Option<String>,
    pub payload: Vec<u8>,
}

impl RawEvent {
    /// Decode the JSON envelope into `EventEnvelope<T>`.
    pub fn deserialize<T: DeserializeOwned>(&self) -> anyhow::Result<EventEnvelope<T>> {
        serde_json::from_slice(&self.payload)
            .map_err(|e| anyhow::anyhow!("failed to decode event on {}: {e}", self.topic))
    }
}

/// Consumer-group wrapper with manual commits and graceful shutdown.
pub struct EventConsumer {
    inner: StreamConsumer,
}

impl EventConsumer {
    /// Subscribe `group_id` to `topics` against `brokers`. Auto-commit is off;
    /// offsets are committed explicitly after successful handling.
    pub fn new(brokers: &str, group_id: &str, topics: &[&str]) -> anyhow::Result<Self> {
        let inner: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .create()?;
        inner.subscribe(topics)?;
        tracing::info!(group_id, ?topics, "kafka consumer subscribed");
        Ok(Self { inner })
    }

    /// Build a consumer from `KAFKA_BOOTSTRAP_SERVERS`.
    pub fn from_env(group_id: &str, topics: &[&str]) -> anyhow::Result<Self> {
        Self::new(&brokers_from_env(), group_id, topics)
    }

    /// Consume until shutdown (Ctrl-C / SIGTERM). For each message, invoke
    /// `handler`; commit the offset only when it returns `Ok` — on `Err` the
    /// message is left uncommitted and will be redelivered (at-least-once).
    pub async fn run<F, Fut>(&self, mut handler: F)
    where
        F: FnMut(RawEvent) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<()>>,
    {
        let shutdown = crate::signal::shutdown();
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!("consumer stopping");
                    break;
                }
                result = self.inner.recv() => match result {
                    Err(e) => tracing::error!(error = %e, "kafka recv error"),
                    Ok(msg) => {
                        let raw = RawEvent {
                            topic: msg.topic().to_string(),
                            key: msg.key().map(|k| String::from_utf8_lossy(k).into_owned()),
                            payload: msg.payload().map(<[u8]>::to_vec).unwrap_or_default(),
                        };
                        match handler(raw).await {
                            Ok(()) => {
                                if let Err(e) = self.inner.commit_message(&msg, CommitMode::Async) {
                                    tracing::error!(error = %e, "offset commit failed");
                                }
                            }
                            Err(e) => tracing::error!(
                                error = %e, topic = msg.topic(),
                                "handler failed; offset not committed (will retry)"
                            ),
                        }
                    }
                }
            }
        }
    }

    /// Receive and decode the next message of a known type, committing it.
    /// Convenience for single-topic consumers; for multi-topic use [`Self::run`].
    pub async fn recv_event<T: DeserializeOwned>(&self) -> anyhow::Result<EventEnvelope<T>> {
        let msg = self.inner.recv().await?;
        let env = serde_json::from_slice(msg.payload().unwrap_or_default())?;
        self.inner.commit_message(&msg, CommitMode::Async)?;
        Ok(env)
    }
}

/// Create the four saga topics if they don't already exist (3 partitions,
/// replication 1). Idempotent — existing topics are treated as success.
pub async fn ensure_saga_topics(brokers: &str) -> anyhow::Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()?;

    let names = [
        topics::ORDER_CREATED,
        topics::PRICE_QUOTED,
        topics::ORDER_EXECUTED,
        topics::ORDER_REJECTED,
    ];
    let new: Vec<NewTopic> = names
        .iter()
        .map(|n| NewTopic::new(n, 3, TopicReplication::Fixed(1)))
        .collect();

    for res in admin.create_topics(&new, &AdminOptions::new()).await? {
        match res {
            Ok(t) => tracing::info!(topic = %t, "topic ensured"),
            // Already-exists (and other per-topic results) are non-fatal here.
            Err((t, e)) => tracing::debug!(topic = %t, error = ?e, "create_topics result"),
        }
    }
    Ok(())
}
