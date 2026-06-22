//! Portfolio Service library — the saga state machine and DB access, exposed so
//! integration tests can drive it directly (the binary in `main.rs` wires these
//! to HTTP + Kafka).

pub mod repo;
pub mod saga;
