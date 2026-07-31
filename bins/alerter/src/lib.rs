//! chainscope alert engine.
//!
//! A separate process that reads the indexed data and pings Telegram on
//! smart-money moves, cluster buys and fresh pools. Kept out of the indexer so a
//! slow or failing third-party call can never backpressure ingestion; it is the
//! independent-reader shape M5 proved. It polls Postgres today; the same binary
//! becomes a log consumer in the Kafka phase.

pub mod config;
pub mod db;
pub mod engine;
pub mod notify;

pub use engine::Alerter;
pub use notify::Notifier;
