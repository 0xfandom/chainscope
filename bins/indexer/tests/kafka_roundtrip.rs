//! The #57 acceptance test: a payload published through the Kafka `EventSink`
//! comes back out of the Kafka `EventSource` byte-identically, for both seams.
//!
//! This runs the *real* `build_transport` path against a *real* Redpanda — the
//! same code `main` wires up — not a mock. It is `#[ignore]`d like the Postgres
//! tests, because it needs a broker running:
//!
//!   docker compose up -d redpanda
//!   cargo test -p chainscope-indexer --test kafka_roundtrip -- --ignored --nocapture
//!
//! The broker defaults to `localhost:9092`; override with `KAFKA_BROKERS`. Each
//! run uses a throwaway topic and a throwaway consumer group, so runs never see
//! each other's records and the retained log never pollutes the assertion.

use std::{
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bigdecimal::BigDecimal;
use chainscope_core::{
    build_transport, BlockUnit, LiqKind, LiqRow, RawLog, RowBatch, SwapRow, TransportSpec,
};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
};

/// Broker list, honouring `KAFKA_BROKERS` so this can point at a non-default
/// Redpanda without editing the test.
fn brokers() -> String {
    std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string())
}

/// A per-run nonce, so the topic and group are unique to this invocation.
fn nonce() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

/// Create a single-partition throwaway topic. One partition so the round-trip
/// order is total and the assertion is exact, not set-wise.
async fn create_topic(brokers: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    let new = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
    let results = admin
        .create_topics([&new], &AdminOptions::new())
        .await
        .expect("create_topics call");
    for r in results {
        r.expect("topic created");
    }
}

#[tokio::test]
#[ignore = "requires a running Redpanda"]
async fn a_block_unit_round_trips_through_kafka() {
    let brokers = brokers();
    let nonce = nonce();
    let topic = format!("chainscope.test.blocks.{nonce}");
    let group = format!("chainscope-test-{nonce}");
    create_topic(&brokers, &topic).await;

    let (sink, mut source) = build_transport::<BlockUnit>(TransportSpec::Kafka {
        brokers: &brokers,
        topic: &topic,
        group_id: &group,
    })
    .expect("build kafka transport");

    let unit = BlockUnit {
        number: 21_000_123,
        hash: [0xab; 32],
        parent_hash: [0xcd; 32],
        timestamp: 1_700_000_000,
        logs: vec![RawLog {
            address: [0x11; 20],
            topics: vec![[0x22; 32], [0x33; 32]],
            data: vec![1, 2, 3, 4, 5, 250, 251, 252],
            block_number: 21_000_123,
            tx_hash: [0x44; 32],
            log_index: 7,
        }],
    };

    sink.publish(unit.clone()).await.expect("publish");

    let got = tokio::time::timeout(Duration::from_secs(20), source.recv())
        .await
        .expect("recv did not time out")
        .expect("recv ok")
        .expect("a record, not end-of-stream");

    assert_eq!(
        got.payload, unit,
        "a BlockUnit must survive Redpanda byte-identically"
    );
    // One partition, first record: partition 0, offset 0.
    assert_eq!(got.receipt.stream, 0, "single partition");
    assert_eq!(got.receipt.position, 0, "first offset");

    source.ack(got.receipt).await.expect("ack");
}

#[tokio::test]
#[ignore = "requires a running Redpanda"]
async fn a_row_batch_round_trips_through_kafka() {
    let brokers = brokers();
    let nonce = nonce();
    let topic = format!("chainscope.test.rows.{nonce}");
    let group = format!("chainscope-test-{nonce}");
    create_topic(&brokers, &topic).await;

    let (sink, mut source) = build_transport::<RowBatch>(TransportSpec::Kafka {
        brokers: &brokers,
        topic: &topic,
        group_id: &group,
    })
    .expect("build kafka transport");

    // The values that make this worth testing: signed int256-scale amounts and a
    // uint160 price that overflow every Rust integer, carried as BigDecimal.
    // If the wire format mangled the NUMERICs, this assertion is where it shows.
    let batch = RowBatch {
        block_number: 21_000_456,
        block_hash: [0x01; 32],
        parent_hash: [0x02; 32],
        block_time: 1_700_000_500,
        swaps: vec![SwapRow {
            tx_hash: [0x0a; 32],
            log_index: 2,
            pool: [0x0b; 20],
            sender: [0x0c; 20],
            recipient: [0x0d; 20],
            amount0: BigDecimal::from_str("-123456789012345678901234567890").unwrap(),
            amount1: BigDecimal::from_str("987654321098765432109876543210").unwrap(),
            sqrt_price_x96: BigDecimal::from_str(
                "1461446703485210103287273052203988822378723970342",
            )
            .unwrap(),
            liquidity: BigDecimal::from_str("340282366920938463463374607431768211455").unwrap(),
            tick: -887272,
        }],
        liq_events: vec![LiqRow {
            tx_hash: [0x0e; 32],
            log_index: 4,
            pool: [0x0b; 20],
            kind: LiqKind::Mint,
            owner: [0x0f; 20],
            tick_lower: -600,
            tick_upper: 600,
            amount: BigDecimal::from_str("42").unwrap(),
            amount0: BigDecimal::from_str("0").unwrap(),
            amount1: BigDecimal::from_str("1000000000000000000").unwrap(),
        }],
    };

    sink.publish(batch.clone()).await.expect("publish");

    let got = tokio::time::timeout(Duration::from_secs(20), source.recv())
        .await
        .expect("recv did not time out")
        .expect("recv ok")
        .expect("a record, not end-of-stream");

    assert_eq!(
        got.payload, batch,
        "a RowBatch, BigDecimals and all, must survive Redpanda byte-identically"
    );
    source.ack(got.receipt).await.expect("ack");
}
