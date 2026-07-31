//! #61: a chain-wide revert is broadcast to *every* partition.
//!
//! Topics are partitioned by pool for per-pool ordering, but a reorg is defined
//! by block number and its orphaned blocks hold trades scattered across every
//! partition — so one revert has to reach all of them, or some pool's consumer
//! would never undo its orphaned rows. This asserts, against a live Redpanda,
//! that `broadcast` puts exactly one copy of the revert on each partition.
//!
//!   docker compose up -d redpanda
//!   cargo test -p chainscope-indexer --test kafka_broadcast -- --ignored --nocapture
//!
//! Broker defaults to `localhost:9092`; override with `KAFKA_BROKERS`.

use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chainscope_core::{build_transport, BlockUnit, Envelope, TransportSpec};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
};

const PARTITIONS: i32 = 4;

fn brokers() -> String {
    std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string())
}
fn nonce() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
}

async fn create_topic(brokers: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    let new = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
    for r in admin
        .create_topics([&new], &AdminOptions::new())
        .await
        .expect("create_topics call")
    {
        r.expect("topic created");
    }
}

fn block(n: u64) -> BlockUnit {
    let mut hash = [0u8; 32];
    hash[..8].copy_from_slice(&n.to_be_bytes());
    BlockUnit {
        number: n,
        hash,
        parent_hash: [0u8; 32],
        timestamp: 1_700_000_000 + n as i64,
        logs: vec![],
    }
}

#[tokio::test]
#[ignore = "requires a running Redpanda"]
async fn a_revert_is_broadcast_to_every_partition() {
    let brokers = brokers();
    let nonce = nonce();
    let topic = format!("chainscope.test.broadcast.{nonce}");
    create_topic(&brokers, &topic, PARTITIONS).await;

    let (sink, _s) = build_transport::<Envelope<BlockUnit>>(TransportSpec::Kafka {
        brokers: &brokers,
        topic: &topic,
        group_id: "producer-unused",
    })
    .expect("build sink");

    // Some ordinary data first, keyed by block number so it scatters across the
    // partitions — the "trades in many pools" a reorg would orphan.
    for n in 1..=8 {
        sink.publish(Envelope::Data(block(n))).await.expect("publish data");
    }
    // The correction: one broadcast.
    sink.broadcast(Envelope::Revert { from_block: 3 })
        .await
        .expect("broadcast revert");

    // Drain the whole topic and record which partitions carried a revert.
    let (_sink, mut source) = build_transport::<Envelope<BlockUnit>>(TransportSpec::Kafka {
        brokers: &brokers,
        topic: &topic,
        group_id: &format!("chainscope-broadcast-{nonce}"),
    })
    .expect("build source");

    let mut revert_partitions: HashSet<u32> = HashSet::new();
    let mut reverts = 0;
    // Read until the topic goes quiet for a beat — everything published is
    // already there, so a short idle means we have seen it all.
    while let Ok(Ok(Some(delivery))) =
        tokio::time::timeout(Duration::from_secs(3), source.recv()).await
    {
        if let Envelope::Revert { from_block } = delivery.payload {
            assert_eq!(from_block, 3, "the broadcast copies carry the same fork point");
            reverts += 1;
            revert_partitions.insert(delivery.receipt.stream);
        }
    }

    assert_eq!(
        reverts, PARTITIONS,
        "one revert copy per partition — {PARTITIONS} expected, got {reverts}"
    );
    assert_eq!(
        revert_partitions,
        (0..PARTITIONS as u32).collect::<HashSet<_>>(),
        "every partition must carry a copy — no pool's consumer is left unaware"
    );
}
