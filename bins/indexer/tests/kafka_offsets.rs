//! The #58 acceptance: the committed offset is the durable cursor. A consumer
//! that acks then "crashes" (drops) resumes, under the same group, from exactly
//! the record after the last one it acked — not from the start, and not skipping
//! the unacked tail. This is the log's replacement for `chain_state.live_cursor`.
//!
//!   docker compose up -d redpanda
//!   cargo test -p chainscope-indexer --test kafka_offsets -- --ignored --nocapture
//!
//! Broker defaults to `localhost:9092`; override with `KAFKA_BROKERS`. One
//! partition so offsets are a single 0..N sequence and the resume point is exact.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chainscope_core::{build_transport, BlockUnit, TransportSpec};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
};

fn brokers() -> String {
    std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string())
}

fn nonce() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

async fn create_topic(brokers: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    let new = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
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

async fn next(source: &mut Box<dyn chainscope_core::EventSource<BlockUnit>>) -> BlockUnit {
    tokio::time::timeout(Duration::from_secs(20), source.recv())
        .await
        .expect("recv did not time out")
        .expect("recv ok")
        .expect("a record, not end-of-stream")
        .payload
}

#[tokio::test]
#[ignore = "requires a running Redpanda"]
async fn a_committed_offset_resumes_after_the_last_ack() {
    let brokers = brokers();
    let nonce = nonce();
    let topic = format!("chainscope.test.offsets.{nonce}");
    let group = format!("chainscope-consumer-{nonce}");
    create_topic(&brokers, &topic).await;

    // Publish blocks 1..=5 to the single partition, in order → offsets 0..4.
    let (sink, _s) = build_transport::<BlockUnit>(TransportSpec::Kafka {
        brokers: &brokers,
        topic: &topic,
        group_id: "producer-unused",
    })
    .expect("build sink");
    for n in 1..=5 {
        sink.publish(block(n)).await.expect("publish");
    }

    // Consumer #1 (group G): read and ack the first three (offsets 0,1,2), then
    // drop it — the in-process equivalent of a crash, mid-stream.
    {
        let (_sink, mut source) = build_transport::<BlockUnit>(TransportSpec::Kafka {
            brokers: &brokers,
            topic: &topic,
            group_id: &group,
        })
        .expect("build source #1");
        for expected in 1..=3 {
            let got = next(&mut source).await;
            assert_eq!(got.number, expected, "consumer #1 reads in order from the head");
            source
                .ack(chainscope_core::Receipt {
                    stream: 0,
                    position: expected - 1, // offset of block n is n-1
                })
                .await
                .expect("ack commits the offset");
        }
        // source dropped here → leaves the group with offset 3 committed.
    }

    // Consumer #2, SAME group: must resume at block 4 (offset 3), the record
    // right after the last ack — proving the committed offset is the cursor.
    let (_sink, mut source2) = build_transport::<BlockUnit>(TransportSpec::Kafka {
        brokers: &brokers,
        topic: &topic,
        group_id: &group,
    })
    .expect("build source #2");
    let resumed = next(&mut source2).await;
    assert_eq!(
        resumed.number, 4,
        "resume must start at the record after the last committed offset, not replay acked work"
    );
    let then = next(&mut source2).await;
    assert_eq!(then.number, 5, "and continue through the unacked tail");
}

#[tokio::test]
#[ignore = "requires a running Redpanda"]
async fn a_fresh_group_reads_the_whole_log_from_the_start() {
    // The flip side: acking in one group must not move any other group's cursor.
    // A brand-new group with no committed offset reads from the earliest record.
    let brokers = brokers();
    let nonce = nonce();
    let topic = format!("chainscope.test.offsets.fresh.{nonce}");
    create_topic(&brokers, &topic).await;

    let (sink, _s) = build_transport::<BlockUnit>(TransportSpec::Kafka {
        brokers: &brokers,
        topic: &topic,
        group_id: "producer-unused",
    })
    .expect("build sink");
    for n in 1..=3 {
        sink.publish(block(n)).await.expect("publish");
    }

    let (_sink, mut source) = build_transport::<BlockUnit>(TransportSpec::Kafka {
        brokers: &brokers,
        topic: &topic,
        group_id: &format!("chainscope-fresh-{nonce}"),
    })
    .expect("build source");
    assert_eq!(next(&mut source).await.number, 1, "a fresh group starts at the head");
}
