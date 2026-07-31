//! The M5 exit criterion (#62): a reorg storm converges over the log, a second
//! consumer sees the same stream, and a killed consumer resumes from its offset.
//!
//! M4's #49 proved the in-process pipeline converges through a storm. This proves
//! the three things the *log* adds, against a real Redpanda + Postgres:
//!
//!   * convergence — the writer, consuming a storm of reverts and canonical data
//!     over the log, lands on exactly the state a sequential replay of that same
//!     stream produces (every revert fully undone, nothing double-counted);
//!   * fan-out — a second, independent consumer group reads the identical stream
//!     and converges to the same state, the property an in-memory channel cannot
//!     provide and the one that unlocks the PnL/alert consumers of M6+;
//!   * crash-resume — a writer killed mid-storm resumes from its committed offset
//!     with no gaps and no duplicates.
//!
//! The event stream is crafted rather than driven by a live producer: the
//! producer's emission is proved in #59, and detection reading the writer's
//! (lagging) DB over the log is a feedback loop that belongs to the producer, not
//! to the consumer-convergence this criterion measures. Convergence is a property
//! of the writer applying whatever revert/data stream the log delivers, in order
//! — so a deterministic crafted storm, checked against a sequential-replay oracle,
//! measures exactly that, without the flakiness of racing a live producer.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test kafka_storm -- --ignored --nocapture

use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chainscope_core::{build_transport, BlockUnit, Envelope, RowBatch, TransportSpec};
use chainscope_indexer::{
    consumer::Writer,
    db,
    testkit::{SyntheticChain, SYNTHETIC_POOL},
    transformer::{decode_block, Transformer},
};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use tokio::task::JoinHandle;

const START: u64 = 90;
const OLD_TIP: u64 = 130;

// ---- Redpanda / Postgres scaffolding -------------------------------------

fn brokers() -> String {
    std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string())
}
fn nonce() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
}
async fn create_topic(brokers: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    // One partition: the block-ordered writer needs total order, and the storm's
    // reverts must stay ordered against the data they undo.
    let new = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
    for r in admin.create_topics([&new], &AdminOptions::new()).await.expect("create") {
        r.expect("topic created");
    }
}

async fn admin_pool() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}
async fn fresh_db(admin: &PgPool, tag: &str) -> (PgPool, String) {
    let name = format!("chainscope_kstorm_{}_{}", std::process::id(), tag);
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(admin).await.unwrap();
    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    let pool = PgPoolOptions::new().max_connections(8).connect(url.as_str()).await.unwrap();
    db::migrate(&pool).await.unwrap();
    db::ensure_partitions(&pool).await.unwrap();
    for parent in ["swaps", "liq_events"] {
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {parent}_20260724 PARTITION OF {parent} \
             FOR VALUES FROM ('2026-07-24') TO ('2026-07-25')"
        ))
        .execute(&pool)
        .await
        .unwrap();
    }
    (pool, name)
}
async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
}

// ---- the crafted storm + its sequential-replay oracle --------------------

/// One reorg: fork at `fork`, rewrite `fork+1..=height` on `branch`.
struct Round {
    fork: u64,
    branch: u8,
    height: u64,
}

fn block_on(branch: u8, height: u64, n: u64) -> BlockUnit {
    SyntheticChain::branched(height, branch).unit(n)
}
fn swaps_of(block: &BlockUnit) -> Vec<String> {
    let (batch, _) = decode_block(block, &[SYNTHETIC_POOL].into_iter().collect());
    batch.swaps.iter().map(|s| hex::encode(s.tx_hash)).collect()
}

/// Build the storm event stream and, in lockstep, the oracle: a plain in-memory
/// map of block_number → swap tx hashes, updated by replaying each event
/// sequentially. `Data(b)` sets the block; `Revert{f}` drops every block above
/// `f`. Whatever the writer does over the log, it must match this replay.
fn build_storm() -> (Vec<Envelope<BlockUnit>>, BTreeMap<u64, Vec<String>>) {
    let mut events = Vec::new();
    let mut oracle: BTreeMap<u64, Vec<String>> = BTreeMap::new();

    let push_data = |events: &mut Vec<_>, oracle: &mut BTreeMap<u64, Vec<String>>, b: BlockUnit| {
        oracle.insert(b.number, swaps_of(&b));
        events.push(Envelope::Data(b));
    };

    // Clean branch-0 index, 90..=130.
    for n in START..=OLD_TIP {
        push_data(&mut events, &mut oracle, block_on(0, OLD_TIP, n));
    }

    // Reorgs of varying depth, each above the finalised line. Every round grows
    // the height by one, as a real storm does.
    let rounds = [
        Round { fork: 125, branch: 1, height: 131 },
        Round { fork: 118, branch: 2, height: 132 },
        Round { fork: 129, branch: 3, height: 133 },
        Round { fork: 110, branch: 4, height: 134 },
        Round { fork: 127, branch: 5, height: 135 },
        Round { fork: 120, branch: 6, height: 136 },
    ];
    for r in &rounds {
        // The correction: undo everything above the fork...
        events.push(Envelope::Revert { from_block: r.fork });
        oracle.retain(|&k, _| k <= r.fork);
        // ...then lay down the canonical branch above it.
        for n in (r.fork + 1)..=r.height {
            push_data(&mut events, &mut oracle, block_on(r.branch, r.height, n));
        }
    }
    (events, oracle)
}

fn top_block(oracle: &BTreeMap<u64, Vec<String>>) -> u64 {
    *oracle.keys().next_back().unwrap()
}

/// The oracle flattened to the same shape as `swap_rows`: (block_number, tx_hex),
/// ordered — every swap the crafted stream should leave behind.
fn oracle_rows(oracle: &BTreeMap<u64, Vec<String>>) -> Vec<(i64, String)> {
    let mut rows: Vec<(i64, String)> = oracle
        .iter()
        .flat_map(|(&n, txs)| txs.iter().map(move |t| (n as i64, t.clone())))
        .collect();
    rows.sort();
    rows
}
async fn swap_rows(pool: &PgPool) -> Vec<(i64, String)> {
    let mut rows: Vec<(i64, String)> =
        sqlx::query("SELECT block_number, encode(tx_hash,'hex') AS tx FROM swaps")
            .fetch_all(pool)
            .await
            .unwrap()
            .iter()
            .map(|r| (r.get::<i64, _>("block_number"), r.get::<String, _>("tx")))
            .collect();
    rows.sort();
    rows
}
async fn cursor(pool: &PgPool) -> Option<u64> {
    db::load_live_cursor(pool).await.unwrap()
}

/// Publish the whole event stream to the blocks topic, in order.
async fn publish_storm(brokers: &str, topic: &str, events: Vec<Envelope<BlockUnit>>) {
    let (sink, _s) = build_transport::<Envelope<BlockUnit>>(TransportSpec::Kafka {
        brokers,
        topic,
        group_id: "producer-unused",
    })
    .expect("build blocks sink");
    for e in events {
        sink.publish(e).await.expect("publish event");
    }
}

/// Spawn the transformer (blocks → rows) and writer (rows → Postgres) over the
/// log. Returned handles are aborted to stop them — a Kafka source has no
/// end-of-stream, so a test stops the stages by aborting the tasks once the work
/// is durably committed.
fn spawn_consumers(
    brokers: &str,
    blocks_topic: &str,
    rows_topic: &str,
    tf_group: &str,
    wr_group: &str,
    pool: &PgPool,
) -> (JoinHandle<anyhow::Result<()>>, JoinHandle<anyhow::Result<()>>) {
    let (row_sink, _rs_for_tf) = build_transport::<Envelope<RowBatch>>(TransportSpec::Kafka {
        brokers,
        topic: rows_topic,
        group_id: "tf-producer-unused",
    })
    .expect("rows sink");
    let (_bs, blocks_source) = build_transport::<Envelope<BlockUnit>>(TransportSpec::Kafka {
        brokers,
        topic: blocks_topic,
        group_id: tf_group,
    })
    .expect("blocks source");
    let transformer = Transformer::new(blocks_source, row_sink, vec![SYNTHETIC_POOL]);

    let (_rs, rows_source) = build_transport::<Envelope<RowBatch>>(TransportSpec::Kafka {
        brokers,
        topic: rows_topic,
        group_id: wr_group,
    })
    .expect("rows source");
    let writer = Writer::new(pool.clone(), rows_source, 8, Duration::from_millis(5), chainscope_indexer::pnl::Numeraire::disabled());

    (tokio::spawn(transformer.run()), tokio::spawn(writer.run()))
}

/// Wait until the live cursor reaches `target`, or fail.
async fn wait_for_cursor(pool: &PgPool, target: u64, within: Duration) {
    let deadline = Instant::now() + within;
    loop {
        if cursor(pool).await.unwrap_or(0) >= target {
            return;
        }
        assert!(Instant::now() < deadline, "cursor never reached {target}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ---- the exit criterion --------------------------------------------------

/// Convergence + fan-out: the writer lands on the oracle state, and a second
/// independent consumer group reads the identical stream and converges the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a running Redpanda and Postgres"]
async fn a_reorg_storm_over_the_log_converges_with_fanout() {
    let Some(admin) = admin_pool().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let brokers = brokers();
    let nonce = nonce();
    let blocks_topic = format!("chainscope.test.storm.blocks.{nonce}");
    let rows_topic = format!("chainscope.test.storm.rows.{nonce}");
    create_topic(&brokers, &blocks_topic).await;
    create_topic(&brokers, &rows_topic).await;

    let (events, oracle) = build_storm();
    let top = top_block(&oracle);
    publish_storm(&brokers, &blocks_topic, events).await;

    let (pool, name) = fresh_db(&admin, "converge").await;
    let (tf, wr) = spawn_consumers(
        &brokers,
        &blocks_topic,
        &rows_topic,
        &format!("tf-{nonce}"),
        &format!("wr-{nonce}"),
        &pool,
    );

    wait_for_cursor(&pool, top, Duration::from_secs(60)).await;
    // The cursor reaching the top means the last canonical block landed, and with
    // a single ordered partition that means every prior event was applied. Give
    // any final in-flight batch a beat, then stop the stages.
    tokio::time::sleep(Duration::from_millis(200)).await;
    tf.abort();
    wr.abort();

    // Convergence: the writer's swaps equal a sequential replay of the same stream.
    assert_eq!(
        swap_rows(&pool).await,
        oracle_rows(&oracle),
        "the writer must converge on the oracle after the storm"
    );

    // Fan-out: a second, independent consumer group reads the identical rows
    // stream from the start and, applying the same revert/data semantics, reaches
    // the identical state — what a channel could never offer a second reader.
    let (_s, mut fanout) = build_transport::<Envelope<RowBatch>>(TransportSpec::Kafka {
        brokers: &brokers,
        topic: &rows_topic,
        group_id: &format!("fanout-{nonce}"),
    })
    .expect("fanout source");
    let mut replay: BTreeMap<u64, Vec<String>> = BTreeMap::new();
    while let Ok(Ok(Some(d))) = tokio::time::timeout(Duration::from_secs(5), fanout.recv()).await {
        match d.payload {
            Envelope::Data(rb) => {
                replay.insert(rb.block_number, rb.swaps.iter().map(|s| hex::encode(s.tx_hash)).collect());
            }
            Envelope::Revert { from_block } => replay.retain(|&k, _| k <= from_block),
        }
    }
    assert_eq!(
        oracle_rows(&replay),
        oracle_rows(&oracle),
        "a second independent consumer must see the same complete stream and converge the same"
    );

    eprintln!(
        "storm over the log converged: {} swaps == oracle; a second consumer converged identically",
        oracle_rows(&oracle).len()
    );
    drop_db(&admin, pool, &name).await;
}

/// Crash-resume: a writer killed mid-storm and restarted under the same group
/// resumes from its committed offset and still converges — no gaps, no doubles.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a running Redpanda and Postgres"]
async fn a_killed_writer_resumes_from_its_offset() {
    let Some(admin) = admin_pool().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let brokers = brokers();
    let nonce = nonce();
    let blocks_topic = format!("chainscope.test.stormcrash.blocks.{nonce}");
    let rows_topic = format!("chainscope.test.stormcrash.rows.{nonce}");
    create_topic(&brokers, &blocks_topic).await;
    create_topic(&brokers, &rows_topic).await;

    let (events, oracle) = build_storm();
    let top = top_block(&oracle);
    publish_storm(&brokers, &blocks_topic, events).await;

    let (pool, name) = fresh_db(&admin, "crash").await;
    let wr_group = format!("wr-{nonce}");

    // First run: let the writer get partway, then kill it mid-storm.
    let (tf1, wr1) = spawn_consumers(
        &brokers,
        &blocks_topic,
        &rows_topic,
        &format!("tf-{nonce}"),
        &wr_group,
        &pool,
    );
    wait_for_cursor(&pool, OLD_TIP, Duration::from_secs(60)).await; // partway through
    wr1.abort(); // crash the writer mid-storm
    // Leave the transformer running so the rows topic keeps filling.

    // Restart the writer under the SAME group: it resumes from its committed
    // offset and finishes the storm.
    let (_rs, rows_source) = build_transport::<Envelope<RowBatch>>(TransportSpec::Kafka {
        brokers: &brokers,
        topic: &rows_topic,
        group_id: &wr_group,
    })
    .expect("rows source #2");
    let wr2 = tokio::spawn(
        Writer::new(
            pool.clone(),
            rows_source,
            8,
            Duration::from_millis(5),
            chainscope_indexer::pnl::Numeraire::disabled(),
        )
        .run(),
    );

    wait_for_cursor(&pool, top, Duration::from_secs(60)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    tf1.abort();
    wr2.abort();

    assert_eq!(
        swap_rows(&pool).await,
        oracle_rows(&oracle),
        "a writer killed mid-storm must resume from its offset and still converge — no gaps, no doubles"
    );

    eprintln!("crash-resume OK: writer killed mid-storm resumed from its offset and converged");
    drop_db(&admin, pool, &name).await;
}
