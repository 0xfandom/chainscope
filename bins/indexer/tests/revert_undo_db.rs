//! #60: the writer undoes its own state when it reads a `Revert`, idempotently.
//!
//! There is no central rewind under the log — the correction is broadcast and
//! each consumer undoes only what it built. This drives the *real* `Writer` with
//! a stream of envelopes (orphans already written, then a revert, then the
//! canonical branch) and asserts it lands on exactly the canonical state, and
//! that a redelivered revert changes nothing.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test revert_undo_db -- --ignored --nocapture

use std::collections::HashSet;
use std::time::Duration;

use chainscope_core::{types::Address20, Envelope, RowBatch};
use chainscope_indexer::{
    consumer::Writer,
    db,
    testkit::{SyntheticChain, SYNTHETIC_POOL},
    transformer::decode_block,
};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

const FIRST: u64 = 90;
const OLD_TIP: u64 = 130;
const HEAD: u64 = 300;
const FORK: u64 = 125;

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool, tag: &str) -> (PgPool, String) {
    let name = format!("chainscope_undo_{}_{}", std::process::id(), tag);
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#))
        .execute(admin)
        .await
        .ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(admin).await.unwrap();
    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    let pool = PgPoolOptions::new().max_connections(4).connect(url.as_str()).await.unwrap();
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

fn watched() -> HashSet<Address20> {
    [SYNTHETIC_POOL].into_iter().collect()
}

/// One decodable RowBatch for block `n` on `branch` (0 = original, 1 = canonical).
fn batch_on(branch: u8, n: u64) -> RowBatch {
    let chain = SyntheticChain::branched(HEAD, branch);
    decode_block(&chain.unit(n), &watched()).0
}

/// Write the branch-0 chain `[FIRST, OLD_TIP]` through the real write path.
async fn seed_branch0(pool: &PgPool) {
    for n in FIRST..=OLD_TIP {
        db::write_row_batches(pool, &[batch_on(0, n)], false).await.unwrap();
    }
}

async fn block_hash(pool: &PgPool, n: u64) -> Option<Vec<u8>> {
    sqlx::query("SELECT block_hash FROM blocks WHERE number = $1")
        .bind(n as i64)
        .fetch_optional(pool)
        .await
        .unwrap()
        .map(|r| r.get::<Vec<u8>, _>("block_hash"))
}
async fn max_block(pool: &PgPool) -> Option<i64> {
    sqlx::query("SELECT max(number) AS m FROM blocks").fetch_one(pool).await.unwrap().get("m")
}
async fn swaps_above(pool: &PgPool, n: u64) -> i64 {
    sqlx::query("SELECT count(*) FROM swaps WHERE block_number > $1")
        .bind(n as i64)
        .fetch_one(pool)
        .await
        .unwrap()
        .get(0)
}
async fn cursor(pool: &PgPool) -> Option<u64> {
    db::load_live_cursor(pool).await.unwrap()
}

/// The writer, fed `[Revert, Revert, canonical…]`, ends on exactly the canonical
/// chain: orphans above the fork undone, the canonical branch written, and the
/// duplicate revert a harmless no-op.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn the_writer_converges_to_canonical_on_a_revert() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "converge").await;
    seed_branch0(&pool).await;
    let branch1 = SyntheticChain::branched(HEAD, 1);

    // Sanity: the orphaned branch-0 rows are present above the fork.
    assert_eq!(max_block(&pool).await, Some(OLD_TIP as i64));
    assert_eq!(swaps_above(&pool, FORK).await, (OLD_TIP - FORK) as i64);
    assert_eq!(
        block_hash(&pool, OLD_TIP).await.unwrap(),
        SyntheticChain::branched(HEAD, 0).hash_at(OLD_TIP).to_vec(),
        "seeded tip is branch 0"
    );

    // Feed the writer the correction, then the canonical branch. The revert is
    // sent twice on purpose — the log is at-least-once, and a redelivery must be
    // a no-op.
    let (sink, source) = chainscope_core::build_transport::<Envelope<RowBatch>>(
        chainscope_core::TransportSpec::Channel { capacity: 256 },
    )
    .unwrap();
    let writer = Writer::new(pool.clone(), source, 8, Duration::from_millis(2));
    let handle = tokio::spawn(writer.run());

    sink.publish(Envelope::Revert { from_block: FORK }).await.unwrap();
    sink.publish(Envelope::Revert { from_block: FORK }).await.unwrap(); // redelivery
    for n in (FORK + 1)..=OLD_TIP {
        sink.publish(Envelope::Data(batch_on(1, n))).await.unwrap();
    }
    drop(sink); // stream ends → writer drains and stops
    handle.await.unwrap().unwrap();

    // Landed on canonical: same height, cursor at the tip, the tip is branch 1,
    // the block below the fork is untouched, and the swap count above the fork is
    // the canonical count (not doubled by the orphans, not lost).
    assert_eq!(max_block(&pool).await, Some(OLD_TIP as i64), "same height, now canonical");
    assert_eq!(cursor(&pool).await, Some(OLD_TIP), "cursor advanced back to the tip");
    assert_eq!(
        block_hash(&pool, OLD_TIP).await.unwrap(),
        branch1.hash_at(OLD_TIP).to_vec(),
        "the tip is the canonical branch, the orphan was undone"
    );
    assert_eq!(
        block_hash(&pool, FORK).await.unwrap(),
        SyntheticChain::branched(HEAD, 0).hash_at(FORK).to_vec(),
        "below the fork is untouched"
    );
    assert_eq!(
        swaps_above(&pool, FORK).await,
        (OLD_TIP - FORK) as i64,
        "exactly the canonical swaps above the fork — orphans gone, not double-counted"
    );

    eprintln!("writer undo OK: reverted orphans, applied canonical, redelivery a no-op");
    drop_db(&admin, pool, &name).await;
}

/// The undo primitive the writer relies on is idempotent: a second identical
/// revert removes nothing and leaves the state byte-for-byte the same.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn a_redelivered_revert_is_idempotent() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "idem").await;
    seed_branch0(&pool).await;

    let removed_first = db::rewind_to(&pool, FORK, false).await.unwrap();
    let tip_after_first = max_block(&pool).await;
    let cursor_after_first = cursor(&pool).await;

    let removed_second = db::rewind_to(&pool, FORK, false).await.unwrap();

    assert_eq!(removed_first, OLD_TIP - FORK, "first revert removes the orphans");
    assert_eq!(removed_second, 0, "a redelivered revert removes nothing");
    assert_eq!(max_block(&pool).await, tip_after_first, "height unchanged by the replay");
    assert_eq!(cursor(&pool).await, cursor_after_first, "cursor unchanged by the replay");
    assert_eq!(cursor(&pool).await, Some(FORK));

    eprintln!("idempotent undo OK: second revert a no-op");
    drop_db(&admin, pool, &name).await;
}
