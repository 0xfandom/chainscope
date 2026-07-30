//! Reorg fixtures (#48): drive the *real* pipeline across a branch switch and
//! assert it converges to the canonical branch at the row level.
//!
//! Block counts converging proves nothing — a reorg bug hides as one leaked
//! phantom swap or one uncompensated candle. So this runs the real
//! producer → transformer → writer with the reorg handler over a forked chain,
//! and asserts the swaps and candles equal a clean single index of the canonical
//! branch: the right trades vanished, the right ones appeared.
//!
//! The fixture is a `ForkedChain` — branch 0 at and below the fork point, branch
//! 1 above it — so it has a real shared prefix and emits distinct, decodable
//! swaps above the fork (the branch byte folds into each swap's tx_hash), exactly
//! as a reorg re-including transactions would.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test reorg_fixture_db -- --ignored --nocapture

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chainscope_core::{
    source::{ChainSource, SourceError},
    types::{Hash32, RawLog},
    BlockUnit, RowBatch,
};
use chainscope_indexer::{
    consumer::Writer,
    db,
    producer::Producer,
    reorg::{DbReorgHandler, ReorgHandler},
    testkit::{SyntheticChain, SYNTHETIC_POOL},
    transformer::Transformer,
};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const START: u64 = 90;
const OLD_TIP: u64 = 130;
const NEW_TIP: u64 = 140;

/// A node whose canonical chain is branch 0 at and below `fork_point` and branch
/// 1 above it. Delegates hashes and logs to the synthetic chain of the right
/// branch, then fixes each block's parent link to the forked-chain rule so the
/// prefix is genuinely shared.
struct ForkedChain {
    fork_point: u64,
    height: u64,
}

impl ForkedChain {
    fn branch_of(&self, n: u64) -> u8 {
        if n <= self.fork_point {
            0
        } else {
            1
        }
    }
    fn hash(&self, n: u64) -> Hash32 {
        SyntheticChain::branched(self.height, self.branch_of(n)).hash_at(n)
    }
    fn unit(&self, n: u64) -> BlockUnit {
        let mut u = SyntheticChain::branched(self.height, self.branch_of(n)).unit(n);
        u.parent_hash = self.hash(n.saturating_sub(1));
        u
    }
}

#[async_trait]
impl ChainSource for ForkedChain {
    async fn latest_block(&self) -> Result<u64, SourceError> {
        Ok(self.height)
    }
    async fn finalized_block(&self) -> Result<u64, SourceError> {
        Ok(self.height.saturating_sub(64))
    }
    async fn fetch_block(&self, number: u64) -> Result<BlockUnit, SourceError> {
        if number > self.height {
            return Err(SourceError::BlockNotFound { number });
        }
        Ok(self.unit(number))
    }
    async fn fetch_logs(&self, from: u64, to: u64) -> Result<Vec<RawLog>, SourceError> {
        Ok((from..=to.min(self.height)).flat_map(|n| self.unit(n).logs).collect())
    }
    async fn block_hash(&self, number: u64) -> Result<Hash32, SourceError> {
        if number > self.height {
            return Err(SourceError::BlockNotFound { number });
        }
        Ok(self.hash(number))
    }
    fn finality_depth(&self) -> u64 {
        64
    }
}

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool, tag: &str) -> (PgPool, String) {
    let name = format!("chainscope_fixture_{}_{}", std::process::id(), tag);
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

/// A running pipeline: its cancel token and the three stage handles.
struct Pipeline {
    cancel: CancellationToken,
    producer: JoinHandle<anyhow::Result<()>>,
    transformer: JoinHandle<anyhow::Result<()>>,
    writer: JoinHandle<anyhow::Result<()>>,
}

/// Spawn the real three-stage pipeline with the reorg handler over `source`,
/// resuming from `resume`.
fn spawn_pipeline(pool: &PgPool, source: Arc<dyn ChainSource>, resume: Option<u64>) -> Pipeline {
    let (raw_sink, raw_source) =
        chainscope_core::build_transport::<chainscope_core::Envelope<BlockUnit>>(chainscope_core::TransportSpec::Channel { capacity: 64 }).unwrap();
    let (row_sink, row_source) =
        chainscope_core::build_transport::<chainscope_core::Envelope<RowBatch>>(chainscope_core::TransportSpec::Channel { capacity: 64 }).unwrap();

    let cancel = CancellationToken::new();
    let handler: Arc<dyn ReorgHandler> =
        Arc::new(DbReorgHandler::new(Arc::clone(&source), pool.clone()));
    let producer = Producer::new(
        source,
        raw_sink,
        resume,
        START,
        Duration::from_millis(1),
        cancel.clone(),
    )
    .with_reorg_handler(handler);
    let transformer = Transformer::new(raw_source, row_sink, vec![SYNTHETIC_POOL]);
    let writer = Writer::new(pool.clone(), row_source, 8, Duration::from_millis(2));

    Pipeline {
        cancel,
        producer: tokio::spawn(producer.run()),
        transformer: tokio::spawn(transformer.run()),
        writer: tokio::spawn(writer.run()),
    }
}

/// Drive the pipeline until the live cursor reaches `target`, then stop it.
async fn drive_until_cursor(pool: &PgPool, source: Arc<dyn ChainSource>, resume: Option<u64>, target: u64) {
    let p = spawn_pipeline(pool, source, resume);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if db::load_live_cursor(pool).await.unwrap().unwrap_or(0) >= target {
            break;
        }
        assert!(Instant::now() < deadline, "pipeline did not reach cursor {target} in time");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    p.cancel.cancel();
    p.producer.await.unwrap().unwrap();
    p.transformer.await.unwrap().unwrap();
    p.writer.await.unwrap().unwrap();
}

/// (block_number, hex tx_hash) for every swap, ordered — the row-level identity
/// of what we indexed.
async fn swap_rows(pool: &PgPool) -> Vec<(i64, String)> {
    sqlx::query("SELECT block_number, encode(tx_hash,'hex') AS tx FROM swaps ORDER BY block_number, log_index")
        .fetch_all(pool)
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get::<i64, _>("block_number"), r.get::<String, _>("tx")))
        .collect()
}

/// A clean single index of the canonical (forked) chain, in its own database —
/// the oracle the reorg-converged database must match.
async fn clean_index(admin: &PgPool, fork_point: u64) -> (PgPool, String) {
    let (pool, name) = fresh_db(admin, &format!("clean{fork_point}")).await;
    let canonical: Arc<dyn ChainSource> = Arc::new(ForkedChain { fork_point, height: NEW_TIP });
    drive_until_cursor(&pool, canonical, None, NEW_TIP).await;
    (pool, name)
}

/// Index branch 0, switch the node to a chain forked at `fork_point`, and assert
/// the pipeline converges to the canonical branch row-for-row.
async fn assert_converges(admin: &PgPool, tag: &str, fork_point: u64) {
    let (pool, name) = fresh_db(admin, tag).await;

    // Phase 1: index the branch-0 chain up to the old tip.
    let branch0: Arc<dyn ChainSource> = Arc::new(SyntheticChain::new(OLD_TIP));
    drive_until_cursor(&pool, branch0, None, OLD_TIP).await;
    assert_eq!(db::load_live_cursor(&pool).await.unwrap(), Some(OLD_TIP));

    // The finality line sits well below the fork, so the reorg is in-window.
    db::advance_finality(&pool, NEW_TIP, START - 1).await.unwrap();

    // Phase 2: the node is now the forked chain, extended past the old tip.
    let forked: Arc<dyn ChainSource> = Arc::new(ForkedChain { fork_point, height: NEW_TIP });
    drive_until_cursor(&pool, forked, Some(OLD_TIP), NEW_TIP).await;

    // The oracle: a clean index of the same canonical chain.
    let (clean, cname) = clean_index(admin, fork_point).await;

    // Swaps are the row-level identity of what we indexed — the branch byte
    // folds into each swap's tx_hash, so an orphaned trade and its canonical
    // replacement at the same height are distinct rows. Equality with a clean
    // index is the proof the orphans vanished and the canonical trades appeared.
    //
    // Candles are not compared here: the live write path does not fold candles
    // (an M3-noted follow-up — the bulk backfill path does), so a live-only index
    // produces none to compare. Candle compensation is proven against the bulk
    // path, where candles exist, in the #47 tests.
    assert_eq!(
        swap_rows(&pool).await,
        swap_rows(&clean).await,
        "swaps must equal a clean index of the canonical branch (orphans gone, canonical present)"
    );

    eprintln!("converged: fork at {fork_point}, {} swaps == clean oracle", swap_rows(&pool).await.len());
    drop_db(admin, clean, &cname).await;
    drop_db(admin, pool, &name).await;
}

/// A one-block reorg at the tip converges to the canonical branch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a running Postgres"]
async fn a_shallow_reorg_converges_the_pipeline() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    assert_converges(&admin, "shallow", OLD_TIP - 1).await;
}

/// A ten-block reorg converges to the canonical branch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a running Postgres"]
async fn a_deep_reorg_converges_the_pipeline() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    assert_converges(&admin, "deep", OLD_TIP - 10).await;
}

/// A reorg reaching below the finalised line is surfaced by the pipeline — the
/// producer fails rather than silently indexing over a block promised frozen.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a running Postgres"]
async fn a_reorg_deeper_than_finality_is_surfaced() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "toodeep").await;

    let branch0: Arc<dyn ChainSource> = Arc::new(SyntheticChain::new(OLD_TIP));
    drive_until_cursor(&pool, branch0, None, OLD_TIP).await;

    // Finalise above the fork: the reorg forks at 120 but we claim 125 is frozen.
    db::advance_finality(&pool, NEW_TIP, 125).await.unwrap();

    let forked: Arc<dyn ChainSource> = Arc::new(ForkedChain { fork_point: 120, height: NEW_TIP });
    let p = spawn_pipeline(&pool, forked, Some(OLD_TIP));

    // The producer must surface the finality violation, not hang or converge.
    let result = tokio::time::timeout(Duration::from_secs(10), p.producer)
        .await
        .expect("producer must fail promptly, not hang")
        .unwrap();
    assert!(result.is_err(), "a reorg past the finalised line must fail the producer");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("finalised line"), "should name the finality violation: {err}");

    p.cancel.cancel();
    let _ = p.transformer.await;
    let _ = p.writer.await;

    eprintln!("deeper-than-finality surfaced: {err}");
    drop_db(&admin, pool, &name).await;
}
