//! The M4 exit criterion (#49): a reorg storm converges to the canonical chain.
//!
//! A reorg bug breaks silently — one leaked phantom swap, one block that should
//! have been rewound but wasn't. A single reorg might miss it; a *storm* of them,
//! at varying depths, measured against an independent oracle, does not. This
//! drives the real pipeline through many reorgs of differing depth (all within
//! the finalised window), ending on a fixed canonical chain, and asserts:
//!
//!   * the indexed swaps equal a clean single index of the final canonical chain
//!     — every reorg fully converged, no orphan survived, nothing double-counted;
//!   * the reorg window stayed bounded — no header at or below the finalised line
//!     survives, so a storm cannot grow the window without limit;
//!   * the finalised prefix was never rewritten — every fork stayed above it.
//!
//! Same shape as M1's crash harness and M3's interrupted-backfill criterion, now
//! for reorg convergence.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test reorg_storm_db -- --ignored --nocapture

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
const FINALIZED: u64 = 100;

/// Each reorg round: fork branch 0 at `fork_point`, switch to `upper_branch`
/// above it, and extend to `height`. Growing the height by one each round is
/// what lets the producer notice the reorg — a chain that only rewrites its tip
/// without advancing would leave the caught-up producer asleep.
#[derive(Clone, Copy)]
struct Round {
    fork_point: u64,
    upper_branch: u8,
    height: u64,
}

/// A node whose chain is branch 0 at and below the fork point and `upper_branch`
/// above it — a real shared prefix with decodable swaps.
struct StormChain {
    fork_point: u64,
    upper_branch: u8,
    height: u64,
}

impl StormChain {
    fn branch_of(&self, n: u64) -> u8 {
        if n <= self.fork_point {
            0
        } else {
            self.upper_branch
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
impl ChainSource for StormChain {
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
    let name = format!("chainscope_storm_{}_{}", std::process::id(), tag);
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

struct Pipeline {
    cancel: CancellationToken,
    producer: JoinHandle<anyhow::Result<()>>,
    transformer: JoinHandle<anyhow::Result<()>>,
    writer: JoinHandle<anyhow::Result<()>>,
}

fn spawn_pipeline(pool: &PgPool, source: Arc<dyn ChainSource>, resume: Option<u64>) -> Pipeline {
    let (raw_sink, raw_source) =
        chainscope_core::build_transport::<chainscope_core::Envelope<BlockUnit>>(chainscope_core::TransportSpec::Channel { capacity: 64 }).unwrap();
    let (row_sink, row_source) =
        chainscope_core::build_transport::<chainscope_core::Envelope<RowBatch>>(chainscope_core::TransportSpec::Channel { capacity: 64 }).unwrap();
    let cancel = CancellationToken::new();
    let handler: Arc<dyn ReorgHandler> =
        Arc::new(DbReorgHandler::new(Arc::clone(&source), pool.clone()));
    let producer = Producer::new(source, raw_sink, resume, START, Duration::from_millis(1), cancel.clone())
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

/// Drive until the live cursor reaches `target`, then stop.
async fn drive_until_cursor(pool: &PgPool, source: Arc<dyn ChainSource>, resume: Option<u64>, target: u64) {
    let p = spawn_pipeline(pool, source, resume);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if db::load_live_cursor(pool).await.unwrap().unwrap_or(0) >= target {
            break;
        }
        assert!(Instant::now() < deadline, "pipeline did not reach cursor {target}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    p.cancel.cancel();
    p.producer.await.unwrap().unwrap();
    p.transformer.await.unwrap().unwrap();
    p.writer.await.unwrap().unwrap();
}

async fn swap_rows(pool: &PgPool) -> Vec<(i64, String)> {
    sqlx::query("SELECT block_number, encode(tx_hash,'hex') AS tx FROM swaps ORDER BY block_number, log_index")
        .fetch_all(pool)
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get::<i64, _>("block_number"), r.get::<String, _>("tx")))
        .collect()
}

async fn block_span(pool: &PgPool) -> (Option<i64>, Option<i64>) {
    let r = sqlx::query("SELECT min(number) AS lo, max(number) AS hi FROM blocks")
        .fetch_one(pool)
        .await
        .unwrap();
    (r.get("lo"), r.get("hi"))
}

/// The storm converges to the canonical chain, the window stays bounded, and the
/// finalised prefix is never touched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a running Postgres"]
async fn a_reorg_storm_converges_to_the_canonical_chain() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "run").await;

    // Reorgs of varying depth, every fork above the finalised line (100). Each
    // round grows the tip by one so the producer notices the reorg.
    let forks = [125u64, 118, 129, 110, 127, 120, 115, 128];
    let rounds: Vec<Round> = forks
        .iter()
        .enumerate()
        .map(|(i, &fork_point)| Round {
            fork_point,
            upper_branch: (i + 1) as u8,
            height: OLD_TIP + 1 + i as u64,
        })
        .collect();
    let last = *rounds.last().unwrap();

    // Index the branch-0 chain, then freeze the prefix at 100.
    let branch0: Arc<dyn ChainSource> = Arc::new(SyntheticChain::new(OLD_TIP));
    drive_until_cursor(&pool, branch0, None, OLD_TIP).await;
    db::advance_finality(&pool, last.height, FINALIZED).await.unwrap();

    // The storm: each round reorgs the chain and the pipeline reconverges.
    for r in &rounds {
        let view: Arc<dyn ChainSource> = Arc::new(StormChain {
            fork_point: r.fork_point,
            upper_branch: r.upper_branch,
            height: r.height,
        });
        drive_until_cursor(&pool, Arc::clone(&view), db::load_live_cursor(&pool).await.unwrap(), r.height).await;
    }

    // The oracle: a clean single index of the final canonical chain.
    let (clean, cname) = fresh_db(&admin, "oracle").await;
    let canonical: Arc<dyn ChainSource> = Arc::new(StormChain {
        fork_point: last.fork_point,
        upper_branch: last.upper_branch,
        height: last.height,
    });
    drive_until_cursor(&clean, canonical, None, last.height).await;

    // Converged: every trade the canonical chain holds, and only those.
    assert_eq!(
        swap_rows(&pool).await,
        swap_rows(&clean).await,
        "after the storm the swaps must equal a clean index of the canonical chain"
    );

    // Bounded: the reorg window holds nothing at or below the finalised line, so
    // a storm cannot grow it without limit.
    let (lo, hi) = block_span(&pool).await;
    assert_eq!(hi, Some(last.height as i64), "window tops out at the canonical tip");
    assert!(lo.unwrap() > FINALIZED as i64, "no finalised header survives in the window (min {lo:?})");

    eprintln!(
        "storm converged: {} reorgs, {} swaps == oracle, window [{}..={}]",
        rounds.len(),
        swap_rows(&pool).await.len(),
        lo.unwrap(),
        hi.unwrap()
    );
    drop_db(&admin, clean, &cname).await;
    drop_db(&admin, pool, &name).await;
}
