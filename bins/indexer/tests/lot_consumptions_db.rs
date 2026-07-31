//! The lot-consumption ledger schema (#70), against a real Postgres.
//!
//! Proves the migration lands the reversal ledger the FIFO engine (#72) and its
//! reorg reversal (#73) will write to, that its natural key makes a replayed
//! drawdown a no-op, and that it adds to — never disturbs — the 0008 wallet
//! tables the engine folds into.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test lot_consumptions_db -- --ignored --nocapture

use chainscope_indexer::db;
use sqlx::{postgres::PgPoolOptions, PgPool};

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool, tag: &str) -> (PgPool, String) {
    let name = format!("chainscope_ledger_{}_{}", std::process::id(), tag);
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
    (pool, name)
}

async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#))
        .execute(admin)
        .await
        .ok();
}

/// One drawdown row. `consume_seq` distinguishes the several lots one sell can take.
async fn insert_row(pool: &PgPool, tx: &[u8], log: i32, seq: i32, block: i64, pnl: &str) -> u64 {
    sqlx::query(
        "INSERT INTO lot_consumptions \
         (sell_tx, sell_log, consume_seq, wallet, token, qty_consumed, \
          lot_unit_cost_usd, lot_acquired_block, proceeds_usd, realized_pnl_usd, sell_block) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
         ON CONFLICT (sell_tx, sell_log, consume_seq) DO NOTHING",
    )
    .bind(tx)
    .bind(log)
    .bind(seq)
    .bind(&b"\x11".repeat(20)[..]) // wallet
    .bind(&b"\x22".repeat(20)[..]) // token
    .bind(sqlx::types::BigDecimal::from(100))
    .bind(sqlx::types::BigDecimal::from(2))
    .bind(block - 1)
    .bind(sqlx::types::BigDecimal::from(300))
    .bind(pnl.parse::<sqlx::types::BigDecimal>().unwrap())
    .bind(block)
    .execute(pool)
    .await
    .unwrap()
    .rows_affected()
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn migration_lands_ledger_and_index() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin, "schema").await;

    let table: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables \
         WHERE table_name = 'lot_consumptions'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(table, 1, "lot_consumptions table exists");

    let idx: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pg_indexes WHERE indexname = $1")
            .bind("lot_consumptions_sell_block_idx")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(idx, 1, "sell_block index exists for the reorg range-delete");

    drop_db(&admin, pool, &name).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn duplicate_drawdown_is_a_noop() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin, "dedupe").await;

    let tx = b"\xaa".repeat(32);
    // Two lots drawn down by one sell: distinct consume_seq, both land.
    assert_eq!(insert_row(&pool, &tx, 7, 0, 200, "50").await, 1);
    assert_eq!(insert_row(&pool, &tx, 7, 1, 200, "-10").await, 1);
    // Replay of the same sell: same (tx, log, seq) -> nothing changes.
    assert_eq!(insert_row(&pool, &tx, 7, 0, 200, "50").await, 0, "replay is idempotent");
    assert_eq!(insert_row(&pool, &tx, 7, 1, 200, "-10").await, 0);

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM lot_consumptions")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 2, "only the two distinct drawdowns persist");

    drop_db(&admin, pool, &name).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn wallet_tables_from_0008_are_intact() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin, "coexist").await;

    // The ledger is additive: the tables the engine folds into still exist,
    // unchanged, with the columns the engine needs.
    for (table, col) in [
        ("wallet_positions", "lots"),
        ("wallet_positions", "cost_basis_usd"),
        ("wallet_stats", "realized_pnl_usd"),
        ("wallet_stats", "excluded"),
    ] {
        let n: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM information_schema.columns \
             WHERE table_name = $1 AND column_name = $2",
        )
        .bind(table)
        .bind(col)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(n, 1, "{table}.{col} still present");
    }

    // The leaderboard partial index from 0008 survives too.
    let has_pnl_idx: i64 = sqlx::query("SELECT 1 FROM pg_indexes WHERE indexname = $1")
        .bind("wallet_stats_pnl_idx")
        .fetch_all(&pool)
        .await
        .unwrap()
        .len() as i64;
    assert_eq!(has_pnl_idx, 1, "leaderboard index intact");

    drop_db(&admin, pool, &name).await;
}
