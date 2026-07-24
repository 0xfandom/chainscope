//! chainscope ingestion pipeline.

mod config;
mod consumer;
mod db;
mod producer;

use std::{sync::Arc, time::Duration};

use chainscope_core::{source::ChainSource, BlockUnit, RowBatch};
use chainscope_eth_source::EthSource;
use config::Config;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // A missing .env is not an error — the environment may already carry the
    // variables, which is how it works in Docker.
    let _ = dotenvy::dotenv();

    // Order matters. Configuration is validated first, before a socket is
    // opened or a query is sent, so a bad address or a missing URL costs
    // nothing but an error message. Everything after this line can assume the
    // configuration is complete and well formed.
    let cfg = Config::load()?;

    init_tracing(&cfg);
    tracing::info!(config = %cfg.summary(), "configuration loaded");

    let pool = db::connect(&cfg.database).await?;
    db::migrate(&pool).await?;
    let created = db::ensure_partitions(&pool).await?;
    tracing::info!(created, "database ready");

    // The two seams the pipeline runs on. Built here, from configuration, and
    // nowhere else — a stage receives boxed traits and never learns which
    // transport it is on. In M5 this same call returns Redpanda-backed
    // implementations and nothing below it changes.
    //
    //   producer --[BlockUnit]--> transformer --[RowBatch]--> writer
    let (raw_sink, raw_source) = chainscope_core::build_transport::<BlockUnit>(
        cfg.pipeline.transport,
        cfg.pipeline.channel_capacity,
    );
    let (row_sink, row_source) = chainscope_core::build_transport::<RowBatch>(
        cfg.pipeline.transport,
        cfg.pipeline.channel_capacity,
    );
    drop((row_sink, row_source)); // the transformer and writer arrive in #7

    // Only the first endpoint is used. The failover pool across all configured
    // endpoints is M3; the trait it hides behind already exists.
    let watched: Vec<_> = cfg
        .chain
        .pools
        .iter()
        .map(|a| a.0)
        .chain(std::iter::once(cfg.chain.factory.0))
        .collect();
    let source: Arc<dyn ChainSource> =
        Arc::new(EthSource::new(&cfg.chain.rpc_endpoints[0], &watched));

    // Reach the chain once before claiming to be ready. An indexer that cannot
    // read the chain has nothing to do, so finding out now — with a clear
    // message — beats discovering it inside a retry loop later.
    let tip = source.latest_block().await?;
    let finalized = source.finalized_block().await?;
    tracing::info!(
        tip,
        finalized,
        lag = tip - finalized,
        watching = watched.len(),
        "chain reachable"
    );

    let cursor = db::load_live_cursor(&pool).await?;
    tracing::info!(?cursor, "live cursor loaded");

    let cancel = CancellationToken::new();

    let producer = producer::Producer::new(
        Arc::clone(&source),
        raw_sink,
        cursor,
        cfg.chain.start_block,
        Duration::from_millis(cfg.chain.poll_interval_ms),
        cancel.clone(),
    );
    let producer_task = tokio::spawn(producer.run());

    // The writer drains blocks and commits them with the cursor, one
    // transaction per batch. In M1 it consumes BlockUnit directly and writes the
    // blocks table; M2 inserts a transformer ahead of it and the writer moves to
    // RowBatch, extending the same transaction with decoded rows.
    let writer = consumer::Writer::new(
        pool.clone(),
        raw_source,
        cfg.pipeline.batch_size,
        Duration::from_millis(cfg.pipeline.flush_interval_ms),
    );
    let writer_task = tokio::spawn(writer.run());

    // Proper supervision — join set, shutdown ordering, failure propagation —
    // is #8. This is the minimum that makes Ctrl-C behave: signal cancellation,
    // let the producer stop and close the seam, let the writer flush its last
    // batch (cursor included) and exit.
    tokio::select! {
        _ = tokio::signal::ctrl_c() => tracing::info!("interrupt received, shutting down"),
        r = producer_task => { r??; tracing::info!("producer finished"); }
    }
    cancel.cancel();
    if let Err(e) = writer_task.await? {
        tracing::error!(error = %e, "writer failed");
        return Err(e);
    }

    Ok(())
}

/// RUST_LOG wins over the config file, so a running process can be made verbose
/// without editing anything on disk.
fn init_tracing(cfg: &Config) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cfg.log.filter));

    tracing_subscriber::fmt().with_env_filter(filter).init();
}
