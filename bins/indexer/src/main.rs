//! chainscope ingestion pipeline.

mod config;
mod db;

use chainscope_core::{source::ChainSource, BlockUnit, RowBatch};
use chainscope_eth_source::EthSource;
use config::Config;

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
    tracing::info!("connected to postgres");

    db::migrate(&pool).await?;
    tracing::info!("migrations up to date");

    let created = db::ensure_partitions(&pool).await?;
    tracing::info!(created, "day partitions ensured");

    // The two seams the pipeline runs on. Built here, from configuration, and
    // nowhere else — a stage receives boxed traits and never learns which
    // transport it is on. In M5 this same call returns Redpanda-backed
    // implementations and nothing below it changes.
    //
    //   producer --[BlockUnit]--> transformer --[RowBatch]--> writer
    //
    // The stages that consume these arrive in #6 and #7; for now building them
    // proves the wiring type-checks end to end.
    let (raw_sink, raw_source) =
        chainscope_core::build_transport::<BlockUnit>(cfg.pipeline.transport, cfg.pipeline.channel_capacity);
    let (row_sink, row_source) =
        chainscope_core::build_transport::<RowBatch>(cfg.pipeline.transport, cfg.pipeline.channel_capacity);
    drop((raw_sink, raw_source, row_sink, row_source));

    // Reach the chain once before claiming to be ready. An indexer that cannot
    // read the chain has nothing to do, so finding out now — with a clear
    // message — beats discovering it inside a retry loop later.
    //
    // Only the first endpoint is used. The failover pool across all configured
    // endpoints is M3; the trait it hides behind already exists.
    let watched: Vec<_> = cfg
        .chain
        .pools
        .iter()
        .map(|a| a.0)
        .chain(std::iter::once(cfg.chain.factory.0))
        .collect();
    let source = EthSource::new(&cfg.chain.rpc_endpoints[0], &watched);

    let tip = source.latest_block().await?;
    let finalized = source.finalized_block().await?;
    tracing::info!(
        tip,
        finalized,
        lag = tip - finalized,
        watching = watched.len(),
        "chain reachable"
    );

    tracing::info!(
        transport = cfg.pipeline.transport.as_str(),
        capacity = cfg.pipeline.channel_capacity,
        pools = cfg.chain.pools.len(),
        chain_id = cfg.chain.chain_id,
        "schema ready; pipeline stages not implemented yet"
    );
    Ok(())
}

/// RUST_LOG wins over the config file, so a running process can be made verbose
/// without editing anything on disk.
fn init_tracing(cfg: &Config) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cfg.log.filter));

    tracing_subscriber::fmt().with_env_filter(filter).init();
}
