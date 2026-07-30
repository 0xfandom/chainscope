//! chainscope ingestion pipeline — binary entry point over the library.

use std::{process::ExitCode, sync::Arc, time::Duration};

use chainscope_core::{source::ChainSource, BlockUnit, Envelope, RowBatch};
use chainscope_eth_source::PooledSource;
use chainscope_indexer::{
    config::Config,
    consumer, db, finality, producer, reorg,
    supervisor::{self, Shutdown, Supervisor},
    transformer,
};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<ExitCode> {
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
    // transport it is on. Selecting `transport = "kafka"` returns the
    // Redpanda-backed implementations and nothing below this line changes.
    //
    //   producer --[BlockUnit]--> transformer --[RowBatch]--> writer
    //
    // Each seam is its own topic with its own consumer group, so the transformer
    // and the writer commit offsets independently — a distinct `group.id` per
    // seam, stable across restarts so a restart resumes rather than replays.
    let (raw_sink, raw_source) = chainscope_core::build_transport::<Envelope<BlockUnit>>(
        transport_spec(&cfg, &cfg.kafka.blocks_topic, "chainscope-transformer"),
    )?;
    let (row_sink, row_source) = chainscope_core::build_transport::<Envelope<RowBatch>>(
        transport_spec(&cfg, &cfg.kafka.rows_topic, "chainscope-writer"),
    )?;

    // Every configured endpoint goes into one failover pool (#32). A call that
    // hits a transiently-unwell endpoint rotates to the next healthy one; the
    // producer above it never learns there is more than one. A single-endpoint
    // config is just a pool of one — same code path, no special case.
    let watched: Vec<_> = cfg
        .chain
        .pools
        .iter()
        .map(|a| a.0)
        .chain(std::iter::once(cfg.chain.factory.0))
        .collect();
    let source: Arc<dyn ChainSource> =
        Arc::new(PooledSource::from_endpoints(&cfg.chain.rpc_endpoints, &watched));

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

    // The reorg guard: before each block is published it is checked against the
    // chain we have recorded. The action on a fork depends on the transport —
    // under the channel the database is rewound in place (M4); over the log it
    // cannot be, so a `Revert` is appended and every consumer undoes its own
    // state (M5). Detection is identical; only the effect differs.
    let reorg_handler: Arc<dyn reorg::ReorgHandler> = match cfg.pipeline.transport {
        chainscope_core::TransportKind::Channel => {
            Arc::new(reorg::DbReorgHandler::new(Arc::clone(&source), pool.clone()))
        }
        chainscope_core::TransportKind::Kafka => {
            Arc::new(reorg::LogReorgHandler::new(Arc::clone(&source), pool.clone()))
        }
    };
    let producer = producer::Producer::new(
        Arc::clone(&source),
        raw_sink,
        cursor,
        cfg.chain.start_block,
        Duration::from_millis(cfg.chain.poll_interval_ms),
        cancel.clone(),
    )
    .with_reorg_handler(reorg_handler);
    // The transformer sits between: it decodes each block's watched logs into a
    // RowBatch. It watches the same contracts the source fetches for — the pools
    // plus the factory — so a pool event decodes and a factory PoolCreated is
    // recognised (though not stored until M7).
    let transformer = transformer::Transformer::new(raw_source, row_sink, watched.clone());

    // The writer drains decoded batches and commits each with the cursor, one
    // transaction per batch — the block header, its swaps, its liq_events, and
    // the cursor advance all together, which is where exactly-once lives.
    let writer = consumer::Writer::new(
        pool.clone(),
        row_source,
        cfg.pipeline.batch_size,
        Duration::from_millis(cfg.pipeline.flush_interval_ms),
    );

    // The finality tracker (#44) polls the tip and its finality line on the same
    // interval as the producer, advances `chain_state.finalized_height`
    // monotonically, and prunes the `blocks` reorg window down to the still
    // reorg-eligible band. It reads the head like the producer does but writes
    // only the finality columns, so it never contends with the writer's cursor.
    let finality = finality::FinalityTracker::new(
        Arc::clone(&source),
        pool.clone(),
        Duration::from_millis(cfg.chain.poll_interval_ms),
        cancel.clone(),
    );

    // Every stage runs under one supervisor sharing one cancellation token.
    // Shutdown order is not scripted here: tripping the token makes the producer
    // stop and drop its sink; that closes the stream into the transformer, which
    // drains, finishes, and drops its own sink; that closes the stream into the
    // writer, which drains and commits its final batch before returning. The
    // signal handler is just another supervised task that trips the token.
    let mut sup = Supervisor::new(
        cancel.clone(),
        Duration::from_millis(cfg.pipeline.shutdown_timeout_ms),
    );
    sup.spawn("producer", producer.run());
    sup.spawn("transformer", transformer.run());
    sup.spawn("writer", writer.run());
    sup.spawn("finality", finality.run());
    sup.spawn("signals", {
        let cancel = cancel.clone();
        async move {
            supervisor::wait_for_shutdown_signal(cancel).await;
            Ok(())
        }
    });

    match sup.supervise().await {
        Shutdown::Clean => {
            tracing::info!("shutdown complete");
            Ok(ExitCode::SUCCESS)
        }
        Shutdown::Failed => {
            tracing::error!("a stage died; exiting non-zero");
            Ok(ExitCode::FAILURE)
        }
        Shutdown::TimedOut => {
            // A stage would not wind down in time. Abort hard rather than hang —
            // a killed process is safe here, since the writer's transaction
            // means an interrupted commit simply replays on the next start.
            tracing::error!("shutdown timed out; aborting");
            std::process::abort();
        }
    }
}

/// Translate the validated config into the factory's construction spec for one
/// seam. `Channel` needs only a capacity; `Kafka` needs the brokers, this seam's
/// topic, and the consumer group to read it under.
fn transport_spec<'a>(
    cfg: &'a Config,
    topic: &'a str,
    group_id: &'a str,
) -> chainscope_core::TransportSpec<'a> {
    match cfg.pipeline.transport {
        chainscope_core::TransportKind::Channel => chainscope_core::TransportSpec::Channel {
            capacity: cfg.pipeline.channel_capacity,
        },
        chainscope_core::TransportKind::Kafka => chainscope_core::TransportSpec::Kafka {
            brokers: &cfg.kafka.brokers_csv,
            topic,
            group_id,
        },
    }
}

/// RUST_LOG wins over the config file, so a running process can be made verbose
/// without editing anything on disk.
fn init_tracing(cfg: &Config) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cfg.log.filter));

    tracing_subscriber::fmt().with_env_filter(filter).init();
}
