//! Task supervision and graceful shutdown.
//!
//! Every stage runs as a task in one `JoinSet` under a shared
//! [`CancellationToken`]. The supervisor's whole job is to make two things true:
//!
//!   * **Shutdown is a planned crash.** Because a real crash is already safe
//!     (the writer's transaction guarantees exactly-once, see
//!     [`crate::db::write_block_batch`]), a clean stop needs no separate save
//!     path. Tripping the token makes the producer stop and drop its sink, that
//!     closure drains through the pipeline, and the writer commits its final
//!     batch and exits. Ordering falls out of the token plus the closed stream;
//!     nothing here sequences the stages by hand.
//!
//!   * **Partial failure is never tolerated.** A pipeline running with one dead
//!     stage silently stops making progress while looking healthy, which is
//!     worse than stopping outright. So any task that exits unexpectedly — an
//!     error, a panic, or even a clean return while nobody asked for shutdown —
//!     cancels everything and brings the process down non-zero, loudly.
//!
//! A bounded timeout backs the whole thing: once shutdown begins, a stage that
//! refuses to finish does not hang the process forever. The timeout is reported
//! as [`Shutdown::TimedOut`] and `main` turns that into an abort.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use tokio::task::{Id, JoinSet};
use tokio_util::sync::CancellationToken;

/// How the process ended, for `main` to turn into an exit code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shutdown {
    /// Cancellation was requested and every stage stopped cleanly within the
    /// timeout. Exit zero.
    Clean,
    /// A stage failed, panicked, or exited unexpectedly. Exit non-zero.
    Failed,
    /// Shutdown began but a stage did not finish in time. `main` aborts.
    TimedOut,
}

pub struct Supervisor {
    tasks: JoinSet<anyhow::Result<()>>,
    names: HashMap<Id, &'static str>,
    cancel: CancellationToken,
    timeout: Duration,
}

impl Supervisor {
    pub fn new(cancel: CancellationToken, timeout: Duration) -> Self {
        Self {
            tasks: JoinSet::new(),
            names: HashMap::new(),
            cancel,
            timeout,
        }
    }

    /// Add a stage. The name is kept so that a panic — which loses the task's
    /// return value — can still be reported against the stage it came from.
    pub fn spawn<F>(&mut self, name: &'static str, fut: F)
    where
        F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let handle = self.tasks.spawn(fut);
        self.names.insert(handle.id(), name);
    }

    /// Run until every task has stopped, then report how it ended.
    ///
    /// The loop has two modes. Before shutdown it waits on the next task exit
    /// *or* on the token being tripped from outside (a signal). After shutdown
    /// begins it waits on the next task exit against a deadline, so a stage that
    /// will not finish cannot stall the process.
    pub async fn supervise(mut self) -> Shutdown {
        let mut failed = false;
        let mut deadline: Option<Instant> = None;

        loop {
            // Decide how to wait for the next event.
            let joined = match deadline {
                // Shutting down: bounded wait.
                Some(when) => {
                    match tokio::time::timeout_at(when.into(), self.tasks.join_next_with_id()).await
                    {
                        Ok(j) => j,
                        Err(_) => {
                            let stuck: Vec<_> = self.names.values().copied().collect();
                            tracing::error!(
                                ?stuck,
                                timeout_ms = self.timeout.as_millis() as u64,
                                "shutdown timed out; aborting"
                            );
                            return Shutdown::TimedOut;
                        }
                    }
                }
                // Running: wait for a task, but also react the instant an
                // external signal trips the token, so shutdown starts promptly
                // rather than only when some task happens to end.
                None => {
                    tokio::select! {
                        j = self.tasks.join_next_with_id() => j,
                        _ = self.cancel.cancelled() => {
                            tracing::info!("shutdown requested");
                            deadline = Some(Instant::now() + self.timeout);
                            continue;
                        }
                    }
                }
            };

            let Some(result) = joined else {
                // JoinSet is empty: every stage has stopped.
                break;
            };

            let bad = match result {
                Ok((id, outcome)) => {
                    let name = self.names.remove(&id).unwrap_or("unknown");
                    match outcome {
                        Ok(()) if self.cancel.is_cancelled() => {
                            tracing::info!(stage = name, "stopped cleanly");
                            false
                        }
                        Ok(()) => {
                            // A stage returning on its own while the pipeline is
                            // meant to be running is a silent-stall bug, treated
                            // exactly like a failure.
                            tracing::error!(
                                stage = name,
                                "stage exited unexpectedly while running; shutting down"
                            );
                            true
                        }
                        Err(e) => {
                            tracing::error!(stage = name, error = %e, "stage failed; shutting down");
                            true
                        }
                    }
                }
                Err(join_err) => {
                    let name = self.names.remove(&join_err.id()).unwrap_or("unknown");
                    // A panic is the loud line the acceptance criterion asks for.
                    tracing::error!(stage = name, "stage panicked; shutting down");
                    true
                }
            };

            if bad {
                failed = true;
            }

            // Any exit — clean-on-request or bad — means we are now shutting
            // down. Trip the token so the remaining stages wind down, and start
            // the clock if it is not already running.
            if bad || self.cancel.is_cancelled() {
                self.cancel.cancel();
                if deadline.is_none() {
                    deadline = Some(Instant::now() + self.timeout);
                }
            }
        }

        if failed {
            Shutdown::Failed
        } else {
            Shutdown::Clean
        }
    }
}

/// Trip `cancel` on SIGINT (Ctrl-C) or SIGTERM (the signal a container runtime
/// or `systemctl stop` sends).
///
/// Returns when a signal arrives or when the token is tripped by something else,
/// so the task never outlives the shutdown it is watching for.
pub async fn wait_for_shutdown_signal(cancel: CancellationToken) {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            // If the handler cannot be installed, just wait on the token so this
            // future still resolves on shutdown rather than never.
            Err(_) => cancel.cancelled().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received SIGINT"),
        _ = terminate => tracing::info!("received SIGTERM"),
        _ = cancel.cancelled() => {} // already shutting down for another reason
    }
    cancel.cancel();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    /// A stage that runs until cancelled, then returns Ok — the shape every real
    /// stage has.
    async fn until_cancelled(cancel: CancellationToken, ran: Arc<AtomicUsize>) -> anyhow::Result<()> {
        cancel.cancelled().await;
        ran.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    #[tokio::test]
    async fn a_cancel_stops_every_stage_cleanly() {
        let cancel = CancellationToken::new();
        let ran = Arc::new(AtomicUsize::new(0));
        let mut sup = Supervisor::new(cancel.clone(), Duration::from_secs(5));
        for name in ["producer", "writer"] {
            sup.spawn(name, until_cancelled(cancel.clone(), ran.clone()));
        }

        let c = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            c.cancel();
        });

        assert_eq!(sup.supervise().await, Shutdown::Clean);
        assert_eq!(ran.load(Ordering::SeqCst), 2, "both stages should have wound down");
    }

    #[tokio::test]
    async fn a_failing_stage_brings_everything_down_nonzero() {
        let cancel = CancellationToken::new();
        let mut sup = Supervisor::new(cancel.clone(), Duration::from_secs(5));

        // A healthy stage, and one that fails almost immediately.
        sup.spawn("healthy", {
            let c = cancel.clone();
            async move {
                c.cancelled().await;
                Ok(())
            }
        });
        sup.spawn("faulty", async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Err(anyhow::anyhow!("boom"))
        });

        assert_eq!(sup.supervise().await, Shutdown::Failed);
        assert!(cancel.is_cancelled(), "a failure must cancel the healthy stage too");
    }

    #[tokio::test]
    async fn a_panicking_stage_does_not_hang_and_reports_failure() {
        let cancel = CancellationToken::new();
        let mut sup = Supervisor::new(cancel.clone(), Duration::from_secs(5));

        sup.spawn("healthy", {
            let c = cancel.clone();
            async move {
                c.cancelled().await;
                Ok(())
            }
        });
        sup.spawn("panicker", async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            panic!("stage went down");
        });

        // The point of the criterion: a panic is caught and turned into an
        // orderly non-zero shutdown, not a hang.
        let outcome = tokio::time::timeout(Duration::from_secs(2), sup.supervise())
            .await
            .expect("supervise must not hang on a panic");
        assert_eq!(outcome, Shutdown::Failed);
        assert!(cancel.is_cancelled());
    }

    #[tokio::test]
    async fn a_clean_return_while_running_is_treated_as_unexpected() {
        let cancel = CancellationToken::new();
        let mut sup = Supervisor::new(cancel.clone(), Duration::from_secs(5));

        // Nobody asked for shutdown, yet this stage just... stops. That is the
        // silent-stall bug, and it must be treated as failure.
        sup.spawn("quitter", async { Ok(()) });
        sup.spawn("healthy", {
            let c = cancel.clone();
            async move {
                c.cancelled().await;
                Ok(())
            }
        });

        assert_eq!(sup.supervise().await, Shutdown::Failed);
    }

    #[tokio::test]
    async fn a_stage_that_ignores_cancellation_hits_the_timeout() {
        let cancel = CancellationToken::new();
        let mut sup = Supervisor::new(cancel.clone(), Duration::from_millis(80));

        // Stubborn: never watches the token.
        sup.spawn("stuck", async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(())
        });

        let c = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            c.cancel();
        });

        let outcome = tokio::time::timeout(Duration::from_secs(2), sup.supervise())
            .await
            .expect("supervise must give up on a stuck stage, not hang");
        assert_eq!(outcome, Shutdown::TimedOut);
    }
}
