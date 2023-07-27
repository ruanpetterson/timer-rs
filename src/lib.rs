//! A simple timer, used to enqueue operations meant to be executed at
//! a given time or after a given delay.

mod shutdown;

use std::future::Future;
use std::pin::{pin, Pin};
use std::task::{Context, Poll};
use std::time::Duration;

use chrono::{NaiveDateTime, Utc};
pub use shutdown::Shutdown;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::Instant;

type Result<T> = core::result::Result<T, TimerError>;

#[derive(Debug, Error)]
pub enum TimerError {
    #[error("operation failed because the timer was closed")]
    Closed,
}

/// A timer, used to schedule execution of callbacks in a given future.
pub struct Timer<F>
where
    F: FnMut() -> Option<NaiveDateTime>,
    F: Send + Sync,
{
    callback: F,
    deadline: Instant,
    watchdog: (mpsc::Sender<NaiveDateTime>, mpsc::Receiver<NaiveDateTime>),
    shutdown: Shutdown,
}

impl<F> Timer<F>
where
    F: FnMut() -> Option<NaiveDateTime>,
    F: Send + Sync,
{
    /// Creates a Timer instance.
    pub fn new(callback: F, shutdown: Shutdown) -> Self {
        let watchdog = mpsc::channel(1);

        Self {
            callback,
            // Since it's the first call, it starts sleeping forever.
            deadline: far_future(),
            watchdog,
            shutdown,
        }
    }

    /// Schedule for execution after a delay.
    pub async fn schedule(&self, deadline: NaiveDateTime) -> Result<()> {
        self.scheduler().schedule(deadline).await
    }

    /// Creates a handler to schedule new executions.
    pub fn scheduler(&self) -> Scheduler {
        Scheduler(self.watchdog.0.clone())
    }
}

impl<F> Future for Timer<F>
where
    F: FnMut() -> Option<NaiveDateTime>,
    F: Send + Sync + Unpin,
{
    type Output = ();

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        let poll_fn = async move {
            let Self {
                callback,
                deadline,
                watchdog: (_, watchdog),
                shutdown,
            } = self.as_mut().get_mut();

            let sleep = tokio::time::sleep_until(*deadline);
            tokio::pin!(sleep);

            // TODO(fixme): the sleep only wakes up when the time's up and receive
            // a new deadline which is earlier than previous one.
            loop {
                tracing::debug!(
                    "sleeping for {} secs",
                    deadline.duration_since(Instant::now()).as_secs()
                );

                tokio::select! {
                    // Wait for the next run.
                    () = &mut sleep => {
                        let deadline = if let Some(new_deadline) = callback() {
                            let duration = Utc::now().naive_utc() - new_deadline;
                            let Ok(duration) = duration.to_std() else {
                                continue;
                            };

                            Instant::now() + duration
                        } else {
                            far_future()
                        };

                        sleep.as_mut().reset(deadline);
                    },
                    // Wait for a new deadline.
                    Some(new_deadline) = watchdog.recv() => {
                        let new_duration = new_deadline - Utc::now().naive_utc();

                        let Ok(new_duration) = new_duration.to_std() else {
                            tracing::debug!("failed to set a timer for past");
                            continue;
                        };

                        tracing::debug!("time will be run {} secs from now", new_duration.as_secs());

                        // Change the sleep time for next iteration.
                        let deadline = Instant::now() + new_duration;
                        sleep.as_mut().reset(deadline);
                    },
                    // Stop if shutdown signal is received.
                    _ = shutdown.recv() => {
                        tracing::debug!("received a SIGINT");
                        break;
                    },
                }
            }
        };

        pin!(poll_fn).poll(cx)
    }
}

#[derive(Clone)]
pub struct Scheduler(mpsc::Sender<NaiveDateTime>);

impl Scheduler {
    pub async fn schedule(&self, deadline: NaiveDateTime) -> Result<()> {
        self.0
            .send(deadline)
            .await
            .map_err(|_| TimerError::Closed)?;

        tracing::debug!("scheduled a new execution for {}", deadline);

        Ok(())
    }
}

pub(crate) fn far_future() -> Instant {
    // Roughly 30 years from now.
    //
    // API does not provide a way to obtain max `Instant` or convert specific
    // date in the future to instant. 1000 years overflows on macOS, 100 years
    // overflows on FreeBSD.
    Instant::now() + Duration::from_secs(86400 * 365 * 30)
}
