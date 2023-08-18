//! A simple timer, used to enqueue operations meant to be executed at
//! a given time or after a given delay.

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::time::Duration;

use chrono::{NaiveDateTime, Utc};
use futures::FutureExt as _;
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
///
/// If the callback does not provide the new time period, the next execution
/// will be scheduled for a distant future.
pub struct Timer<F: 'static, S: 'static> {
    callback: Pin<Box<F>>,
    deadline: Instant,
    watchdog: (mpsc::Sender<NaiveDateTime>, mpsc::Receiver<NaiveDateTime>),
    shutdown: Pin<Box<S>>,
}

impl<F, S> Timer<F, S>
where
    F: Fn() -> Option<NaiveDateTime> + Send,
    S: Future + Send,
{
    /// Creates a Timer instance.
    pub fn new(callback: F, shutdown: S) -> Self {
        let watchdog = mpsc::channel(1);

        Self {
            callback: Box::pin(callback),
            // Since it's the first call, it starts sleeping forever.
            deadline: far_future(),
            watchdog,
            shutdown: Box::pin(shutdown),
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

impl<F, S> IntoFuture for Timer<F, S>
where
    F: Fn() -> Option<NaiveDateTime> + Send,
    S: Future + Send,
{
    type Output = ();
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output>>>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            let Self {
                callback,
                deadline,
                watchdog: (_, watchdog),
                shutdown,
            } = self;

            let sleep = tokio::time::sleep_until(deadline);

            futures::pin_mut!(callback);
            futures::pin_mut!(sleep);
            futures::pin_mut!(shutdown);
            futures::pin_mut!(watchdog);

            loop {
                let duration = sleep.deadline() - Instant::now();
                tracing::trace!("sleeping for {} secs", duration.as_secs());

                tokio::select! {
                    // Wait for a new deadline.
                    Some(new_deadline) = watchdog.recv() => {
                        let new_duration = new_deadline - Utc::now().naive_utc();
                        let Ok(new_duration) = new_duration.to_std() else {
                            tracing::trace!("unable to schedule a timer for a time in the past");
                            continue;
                        };

                        tracing::trace!("task will be executed {} secs from now", new_duration.as_secs());

                        // Change the sleep time for next iteration.
                        let deadline = Instant::now() + new_duration;
                        sleep.as_mut().reset(deadline);
                    },
                    // Wait for the next run.
                    () = &mut sleep => {
                        tracing::trace!("timer elapsed");
                        let deadline = if let Some(new_deadline) = (callback)() {
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
                    _ = &mut shutdown => {
                        tracing::trace!("received shutdown signal");
                        break;
                    }
                }
            }
        }.boxed()
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

        tracing::trace!("scheduled a new execution for {}", deadline);

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
