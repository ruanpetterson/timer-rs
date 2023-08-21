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
pub struct Timer {
    callback: Pin<Box<dyn Fn() -> Option<NaiveDateTime> + Send>>,
    deadline: Instant,
    watchdog: (mpsc::Sender<NaiveDateTime>, mpsc::Receiver<NaiveDateTime>),
    shutdown: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl Timer {
    /// Creates a Timer instance.
    pub fn new(
        callback: impl Fn() -> Option<NaiveDateTime> + Send + 'static,
    ) -> Self {
        let watchdog = mpsc::channel(1);

        Self {
            callback: Box::pin(callback),
            // Since it's the first call, it starts sleeping forever.
            deadline: far_future(),
            watchdog,
            shutdown: None,
        }
    }

    pub fn with_graceful_shutdown<O>(
        self,
        shutdown: impl Future<Output = O> + Send + 'static,
    ) -> Self {
        Self {
            shutdown: Some(Box::pin(shutdown.map(|_| ()))),
            ..self
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

impl IntoFuture for Timer {
    type Output = ();
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            let Self {
                callback,
                deadline,
                watchdog: (_, watchdog),
                shutdown,
            } = self;

            let sleep = tokio::time::sleep_until(deadline);
            let mut shutdown = shutdown.unwrap_or_else(|| Box::pin(futures::future::pending()));

            futures::pin_mut!(sleep);
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

    pub fn blocking_schedule(&self, deadline: NaiveDateTime) -> Result<()> {
        self.0
            .blocking_send(deadline)
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

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::{Arc, Mutex, PoisonError};

    use tokio::sync::mpsc;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn it_works() {
        let buf = Vec::with_capacity(1024);
        let shared_buf = Arc::new(Mutex::new(buf));

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

        let (evaluate_tx, mut evaluate_rx) = mpsc::channel::<()>(1);

        let shared_buf_ = shared_buf.clone();
        let timer = Timer::new(move || {
            let mut buf = shared_buf_.lock().unwrap();
            buf.write(b"it works").unwrap();
            tokio::task::block_in_place(|| {
                shutdown_tx.blocking_send(()).unwrap();
            });
            None
        })
        .with_graceful_shutdown(async move {
            shutdown_rx.recv().await;
        });

        let scheduler = timer.scheduler();

        tokio::spawn(async move {
            timer.await;
            evaluate_tx.send(()).await.unwrap();
        });

        scheduler
            .schedule(Utc::now().naive_utc() + chrono::Duration::seconds(3))
            .await
            .unwrap();

        evaluate_rx
            .recv()
            .then(|_| async {
                assert_eq!(
                    shared_buf
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .as_slice(),
                    b"it works"
                )
            })
            .await;
    }
}
