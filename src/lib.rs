//! A simple timer, used to enqueue operations meant to be executed at
//! a given time or after a given delay.

use std::collections::BTreeMap;
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
    watchdog: (
        mpsc::Sender<(NaiveDateTime, Box<dyn FnOnce() + Send>)>,
        mpsc::Receiver<(NaiveDateTime, Box<dyn FnOnce() + Send>)>,
    ),
    shutdown: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    callbacks: BTreeMap<NaiveDateTime, Vec<Box<dyn FnOnce() + Send>>>,
}

impl Timer {
    /// Creates a `Timer` instance within a given closure.
    ///
    /// To provide a new closure in every schedule, see
    pub fn new() -> Self {
        let watchdog = mpsc::channel(1);

        Timer {
            watchdog,
            shutdown: None,
            callbacks: BTreeMap::default(),
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

    /// Creates a handler to schedule new executions.
    pub fn scheduler(&self) -> Scheduler {
        Scheduler(self.watchdog.0.clone())
    }

    /// Schedule for execution after a delay.
    pub async fn schedule(
        &self,
        deadline: NaiveDateTime,
        callback: impl FnOnce() + Send + 'static,
    ) -> Result<()> {
        self.scheduler().schedule(deadline, callback).await
    }
}

impl IntoFuture for Timer {
    type Output = ();
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            let Self {
                watchdog: (_, watchdog),
                shutdown,
                mut callbacks,
            } = self;

            let sleep = tokio::time::sleep_until(far_future());
            let mut shutdown = shutdown
                .unwrap_or_else(|| Box::pin(futures::future::pending()));

            futures::pin_mut!(sleep);
            futures::pin_mut!(watchdog);

            loop {
                if let Some(entry) = callbacks.first_entry() {
                    let deadline = {
                        let duration = *entry.key() - Utc::now().naive_utc();
                        // The occurrence of failure is limited to cases where the
                        // `duration` is negative and only arises once the deadline is
                        // reached. Consequently, it is essential to execute the stored
                        // closures in such situations.
                        let Ok(duration) = duration.to_std() else {
                            for callback in entry.remove() {
                                (callback)();
                            }
                            continue;
                        };

                        Instant::now() + duration
                    };

                    sleep.as_mut().reset(deadline);

                    tokio::select! {
                        // Wait for a new deadline.
                        Some((new_deadline, callback)) = watchdog.recv() => {
                            callbacks.entry(new_deadline).or_default().push(callback);
                        },
                        // Wait for the next run.
                        () = &mut sleep => {
                            tracing::trace!("timer elapsed");
                            for callback in entry.remove() {
                                (callback)();
                            }
                        },
                        _ = &mut shutdown => {
                            tracing::trace!("received shutdown signal");
                            break;
                        }
                    };
                } else {
                    tokio::select! {
                        // Wait for a new deadline.
                        Some((new_deadline, callback)) = watchdog.recv() => {
                            callbacks.entry(new_deadline).or_default().push(callback);
                        },
                        _ = &mut shutdown => {
                            tracing::trace!("received shutdown signal");
                            break;
                        }
                    };
                }
            }
        }
        .boxed()
    }
}

#[derive(Clone)]
pub struct Scheduler(mpsc::Sender<(NaiveDateTime, Box<dyn FnOnce() + Send>)>);

impl Scheduler {
    /// Schedule for execution after a delay.
    pub async fn schedule(
        &self,
        deadline: NaiveDateTime,
        callback: impl FnOnce() + Send + 'static,
    ) -> Result<()> {
        self.0
            .send((deadline, Box::new(callback)))
            .await
            .map_err(|_| TimerError::Closed)?;

        tracing::trace!("scheduled a new execution for {}", deadline);

        Ok(())
    }

    pub fn blocking_schedule(
        &self,
        deadline: NaiveDateTime,
        callback: impl FnOnce() + Send + 'static,
    ) -> Result<()> {
        self.0
            .blocking_send((deadline, Box::new(callback)))
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
        // Create a buffer and make it shareable.
        let buf = Vec::with_capacity(1024);
        let shared_buf = Arc::new(Mutex::new(buf));

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

        let (evaluate_tx, mut evaluate_rx) = mpsc::channel::<()>(1);

        // Clone the sharde buffer and send it to the Timer task.
        let shared_buf_ = shared_buf.clone();
        let timer = Timer::new().with_graceful_shutdown(async move {
            // Waits the shutdown signal to be received.
            shutdown_rx.recv().await;
        });

        // Schedule a new run in the next 3 seconds.
        timer
            .schedule(
                Utc::now().naive_utc() + chrono::Duration::seconds(3),
                move || {
                    let mut buf = shared_buf_
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    buf.write(b"it works").unwrap();
                    tokio::task::block_in_place(|| {
                        // As soon as the task is run, send a shutdown signal.
                        shutdown_tx.blocking_send(()).unwrap();
                    });
                },
            )
            .await
            .unwrap();

        // Spawn the Timer.
        tokio::spawn(async move {
            timer.await;
            evaluate_tx.send(()).await.unwrap();
        });

        // Waits the Timer be shutdown and evaluate the buffer.
        evaluate_rx
            .recv()
            .then(|_| async {
                let buf = Arc::into_inner(shared_buf)
                    .map(|b| {
                        Mutex::into_inner(b)
                            .unwrap_or_else(PoisonError::into_inner)
                    })
                    .unwrap();

                assert_eq!(buf, b"it works")
            })
            .await;
    }
}
