use anyhow::Result;
use axum::routing::post;
use axum::Router;
use timer_rs::Timer;
use tokio::signal;
use tokio::sync::broadcast;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing.
    tracing_subscriber::fmt::init();

    // When the provided `shutdown` future completes, we must send a shutdown
    // message to all active connections. We use a broadcast channel for this
    // purpose. The call below ignores the receiver of the broadcast pair, and
    // when a receiver is needed, the subscribe() method on the sender is used
    // to create one.
    let (notify_shutdown, _) = broadcast::channel(1);

    let timer = Timer::new(
        || {
            tracing::info!("Task was executed");
            None
        },
        notify_shutdown.subscribe().into(),
    );

    let scheduler = timer.scheduler();
    tokio::spawn(async move {
        // Build a application with a route.
        let app = Router::new()
            // `GET /` goes to `root`.
            .route("/", post(http::root))
            .with_state(http::AppState { scheduler });

        axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
            .serve(app.into_make_service())
            .await
            .unwrap();
    });

    let timer = tokio::spawn(async move {
        timer.await;
    });

    signal::ctrl_c().await?;

    // When `notify_shutdown` is dropped, all tasks which have `subscribe`d will
    // receive the shutdown signal and can exit
    drop(notify_shutdown);
    drop(timer);

    Ok(())
}

mod http {
    use axum::{extract::State, response::IntoResponse, Json};
    use chrono::NaiveDateTime;
    use timer_rs::Scheduler;

    #[derive(Clone)]
    pub struct AppState {
        pub scheduler: Scheduler,
    }

    #[derive(serde::Deserialize, serde::Serialize)]
    pub struct Payload {
        date: NaiveDateTime,
    }

    #[axum::debug_handler]
    pub async fn root(
        State(state): State<AppState>,
        Json(payload): Json<Payload>,
    ) -> impl IntoResponse {
        tracing::info!("received scheduling for {}", payload.date);
        state.scheduler.schedule(payload.date).await.unwrap();
    }
}
