use anyhow::Result;
use axum::routing::post;
use axum::Router;
use timer_rs::Timer;
use tokio::signal;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing.
    tracing_subscriber::fmt::init();

    let timer = Timer::new(|| {
        tracing::info!("task was executed");
        None
    })
    .with_graceful_shutdown(signal::ctrl_c());

    let scheduler = timer.scheduler();
    tokio::spawn(async move {
        let addr = "0.0.0.0:3000".parse().unwrap();

        // Build a application with a route.
        let app = Router::new()
            // `GET /` goes to `root`.
            .route("/", post(http::root))
            .with_state(http::AppState { scheduler });

        tracing::info!("listening on {}", addr);

        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    });

    tokio::spawn(async move { timer.await }).await?;

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
