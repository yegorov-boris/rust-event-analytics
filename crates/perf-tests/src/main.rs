use chrono::{Duration as ChDuration, Utc};
use rand::Rng;
use reqwest::Client;
use serde::Serialize;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::Instant;
use uuid::Uuid;

#[derive(Serialize)]
struct ViewEventMetadata {
    source: String,
    duration_ms: u64,
    referrer: String,
}

#[derive(Serialize)]
struct ViewEvent {
    user_id: String,
    product_id: String,
    timestamp: chrono::DateTime<Utc>,
    session_id: String,
    metadata: ViewEventMetadata,
}

fn random_view_event() -> ViewEvent {
    let offset = rand::thread_rng().gen_range(0u32..3600);
    ViewEvent {
        user_id: Uuid::new_v4().to_string(),
        product_id: Uuid::new_v4().to_string(),
        timestamp: Utc::now() - ChDuration::seconds(offset as i64),
        session_id: Uuid::new_v4().to_string(),
        metadata: ViewEventMetadata {
            source: "web".to_string(),
            duration_ms: 500,
            referrer: "https://example.com".to_string(),
        },
    }
}

const TICK_MS: u64 = 100;

async fn run_ramp<F, Fut>(
    name: &'static str,
    start_rps: f64,
    max_rps: f64,
    ramp_duration: Duration,
    max_latency: Duration,
    request: F,
) where
    F: Fn() -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Duration> + Send + 'static,
{
    let start = Instant::now();
    let stopped = Arc::new(AtomicBool::new(false));
    let tick = Duration::from_millis(TICK_MS);
    let mut ticker = tokio::time::interval(tick);

    loop {
        ticker.tick().await;

        if stopped.load(Ordering::Relaxed) {
            break;
        }

        let elapsed = start.elapsed();
        if elapsed >= ramp_duration {
            println!("{name}: ramp complete, reached {max_rps:.0} RPS");
            break;
        }

        let progress = elapsed.as_secs_f64() / ramp_duration.as_secs_f64();
        let current_rps = start_rps + (max_rps - start_rps) * progress;
        let n = (current_rps * TICK_MS as f64 / 1000.0).round() as u32;

        for _ in 0..n {
            let f = request.clone();
            let stopped = stopped.clone();

            tokio::spawn(async move {
                let latency = f().await;

                if latency > max_latency && !stopped.swap(true, Ordering::Relaxed) {
                    println!(
                        "{name}: stopping at {current_rps:.0} RPS — \
                         latency {latency:.2?} exceeded limit {max_latency:.2?}"
                    );
                }
            });
        }
    }
}

#[tokio::main]
async fn main() {
    let client = Client::new();

    let ingestion_api = std::env::var("INGESTION_API_URL")
        .unwrap_or_else(|_| "http://ingestion-api:8080".to_string());
    let analytics_api = std::env::var("ANALYTICS_API_URL")
        .unwrap_or_else(|_| "http://analytics-api:8080".to_string());

    let ingestion_client = client.clone();
    let ingestion_url = format!("{ingestion_api}/api/v1/events/view");

    let analytics_client = client.clone();
    let analytics_url = format!(
        "{analytics_api}/api/v1/analytics/top-products?limit=20&period=1h&metric=views"
    );

    let ingestion = tokio::spawn(run_ramp(
        "ingestion-api",
        100.0,
        10_000.0,
        Duration::from_secs(300),
        Duration::from_secs(1),
        move || {
            let client = ingestion_client.clone();
            let url = ingestion_url.clone();
            async move {
                let start = Instant::now();
                let _ = client.post(&url).json(&random_view_event()).send().await;
                start.elapsed()
            }
        },
    ));

    let analytics = tokio::spawn(run_ramp(
        "analytics-api",
        100.0,
        1_000.0,
        Duration::from_secs(300),
        Duration::from_secs(30),
        move || {
            let client = analytics_client.clone();
            let url = analytics_url.clone();
            async move {
                let start = Instant::now();
                let _ = client.get(&url).send().await;
                start.elapsed()
            }
        },
    ));

    let _ = tokio::join!(ingestion, analytics);

    println!("Perf tests complete!");
}
