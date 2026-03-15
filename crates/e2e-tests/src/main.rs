use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration as StdDuration;
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
    timestamp: DateTime<Utc>,
    session_id: String,
    metadata: ViewEventMetadata,
}

#[derive(Deserialize, Debug)]
struct TopProduct {
    product_id: String,
    count: u64,
}

async fn wait_healthy(client: &reqwest::Client, url: &str) {
    loop {
        match client.get(url).send().await {
            Ok(r) if r.status().is_success() => {
                println!("{url} is healthy");
                return;
            }
            Ok(r) => eprintln!("{url} returned {}", r.status()),
            Err(e) => eprintln!("{url} not reachable: {e}"),
        }
        tokio::time::sleep(StdDuration::from_secs(1)).await;
    }
}

async fn send_view(
    client: &reqwest::Client,
    ingestion_url: &str,
    product_id: &str,
    timestamp: DateTime<Utc>,
) {
    let event = ViewEvent {
        user_id: Uuid::new_v4().to_string(),
        product_id: product_id.to_string(),
        timestamp,
        session_id: Uuid::new_v4().to_string(),
        metadata: ViewEventMetadata {
            source: "web".to_string(),
            duration_ms: 1000,
            referrer: "https://example.com".to_string(),
        },
    };
    let status = client
        .post(format!("{ingestion_url}/api/v1/events/view"))
        .json(&event)
        .send()
        .await
        .expect("failed to send view event")
        .status();
    if !status.is_success() {
        panic!("ingestion-api returned {status}");
    }
}

async fn top_products(
    client: &reqwest::Client,
    analytics_url: &str,
    period: &str,
) -> Vec<TopProduct> {
    client
        .get(format!(
            "{analytics_url}/api/v1/analytics/top-products?limit=20&period={period}&metric=views"
        ))
        .send()
        .await
        .expect("analytics request failed")
        .json()
        .await
        .expect("failed to parse top-products response")
}

#[tokio::main]
async fn main() {
    let client = reqwest::Client::new();

    let ingestion_api = std::env::var("INGESTION_API_URL")
        .unwrap_or_else(|_| "http://ingestion-api:8080".to_string());
    let analytics_api = std::env::var("ANALYTICS_API_URL")
        .unwrap_or_else(|_| "http://analytics-api:8080".to_string());
    let ch_url = std::env::var("CLICKHOUSE_URL")
        .unwrap_or_else(|_| "http://clickhouse:8123".to_string());
    let ch_user = std::env::var("CLICKHOUSE_USER")
        .unwrap_or_else(|_| "default".to_string());
    let ch_password = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();

    wait_healthy(&client, &format!("{ingestion_api}/health")).await;
    wait_healthy(&client, &format!("{analytics_api}/health")).await;

    // Reset state
    let ch = clickhouse::Client::default()
        .with_url(&ch_url)
        .with_user(ch_user)
        .with_password(ch_password);
    ch.query("TRUNCATE TABLE views").execute().await.expect("truncate views");
    ch.query("TRUNCATE TABLE views_per_minute").execute().await.expect("truncate views_per_minute");

    let now = Utc::now();
    let product_a = Uuid::new_v4().to_string();
    let product_b = Uuid::new_v4().to_string();

    // product_a: 10 views in the last 30 min  → #1 for 1h, #2 for 2h
    for _ in 0..10 {
        send_view(&client, &ingestion_api, &product_a, now - Duration::minutes(30)).await;
    }

    // product_b: 3 views at -30 min + 10 views at -90 min → #2 for 1h, #1 for 2h (13 total)
    for _ in 0..3 {
        send_view(&client, &ingestion_api, &product_b, now - Duration::minutes(30)).await;
    }
    for _ in 0..10 {
        send_view(&client, &ingestion_api, &product_b, now - Duration::minutes(90)).await;
    }

    println!("Sent events, waiting for worker to process...");
    tokio::time::sleep(StdDuration::from_secs(5)).await;

    // --- 1h assertion: product_a is #1 ---
    let results_1h = top_products(&client, &analytics_api, "1h").await;
    println!("1h results: {results_1h:?}");
    assert!(
        !results_1h.is_empty(),
        "1h top-products returned no results"
    );
    assert_eq!(
        results_1h[0].product_id, product_a,
        "expected product_a to be #1 for 1h, got {:?}",
        results_1h
    );
    assert_eq!(
        results_1h[0].count, 10,
        "expected product_a count=10 for 1h, got {:?}",
        results_1h
    );

    // --- 2h assertion: product_b is #1 ---
    let results_2h = top_products(&client, &analytics_api, "2h").await;
    println!("2h results: {results_2h:?}");
    assert!(
        !results_2h.is_empty(),
        "2h top-products returned no results"
    );
    assert_eq!(
        results_2h[0].product_id, product_b,
        "expected product_b to be #1 for 2h, got {:?}",
        results_2h
    );
    assert_eq!(
        results_2h[0].count, 13,
        "expected product_b count=13 for 2h, got {:?}",
        results_2h
    );

    println!("All tests passed!");
}