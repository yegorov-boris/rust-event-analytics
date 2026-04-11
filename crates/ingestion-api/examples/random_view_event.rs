use chrono::Utc;
use rand::Rng;
use serde::Serialize;
use uuid::Uuid;

#[derive(Serialize)]
struct ViewEventMetadata {
    source: String,
    duration_ms: usize,
    referrer: String,
}

#[derive(Serialize)]
struct ViewEvent {
    user_id: Uuid,
    product_id: Uuid,
    timestamp: chrono::DateTime<Utc>,
    session_id: Uuid,
    metadata: ViewEventMetadata,
}

fn main() {
    let mut rng = rand::thread_rng();

    let sources = ["web", "mobile", "tablet"];
    let referrers = [
        "https://google.com",
        "https://example.com",
        "https://twitter.com",
        "direct",
    ];

    let event = ViewEvent {
        user_id: Uuid::new_v4(),
        product_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        session_id: Uuid::new_v4(),
        metadata: ViewEventMetadata {
            source: sources[rng.gen_range(0..sources.len())].to_string(),
            duration_ms: rng.gen_range(100..30_000),
            referrer: referrers[rng.gen_range(0..referrers.len())].to_string(),
        },
    };

    println!("{}", serde_json::to_string_pretty(&event).unwrap());
}