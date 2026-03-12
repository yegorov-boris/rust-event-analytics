use clickhouse::Row;
use futures::StreamExt;
use prost::Message;
use rskafka::client::{
    consumer::{StartOffset, StreamConsumerBuilder},
    partition::{PartitionClient, UnknownTopicHandling},
    ClientBuilder,
};
use serde::Serialize;
use shared::events;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Row, Serialize)]
struct ViewRow {
    #[serde(with = "clickhouse::serde::uuid")]
    user_id: Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    product_id: Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    session_id: Uuid,
    timestamp: u32,
    source: String,
    duration_ms: u64,
    referrer: String,
}

async fn partition_client_with_retry(
    client: &rskafka::client::Client,
    topic: &str,
    partition: i32,
) -> Arc<PartitionClient> {
    loop {
        match client
            .partition_client(topic, partition, UnknownTopicHandling::Retry)
            .await
        {
            Ok(pc) => return Arc::new(pc),
            Err(e) => {
                eprintln!("Waiting for {topic}:{partition}: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

fn consume_clicks(partition: Arc<PartitionClient>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut stream = StreamConsumerBuilder::new(partition, StartOffset::Latest)
            .with_max_wait_ms(500)
            .build();
        while let Some(result) = stream.next().await {
            match result {
                Ok((record_and_offset, _high_watermark)) => {
                    if let Some(bytes) = record_and_offset.record.value {
                        match events::ClickEvent::decode(bytes.as_slice()) {
                            Ok(event) => println!("click: {event:?}"),
                            Err(e) => eprintln!("failed to decode click: {e}"),
                        }
                    }
                }
                Err(e) => eprintln!("consumer error: {e}"),
            }
        }
    })
}

fn consume_views(
    partition: Arc<PartitionClient>,
    ch: clickhouse::Client,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut stream = StreamConsumerBuilder::new(partition, StartOffset::Latest)
            .with_max_wait_ms(500)
            .build();
        while let Some(result) = stream.next().await {
            match result {
                Ok((record_and_offset, _high_watermark)) => {
                    if let Some(bytes) = record_and_offset.record.value {
                        match events::ViewEvent::decode(bytes.as_slice()) {
                            Ok(event) => {
                                let meta = event.metadata.unwrap_or_default();
                                let ts = event.timestamp.unwrap_or_default();
                                let row = ViewRow {
                                    user_id: event.user_id.parse().unwrap_or_default(),
                                    product_id: event.product_id.parse().unwrap_or_default(),
                                    session_id: event.session_id.parse().unwrap_or_default(),
                                    timestamp: ts.seconds as u32,
                                    source: meta.source,
                                    duration_ms: meta.duration_ms,
                                    referrer: meta.referrer,
                                };
                                match ch.insert("views") {
                                    Ok(mut insert) => {
                                        if let Err(e) = insert.write(&row).await {
                                            eprintln!("failed to write view: {e}");
                                        } else if let Err(e) = insert.end().await {
                                            eprintln!("failed to commit view insert: {e}");
                                        }
                                    }
                                    Err(e) => eprintln!("failed to create insert: {e}"),
                                }
                            }
                            Err(e) => eprintln!("failed to decode view: {e}"),
                        }
                    }
                }
                Err(e) => eprintln!("consumer error: {e}"),
            }
        }
    })
}

fn consume_purchases(partition: Arc<PartitionClient>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut stream = StreamConsumerBuilder::new(partition, StartOffset::Latest)
            .with_max_wait_ms(500)
            .build();
        while let Some(result) = stream.next().await {
            match result {
                Ok((record_and_offset, _high_watermark)) => {
                    if let Some(bytes) = record_and_offset.record.value {
                        match events::PurchaseEvent::decode(bytes.as_slice()) {
                            Ok(event) => println!("purchase: {event:?}"),
                            Err(e) => eprintln!("failed to decode purchase: {e}"),
                        }
                    }
                }
                Err(e) => eprintln!("consumer error: {e}"),
            }
        }
    })
}

#[tokio::main]
async fn main() {
    let kafka = ClientBuilder::new(vec!["kafka:9092".to_string()])
        .build()
        .await
        .expect("failed to connect to Kafka");

    let user = std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string());
    let password = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();
    let ch = clickhouse::Client::default()
        .with_url("http://clickhouse:8123")
        .with_user(user)
        .with_password(password);

    let mut handles = Vec::new();

    for partition in 0..3 {
        let clicks = partition_client_with_retry(&kafka, "events.clicks", partition).await;
        let views = partition_client_with_retry(&kafka, "events.views", partition).await;
        let purchases =
            partition_client_with_retry(&kafka, "events.purchases", partition).await;

        handles.push(consume_clicks(clicks));
        handles.push(consume_views(views, ch.clone()));
        handles.push(consume_purchases(purchases));
    }

    for handle in handles {
        handle.await.unwrap();
    }
}
