use futures::StreamExt;
use prost::Message;
use rskafka::client::{
    consumer::{StartOffset, StreamConsumerBuilder},
    partition::{PartitionClient, UnknownTopicHandling},
    ClientBuilder,
};
use shared::events;
use std::sync::Arc;

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

fn consume_views(partition: Arc<PartitionClient>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut stream = StreamConsumerBuilder::new(partition, StartOffset::Latest)
            .with_max_wait_ms(500)
            .build();
        while let Some(result) = stream.next().await {
            match result {
                Ok((record_and_offset, _high_watermark)) => {
                    if let Some(bytes) = record_and_offset.record.value {
                        match events::ViewEvent::decode(bytes.as_slice()) {
                            Ok(event) => println!("view: {event:?}"),
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
    let client = ClientBuilder::new(vec!["kafka:9092".to_string()])
        .build()
        .await
        .expect("failed to connect to Kafka");

    let mut handles = Vec::new();

    for partition in 0..3 {
        let clicks = partition_client_with_retry(&client, "events.clicks", partition).await;
        let views = partition_client_with_retry(&client, "events.views", partition).await;
        let purchases =
            partition_client_with_retry(&client, "events.purchases", partition).await;

        handles.push(consume_clicks(clicks));
        handles.push(consume_views(views));
        handles.push(consume_purchases(purchases));
    }

    for handle in handles {
        handle.await.unwrap();
    }
}
