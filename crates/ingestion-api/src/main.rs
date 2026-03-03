use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use actix_web_validator::Json;
use chrono::{DateTime, Utc};
use prost::Message;
use prost_types::Timestamp;
use rskafka::{
    client::{
        partition::{Compression, PartitionClient, UnknownTopicHandling},
        ClientBuilder,
    },
    record::Record,
};
use serde::Deserialize;
use shared::events;
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use uuid::Uuid;
use validator::Validate;

#[get("/health")]
async fn health() -> impl Responder {
    HttpResponse::Ok().finish()
}

#[derive(Deserialize, Validate)]
struct ClickEventMetadata {
    #[validate(length(min = 1))]
    source: String,
    position: usize,
    #[validate(length(min = 1))]
    category: String,
}

#[derive(Deserialize, Validate)]
struct ClickEvent {
    user_id: Uuid,
    product_id: Uuid,
    timestamp: DateTime<Utc>,
    session_id: Uuid,
    #[validate]
    metadata: ClickEventMetadata,
}

#[actix_web::post("/api/v1/events/click")]
async fn ingest_click(
    body: Json<ClickEvent>,
    producer: web::Data<EventProducer>,
) -> impl Responder {
    let event = body.into_inner();
    let producer = producer.clone();
    tokio::spawn(async move {
        let user_id = event.user_id.to_string();
        let proto = events::ClickEvent {
            user_id: user_id.clone(),
            product_id: event.product_id.to_string(),
            timestamp: Some(to_timestamp(event.timestamp)),
            session_id: event.session_id.to_string(),
            metadata: Some(events::ClickEventMetadata {
                source: event.metadata.source,
                position: event.metadata.position as u64,
                category: event.metadata.category,
            }),
        };
        produce(producer.next(&producer.clicks), user_id, event.timestamp, proto).await;
    });
    HttpResponse::Ok().finish()
}

#[derive(Deserialize, Validate)]
struct ViewEventMetadata {
    #[validate(length(min = 1))]
    source: String,
    duration_ms: usize,
    #[validate(length(min = 1))]
    referrer: String,
}

#[derive(Deserialize, Validate)]
struct ViewEvent {
    user_id: Uuid,
    product_id: Uuid,
    timestamp: DateTime<Utc>,
    session_id: Uuid,
    #[validate]
    metadata: ViewEventMetadata,
}

#[actix_web::post("/api/v1/events/view")]
async fn ingest_view(
    body: Json<ViewEvent>,
    producer: web::Data<EventProducer>,
) -> impl Responder {
    let event = body.into_inner();
    let producer = producer.clone();
    tokio::spawn(async move {
        let user_id = event.user_id.to_string();
        let proto = events::ViewEvent {
            user_id: user_id.clone(),
            product_id: event.product_id.to_string(),
            timestamp: Some(to_timestamp(event.timestamp)),
            session_id: event.session_id.to_string(),
            metadata: Some(events::ViewEventMetadata {
                source: event.metadata.source,
                duration_ms: event.metadata.duration_ms as u64,
                referrer: event.metadata.referrer,
            }),
        };
        produce(producer.next(&producer.views), user_id, event.timestamp, proto).await;
    });
    HttpResponse::Ok().finish()
}

#[derive(Deserialize, Validate)]
struct PurchaseEventMetadata {
    quantity: usize,
    price_cents: u64,
    #[validate(length(min = 1))]
    currency: String,
    #[validate(length(min = 1))]
    payment_method: String,
}

#[derive(Deserialize, Validate)]
struct PurchaseEvent {
    user_id: Uuid,
    product_id: Uuid,
    order_id: Uuid,
    timestamp: DateTime<Utc>,
    session_id: Uuid,
    #[validate]
    metadata: PurchaseEventMetadata,
}

#[actix_web::post("/api/v1/events/purchase")]
async fn ingest_purchase(
    body: Json<PurchaseEvent>,
    producer: web::Data<EventProducer>,
) -> impl Responder {
    let event = body.into_inner();
    let producer = producer.clone();
    tokio::spawn(async move {
        let user_id = event.user_id.to_string();
        let proto = events::PurchaseEvent {
            user_id: user_id.clone(),
            product_id: event.product_id.to_string(),
            order_id: event.order_id.to_string(),
            timestamp: Some(to_timestamp(event.timestamp)),
            session_id: event.session_id.to_string(),
            metadata: Some(events::PurchaseEventMetadata {
                quantity: event.metadata.quantity as u64,
                price_cents: event.metadata.price_cents,
                currency: event.metadata.currency,
                payment_method: event.metadata.payment_method,
            }),
        };
        produce(
            producer.next(&producer.purchases),
            user_id,
            event.timestamp,
            proto,
        )
        .await;
    });
    HttpResponse::Ok().finish()
}

async fn produce(
    partition: Arc<PartitionClient>,
    key: String,
    timestamp: DateTime<Utc>,
    message: impl Message,
) {
    let mut buf = Vec::new();
    if message.encode(&mut buf).is_err() {
        return;
    }
    let record = Record {
        key: Some(key.into_bytes()),
        value: Some(buf),
        headers: BTreeMap::new(),
        timestamp,
    };
    partition
        .produce(vec![record], Compression::NoCompression)
        .await
        .ok();
}

fn to_timestamp(dt: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

struct EventProducer {
    clicks: Vec<Arc<PartitionClient>>,
    views: Vec<Arc<PartitionClient>>,
    purchases: Vec<Arc<PartitionClient>>,
    counter: AtomicU64,
}

impl EventProducer {
    fn next(&self, partitions: &[Arc<PartitionClient>]) -> Arc<PartitionClient> {
        let idx = self.counter.fetch_add(1, Ordering::Relaxed) as usize % partitions.len();
        Arc::clone(&partitions[idx])
    }
}

async fn build_producer(brokers: &str) -> EventProducer {
    let client = ClientBuilder::new(vec![brokers.to_string()])
        .build()
        .await
        .expect("failed to connect to Kafka");

    let mut clicks = Vec::new();
    let mut views = Vec::new();
    let mut purchases = Vec::new();

    for partition in 0..3 {
        clicks.push(Arc::new(
            client
                .partition_client("events.clicks", partition, UnknownTopicHandling::Retry)
                .await
                .expect("failed to create partition client for events.clicks"),
        ));
        views.push(Arc::new(
            client
                .partition_client("events.views", partition, UnknownTopicHandling::Retry)
                .await
                .expect("failed to create partition client for events.views"),
        ));
        purchases.push(Arc::new(
            client
                .partition_client("events.purchases", partition, UnknownTopicHandling::Retry)
                .await
                .expect("failed to create partition client for events.purchases"),
        ));
    }

    EventProducer {
        clicks,
        views,
        purchases,
        counter: AtomicU64::new(0),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let producer = web::Data::new(build_producer("kafka:9092").await);

    HttpServer::new(move || {
        App::new()
            .app_data(producer.clone())
            .service(health)
            .service(ingest_click)
            .service(ingest_view)
            .service(ingest_purchase)
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}