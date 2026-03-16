use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use actix_web_prom::PrometheusMetricsBuilder;
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
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;
use validator::Validate;

#[get("/health")]
async fn health() -> impl Responder {
    HttpResponse::Ok().finish()
}

#[derive(Deserialize, Validate, ToSchema)]
struct ClickEventMetadata {
    #[validate(length(min = 1))]
    source: String,
    position: usize,
    #[validate(length(min = 1))]
    category: String,
}

#[derive(Deserialize, Validate, ToSchema)]
struct ClickEvent {
    user_id: Uuid,
    product_id: Uuid,
    timestamp: DateTime<Utc>,
    session_id: Uuid,
    #[validate]
    metadata: ClickEventMetadata,
}

#[utoipa::path(
    post,
    path = "/api/v1/events/click",
    request_body = ClickEvent,
    responses(
        (status = 200, description = "Event accepted"),
        (status = 400, description = "Invalid request body"),
    )
)]
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

#[derive(Deserialize, Validate, ToSchema)]
struct ViewEventMetadata {
    #[validate(length(min = 1))]
    source: String,
    duration_ms: usize,
    #[validate(length(min = 1))]
    referrer: String,
}

#[derive(Deserialize, Validate, ToSchema)]
struct ViewEvent {
    user_id: Uuid,
    product_id: Uuid,
    timestamp: DateTime<Utc>,
    session_id: Uuid,
    #[validate]
    metadata: ViewEventMetadata,
}

#[utoipa::path(
    post,
    path = "/api/v1/events/view",
    request_body = ViewEvent,
    responses(
        (status = 200, description = "Event accepted"),
        (status = 400, description = "Invalid request body"),
    )
)]
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

#[derive(Deserialize, Validate, ToSchema)]
struct PurchaseEventMetadata {
    quantity: usize,
    price_cents: u64,
    #[validate(length(min = 1))]
    currency: String,
    #[validate(length(min = 1))]
    payment_method: String,
}

#[derive(Deserialize, Validate, ToSchema)]
struct PurchaseEvent {
    user_id: Uuid,
    product_id: Uuid,
    order_id: Uuid,
    timestamp: DateTime<Utc>,
    session_id: Uuid,
    #[validate]
    metadata: PurchaseEventMetadata,
}

#[utoipa::path(
    post,
    path = "/api/v1/events/purchase",
    request_body = PurchaseEvent,
    responses(
        (status = 200, description = "Event accepted"),
        (status = 400, description = "Invalid request body"),
    )
)]
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

async fn build_producer(brokers: &str) -> EventProducer {
    let client = ClientBuilder::new(vec![brokers.to_string()])
        .build()
        .await
        .expect("failed to connect to Kafka");

    let mut clicks = Vec::new();
    let mut views = Vec::new();
    let mut purchases = Vec::new();

    for partition in 0..3 {
        clicks.push(partition_client_with_retry(&client, "events.clicks", partition).await);
        views.push(partition_client_with_retry(&client, "events.views", partition).await);
        purchases.push(partition_client_with_retry(&client, "events.purchases", partition).await);
    }

    EventProducer {
        clicks,
        views,
        purchases,
        counter: AtomicU64::new(0),
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(ingest_click, ingest_view, ingest_purchase),
    components(schemas(
        ClickEvent, ClickEventMetadata,
        ViewEvent, ViewEventMetadata,
        PurchaseEvent, PurchaseEventMetadata,
    ))
)]
struct ApiDoc;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let producer = web::Data::new(build_producer("kafka:9092").await);
    let prometheus = PrometheusMetricsBuilder::new("api")
        .endpoint("/metrics")
        .build()
        .unwrap();

    HttpServer::new(move || {
        App::new()
            .wrap(prometheus.clone())
            .app_data(producer.clone())
            .service(health)
            .service(ingest_click)
            .service(ingest_view)
            .service(ingest_purchase)
            .service(
                SwaggerUi::new("/swagger-ui/{_:.*}")
                    .url("/api-doc/openapi.json", ApiDoc::openapi()),
            )
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}