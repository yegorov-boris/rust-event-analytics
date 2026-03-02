use actix_web::{get, App, HttpResponse, HttpServer, Responder};
use actix_web_validator::Json;
use chrono::{DateTime, Utc};
use serde::Deserialize;
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
async fn ingest_click(body: Json<ClickEvent>) -> impl Responder {
    let _ = body.into_inner();
    HttpResponse::Created().finish()
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
async fn ingest_view(body: Json<ViewEvent>) -> impl Responder {
    let _ = body.into_inner();
    HttpResponse::Created().finish()
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
async fn ingest_purchase(body: Json<PurchaseEvent>) -> impl Responder {
    let _ = body.into_inner();
    HttpResponse::Created().finish()
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .service(health)
            .service(ingest_click)
            .service(ingest_view)
            .service(ingest_purchase)
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}