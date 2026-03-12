use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use actix_web_validator::Query;
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;
use validator::Validate;

#[derive(Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
enum Metric {
    Views,
}

fn validate_period(period: &str) -> Result<(), validator::ValidationError> {
    if !period.ends_with('h') {
        return Err(validator::ValidationError::new("invalid_format"));
    }
    let hours: u32 = period[..period.len() - 1]
        .parse()
        .map_err(|_| validator::ValidationError::new("invalid_format"))?;
    if !(1..=24).contains(&hours) {
        return Err(validator::ValidationError::new("out_of_range"));
    }
    Ok(())
}

#[derive(Deserialize, Validate, IntoParams)]
struct TopProductsQuery {
    /// Number of products to return (1-100)
    #[validate(range(min = 1, max = 100))]
    limit: u8,
    /// Time period (e.g. 1h, 12h, 24h)
    #[validate(custom(function = "validate_period"))]
    period: String,
    metric: Metric,
}

#[derive(Row, Deserialize)]
struct TopProductRow {
    #[serde(with = "clickhouse::serde::uuid")]
    product_id: Uuid,
    count: u64,
}

#[derive(Serialize, ToSchema)]
struct TopProduct {
    product_id: String,
    count: u64,
}

#[utoipa::path(
    get,
    path = "/api/v1/analytics/top-products",
    params(TopProductsQuery),
    responses(
        (status = 200, description = "Top products by metric", body = Vec<TopProduct>),
        (status = 400, description = "Invalid query parameters"),
        (status = 500, description = "Internal server error"),
    )
)]
#[get("/api/v1/analytics/top-products")]
async fn top_products(
    query: Query<TopProductsQuery>,
    ch: web::Data<clickhouse::Client>,
) -> impl Responder {
    let hours: u32 = query.period.strip_suffix('h').unwrap().parse().unwrap();
    let table = match query.metric {
        Metric::Views => "views_per_minute",
    };
    let sql = format!(
        "SELECT product_id, count() AS count \
         FROM {table} \
         WHERE minute >= now() - INTERVAL {hours} HOUR \
         GROUP BY product_id \
         ORDER BY count DESC \
         LIMIT {limit}",
        limit = query.limit,
    );
    match ch.query(&sql).fetch_all::<TopProductRow>().await {
        Ok(rows) => HttpResponse::Ok().json(
            rows.into_iter()
                .map(|r| TopProduct { product_id: r.product_id.to_string(), count: r.count })
                .collect::<Vec<_>>(),
        ),
        Err(e) => {
            eprintln!("ClickHouse error: {e}");
            HttpResponse::InternalServerError().finish()
        }
    }
}

#[derive(OpenApi)]
#[openapi(paths(top_products), components(schemas(TopProduct, Metric)))]
struct ApiDoc;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let user = std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string());
    let password = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();
    let ch = web::Data::new(
        clickhouse::Client::default()
            .with_url("http://clickhouse:8123")
            .with_user(user)
            .with_password(password),
    );

    HttpServer::new(move || {
        App::new()
            .app_data(ch.clone())
            .service(top_products)
            .service(
                SwaggerUi::new("/swagger-ui/{_:.*}")
                    .url("/api-doc/openapi.json", ApiDoc::openapi()),
            )
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}