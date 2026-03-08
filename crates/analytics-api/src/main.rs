use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use actix_web_validator::Query;
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Deserialize)]
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

#[derive(Deserialize, Validate)]
struct TopProductsQuery {
    #[validate(range(min = 1, max = 100))]
    limit: u8,
    #[validate(custom(function = "validate_period"))]
    period: String,
    metric: Metric,
}

#[derive(Row, Deserialize, Serialize)]
struct TopProduct {
    product_id: String,
    count: u64,
}

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
         WHERE timestamp >= now() - INTERVAL {hours} HOUR \
         GROUP BY product_id \
         ORDER BY count DESC \
         LIMIT {limit}",
        limit = query.limit,
    );
    match ch.query(&sql).fetch_all::<TopProduct>().await {
        Ok(rows) => HttpResponse::Ok().json(rows),
        Err(e) => {
            eprintln!("ClickHouse error: {e}");
            HttpResponse::InternalServerError().finish()
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let ch = web::Data::new(
        clickhouse::Client::default().with_url("http://clickhouse:8123"),
    );

    HttpServer::new(move || {
        App::new()
            .app_data(ch.clone())
            .service(top_products)
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}