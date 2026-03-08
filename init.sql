CREATE TABLE IF NOT EXISTS clicks (
    user_id     UUID,
    product_id  UUID,
    session_id  UUID,
    timestamp   DateTime,
    source      String,
    position    UInt64,
    category    String
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, user_id);

CREATE TABLE IF NOT EXISTS views (
    user_id     UUID,
    product_id  UUID,
    session_id  UUID,
    timestamp   DateTime,
    source      String,
    duration_ms UInt64,
    referrer    String
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, product_id);

CREATE TABLE IF NOT EXISTS purchases (
    user_id         UUID,
    product_id      UUID,
    order_id        UUID,
    session_id      UUID,
    timestamp       DateTime,
    quantity        UInt64,
    price_cents     UInt64,
    currency        String,
    payment_method  String
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, product_id);

CREATE TABLE IF NOT EXISTS views_per_minute (
    minute     DateTime,
    product_id UInt64,
    views      UInt64
) ENGINE = SummingMergeTree()
ORDER BY (minute, product_id)
TTL minute + INTERVAL 24 HOUR;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_views
TO views_per_minute
AS SELECT
    toStartOfMinute(timestamp) AS minute,
    product_id,
    count()                    AS views
FROM views
GROUP BY minute, product_id;

CREATE TABLE IF NOT EXISTS revenue_by_minute (
    minute        DateTime,
    revenue_cents UInt64
) ENGINE = SummingMergeTree()
ORDER BY (minute)
TTL minute + INTERVAL 24 HOUR;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_revenue
TO revenue_by_minute
AS SELECT
FROM purchases
    toStartOfMinute(timestamp)  AS minute,
    sum(quantity * price_cents) AS revenue
GROUP BY minute;

CREATE TABLE IF NOT EXISTS conversions_per_minute (
    minute    DateTime,
    clicks    UInt64,
    purchases UInt64
) ENGINE = SummingMergeTree()
ORDER BY (minute)
TTL minute + INTERVAL 24 HOUR;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_conversions_clicks
TO conversions_per_minute
AS SELECT
    toStartOfMinute(timestamp) AS minute,
    count()                    AS clicks,
    0                          AS purchases
FROM clicks
GROUP BY minute;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_conversions_purchases
TO conversions_per_minute
AS SELECT
    toStartOfMinute(timestamp) AS minute,
    0                          AS clicks,
    count()                    AS purchases
FROM purchases
GROUP BY minute;