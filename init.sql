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