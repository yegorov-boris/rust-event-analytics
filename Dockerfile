FROM rust:slim AS builder

RUN rustup target add x86_64-unknown-linux-musl && \
    apt-get update && apt-get install -y --no-install-recommends musl-tools && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/

RUN RUSTFLAGS='-C target-feature=+crt-static' \
    cargo build --release --target x86_64-unknown-linux-musl -p ingestion-api

FROM scratch

COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/ingestion-api /ingestion-api

EXPOSE 8080

CMD ["/ingestion-api"]