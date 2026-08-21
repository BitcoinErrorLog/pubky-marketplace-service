FROM rust:1.89-slim-bookworm AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY contracts ./contracts
RUN cargo build --release -p marketplace-service

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/marketplace-service /usr/local/bin/marketplace-service
# Railway injects PORT; the service reads BIND_ADDR. Bridge them at startup.
ENV RUST_LOG=info
CMD ["sh", "-c", "BIND_ADDR=0.0.0.0:${PORT:-8080} exec marketplace-service"]
