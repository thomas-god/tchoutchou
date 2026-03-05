FROM rust:1.93-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Cache dependencies
COPY Cargo.toml Cargo.lock ./
COPY app/Cargo.toml app/Cargo.toml
RUN mkdir -p app/src && echo "fn main() {}" > app/src/main.rs
RUN cargo build --release --locked --bin app
RUN rm app/src/main.rs

# Build the real binary
COPY app/src app/src
RUN touch app/src/main.rs && cargo build --release --locked --bin app

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/app /usr/local/bin/app

ENTRYPOINT ["/usr/local/bin/app"]
