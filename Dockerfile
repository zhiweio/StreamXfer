# ============================================================
# Stage 1: Build the Rust binary
# ============================================================
FROM rust:1.86-slim AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/

RUN cargo build --release --bin stx && \
    strip target/release/stx

# ============================================================
# Stage 2: Minimal runtime image
# ============================================================
FROM debian:bookworm-slim

LABEL maintainer="Wang Zhiwei <noparking188@gmail.com>"
LABEL org.opencontainers.image.source="https://github.com/zhiweio/StreamXfer"
LABEL org.opencontainers.image.description="StreamXfer: High-performance SQL Server data export tool"

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/stx /usr/local/bin/stx

RUN useradd -m -s /bin/bash streamxfer
USER streamxfer
WORKDIR /home/streamxfer

ENTRYPOINT ["stx"]
CMD ["--help"]
