# Multi-stage build for the rs-waku node binary.
FROM rust:1-bookworm AS builder
WORKDIR /build

# Cache dependencies, then build the binary in release mode.
COPY . .
RUN cargo build --release -p wakunode

# Minimal runtime image.
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Run as a non-root user; persist identity + store under /data.
RUN useradd -m -u 10001 waku
USER waku
WORKDIR /data

COPY --from=builder /build/target/release/wakunode /usr/local/bin/wakunode

# discv5 (UDP), libp2p TCP, REST API.
EXPOSE 9000/udp 60000/tcp 8645/tcp

ENTRYPOINT ["wakunode"]
# Sensible defaults: join TWN mainnet, durable identity + store, REST API.
CMD ["--dns-discovery", "--node-key-file", "/data/node.key", \
     "--store-path", "/data/store.db", "--rest-port", "8645"]
