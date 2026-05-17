# Build stage
FROM rust:1.87 as builder
WORKDIR /usr/src/app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release

# Run stage
FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libssl3 \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /usr/local/bin
COPY --from=builder /usr/src/app/target/release/aqua .
EXPOSE 9090
CMD ["./aqua"]
