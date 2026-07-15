FROM rust:1.96 AS base

RUN cargo install cargo-chef

FROM base AS planner

WORKDIR /app
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM base AS build

WORKDIR /app
COPY --from=planner /app/recipe.json recipe.json

ENV SQLX_OFFLINE=true
RUN cargo chef cook --release --recipe-path recipe.json

COPY askama.toml .
COPY Cargo.toml .
COPY Cargo.lock .
COPY crates crates
COPY tools tools
COPY migrations migrations
COPY .sqlx .sqlx
COPY src src
COPY web web

# Precompress static web assets with gzip to avoid paying for their compression cost at runtime
RUN gzip --keep --best --force --recursive web/assets

ARG GIT_VERSION=unknown
ENV GIT_VERSION=${GIT_VERSION}

RUN cargo build --release --locked

FROM node:22-bookworm-slim AS runtime

WORKDIR /

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        jq \
        openssl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/target/release/bors .
COPY --from=build /app/target/release/henosis /usr/local/bin/henosis

EXPOSE 8080

HEALTHCHECK --timeout=10s --start-period=10s \
    CMD curl -f http://localhost:8080/health || exit 1

ENTRYPOINT ["./bors"]
