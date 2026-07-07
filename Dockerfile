FROM rust:1.92 AS base

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
COPY migrations migrations
COPY .sqlx .sqlx
COPY src src
COPY web web

# Precompress static web assets with gzip to avoid paying for their compression cost at runtime
RUN gzip --keep --best --force --recursive web/assets

ARG GIT_VERSION=unknown
ENV GIT_VERSION=${GIT_VERSION}

RUN cargo build --release --locked

FROM node:22-trixie-slim AS runtime

WORKDIR /

ARG PLATFORM_REPO=https://github.com/henosis-playground/platform.git
ARG PLATFORM_REF=main

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        jq \
        openssl \
    && rm -rf /var/lib/apt/lists/*

ADD https://api.github.com/repos/henosis-playground/platform/commits/main /tmp/platform-main.json

RUN corepack enable \
    && corepack prepare pnpm@11.3.0 --activate \
    && git clone --depth=1 --branch "$PLATFORM_REF" "$PLATFORM_REPO" /opt/henosis-platform \
    && cd /opt/henosis-platform \
    && pnpm install --frozen-lockfile \
    && pnpm build \
    && printf '#!/bin/sh\nexec node /opt/henosis-platform/packages/renderer/dist/gate.js "$@"\n' > /usr/local/bin/henosis-gate \
    && chmod +x /usr/local/bin/henosis-gate \
    && pnpm store prune

COPY --from=build /app/target/release/bors .

EXPOSE 8080

HEALTHCHECK --timeout=10s --start-period=10s \
    CMD curl -f http://localhost:8080/health || exit 1

ENTRYPOINT ["./bors"]
