FROM rust:1.96 AS build-stage

RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash
RUN cargo binstall dioxus-cli

WORKDIR /usr/src/macpepdb
COPY . .

RUN dx build --release --web --package macpepdb-frontend


FROM caddy:2

COPY docker/frontend/Caddyfile /etc/caddy/Caddyfile
COPY --from=build-stage /usr/src/macpepdb/target/dx/macpepdb-frontend/release/web/public  /usr/share/caddy
