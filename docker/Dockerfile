FROM rust:1.89 AS build-stage

WORKDIR /usr/src/macpepdb-frontend
COPY . .


RUN cargo install dioxus-cli
RUN dx build --release


FROM caddy:2

COPY --from=build-stage /usr/src/macpepdb-frontend/target/dx/macpepdb-frontend/release/web/public  /usr/share/caddy

