FROM rust:1.89 AS build-stage

WORKDIR /usr/src/macpepdb-frontend
COPY . .

RUN dx build --release


FROM nginx:mainline

COPY ./frontend.config.toml
COPY --from=build-stage /usr/src/macpepdb-frontend/target/dx/macpepdb-frontend/release/web/public  /var/www/html

