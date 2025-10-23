FROM rust:1.89 AS build-stage

WORKDIR /usr/src/macpepdb
COPY . .

RUN cargo build -r



FROM debian:trixie-slim

COPY --from=build-stage /usr/src/macpepdb/target/release/macpepdb /usr/bin/macpepdb

ENTRYPOINT [ "/usr/bin/macpepdb" ]
