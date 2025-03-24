# Image for building and testing
FROM rust:latest AS rsbuilder

# By default this image will build and install the binary
# However this is unnecessary for testing, the
ARG SKIP_INSTALL=false
ENV SKIP_INSTALL=$SKIP_INSTALL

WORKDIR /usr/src/macpepdb

COPY . .

RUN apt update \
    && apt install -y libssl-dev pkg-config libhdf5-dev \
    && apt clean \
    && rm -rf /var/lib/apt/lists/* \
    && [ "$SKIP_INSTALL" = "false" ] && cargo install --path . --root /usr/local || echo "Skipping install"


# Minimal Image for running
FROM ubuntu:noble

COPY --from=rsbuilder /usr/local/bin/macpepdb /usr/local/bin/macpepdb
RUN apt update \
    && apt install -y libssl-dev libhdf5-dev \
    && apt clean \
    && rm -rf /var/lib/apt/lists/*


ENTRYPOINT [ "macpepdb" ]
