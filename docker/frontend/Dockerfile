FROM rust:1.96 AS build-stage

RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash
RUN cargo binstall dioxus-cli

WORKDIR /usr/src/macpepdb
COPY . .

RUN dx build --release --web --package macpepdb-frontend

RUN CSP_DYNAMIC="$(bash docker/frontend/create_content_security_header.sh)" && \
    esc_csp=$(printf '%s' "$CSP_DYNAMIC" | sed 's/[&#\]/\\&/g') && \
    sed -i "s#<CSP_HEADER_ATTR>#${esc_csp}#" docker/frontend/Caddyfile


FROM caddy:2

COPY --from=build-stage /usr/src/macpepdb/docker/frontend/Caddyfile /etc/caddy/Caddyfile
COPY --from=build-stage /usr/src/macpepdb/target/dx/macpepdb-frontend/release/web/public  /usr/share/caddy
COPY docker/frontend/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
