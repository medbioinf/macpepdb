#!/bin/sh
set -e

CONFIG_TOML=/usr/share/caddy/assets/config.toml
CONNECT_SRC="connect-src 'self'"

if [ -f "$CONFIG_TOML" ]; then
    base_url=$(grep -E '^[[:space:]]*macpepdb_base_url[[:space:]]*=' "$CONFIG_TOML" | sed -E 's/^[^=]*=[[:space:]]*"([^"]*)".*/\1/')
    matomo_url=$(grep -E '^[[:space:]]*matomo_url[[:space:]]*=' "$CONFIG_TOML" | sed -E 's/^[^=]*=[[:space:]]*"([^"]*)".*/\1/')
    [ -n "$base_url" ] && CONNECT_SRC="${CONNECT_SRC} ${base_url}"
    [ -n "$matomo_url" ] && CONNECT_SRC="${CONNECT_SRC} ${matomo_url}"
fi
CONNECT_SRC="${CONNECT_SRC};"

esc_connect_src=$(printf '%s' "$CONNECT_SRC" | sed 's/[&#\]/\\&/g')
sed -i "s#<CONNECT_SRC>#${esc_connect_src}#" /etc/caddy/Caddyfile

exec caddy run --config /etc/caddy/Caddyfile --adapter caddyfile
