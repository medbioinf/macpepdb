#!/bin/bash

# Plotly renders via SVG/HTML with per-frame inline style attributes (tooltips, drag
# rectangles, hover layers) whose values vary with data/mouse position, so a finite hash
# allowlist can't cover them; 'unsafe-inline' is required here. Note hash-sources and
# 'unsafe-inline' don't compose (a hash-source present in a directive makes browsers ignore
# 'unsafe-inline' for that directive), so this directive must stay hash-free.
style_source="style-src 'self' 'unsafe-inline'"

app_urls="$(grep -oE 'http(s)?://[^\"]+' packages/frontend/src/app.rs)"

style_source="${style_source} $(printf '%s\n' "$app_urls" | grep -E '\.css$' | paste -sd " " -);"

script_source="script-src 'self' 'wasm-unsafe-eval' 'unsafe-eval'"
script_source="${script_source} $(printf '%s\n' "$app_urls" | grep -E '\.js$' | paste -sd " " -);"

font_source="font-src 'self'"
font_source="${font_source} $(printf '%s\n' "$app_urls" | grep -E 'font-awesome.*\.css$' | sed 's;\(.*font-awesome/[[:digit:]]\.[[:digit:]]\.[[:digit:]]\).*;\1/webfonts/;g');"


echo "${style_source} ${script_source} ${font_source}"
