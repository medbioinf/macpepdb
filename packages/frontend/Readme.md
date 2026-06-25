# MaCPepDB Frontend

MaCPepDB only provides an web API for accessing the data. This is a separate frontend to make data access much easier.

## Development

### Installation
1. Install Dioxus: `cargo install dioxus-cli`
2. Install WASM toolchain: `rustup target add wasm32-unknown-unknown`
3. `dx serve`

Per default the application used the public MaCPepDB server (https://macpepdb.cubimed.rub.de) to fetch data. A custom URL can be used via the environment variable `MACPEPDB_BASE_URL`, e.g.

```shell
env MACPEPDB_BASE_URL=http://127.0.0.1:8000 dx serve
```

## Deplyoment

## Native
1. `dx build --release`
2. Serve the created `target/dx/macpepdb-frontend/release/web/public` folder with any web server, e.g. NginX. A very simple config is provided `nginx.conf`. using Docker simply call:

    ```shell
    docker run --rm -v $(pwd)/nginx.conf:/etc/nginx/conf.d/default.conf:ro -v $(pwd)/target/dx/macpepdb-frontend/release/web/public:/var/www/html:ro -v <PATH_TO_ADJUSTED_CONFIG>:/var/www/html/assets/config.toml:ro -p 8888:80 nginx:alpine-slim 
    ```

    in the root of the repository

A simpler version is comming soon.


### Configuration
Copy `config.template.toml` adjust it and put it into the `target/dx/macpepdb-frontend/release/web/public/assets/config.toml`.


## Docker
1. Check the [available images](https://github.com/orgs/medbioinf/packages/container/package/macpepdb-frontend)
2. Start one `docker run --rm -p <HOST_PORT>:80 <IMAGE_TAG>`

### Configuration
Copy `config.template.toml` adjust it and mount it at `/usr/share/caddy/assets/config.toml`

### SSL
Let another proxy like NginX, Caddy or HAProxy handle this.
