# MaCPepDB Frontend

MaCPepDB only provides an web API for accessing the data. This is a separate frontend to make data access much easier.


## Technical details
With MaCPepDBs new database engine ScyllaDB, a whole lot more data can be stored and quickly accessed. With the new database some basic functions need to be transferred form the database to the application code, like sorting and filtering. While a lot filtering domain specific filtering is done on server side, e.g. masses + tolerances, PTMs, taxonomies, etc. other functions is now up to the client.

To deal with the amount of available data as efficient as possible, this frontend is build using [Dioxus](https://dioxuslabs.com/), which uses Rust to render and manage the DOM, handling any data and compiles into a WASM application. It is similar to React or VueJS.   
Another benefit of Dioxus is also the possibility to build an Electron Desktop App if necessary.

## Development

### Installation
1. Clone the repositry
2. Install rustup
3. Install Dioxus: `cargo install dioxus-cli`
4. Install WASM toolchain: `rustup target add wasm32-unknown-unknown`
5. `dx serve`

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
