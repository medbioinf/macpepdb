# MaCPepDB Frontend

MaCPepDB only provides an web API for accessing the data. This is a separate frontend to make data access much easier.


## Technical details
With MaCPepDBs new database engine ScyllaDB, a whole lot more data can be stored and quickly accessed. With the new database some basic functions need to be transferred form the database to the application code, like sorting and filtering. While a lot filtering domain specific filtering is done on server side, e.g. masses + tolerances, PTMs, taxonomies, etc. other functions is now up to the client.

To deal with the amount of available data as efficient as possible, this frontend is build using [Dioxus](https://dioxuslabs.com/), which uses Rust to render and manage the DOM, handling any data and compiles into a WASM application. It is similar to React or VueJS.   
Another benefit of Dioxus is also the possibility to build an Electron Desktop App if necessary.

## Installation
1. Clone the repositry
2. Install rustup
3. Install Dioxus: `cargo install dioxus-cli`
3. Install WASM toolchain: `rustup target add wasm32-unknown-unknown`
4. `dx serve`

Per default the application uses the configuration `frontend.config.toml`. If your wat to adjust it:
1. Make a copy of `frontend.config.toml` and adjust it
2. Run: `env MDBF_CONFIG=<PATH_TO_NEW_CONFIG> dx serve`

## Configuration
While a WASM application has not access to any filesystem, the configuration is compiled into the binary. Therefore it needs to be selected during compiling.

## Deplyoment
1. `dx build --release`
2. Serve the created `dist` folder with any web server, e.g. NginX

