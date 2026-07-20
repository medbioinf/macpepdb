# MaCPepDB Lite

## Dependencies
* openssl|libressl

## Features

| Feature | Description | Additional dependencies |
| --- | --- | --- |
| jemalloc | Drops in `jemelloc` as allocatior | |
| mimalloc | Drops in  Microsoft's `mimalloc` as allocatior | |
| tcmalloc | Drops in Google's `tcmalloc` as allocator | `libstdc++`, `libclang` and `libunwind` |
| tokio-console | Activates options to enable tokio console | Need manually setting `RUSTFLAGS="--cfg tokio_unstable"` |
