// std imports
use std::{
    future::Future,
    pin::Pin,
    str::FromStr,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use deadpool_postgres::{Manager, ManagerConfig, Object, Pool, RecyclingMethod};
use futures::stream::Stream;
use metrics::{Counter, Gauge};
use rand::Rng;
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio_postgres::{
    Config as PgConfig, CopyInSink, NoTls, Row, RowStream,
    binary_copy::BinaryCopyInWriter,
    error::SqlState,
    types::{ToSql, Type},
};

/// Default deadpool connection-pool size when the URL omits `pool_size`.
const DEFAULT_POOL_SIZE: usize = 16;

/// Consecutive retries of a single operation between warning logs. Retries are
/// unbounded, so this surfaces a persistently failing cluster without spamming.
const RETRY_WARN_INTERVAL: u32 = 20;

/// Errors returned by [`Client`] and its connection/pool setup.
#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid database URL: {0}")]
    InvalidUrl(String),
    #[error("Failed to parse attribute {0} with value {2} into {1}")]
    ParsingAttribute(&'static str, &'static str, String),
    #[error("Failed to build connection pool: {0}")]
    CreatePool(Box<deadpool_postgres::BuildError>),
    #[error("Failed to acquire a connection from the pool: {0}")]
    Pool(Box<deadpool_postgres::PoolError>),
    #[error("PostgreSQL error: {0}")]
    Db(Box<tokio_postgres::Error>),
}

impl From<tokio_postgres::Error> for Error {
    fn from(err: tokio_postgres::Error) -> Self {
        Error::Db(Box::new(err))
    }
}

impl From<deadpool_postgres::PoolError> for Error {
    fn from(err: deadpool_postgres::PoolError) -> Self {
        Error::Pool(Box::new(err))
    }
}

impl Error {
    /// Transient failures that a retry can recover from: connection drops, cluster
    /// failover/restart, and resource-limit blips. Everything deterministic
    /// (syntax, type, constraint, auth) is fatal and propagated immediately.
    fn is_retryable(&self) -> bool {
        match self {
            // A connection could not be obtained from the pool — almost always a
            // transient backend/timeout condition during a build.
            Error::Pool(_) => true,
            Error::Db(err) => {
                if err.is_closed() {
                    return true;
                }
                match err.code() {
                    Some(code) => is_retryable_sqlstate(code),
                    // No SQLSTATE => a client/IO-side error (connection reset, etc.).
                    None => true,
                }
            }
            // Deterministic, caller-side problems.
            Error::InvalidUrl(_) | Error::ParsingAttribute(..) | Error::CreatePool(_) => false,
        }
    }
}

fn is_retryable_sqlstate(code: &SqlState) -> bool {
    matches!(
        *code,
        // class 40 — transaction rollback (serialization / deadlock)
        SqlState::T_R_SERIALIZATION_FAILURE
        | SqlState::T_R_DEADLOCK_DETECTED
        // class 53 — insufficient resources
        | SqlState::INSUFFICIENT_RESOURCES
        | SqlState::OUT_OF_MEMORY
        | SqlState::TOO_MANY_CONNECTIONS
        | SqlState::CONFIGURATION_LIMIT_EXCEEDED
        // class 57 — operator intervention
        | SqlState::CANNOT_CONNECT_NOW
        | SqlState::ADMIN_SHUTDOWN
        | SqlState::CRASH_SHUTDOWN
        // class 08 — connection exception
        | SqlState::CONNECTION_EXCEPTION
        | SqlState::CONNECTION_DOES_NOT_EXIST
        | SqlState::CONNECTION_FAILURE
        | SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION
        | SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION
    )
}

/// Parsed connection settings. The standard libpq parameters are handed straight to
/// `tokio_postgres::Config`; only `pool_size` is a MaCPepDB-specific extension that we
/// strip out before parsing (Postgres has no notion of an application pool size).
struct ParsedUrl {
    pg_config: PgConfig,
    pool_size: usize,
    database: String,
    num_hosts: usize,
}

fn parse_url(database_url: &str) -> Result<ParsedUrl, Error> {
    // Split off the query string ourselves: tokio-postgres parses multi-host URLs
    // (`host1,host2`) natively, but does not know our `pool_size` extension, so we
    // strip it out and hand the rest (connect_timeout, sslmode, ...) to PgConfig.
    let (base, query) = match database_url.split_once('?') {
        Some((base, query)) => (base, Some(query)),
        None => (database_url, None),
    };

    if !(base.starts_with("postgresql://") || base.starts_with("postgres://")) {
        return Err(Error::InvalidUrl(format!(
            "expected a postgresql:// URL, got `{database_url}`"
        )));
    }

    let mut pool_size = DEFAULT_POOL_SIZE;
    let mut kept_params: Vec<&str> = Vec::new();
    if let Some(query) = query {
        for pair in query.split('&').filter(|pair| !pair.is_empty()) {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            if key == "pool_size" {
                pool_size = value.parse().map_err(|_| {
                    Error::ParsingAttribute("pool_size", "usize", value.to_string())
                })?;
            } else {
                kept_params.push(pair);
            }
        }
    }

    let conn_str = if kept_params.is_empty() {
        base.to_string()
    } else {
        format!("{base}?{}", kept_params.join("&"))
    };

    let pg_config = PgConfig::from_str(&conn_str)
        .map_err(|err| Error::InvalidUrl(format!("{database_url}: {err}")))?;
    let database = pg_config
        .get_dbname()
        .ok_or_else(|| Error::InvalidUrl("missing database name".to_string()))?
        .to_string();
    let num_hosts = pg_config.get_hosts().len().max(1);

    Ok(ParsedUrl {
        pg_config,
        pool_size,
        database,
        num_hosts,
    })
}

/// A row stream that keeps its backing pooled connection (and concurrency permit)
/// alive for the lifetime of the stream.
pub struct OwnedRowStream {
    _permit: CongestionPermit,
    _conn: Object,
    stream: Pin<Box<RowStream>>,
}

impl Stream for OwnedRowStream {
    type Item = Result<Row, tokio_postgres::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().stream.as_mut().poll_next(cx)
    }
}

/// A binary `COPY ... FROM STDIN (FORMAT binary)` writer that keeps its backing
/// pooled connection alive until [`OwnedBinaryCopy::finish`].
pub struct OwnedBinaryCopy {
    _conn: Object,
    writer: Pin<Box<BinaryCopyInWriter>>,
}

impl OwnedBinaryCopy {
    /// Write one row. `values` must match the column types passed to
    /// [`Client::copy_in_binary`], in order.
    ///
    /// # Arguments
    /// * `values` - The values to write
    ///
    pub async fn write(&mut self, values: &[&(dyn ToSql + Sync)]) -> Result<(), Error> {
        self.writer
            .as_mut()
            .write(values)
            .await
            .map_err(Error::from)
    }

    /// Flush the COPY and return the number of rows written. Consumes the writer
    /// (and releases the connection back to the pool).
    pub async fn finish(mut self) -> Result<u64, Error> {
        self.writer.as_mut().finish().await.map_err(Error::from)
    }
}

/// PostgreSQL/Citus client backed by a deadpool connection pool, with a congestion
/// gate and retry-on-transient-error wrapping all writes.
pub struct Client {
    /// Connection pool
    pool: Pool,
    /// Database name
    database: String,
    /// Original connection URL
    url: String,
    /// Number of nodes
    num_nodes: usize,
    /// Concurrency gate for writes, with jittered backoff on transient errors.
    congestion: CongestionController,
}

impl Client {
    /// Creates a new PostgreSQL/Citus client backed by a deadpool connection pool.
    ///
    /// # Arguments
    /// * `database_url` - `postgresql://[user[:pass]@]host[:port][,host...]/dbname[?param=val&...]`
    ///   plus the MaCPepDB-specific `pool_size` query parameter.
    pub async fn new(database_url: &str) -> Result<Self, Error> {
        let ParsedUrl {
            pg_config,
            pool_size,
            database,
            num_hosts,
        } = parse_url(database_url)?;

        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let manager = Manager::from_config(pg_config, NoTls, mgr_config);
        let pool = Pool::builder(manager)
            .max_size(pool_size)
            .build()
            .map_err(|err| Error::CreatePool(Box::new(err)))?;

        Ok(Self {
            pool,
            database,
            url: database_url.to_string(),
            num_nodes: num_hosts,
            // Cap concurrent congested writes at the pool size: reads are already
            // bounded by the pool, and a fixed window matches Postgres's
            // block-or-error model (no graded overload signal to react to).
            congestion: CongestionController::new(pool_size),
        })
    }

    /// Name of the database this client is connected to.
    pub fn database(&self) -> &str {
        &self.database
    }

    /// The database URL this client was created from.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Number of distinct hosts parsed from the connection URL.
    pub fn num_nodes(&self) -> usize {
        self.num_nodes
    }

    /// The congestion gate guarding writes through [`Client::run_congested`].
    pub fn congestion(&self) -> &CongestionController {
        &self.congestion
    }

    /// Checks out a connection from the pool.
    pub async fn get(&self) -> Result<Object, Error> {
        self.pool.get().await.map_err(Error::from)
    }

    /// Runs `op` under the congestion gate: acquires a slot, executes, and on a
    /// transient error retries with jittered backoff (writes are idempotent here).
    /// Deterministic errors are propagated immediately.
    ///
    /// # Arguments
    /// * `op` - The operation to run under the congestion gate. Must return a `Result<T, Error>`.
    ///
    pub async fn run_congested<T, F, Fut>(&self, mut op: F) -> Result<T, Error>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, Error>>,
    {
        let mut attempt: u32 = 0;
        loop {
            let permit = self.congestion.acquire().await;
            match op().await {
                Ok(value) => return Ok(value),
                Err(err) if err.is_retryable() => {
                    attempt += 1;
                    if attempt.is_multiple_of(RETRY_WARN_INTERVAL) {
                        tracing::warn!("operation still retrying after {attempt} attempts: {err}");
                    }
                    self.congestion.note_retry();
                    drop(permit);
                    tokio::time::sleep(self.congestion.backoff(attempt)).await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Prepares (cached) and executes a statement, returning the affected row count.
    /// Retries transient failures via the congestion gate.
    ///
    /// # Arguments
    /// * `sql` - The SQL statement to execute
    /// * `params` - The parameters to bind to the statement
    ///
    pub async fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error> {
        self.run_congested(|| async {
            let conn = self.get().await?;
            let stmt = conn.prepare_cached(sql).await?;
            conn.execute(&stmt, params).await.map_err(Error::from)
        })
        .await
    }

    /// Prepares (cached) and runs a query, collecting all rows. For small result
    /// sets (single-peptide lookups, blob parts, stats). Retries transient failures.
    ///
    /// # Arguments
    /// * `sql` - The SQL statement to execute
    /// * `params` - The parameters to bind to the statement
    ///
    pub async fn query(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        self.run_congested(|| async {
            let conn = self.get().await?;
            let stmt = conn.prepare_cached(sql).await?;
            conn.query(&stmt, params).await.map_err(Error::from)
        })
        .await
    }

    /// Opens a streaming query, holding a connection (and concurrency permit) for the
    /// lifetime of the returned stream. Used for large search result sets. Not
    /// retried — a failed read surfaces to the caller.
    ///
    /// # Arguments
    /// * `sql` - The SQL statement to execute
    /// * `params` - The parameters to bind to the statement
    ///
    pub async fn query_stream(
        &self,
        sql: &str,
        params: Vec<Box<dyn ToSql + Sync + Send>>,
    ) -> Result<OwnedRowStream, Error> {
        // Timing split: `acquire` = time blocked on the congestion permit (pool
        // saturation signal); `setup` = connection checkout + prepare + query_raw,
        // which on Citus includes distributed query planning/dispatch. The actual
        // columnar scan happens later as the returned stream is polled.
        let t0 = std::time::Instant::now();
        let permit = self.congestion.acquire().await;
        let acquire_us = t0.elapsed().as_micros();
        // Split setup into its three sub-phases to attribute the cost:
        //   get      = pool checkout; a COLD slot establishes a fresh TCP+auth
        //              connection to the coordinator here (the warm-pool suspect).
        //   prepare  = prepare_cached; Citus may do distributed planning here.
        //   exec     = query_raw; dispatch/execute (planning may also land here).
        let tg = std::time::Instant::now();
        let conn = self.get().await?;
        let get_us = tg.elapsed().as_micros();
        let is_recycled = Object::metrics(&conn).recycle_count > 0;
        let tp = std::time::Instant::now();
        let stmt = conn.prepare_cached(sql).await?;
        let prepare_us = tp.elapsed().as_micros();
        let te = std::time::Instant::now();
        let stream = conn
            .query_raw(
                &stmt,
                params.iter().map(|p| p.as_ref() as &(dyn ToSql + Sync)),
            )
            .await?;
        let exec_us = te.elapsed().as_micros();
        tracing::debug!(
            target: "search_timing",
            acquire_us,
            get_us,
            prepare_us,
            exec_us,
            is_recycled,
            in_flight = self.congestion.in_flight(),
            window = self.congestion.window(),
            "query_stream opened"
        );
        Ok(OwnedRowStream {
            _permit: permit,
            _conn: conn,
            stream: Box::pin(stream),
        })
    }

    /// Like [`Client::query_stream`] but for SQL that already has all its values
    /// inlined as literals (no bind parameters). The statement is NOT prepared/cached:
    /// it is passed straight to `query_raw` as an unnamed statement. This matters for
    /// Citus — a parameterized distributed query cannot use a cached generic plan
    /// (Citus errors on it) and re-plans on every execute (~11 ms planning), whereas an
    /// inlined-literal query plans in ~0.2 ms because the coordinator can prune shards
    /// and columnar chunk groups at plan time. Since the SQL is unique per call, caching
    /// it would also grow the per-connection statement cache without bound.
    ///
    /// # Arguments
    /// * `sql` - The SQL statement to execute without placeholders
    ///
    pub async fn query_stream_inline(&self, sql: &str) -> Result<OwnedRowStream, Error> {
        let t0 = std::time::Instant::now();
        let permit = self.congestion.acquire().await;
        let acquire_us = t0.elapsed().as_micros();
        let tg = std::time::Instant::now();
        let conn = self.get().await?;
        let get_us = tg.elapsed().as_micros();
        let is_recycled = Object::metrics(&conn).recycle_count > 0;
        let te = std::time::Instant::now();
        let stream = conn
            .query_raw(sql, std::iter::empty::<&(dyn ToSql + Sync)>())
            .await?;
        let exec_us = te.elapsed().as_micros();
        tracing::debug!(
            target: "search_timing",
            acquire_us,
            get_us,
            exec_us,
            is_recycled,
            in_flight = self.congestion.in_flight(),
            window = self.congestion.window(),
            "query_stream_inline opened"
        );
        Ok(OwnedRowStream {
            _permit: permit,
            _conn: conn,
            stream: Box::pin(stream),
        })
    }

    /// Opens a binary `COPY ... FROM STDIN` into `table (columns...)`. The caller
    /// writes rows and calls [`OwnedBinaryCopy::finish`]. Wrap the whole
    /// build-row-write-finish in [`Client::run_congested`] for retry on transient
    /// failures (COPY is one transaction, so a failed attempt rolls back cleanly).
    ///
    /// # Arguments
    /// * `copy_sql` - The `COPY ... FROM STDIN (FORMAT binary)` SQL statement
    /// * `column_types` - The column types in order, used to validate the written rows
    ///
    pub async fn copy_in_binary(
        &self,
        copy_sql: &str,
        column_types: &[Type],
    ) -> Result<OwnedBinaryCopy, Error> {
        let conn = self.get().await?;
        let sink: CopyInSink<Bytes> = conn.copy_in(copy_sql).await?;
        let writer = BinaryCopyInWriter::new(sink, column_types);
        Ok(OwnedBinaryCopy {
            _conn: conn,
            writer: Box::pin(writer),
        })
    }
}

/// Gauge name for the [`CongestionController`]'s fixed concurrency window size.
pub static CONGESTION_WINDOW_METRIC: &str = "client::congestion::window";
/// Gauge name for the [`CongestionController`]'s current number of in-flight operations.
pub static CONGESTION_IN_FLIGHT_METRIC: &str = "client::congestion::in_flight";
/// Counter name for the [`CongestionController`]'s transient-error retries.
pub static CONGESTION_RETRY_METRIC: &str = "client::congestion::retries";

/// A fixed-size concurrency gate plus jittered backoff. Unlike the Cassandra-era
/// AIMD controller this no longer grows/shrinks: Postgres has no graded overload
/// signal to react to, and columnar storage has no background compaction to ride out
/// — so a constant window sized to the pool, plus retry-on-transient, is the right
/// model. `window`/`in_flight`/`retries` metrics are kept for observability.
#[derive(Clone)]
pub struct CongestionController {
    inner: Arc<ControllerInner>,
}

struct ControllerInner {
    sem: Arc<Semaphore>,
    limit: usize,
    in_flight: Mutex<usize>,
    in_flight_gauge: Gauge,
    retry_counter: Counter,
    base_backoff: Duration,
    max_backoff: Duration,
}

impl CongestionController {
    /// Creates a controller with a fixed concurrency window of `window` slots.
    ///
    /// # Arguments
    /// * `window` - The maximum number of concurrent operations allowed. Must be at least
    ///
    pub fn new(window: usize) -> Self {
        let limit = window.max(1);
        let window_gauge = metrics::gauge!(CONGESTION_WINDOW_METRIC);
        window_gauge.set(limit as f64);
        let in_flight_gauge = metrics::gauge!(CONGESTION_IN_FLIGHT_METRIC);
        in_flight_gauge.set(0.0);
        Self {
            inner: Arc::new(ControllerInner {
                sem: Arc::new(Semaphore::new(limit)),
                limit,
                in_flight: Mutex::new(0),
                in_flight_gauge,
                retry_counter: metrics::counter!(CONGESTION_RETRY_METRIC),
                base_backoff: Duration::from_millis(50),
                max_backoff: Duration::from_secs(2),
            }),
        }
    }

    /// Blocks until a concurrency slot is free, then returns an RAII permit that
    /// releases the slot on drop.
    pub async fn acquire(&self) -> CongestionPermit {
        let permit = self
            .inner
            .sem
            .clone()
            .acquire_owned()
            .await
            .expect("congestion semaphore unexpectedly closed");
        {
            let mut in_flight = self
                .inner
                .in_flight
                .lock()
                .expect("in_flight mutex poisoned");
            *in_flight += 1;
            self.inner.in_flight_gauge.set(*in_flight as f64);
        }
        CongestionPermit {
            inner: self.inner.clone(),
            _permit: permit,
        }
    }

    /// The fixed concurrency window size (the number of slots the gate was created with).
    pub fn window(&self) -> usize {
        self.inner.limit
    }

    /// The number of concurrency slots currently checked out.
    pub fn in_flight(&self) -> usize {
        *self
            .inner
            .in_flight
            .lock()
            .expect("in_flight mutex poisoned")
    }

    fn note_retry(&self) {
        self.inner.retry_counter.increment(1);
    }

    fn backoff(&self, attempt: u32) -> Duration {
        let shift = attempt.min(16);
        let base_ms = self.inner.base_backoff.as_millis() as u64;
        let cap_ms = self.inner.max_backoff.as_millis().max(1) as u64;
        let ceiling = base_ms.saturating_mul(1u64 << shift).min(cap_ms).max(1);
        Duration::from_millis(rand::rng().random_range((ceiling / 2)..=ceiling))
    }
}

/// RAII concurrency slot. Returns capacity to the gate on drop.
pub struct CongestionPermit {
    inner: Arc<ControllerInner>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Drop for CongestionPermit {
    fn drop(&mut self) {
        let mut in_flight = self
            .inner
            .in_flight
            .lock()
            .expect("in_flight mutex poisoned");
        *in_flight = in_flight.saturating_sub(1);
        self.inner.in_flight_gauge.set(*in_flight as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_database_url() {
        let url_credentials_attributes = "postgresql://gandalf:mellon@10.0.0.168:5432/mdb_uniprot?connect_timeout=60&pool_size=8";
        let url_mandatory = "postgresql://10.0.0.168/mdb_uniprot";

        let parsed = parse_url(url_credentials_attributes).unwrap();
        assert_eq!(parsed.database, "mdb_uniprot");
        assert_eq!(parsed.pool_size, 8);
        assert_eq!(parsed.pg_config.get_user(), Some("gandalf"));
        assert_eq!(parsed.num_hosts, 1);

        let parsed = parse_url(url_mandatory).unwrap();
        assert_eq!(parsed.database, "mdb_uniprot");
        assert_eq!(parsed.pool_size, DEFAULT_POOL_SIZE);
        assert_eq!(parsed.pg_config.get_user(), None);
    }

    #[test]
    fn test_parse_multi_host_url() {
        let parsed = parse_url("postgresql://h1:5432,h2:5432/macpepdb").unwrap();
        assert_eq!(parsed.num_hosts, 2);
        assert_eq!(parsed.database, "macpepdb");
    }

    #[test]
    fn test_reject_non_postgres_scheme() {
        assert!(matches!(
            parse_url("scylla://127.0.0.1/macpepdb"),
            Err(Error::InvalidUrl(_))
        ));
    }

    #[test]
    fn classification_separates_transient_from_deterministic() {
        assert!(is_retryable_sqlstate(&SqlState::T_R_SERIALIZATION_FAILURE));
        assert!(is_retryable_sqlstate(&SqlState::TOO_MANY_CONNECTIONS));
        assert!(is_retryable_sqlstate(&SqlState::CANNOT_CONNECT_NOW));
        assert!(!is_retryable_sqlstate(&SqlState::SYNTAX_ERROR));
        assert!(!is_retryable_sqlstate(&SqlState::INSUFFICIENT_PRIVILEGE));
        assert!(!is_retryable_sqlstate(&SqlState::UNIQUE_VIOLATION));
    }

    #[tokio::test]
    async fn gate_caps_concurrency() {
        let c = CongestionController::new(1);
        let p1 = c.acquire().await;
        assert_eq!(c.in_flight(), 1);

        let c2 = c.clone();
        let mut pending = Box::pin(c2.acquire());
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut pending)
                .await
                .is_err(),
            "acquire must block while the only slot is held"
        );

        drop(p1);
        let p2 = tokio::time::timeout(Duration::from_millis(200), pending)
            .await
            .expect("acquire must resolve once the slot is released");
        assert_eq!(c.in_flight(), 1);
        drop(p2);
        assert_eq!(c.in_flight(), 0);
    }
}
