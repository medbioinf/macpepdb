// std imports
use std::{
    collections::HashMap,
    future::Future,
    num::NonZeroUsize,
    ops::Deref,
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};

use fancy_regex::Regex;
use metrics::{Counter, Gauge};
use rand::Rng;
use scylla::{
    client::{PoolSize, caching_session::CachingSession, session_builder::SessionBuilder},
    errors::{DbError, ExecutionError, RequestAttemptError},
    frame::Compression,
    statement::Consistency,
};
use thiserror::Error;
use tokio::sync::Semaphore;

pub static URL_PARSER_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?m)scylla://((?P<credentials>[^:]*?:[^:]+)@){0,1}(?P<hosts>.+)/(?P<keyspace>[^/?]+)(\?(?P<attributes>.+)){0,1}").unwrap()
});

static DEFAULT_CACHE_SIZE: usize = 100;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Poolsize need to be larger than 0")]
    InvalidPoolSize,
    #[error("Invalid database URL: {0}")]
    InvalidUrl(String),
    #[error("Failed to create ScyllaDB session: {0}")]
    Session(Box<scylla::errors::NewSessionError>),
    #[error("Failed to parse attribute {0} with value {2} into {1}")]
    ParsingAttribute(&'static str, &'static str, String),
}

impl From<scylla::errors::NewSessionError> for Error {
    fn from(err: scylla::errors::NewSessionError) -> Self {
        Error::Session(Box::new(err))
    }
}

/// Pool type for the ScyllaDB client
/// default is PerHost
///
#[derive(PartialEq, Eq, Debug)]
enum PoolType {
    PerHost,
    PerShard,
}

impl From<&str> for PoolType {
    fn from(s: &str) -> Self {
        match s {
            "host" => PoolType::PerHost,
            "shard" => PoolType::PerShard,
            _ => PoolType::PerHost,
        }
    }
}

struct ClientSettings {
    pub hosts: Vec<String>,
    pub keyspace: String,
    pub cache_size: usize,
    pub user: Option<String>,
    pub password: Option<String>,
    pub connection_timeout: Option<Duration>,
    pub pool_size: Option<usize>,
    pub pool_type: PoolType,
    pub read_consistency_level: Option<Consistency>,
    pub write_consistency_level: Option<Consistency>,
}

impl ClientSettings {
    pub fn new(database_url: &str) -> Result<Self, Error> {
        // Parse url
        let matches = URL_PARSER_REGEX
            .captures(database_url)
            .map_err(|_| {
                Error::InvalidUrl("Format seems not to correspond to the expectations".to_string())
            })?
            .ok_or(Error::InvalidUrl(
                "Format seems not to correspond to the expectations".to_string(),
            ))?;

        // Extract hosts and keyspace as they are mandatory
        let hosts: Vec<String> = matches
            .name("hosts")
            .ok_or(Error::InvalidUrl("Hosts not found".to_string()))?
            .as_str()
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        tracing::info!("hosts {:?}", hosts);

        let keyspace = matches
            .name("keyspace")
            .ok_or(Error::InvalidUrl("keyspace not found".to_string()))?
            .as_str()
            .to_string();

        // Create settings
        let mut settings = Self {
            hosts,
            keyspace,
            cache_size: DEFAULT_CACHE_SIZE,
            user: None,
            password: None,
            connection_timeout: None,
            pool_size: None,
            pool_type: PoolType::PerHost,
            read_consistency_level: None,
            write_consistency_level: None,
        };

        // Extract credentials and attributes if present
        if let Some(credentials) = matches.name("credentials") {
            let credentials = credentials.as_str().split(':').collect::<Vec<&str>>();
            settings.user = credentials.first().map(|value| value.to_string());
            settings.password = credentials.get(1).map(|value| value.to_string());
        }

        // Extract attributes if present
        let attributes = match matches.name("attributes") {
            Some(attributes) => attributes.as_str().to_owned(),
            None => "".to_owned(),
        };

        // Parse attributes
        if !attributes.is_empty() {
            let attributes: HashMap<&str, &str> = attributes
                .split('&')
                .map(|attribute| {
                    let attribute: Vec<&str> = attribute.split('=').collect();
                    (attribute[0], attribute[1])
                })
                .collect();

            if let Some(timeout) = attributes.get("connection_timeout") {
                settings.connection_timeout =
                    Some(Duration::from_secs(timeout.parse().map_err(|_| {
                        Error::ParsingAttribute("timeout", "u64", timeout.to_string())
                    })?));
            }

            if let Some(pool_size) = attributes.get("pool_size") {
                settings.pool_size = Some(pool_size.parse().map_err(|_| {
                    Error::ParsingAttribute("pool_size", "usize", pool_size.to_string())
                })?);
            }

            if let Some(pool_type) = attributes.get("pool_type") {
                settings.pool_type = PoolType::from(*pool_type);
            }

            if let Some(level) = attributes.get("read_consistency_level") {
                settings.read_consistency_level =
                    Some(Self::str_to_consistency_level(level).map_err(|_| {
                        Error::ParsingAttribute(
                            "read_consistency_level",
                            "Consistency",
                            level.to_string(),
                        )
                    })?);
            };

            if let Some(level) = attributes.get("write_consistency_level") {
                settings.write_consistency_level =
                    Some(Self::str_to_write_consistency_level(level).map_err(|_| {
                        Error::ParsingAttribute(
                            "write_consistency_level",
                            "Consistency",
                            level.to_string(),
                        )
                    })?);
            };

            if let Some(cache_size) = attributes.get("cache_size") {
                settings.cache_size = (cache_size).parse().map_err(|_| {
                    Error::ParsingAttribute("cache_size", "usize", cache_size.to_string())
                })?;
            }
        }
        Ok(settings)
    }

    pub async fn to_session(&self) -> Result<CachingSession, Error> {
        let mut builder = SessionBuilder::new()
            .known_nodes(self.hosts.clone())
            .compression(Some(Compression::Lz4))
            .use_keyspace(self.keyspace.clone(), true);

        if let Some(user) = self.user.as_ref()
            && let Some(password) = self.password.as_ref()
        {
            builder = builder.user(user, password);
        }

        if let Some(timeout) = self.connection_timeout {
            builder = builder.connection_timeout(timeout);
        }

        if let Some(pool_size) = self.pool_size {
            let pool_size = NonZeroUsize::new(pool_size).ok_or(Error::InvalidPoolSize)?;

            let pool_size = match self.pool_type {
                PoolType::PerHost => PoolSize::PerHost(pool_size),
                PoolType::PerShard => PoolSize::PerShard(pool_size),
            };
            builder = builder.pool_size(pool_size);
        }

        let session = builder
            .build()
            .await
            .map_err(|e| Error::Session(Box::new(e)))?;

        Ok(CachingSession::from(session, self.cache_size))
    }

    /// Returns the ScyllaDB consistency level from a string representation
    /// which can be used for read and write operations.
    ///
    fn str_to_consistency_level(s: &str) -> Result<Consistency, ()> {
        match s {
            "one" => Ok(Consistency::One),
            "two" => Ok(Consistency::Two),
            "three" => Ok(Consistency::Three),
            "quorum" => Ok(Consistency::Quorum),
            "local_quorum" => Ok(Consistency::LocalQuorum),
            "all" => Ok(Consistency::All),
            "local_one" => Ok(Consistency::LocalOne),
            "local_serial" => Ok(Consistency::LocalSerial),
            "serial" => Ok(Consistency::Serial),
            _ => Err(()),
        }
    }

    /// Returns the ScyllaDB consistency level from a string representation
    /// which can be used for write operations.
    ///
    fn str_to_write_consistency_level(s: &str) -> Result<Consistency, ()> {
        if let Ok(level) = Self::str_to_consistency_level(s) {
            return Ok(level);
        }

        match s {
            "any" => Ok(Consistency::Any),
            "each_quorum" => Ok(Consistency::EachQuorum),
            _ => Err(()),
        }
    }
}

pub struct Client {
    session: CachingSession,
    database: String,
    url: String,
    num_nodes: usize,
    read_consistency_level: Option<Consistency>,
    write_consistency_level: Option<Consistency>,
    congestion: CongestionController,
}

impl Client {
    /// Creates a new ScyllaDB client
    ///
    /// # Arguments
    /// * `database_url` - A string slice that holds the database URL
    ///
    pub async fn new(database_url: &str) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let settings = ClientSettings::new(database_url)?;
        Ok(Self {
            session: settings.to_session().await?,
            database: settings.keyspace.clone(),
            url: database_url.to_string(),
            num_nodes: settings.hosts.len(),
            read_consistency_level: settings.read_consistency_level,
            write_consistency_level: settings.write_consistency_level,
            congestion: CongestionController::new(CongestionConfig::default()),
        })
    }

    pub fn inner_client(&self) -> &CachingSession {
        &self.session
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn num_nodes(&self) -> usize {
        self.num_nodes
    }

    pub fn read_consistency_level(&self) -> Option<Consistency> {
        self.read_consistency_level
    }

    pub fn write_consistency_level(&self) -> Option<Consistency> {
        self.write_consistency_level
    }

    pub fn congestion(&self) -> &CongestionController {
        &self.congestion
    }

    /// Runs `op` under the congestion window: acquires a slot, executes, and on a
    /// non-deterministic Scylla error shrinks the window and retries with jittered
    /// backoff. Deterministic errors are propagated immediately without touching
    /// the window.
    pub async fn run_congested<T, F, Fut>(&self, mut op: F) -> Result<T, ExecutionError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, ExecutionError>>,
    {
        let mut attempt: u32 = 0;
        loop {
            let permit = self.congestion.acquire().await;
            match op().await {
                Ok(value) => {
                    permit.report(Outcome::Success);
                    return Ok(value);
                }
                Err(err) => match classify_execution_error(&err) {
                    RetryClass::Retryable => {
                        // Writes are idempotent, so retry transient overload
                        // indefinitely rather than surfacing a failure that would
                        // abort a long build. Warn periodically so a cluster that
                        // is down (not just briefly stalled) stays visible.
                        permit.report(Outcome::Overload);
                        attempt += 1;
                        if attempt.is_multiple_of(RETRY_WARN_INTERVAL) {
                            tracing::warn!("write still retrying after {attempt} attempts: {err}");
                        }
                        self.congestion.note_retry();
                        tokio::time::sleep(self.congestion.backoff(attempt)).await;
                    }
                    RetryClass::Fatal => {
                        drop(permit);
                        return Err(err);
                    }
                },
            }
        }
    }
}

impl Deref for Client {
    type Target = CachingSession;

    fn deref(&self) -> &Self::Target {
        &self.session
    }
}

pub static CONGESTION_WINDOW_METRIC: &str = "client::congestion::window";
pub static CONGESTION_IN_FLIGHT_METRIC: &str = "client::congestion::in_flight";
pub static CONGESTION_RETRY_METRIC: &str = "client::congestion::retries";
pub static CONGESTION_OVERLOAD_METRIC: &str = "client::congestion::overload_signals";

/// Consecutive retries of a single write between warning logs. Retries are
/// unbounded, so this surfaces a persistently failing cluster without spamming.
const RETRY_WARN_INTERVAL: u32 = 20;

#[derive(Debug, Clone)]
pub struct CongestionConfig {
    pub min_window: usize,
    pub initial_window: usize,
    pub max_window: usize,
    pub increase_by: usize,
    pub decrease_factor: f64,
    pub min_utilisation: f64,
    pub base_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for CongestionConfig {
    fn default() -> Self {
        Self {
            min_window: 1,
            initial_window: 32,
            max_window: 1024,
            increase_by: 1,
            decrease_factor: 0.6,
            min_utilisation: 0.9,
            base_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(2),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Success,
    Overload,
}

enum RetryClass {
    Retryable,
    Fatal,
}

struct WindowState {
    /// Target window: the number of requests allowed in flight concurrently.
    limit: usize,
    /// Requests currently holding a slot.
    in_flight: usize,
    /// Capacity the window has shrunk by that could not be reclaimed immediately
    /// because the slots were in flight. Paid down (rather than returned to the
    /// semaphore) as those requests complete.
    debt: usize,
}

struct ControllerInner {
    cfg: CongestionConfig,
    /// Gate for the window. A permit is acquired (and immediately `forget`-ten) per
    /// in-flight request, and capacity is handed back explicitly on completion via
    /// `add_permits`. Releasing only the slots that actually freed up — instead of
    /// waking every waiter — keeps wakeups O(1) under high fan-out.
    sem: Semaphore,
    state: Mutex<WindowState>,
    window_gauge: Gauge,
    in_flight_gauge: Gauge,
    retry_counter: Counter,
    overload_counter: Counter,
}

impl ControllerInner {
    fn lock(&self) -> std::sync::MutexGuard<'_, WindowState> {
        self.state.lock().expect("congestion state mutex poisoned")
    }

    fn record_success(&self) {
        let give_back = {
            let mut state = self.lock();
            let in_flight_during = state.in_flight;
            state.in_flight = state.in_flight.saturating_sub(1);
            let give_back = if state.debt > 0 {
                // Retire the freed slot against outstanding shrink debt rather than
                // returning it to the pool.
                state.debt -= 1;
                0
            } else if (in_flight_during as f64) >= self.cfg.min_utilisation * (state.limit as f64)
                && state.limit < self.cfg.max_window
            {
                let grow = self.cfg.increase_by.min(self.cfg.max_window - state.limit);
                state.limit += grow;
                self.window_gauge.set(state.limit as f64);
                // Return the freed slot plus the newly opened capacity.
                1 + grow
            } else {
                1
            };
            self.in_flight_gauge.set(state.in_flight as f64);
            give_back
        };
        if give_back > 0 {
            self.sem.add_permits(give_back);
        }
    }

    fn record_overload(&self) {
        let give_back = {
            let mut state = self.lock();
            state.in_flight = state.in_flight.saturating_sub(1);
            let new_limit = ((state.limit as f64) * self.cfg.decrease_factor).floor() as usize;
            let new_limit = new_limit.max(self.cfg.min_window);
            let cut = state.limit - new_limit;
            state.limit = new_limit;
            self.window_gauge.set(state.limit as f64);
            self.in_flight_gauge.set(state.in_flight as f64);
            if cut == 0 {
                1
            } else {
                // Drop the freed slot to retire one unit of the shrink; defer the
                // remainder as debt to be retired as in-flight requests complete.
                state.debt += cut - 1;
                0
            }
        };
        if give_back > 0 {
            self.sem.add_permits(give_back);
        }
        self.overload_counter.increment(1);
    }

    fn release_neutral(&self) {
        let give_back = {
            let mut state = self.lock();
            state.in_flight = state.in_flight.saturating_sub(1);
            let give_back = if state.debt > 0 {
                state.debt -= 1;
                0
            } else {
                1
            };
            self.in_flight_gauge.set(state.in_flight as f64);
            give_back
        };
        if give_back > 0 {
            self.sem.add_permits(give_back);
        }
    }
}

#[derive(Clone)]
pub struct CongestionController {
    inner: Arc<ControllerInner>,
}

impl CongestionController {
    pub fn new(cfg: CongestionConfig) -> Self {
        let initial = cfg
            .initial_window
            .clamp(cfg.min_window.max(1), cfg.max_window.max(1));
        let window_gauge = metrics::gauge!(CONGESTION_WINDOW_METRIC);
        window_gauge.set(initial as f64);
        let in_flight_gauge = metrics::gauge!(CONGESTION_IN_FLIGHT_METRIC);
        in_flight_gauge.set(0.0);
        Self {
            inner: Arc::new(ControllerInner {
                sem: Semaphore::new(initial),
                state: Mutex::new(WindowState {
                    limit: initial,
                    in_flight: 0,
                    debt: 0,
                }),
                window_gauge,
                in_flight_gauge,
                retry_counter: metrics::counter!(CONGESTION_RETRY_METRIC),
                overload_counter: metrics::counter!(CONGESTION_OVERLOAD_METRIC),
                cfg,
            }),
        }
    }

    pub async fn acquire(&self) -> CongestionPermit {
        // Take a slot from the window and `forget` it: capacity is accounted for
        // explicitly and handed back on completion (see `ControllerInner`), so the
        // permit's own RAII release would double-count it.
        self.inner
            .sem
            .acquire()
            .await
            .expect("congestion semaphore unexpectedly closed")
            .forget();
        let mut state = self.inner.lock();
        state.in_flight += 1;
        self.inner.in_flight_gauge.set(state.in_flight as f64);
        CongestionPermit {
            inner: self.inner.clone(),
            reported: false,
        }
    }

    pub fn window(&self) -> usize {
        self.inner.lock().limit
    }

    pub fn in_flight(&self) -> usize {
        self.inner.lock().in_flight
    }

    fn note_retry(&self) {
        self.inner.retry_counter.increment(1);
    }

    fn backoff(&self, attempt: u32) -> Duration {
        let shift = attempt.min(16);
        let base_ms = self.inner.cfg.base_backoff.as_millis() as u64;
        let cap_ms = self.inner.cfg.max_backoff.as_millis().max(1) as u64;
        let ceiling = base_ms.saturating_mul(1u64 << shift).min(cap_ms).max(1);
        Duration::from_millis(rand::rng().random_range((ceiling / 2)..=ceiling))
    }
}

pub struct CongestionPermit {
    inner: Arc<ControllerInner>,
    reported: bool,
}

impl CongestionPermit {
    pub fn report(mut self, outcome: Outcome) {
        match outcome {
            Outcome::Success => self.inner.record_success(),
            Outcome::Overload => self.inner.record_overload(),
        }
        self.reported = true;
    }
}

impl Drop for CongestionPermit {
    fn drop(&mut self) {
        if !self.reported {
            self.inner.release_neutral();
        }
    }
}

// Deterministic failures (bad query, serialization, auth, ...) map to `Fatal` and
// are propagated as-is. Only transient overload/availability signals are retried.
fn classify_execution_error(err: &ExecutionError) -> RetryClass {
    match err {
        ExecutionError::RequestTimeout(_) | ExecutionError::ConnectionPoolError(_) => {
            RetryClass::Retryable
        }
        ExecutionError::LastAttemptError(attempt) => classify_attempt_error(attempt),
        _ => RetryClass::Fatal,
    }
}

fn classify_attempt_error(err: &RequestAttemptError) -> RetryClass {
    match err {
        RequestAttemptError::UnableToAllocStreamId
        | RequestAttemptError::BrokenConnectionError(_) => RetryClass::Retryable,
        RequestAttemptError::DbError(db_error, _) => classify_db_error(db_error),
        _ => RetryClass::Fatal,
    }
}

fn classify_db_error(err: &DbError) -> RetryClass {
    match err {
        DbError::Overloaded
        | DbError::RateLimitReached { .. }
        | DbError::WriteTimeout { .. }
        | DbError::ReadTimeout { .. }
        | DbError::Unavailable { .. }
        | DbError::IsBootstrapping
        | DbError::TruncateError => RetryClass::Retryable,
        _ => RetryClass::Fatal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_database_url() {
        let url_credentials_attributes = "scylla://gandalf:mellon@10.0.0.168,10.0.0.35,10.0.0.139,10.0.0.11,10.0.0.73,10.0.0.194/mdb_uniprot?connection_timeout=60&pool_size=1&pool_type=shard";
        let url_attributes = "scylla://10.0.0.168,10.0.0.35,10.0.0.139,10.0.0.11,10.0.0.73,10.0.0.194/mdb_uniprot?connection_timeout=60&pool_size=1&write_consistency_level=quorum&read_consistency_level=one";
        let url_mandatory =
            "scylla://10.0.0.168,10.0.0.35,10.0.0.139,10.0.0.11,10.0.0.73,10.0.0.194/mdb_uniprot";

        let settings = ClientSettings::new(url_credentials_attributes).unwrap();
        assert_eq!(settings.user, Some("gandalf".to_string()));
        assert_eq!(settings.password, Some("mellon".to_string()));
        assert_eq!(
            settings.hosts,
            vec![
                "10.0.0.168".to_string(),
                "10.0.0.35".to_string(),
                "10.0.0.139".to_string(),
                "10.0.0.11".to_string(),
                "10.0.0.73".to_string(),
                "10.0.0.194".to_string()
            ]
        );
        assert_eq!(settings.connection_timeout, Some(Duration::from_secs(60)));
        assert_eq!(settings.pool_size, Some(1));
        assert_eq!(settings.pool_type, PoolType::PerShard);

        let settings = ClientSettings::new(url_attributes).unwrap();
        assert_eq!(settings.user, None);
        assert_eq!(settings.password, None);
        assert_eq!(
            settings.hosts,
            vec![
                "10.0.0.168".to_string(),
                "10.0.0.35".to_string(),
                "10.0.0.139".to_string(),
                "10.0.0.11".to_string(),
                "10.0.0.73".to_string(),
                "10.0.0.194".to_string()
            ]
        );
        assert_eq!(settings.connection_timeout, Some(Duration::from_secs(60)));
        assert_eq!(settings.pool_size, Some(1));
        assert_eq!(settings.pool_type, PoolType::PerHost);
        assert_eq!(settings.read_consistency_level, Some(Consistency::One));
        assert_eq!(settings.write_consistency_level, Some(Consistency::Quorum));

        let settings = ClientSettings::new(url_mandatory).unwrap();
        assert_eq!(settings.user, None);
        assert_eq!(settings.password, None);
        assert_eq!(
            settings.hosts,
            vec![
                "10.0.0.168".to_string(),
                "10.0.0.35".to_string(),
                "10.0.0.139".to_string(),
                "10.0.0.11".to_string(),
                "10.0.0.73".to_string(),
                "10.0.0.194".to_string()
            ]
        );
        assert_eq!(settings.connection_timeout, None);
        assert_eq!(settings.pool_size, None);
        assert_eq!(settings.pool_type, PoolType::PerHost);
    }

    fn controller(initial: usize) -> CongestionController {
        CongestionController::new(CongestionConfig {
            initial_window: initial,
            ..Default::default()
        })
    }

    #[test]
    fn overload_shrinks_window_with_floor() {
        let c = controller(10);
        c.inner.record_overload();
        assert_eq!(c.window(), 6, "floor(10 * 0.6) = 6");

        let c = controller(2);
        c.inner.record_overload();
        assert_eq!(c.window(), 1, "floor() must shrink small windows");

        let c = controller(1);
        c.inner.record_overload();
        assert_eq!(c.window(), 1, "must not drop below min_window");
    }

    #[test]
    fn success_increases_only_when_saturated() {
        let c = controller(10);
        c.inner.lock().in_flight = 4;
        c.inner.record_success();
        assert_eq!(c.window(), 10, "low utilisation must not grow the window");

        let c = controller(10);
        c.inner.lock().in_flight = 9;
        c.inner.record_success();
        assert_eq!(c.window(), 11, "high utilisation grows the window");
    }

    #[tokio::test]
    async fn gate_blocks_at_limit_and_releases() {
        let c = controller(1);
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

    #[tokio::test]
    async fn shrink_while_saturated_retires_capacity_via_debt() {
        // Shrinking the window while every slot is in flight cannot reclaim those
        // slots immediately; the deficit is carried as debt and retired as the
        // in-flight requests complete, so the gate must never over-supply.
        let c = CongestionController::new(CongestionConfig {
            initial_window: 100,
            min_window: 1,
            max_window: 1000,
            decrease_factor: 0.9, // floor(100 * 0.9) = 90 -> a cut of 10
            ..Default::default()
        });

        // Saturate the window.
        let mut permits = Vec::new();
        for _ in 0..100 {
            permits.push(c.acquire().await);
        }
        assert_eq!(c.in_flight(), 100);

        // One request hits overload: window 100 -> 90. Only the freed slot can be
        // retired now; the other 9 units of the cut become debt.
        permits.pop().unwrap().report(Outcome::Overload);
        assert_eq!(c.window(), 90);

        // Release the rest neutrally; the first 9 pay down debt rather than handing
        // capacity back, the remaining 90 return their slots.
        drop(permits);
        assert_eq!(c.in_flight(), 0);
        assert_eq!(c.window(), 90);

        // Exactly `window()` slots are acquirable, proving the shrink was retired.
        let mut held = Vec::new();
        for _ in 0..c.window() {
            held.push(c.acquire().await);
        }
        assert_eq!(c.in_flight(), 90);

        let c2 = c.clone();
        let mut pending = Box::pin(c2.acquire());
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut pending)
                .await
                .is_err(),
            "acquire must block: the window must not exceed the shrunken limit"
        );
    }

    #[test]
    fn classification_separates_transient_from_deterministic() {
        assert!(matches!(
            classify_db_error(&DbError::Overloaded),
            RetryClass::Retryable
        ));
        assert!(matches!(
            classify_db_error(&DbError::SyntaxError),
            RetryClass::Fatal
        ));
        assert!(matches!(
            classify_db_error(&DbError::Unauthorized),
            RetryClass::Fatal
        ));
        assert!(matches!(
            classify_attempt_error(&RequestAttemptError::UnableToAllocStreamId),
            RetryClass::Retryable
        ));
    }
}
