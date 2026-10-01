//! Address autocomplete proxy over OpenStreetMap data (Photon).
//!
//! `POST /v0/address/suggest` lets the Shop offer address suggestions without
//! the buyer's browser ever contacting a third party: the upstream sees this
//! service's address, a fixed User-Agent and the query, and nothing else. The
//! route is public (a query is never bound to a session), takes its query in
//! a POST body (never a URL an edge could log), and never logs, persists or
//! echoes query text. Cache keys are salted hashes held in memory only.
//!
//! The public Nominatim instance forbids autocomplete; Photon is built for
//! it. The default upstream is the public Photon instance, capped well below
//! its "reasonable use" wording; `ADDRESS_SEARCH_UPSTREAM_URL` points the
//! service at a self-hosted instance without a code change.

use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::to_bytes;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::AppState;

pub const ENV_UPSTREAM_URL: &str = "ADDRESS_SEARCH_UPSTREAM_URL";
pub const ENV_DISABLED: &str = "ADDRESS_SEARCH_DISABLED";
pub const ENV_CLIENT_IP_HEADER: &str = "ADDRESS_SEARCH_CLIENT_IP_HEADER";
pub const ENV_CLIENT_PER_MINUTE: &str = "ADDRESS_SEARCH_CLIENT_PER_MINUTE";
pub const ENV_UPSTREAM_PER_SECOND: &str = "ADDRESS_SEARCH_UPSTREAM_PER_SECOND";

pub const DEFAULT_UPSTREAM_URL: &str = "https://photon.komoot.io";
pub const DEFAULT_CLIENT_PER_MINUTE: u32 = 30;
pub const DEFAULT_UPSTREAM_PER_SECOND: u32 = 2;

pub const MIN_QUERY_CHARS: usize = 4;
pub const MAX_QUERY_CHARS: usize = 120;
pub const MAX_REQUEST_BODY_BYTES: usize = 1024;
pub const MAX_SUGGESTIONS: usize = 5;
pub const CLIENT_BURST: f64 = 8.0;
pub const UPSTREAM_BURST_SECONDS: f64 = 3.0;
pub const CACHE_TTL_SECONDS: i64 = 30 * 60;
pub const CACHE_CAPACITY: usize = 4096;
pub const CLIENT_TABLE_CAPACITY: usize = 20_000;
/// Consecutive upstream failures (timeout, transport, 5xx, unreadable answer)
/// that open the breaker. A single slow answer from the public instance is
/// routine and must not take suggestions away from every buyer.
pub const BREAKER_FAILURE_THRESHOLD: u32 = 3;
/// First breaker cooldown; each reopening without a success in between
/// doubles it up to `BREAKER_MAX_SECONDS`.
pub const BREAKER_BASE_SECONDS: i64 = 10;
pub const BREAKER_MAX_SECONDS: i64 = 60;
/// Upper bound on an upstream 429's `Retry-After` that the breaker honours.
pub const THROTTLE_MAX_SECONDS: i64 = 600;
/// `Retry-After` for a failure that did not open the breaker.
pub const FAILURE_RETRY_AFTER_SECONDS: u64 = 2;
/// `Retry-After` while another request is probing a half-open breaker.
pub const PROBE_RETRY_AFTER_SECONDS: u64 = 1;
/// Public Photon answers in about 0.7–2.5 s from the service's region, so
/// the budget leaves headroom over its tail; a refused connection still
/// fails fast on the connect timeout.
pub const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(6);
pub const UPSTREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
pub const UPSTREAM_BODY_LIMIT: usize = 256 * 1024;
const UPSTREAM_FETCH_LIMIT: usize = 8;
const FIELD_MAX_CHARS: usize = 200;
const USER_AGENT: &str = "pubky-marketplace-service/address-search (+https://shop.pubky.app)";

pub const ATTRIBUTION_TEXT: &str = "© OpenStreetMap contributors";
pub const ATTRIBUTION_URL: &str = "https://www.openstreetmap.org/copyright";

/// Countries whose postal convention writes the house number before the
/// street name. Everywhere else the street comes first.
const NUMBER_FIRST_COUNTRIES: &[&str] = &["US", "CA", "GB", "IE", "AU", "NZ", "FR"];

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SuggestedAddress {
    pub line1: String,
    pub city: String,
    pub region: String,
    pub postal_code: String,
    pub country_code: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Suggestion {
    pub id: String,
    pub primary: String,
    pub secondary: String,
    /// `house` when OpenStreetMap knows the house number, `street` when only
    /// the street matched (a typed leading number is kept on line 1).
    pub precision: &'static str,
    pub address: SuggestedAddress,
}

#[derive(Debug, Deserialize)]
struct PhotonCollection {
    #[serde(default)]
    features: Vec<PhotonFeature>,
}

#[derive(Debug, Deserialize)]
struct PhotonFeature {
    properties: PhotonProperties,
}

#[derive(Debug, Default, Deserialize)]
struct PhotonProperties {
    name: Option<String>,
    housenumber: Option<String>,
    street: Option<String>,
    postcode: Option<String>,
    city: Option<String>,
    district: Option<String>,
    locality: Option<String>,
    state: Option<String>,
    countrycode: Option<String>,
    osm_type: Option<String>,
    osm_id: Option<i64>,
    #[serde(rename = "type")]
    kind: Option<String>,
}

fn clean(value: Option<&str>) -> String {
    value
        .unwrap_or("")
        .chars()
        .filter(|c| !c.is_control())
        .collect::<String>()
        .trim()
        .chars()
        .take(FIELD_MAX_CHARS)
        .collect()
}

fn first_non_empty(values: &[&Option<String>]) -> String {
    values
        .iter()
        .map(|value| clean(value.as_deref()))
        .find(|value| !value.is_empty())
        .unwrap_or_default()
}

/// The house number a buyer typed ahead of the street ("42 Union St" ->
/// "42"). Ordinals ("5th", "42nd") are street names, not house numbers.
fn typed_house_number(query: &str) -> Option<String> {
    let mut tokens = query.split_whitespace();
    let first = tokens.next()?;
    tokens.next()?;
    let (digits, suffix) = first.split_at(
        first
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(first.len()),
    );
    let plain = !digits.is_empty()
        && digits.len() <= 6
        && (suffix.is_empty()
            || (suffix.len() == 1 && suffix.chars().all(|c| c.is_ascii_alphabetic())));
    let range = first.split_once(['-', '/']).is_some_and(|(left, right)| {
        !left.is_empty()
            && !right.is_empty()
            && left.len() <= 6
            && right.len() <= 6
            && left.chars().all(|c| c.is_ascii_digit())
            && right.chars().all(|c| c.is_ascii_digit())
    });
    (plain || range).then(|| first.to_string())
}

/// True when the street plausibly is the one the buyer is typing: the typed
/// text after the number starts the street name, or the street name starts
/// the typed text. Photon's fuzzy neighbours ("Bedford Street" for "Union
/// Street New Bedford") never receive the buyer's number.
fn street_matches_typed(street: &str, query: &str, number: &str) -> bool {
    let rest = query
        .trim()
        .strip_prefix(number)
        .unwrap_or("")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase();
    let street = street.to_lowercase();
    if rest.is_empty() || street.starts_with(&format!("{} ", number.to_lowercase())) {
        return false;
    }
    let first_word = rest.split(' ').next().unwrap_or("");
    street.starts_with(first_word) || rest.starts_with(&street)
}

fn compose_line1(country: &str, number: &str, street: &str) -> String {
    if number.is_empty() {
        return street.to_string();
    }
    if NUMBER_FIRST_COUNTRIES.contains(&country) {
        format!("{number} {street}")
    } else {
        format!("{street} {number}")
    }
}

fn osm_id(properties: &PhotonProperties, index: usize) -> String {
    let letter = match properties.osm_type.as_deref() {
        Some("N") => "n",
        Some("W") => "w",
        Some("R") => "r",
        _ => return format!("p{index}"),
    };
    match properties.osm_id {
        Some(id) if id > 0 => format!("{letter}{id}"),
        _ => format!("p{index}"),
    }
}

/// The upstream answered 200 with something that is not a Photon result set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MalformedUpstream;

/// Maps a Photon response to address suggestions. Results that cannot fill
/// Address line 1 (a city, a country) are dropped.
pub fn normalize(
    body: &[u8],
    query: &str,
    country_filter: Option<&str>,
) -> Result<Vec<Suggestion>, MalformedUpstream> {
    let collection: PhotonCollection =
        serde_json::from_slice(body).map_err(|_| MalformedUpstream)?;
    let typed_number = typed_house_number(query);
    let mut seen: Vec<(String, String, String)> = Vec::new();
    let mut suggestions = Vec::new();
    for (index, feature) in collection.features.into_iter().enumerate() {
        let properties = feature.properties;
        let country = clean(properties.countrycode.as_deref()).to_ascii_uppercase();
        if country.len() != 2 {
            continue;
        }
        if country_filter.is_some_and(|wanted| wanted != country) {
            continue;
        }
        let kind = properties.kind.as_deref().unwrap_or("");
        let street_name = if kind == "street" {
            clean(properties.name.as_deref())
        } else {
            clean(properties.street.as_deref())
        };
        if street_name.is_empty() {
            continue;
        }
        let number = if kind == "street" {
            String::new()
        } else {
            clean(properties.housenumber.as_deref())
        };
        let (line1, precision) = if !number.is_empty() {
            (compose_line1(&country, &number, &street_name), "house")
        } else {
            let typed = typed_number
                .as_deref()
                .filter(|number| street_matches_typed(&street_name, query, number))
                .unwrap_or("");
            (compose_line1(&country, typed, &street_name), "street")
        };
        let city = first_non_empty(&[&properties.city, &properties.district, &properties.locality]);
        let region = clean(properties.state.as_deref());
        let postal_code = clean(properties.postcode.as_deref());
        let key = (
            line1.to_lowercase(),
            city.to_lowercase(),
            postal_code.to_lowercase(),
        );
        if seen.contains(&key) {
            continue;
        }
        seen.push(key);

        let region_and_postal = [region.as_str(), postal_code.as_str()]
            .iter()
            .filter(|part| !part.is_empty())
            .copied()
            .collect::<Vec<_>>()
            .join(" ");
        let area = [city.as_str(), region_and_postal.as_str()]
            .iter()
            .filter(|part| !part.is_empty())
            .copied()
            .collect::<Vec<_>>()
            .join(", ");
        let place_name = clean(properties.name.as_deref());
        let secondary = if precision == "house" && !place_name.is_empty() && place_name != line1 {
            [place_name.as_str(), area.as_str()]
                .iter()
                .filter(|part| !part.is_empty())
                .copied()
                .collect::<Vec<_>>()
                .join(" · ")
        } else {
            area
        };
        suggestions.push(Suggestion {
            id: osm_id(&properties, index),
            primary: line1.clone(),
            secondary,
            precision,
            address: SuggestedAddress {
                line1,
                city,
                region,
                postal_code,
                country_code: country,
            },
        });
        if suggestions.len() == MAX_SUGGESTIONS {
            break;
        }
    }
    Ok(suggestions)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamError {
    /// A 429, with the upstream's `Retry-After` seconds when it sent them.
    Throttled { retry_after_seconds: Option<u64> },
    /// Transport failure, timeout, 5xx, or an unreadable answer.
    Unavailable,
    /// A 4xx the upstream gave for this one query.
    Rejected,
}

/// The Photon HTTP client. Sends the query and nothing else identifying: no
/// cookies, no Referer, no forwarded client address, no Accept-Language.
pub struct PhotonClient {
    base_url: url::Url,
    http: reqwest::Client,
}

impl std::fmt::Debug for PhotonClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PhotonClient")
            .field("base_url", &self.base_url.as_str())
            .finish()
    }
}

impl PhotonClient {
    pub fn new(base_url: &str) -> anyhow::Result<Self> {
        Self::with_timeout(base_url, UPSTREAM_TIMEOUT)
    }

    /// `timeout` bounds the whole exchange, body included.
    pub fn with_timeout(base_url: &str, timeout: Duration) -> anyhow::Result<Self> {
        let parsed = url::Url::parse(base_url.trim())
            .map_err(|_| anyhow::anyhow!("{ENV_UPSTREAM_URL} must be an absolute URL"))?;
        let loopback = matches!(
            parsed.host_str(),
            Some("127.0.0.1") | Some("localhost") | Some("[::1]")
        );
        match parsed.scheme() {
            "https" => {}
            "http" if loopback => {}
            _ => anyhow::bail!("{ENV_UPSTREAM_URL} must be https (http only for loopback)"),
        }
        if parsed.query().is_some() || parsed.fragment().is_some() {
            anyhow::bail!("{ENV_UPSTREAM_URL} must not carry a query or fragment");
        }
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(UPSTREAM_CONNECT_TIMEOUT.min(timeout))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(USER_AGENT)
            .build()?;
        Ok(Self {
            base_url: parsed,
            http,
        })
    }

    pub async fn search(
        &self,
        query: &str,
        country: Option<&str>,
    ) -> Result<Vec<u8>, UpstreamError> {
        let mut url = self.base_url.clone();
        let path = format!("{}/api/", url.path().trim_end_matches('/'));
        url.set_path(&path);
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("q", query);
            pairs.append_pair("limit", &UPSTREAM_FETCH_LIMIT.to_string());
            pairs.append_pair("layer", "house");
            pairs.append_pair("layer", "street");
            if let Some(country) = country {
                pairs.append_pair("countrycode", country);
            }
        }
        let started = std::time::Instant::now();
        let elapsed_ms = || u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let mut response = self
            .http
            .get(url)
            .header(header::ACCEPT, "application/json")
            .send()
            .await
            .map_err(|failure| {
                tracing::warn!(
                    cause = transport_cause(&failure),
                    elapsed_ms = elapsed_ms(),
                    "address search upstream transport failure"
                );
                UpstreamError::Unavailable
            })?;
        let status = response.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after_seconds = response
                .headers()
                .get(header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.trim().parse::<u64>().ok());
            tracing::warn!(
                status = status.as_u16(),
                retry_after_seconds,
                elapsed_ms = elapsed_ms(),
                "address search upstream refused"
            );
            return Err(UpstreamError::Throttled {
                retry_after_seconds,
            });
        }
        if status.is_server_error() {
            tracing::warn!(
                status = status.as_u16(),
                elapsed_ms = elapsed_ms(),
                "address search upstream refused"
            );
            return Err(UpstreamError::Unavailable);
        }
        if !status.is_success() {
            return Err(UpstreamError::Rejected);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|failure| {
            tracing::warn!(
                cause = transport_cause(&failure),
                elapsed_ms = elapsed_ms(),
                "address search upstream transport failure"
            );
            UpstreamError::Unavailable
        })? {
            if body.len() + chunk.len() > UPSTREAM_BODY_LIMIT {
                tracing::warn!("address search upstream answer exceeded the size limit");
                return Err(UpstreamError::Unavailable);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

fn transport_cause(failure: &reqwest::Error) -> &'static str {
    if failure.is_timeout() {
        "timeout"
    } else if failure.is_connect() {
        "connect"
    } else {
        "transport"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchError {
    RateLimited { retry_after_seconds: u64 },
    Unavailable { retry_after_seconds: u64 },
}

struct Bucket {
    tokens: f64,
    updated: DateTime<Utc>,
}

impl Bucket {
    fn full(capacity: f64, now: DateTime<Utc>) -> Self {
        Self {
            tokens: capacity,
            updated: now,
        }
    }

    fn refill(&mut self, now: DateTime<Utc>, per_second: f64, capacity: f64) {
        let elapsed = (now - self.updated).num_milliseconds().max(0) as f64 / 1_000.0;
        self.tokens = (self.tokens + elapsed * per_second).min(capacity);
        self.updated = now;
    }

    /// Takes one token, or returns the whole seconds until one is available.
    fn take(&mut self, now: DateTime<Utc>, per_second: f64, capacity: f64) -> Result<(), u64> {
        self.refill(now, per_second, capacity);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            Ok(())
        } else {
            Err((((1.0 - self.tokens) / per_second).ceil() as u64).max(1))
        }
    }
}

type CacheKey = [u8; 32];

struct Inner {
    cache: HashMap<CacheKey, (DateTime<Utc>, Arc<Vec<Suggestion>>)>,
    order: VecDeque<CacheKey>,
    clients: HashMap<[u8; 16], Bucket>,
    upstream: Option<Bucket>,
    breaker: Breaker,
}

/// Closed until `BREAKER_FAILURE_THRESHOLD` consecutive failures or one
/// upstream 429; then open for a cooldown; then half-open, where exactly one
/// lookup probes the upstream and either closes the breaker or reopens it
/// with a doubled cooldown.
struct Breaker {
    failures: u32,
    open_until: Option<DateTime<Utc>>,
    next_cooldown_seconds: i64,
    probing: bool,
}

impl Breaker {
    fn closed() -> Self {
        Self {
            failures: 0,
            open_until: None,
            next_cooldown_seconds: BREAKER_BASE_SECONDS,
            probing: false,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Failure {
    Throttled { retry_after_seconds: Option<u64> },
    Unavailable,
}

/// The half-open probe a lookup holds. Dropping it unsettled, when the
/// lookup is cancelled or refused by the upstream budget, frees the probe
/// for the next lookup.
struct ProbeSlot<'a> {
    inner: &'a Mutex<Inner>,
    held: bool,
}

impl Drop for ProbeSlot<'_> {
    fn drop(&mut self) {
        if !self.held {
            return;
        }
        if let Ok(mut inner) = self.inner.lock() {
            inner.breaker.probing = false;
        }
    }
}

pub struct AddressSearchRuntime {
    client: PhotonClient,
    client_ip_header: Option<HeaderName>,
    salt: [u8; 16],
    client_per_second: f64,
    upstream_per_second: f64,
    upstream_capacity: f64,
    inner: Mutex<Inner>,
    flights: Flights,
}

impl std::fmt::Debug for AddressSearchRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AddressSearchRuntime")
            .field("client", &self.client)
            .finish()
    }
}

type Flights = Mutex<HashMap<CacheKey, Arc<tokio::sync::Mutex<()>>>>;

/// One lookup's hold on its query's single flight. Dropping it, including
/// when the request is cancelled mid-upstream call, removes the key once no
/// other lookup holds it.
struct FlightSlot<'a> {
    gate: Option<Arc<tokio::sync::Mutex<()>>>,
    key: CacheKey,
    flights: &'a Flights,
}

impl Drop for FlightSlot<'_> {
    fn drop(&mut self) {
        self.gate.take();
        let Ok(mut flights) = self.flights.lock() else {
            return;
        };
        if flights
            .get(&self.key)
            .is_some_and(|gate| Arc::strong_count(gate) == 1)
        {
            flights.remove(&self.key);
        }
    }
}

fn normalize_query(query: &str) -> String {
    query
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

impl AddressSearchRuntime {
    pub fn new(
        upstream_url: &str,
        client_ip_header: Option<HeaderName>,
        client_per_minute: u32,
        upstream_per_second: u32,
    ) -> anyhow::Result<Self> {
        if client_per_minute == 0 || upstream_per_second == 0 {
            anyhow::bail!("address search rate limits must be at least 1");
        }
        let mut salt = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut salt);
        let upstream_per_second = f64::from(upstream_per_second);
        let upstream_capacity = (upstream_per_second * UPSTREAM_BURST_SECONDS).max(1.0);
        Ok(Self {
            client: PhotonClient::new(upstream_url)?,
            client_ip_header,
            salt,
            client_per_second: f64::from(client_per_minute) / 60.0,
            upstream_per_second,
            upstream_capacity,
            inner: Mutex::new(Inner {
                cache: HashMap::new(),
                order: VecDeque::new(),
                clients: HashMap::new(),
                upstream: None,
                breaker: Breaker::closed(),
            }),
            flights: Mutex::new(HashMap::new()),
        })
    }

    /// Replaces the default upstream time budget (`UPSTREAM_TIMEOUT`).
    pub fn with_upstream_timeout(mut self, timeout: Duration) -> anyhow::Result<Self> {
        self.client = PhotonClient::with_timeout(self.client.base_url.as_str(), timeout)?;
        Ok(self)
    }

    fn client_key(&self, ip: &str) -> [u8; 16] {
        let mut hasher = Sha256::new();
        hasher.update(self.salt);
        hasher.update(ip.as_bytes());
        let digest = hasher.finalize();
        let mut key = [0u8; 16];
        key.copy_from_slice(&digest[..16]);
        key
    }

    fn cache_key(&self, query: &str, country: Option<&str>) -> CacheKey {
        let mut hasher = Sha256::new();
        hasher.update(self.salt);
        hasher.update(country.unwrap_or("").as_bytes());
        hasher.update([0u8]);
        hasher.update(normalize_query(query).as_bytes());
        hasher.finalize().into()
    }

    /// The client address the limiter keys on: the configured trusted header
    /// (last value) when present and valid, else the socket peer.
    pub fn client_address(&self, headers: &HeaderMap, peer: Option<IpAddr>) -> String {
        if let Some(name) = &self.client_ip_header {
            let from_header = headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.rsplit(',').next())
                .and_then(|value| value.trim().parse::<IpAddr>().ok());
            if let Some(ip) = from_header {
                return ip.to_string();
            }
        }
        peer.map(|ip| ip.to_string()).unwrap_or_default()
    }

    fn cached(&self, key: &CacheKey, now: DateTime<Utc>) -> Option<Arc<Vec<Suggestion>>> {
        let inner = self.inner.lock().expect("address search lock");
        inner
            .cache
            .get(key)
            .filter(|(expires, _)| *expires > now)
            .map(|(_, value)| value.clone())
    }

    fn store(&self, key: CacheKey, value: Arc<Vec<Suggestion>>, now: DateTime<Utc>) {
        let mut inner = self.inner.lock().expect("address search lock");
        if inner
            .cache
            .insert(
                key,
                (now + ChronoDuration::seconds(CACHE_TTL_SECONDS), value),
            )
            .is_none()
        {
            inner.order.push_back(key);
        }
        while inner.cache.len() > CACHE_CAPACITY {
            match inner.order.pop_front() {
                Some(oldest) => {
                    inner.cache.remove(&oldest);
                }
                None => break,
            }
        }
    }

    fn admit_client(&self, ip: &str, now: DateTime<Utc>) -> Result<(), SearchError> {
        let key = self.client_key(ip);
        let mut inner = self.inner.lock().expect("address search lock");
        if inner.clients.len() >= CLIENT_TABLE_CAPACITY && !inner.clients.contains_key(&key) {
            let per_second = self.client_per_second;
            inner.clients.retain(|_, bucket| {
                bucket.refill(now, per_second, CLIENT_BURST);
                bucket.tokens < CLIENT_BURST
            });
            if inner.clients.len() >= CLIENT_TABLE_CAPACITY {
                return Err(SearchError::RateLimited {
                    retry_after_seconds: 60,
                });
            }
        }
        inner
            .clients
            .entry(key)
            .or_insert_with(|| Bucket::full(CLIENT_BURST, now))
            .take(now, self.client_per_second, CLIENT_BURST)
            .map_err(|retry_after_seconds| SearchError::RateLimited {
                retry_after_seconds,
            })
    }

    fn breaker_remaining(&self, now: DateTime<Utc>) -> Option<u64> {
        let inner = self.inner.lock().expect("address search lock");
        inner
            .breaker
            .open_until
            .filter(|until| *until > now)
            .map(|until| (until - now).num_seconds().max(1) as u64)
    }

    /// Admits a lookup through the breaker. `Ok(true)` when this lookup is
    /// the half-open probe.
    fn admit_breaker(&self, now: DateTime<Utc>) -> Result<bool, SearchError> {
        let mut inner = self.inner.lock().expect("address search lock");
        let breaker = &mut inner.breaker;
        match breaker.open_until {
            None => Ok(false),
            Some(until) if until > now => Err(SearchError::Unavailable {
                retry_after_seconds: (until - now).num_seconds().max(1) as u64,
            }),
            Some(_) if breaker.probing => Err(SearchError::Unavailable {
                retry_after_seconds: PROBE_RETRY_AFTER_SECONDS,
            }),
            Some(_) => {
                breaker.probing = true;
                Ok(true)
            }
        }
    }

    fn admit_upstream(&self, now: DateTime<Utc>) -> Result<(), SearchError> {
        let mut inner = self.inner.lock().expect("address search lock");
        let capacity = self.upstream_capacity;
        inner
            .upstream
            .get_or_insert_with(|| Bucket::full(capacity, now))
            .take(now, self.upstream_per_second, capacity)
            .map_err(|retry_after_seconds| {
                tracing::warn!(
                    retry_after_seconds,
                    "address search upstream budget exhausted"
                );
                SearchError::Unavailable {
                    retry_after_seconds,
                }
            })
    }

    fn record_success(&self, probe: &mut ProbeSlot<'_>) {
        let mut inner = self.inner.lock().expect("address search lock");
        if inner.breaker.open_until.is_some() {
            tracing::info!("address search breaker closed");
        }
        inner.breaker = Breaker::closed();
        probe.held = false;
    }

    /// Counts a failed upstream call and returns the `Retry-After` seconds
    /// for the lookup that saw it.
    fn record_failure(
        &self,
        now: DateTime<Utc>,
        failure: Failure,
        probe: &mut ProbeSlot<'_>,
    ) -> u64 {
        let mut inner = self.inner.lock().expect("address search lock");
        let breaker = &mut inner.breaker;
        let was_probe = std::mem::replace(&mut probe.held, false);
        if was_probe {
            breaker.probing = false;
        }
        if let Some(until) = breaker.open_until.filter(|until| *until > now) {
            return (until - now).num_seconds().max(1) as u64;
        }
        breaker.failures = breaker.failures.saturating_add(1);
        let cooldown = match failure {
            Failure::Throttled {
                retry_after_seconds,
            } => retry_after_seconds
                .map(|seconds| i64::try_from(seconds).unwrap_or(THROTTLE_MAX_SECONDS))
                .unwrap_or(0)
                .clamp(breaker.next_cooldown_seconds, THROTTLE_MAX_SECONDS),
            Failure::Unavailable if was_probe || breaker.failures >= BREAKER_FAILURE_THRESHOLD => {
                breaker.next_cooldown_seconds
            }
            Failure::Unavailable => return FAILURE_RETRY_AFTER_SECONDS,
        };
        breaker.open_until = Some(now + ChronoDuration::seconds(cooldown));
        breaker.next_cooldown_seconds =
            (breaker.next_cooldown_seconds * 2).min(BREAKER_MAX_SECONDS);
        tracing::warn!(
            cooldown_seconds = cooldown,
            consecutive_failures = breaker.failures,
            "address search breaker opened"
        );
        cooldown as u64
    }

    /// One suggestion lookup: per-client limit, cache, breaker, per-query
    /// single flight, global upstream limit, then the upstream call.
    pub async fn search<F>(
        &self,
        now: F,
        client_ip: &str,
        query: &str,
        country: Option<&str>,
    ) -> Result<Arc<Vec<Suggestion>>, SearchError>
    where
        F: Fn() -> DateTime<Utc>,
    {
        self.admit_client(client_ip, now())?;
        let key = self.cache_key(query, country);
        if let Some(hit) = self.cached(&key, now()) {
            return Ok(hit);
        }
        if let Some(retry_after_seconds) = self.breaker_remaining(now()) {
            return Err(SearchError::Unavailable {
                retry_after_seconds,
            });
        }
        let slot = FlightSlot {
            gate: Some(
                self.flights
                    .lock()
                    .expect("address search flights lock")
                    .entry(key)
                    .or_default()
                    .clone(),
            ),
            key,
            flights: &self.flights,
        };
        let gate = slot.gate.as_deref().expect("flight gate is set");
        self.fetch_once(gate, key, &now, query, country).await
    }

    async fn fetch_once<F>(
        &self,
        gate: &tokio::sync::Mutex<()>,
        key: CacheKey,
        now: &F,
        query: &str,
        country: Option<&str>,
    ) -> Result<Arc<Vec<Suggestion>>, SearchError>
    where
        F: Fn() -> DateTime<Utc>,
    {
        let _turn = gate.lock().await;
        if let Some(hit) = self.cached(&key, now()) {
            return Ok(hit);
        }
        let mut probe = ProbeSlot {
            inner: &self.inner,
            held: self.admit_breaker(now())?,
        };
        self.admit_upstream(now())?;
        let outcome = match self.client.search(query, country).await {
            Ok(body) => normalize(&body, query, country).map_err(|MalformedUpstream| {
                tracing::warn!("address search upstream answer was unreadable");
                Failure::Unavailable
            }),
            Err(UpstreamError::Rejected) => Ok(Vec::new()),
            Err(UpstreamError::Throttled {
                retry_after_seconds,
            }) => Err(Failure::Throttled {
                retry_after_seconds,
            }),
            Err(UpstreamError::Unavailable) => Err(Failure::Unavailable),
        };
        match outcome {
            Ok(suggestions) => {
                self.record_success(&mut probe);
                let suggestions = Arc::new(suggestions);
                self.store(key, suggestions.clone(), now());
                Ok(suggestions)
            }
            Err(failure) => Err(SearchError::Unavailable {
                retry_after_seconds: self.record_failure(now(), failure, &mut probe),
            }),
        }
    }
}

/// Builds the runtime from the environment. `None` when the kill switch is
/// set: the route then answers `address_search_unavailable`.
pub fn runtime_from_env() -> anyhow::Result<Option<Arc<AddressSearchRuntime>>> {
    let disabled = matches!(
        std::env::var(ENV_DISABLED).ok().as_deref().map(str::trim),
        Some("1") | Some("true") | Some("TRUE") | Some("yes")
    );
    if disabled {
        return Ok(None);
    }
    let upstream = std::env::var(ENV_UPSTREAM_URL)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_UPSTREAM_URL.to_string());
    let header = match std::env::var(ENV_CLIENT_IP_HEADER)
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        Some(name) => Some(
            HeaderName::from_bytes(name.trim().as_bytes())
                .map_err(|_| anyhow::anyhow!("{ENV_CLIENT_IP_HEADER} is not a header name"))?,
        ),
        None => None,
    };
    let per_minute = env_u32(ENV_CLIENT_PER_MINUTE, DEFAULT_CLIENT_PER_MINUTE)?;
    let per_second = env_u32(ENV_UPSTREAM_PER_SECOND, DEFAULT_UPSTREAM_PER_SECOND)?;
    Ok(Some(Arc::new(AddressSearchRuntime::new(
        &upstream, header, per_minute, per_second,
    )?)))
}

fn env_u32(name: &str, default: u32) -> anyhow::Result<u32> {
    match std::env::var(name).ok().filter(|v| !v.trim().is_empty()) {
        None => Ok(default),
        Some(raw) => raw
            .trim()
            .parse::<u32>()
            .ok()
            .filter(|value| *value >= 1 && *value <= 10_000)
            .ok_or_else(|| anyhow::anyhow!("{name} must be an integer from 1 to 10000")),
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SuggestRequest {
    q: String,
    #[serde(default)]
    country: Option<String>,
}

fn error(status: StatusCode, code: &str, message: &str, retry_after: Option<u64>) -> Response {
    let mut response = (
        status,
        axum::Json(json!({"ok": false, "error": {"code": code, "message": message}})),
    )
        .into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if let Some(seconds) = retry_after {
        response.headers_mut().insert(
            header::RETRY_AFTER,
            HeaderValue::from_str(&seconds.to_string()).expect("digits are a header value"),
        );
    }
    response
}

fn invalid(message: &str) -> Response {
    error(StatusCode::BAD_REQUEST, "invalid_request", message, None)
}

/// `POST /v0/address/suggest` — public, body `{ "q": string, "country"?: "US" }`.
pub async fn suggest(State(state): State<AppState>, request: Request) -> Response {
    let Some(runtime) = state.address_search.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "address_search_unavailable",
            "Address suggestions are unavailable.",
            None,
        );
    };
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(address)| address.ip());
    let headers = request.headers().clone();
    let bytes = match to_bytes(request.into_body(), MAX_REQUEST_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => return invalid("The request body is too large."),
    };
    let parsed: SuggestRequest = match serde_json::from_slice(&bytes) {
        Ok(parsed) => parsed,
        Err(_) => {
            return invalid("The request body must be {\"q\": string, \"country\"?: string}.")
        }
    };
    let query = parsed.q.trim();
    let length = query.chars().count();
    if !(MIN_QUERY_CHARS..=MAX_QUERY_CHARS).contains(&length)
        || query.chars().any(|c| c.is_control())
    {
        return invalid("The query must be 4 to 120 printable characters.");
    }
    let country = match parsed.country.as_deref().map(str::trim) {
        None | Some("") => None,
        Some(code) if code.len() == 2 && code.chars().all(|c| c.is_ascii_alphabetic()) => {
            Some(code.to_ascii_uppercase())
        }
        Some(_) => return invalid("The country must be a two-letter code."),
    };
    let client_ip = runtime.client_address(&headers, peer);
    let clock = state.clock.clone();
    match runtime
        .search(move || clock.now(), &client_ip, query, country.as_deref())
        .await
    {
        Ok(suggestions) => {
            let mut response = axum::Json(json!({
                "suggestions": Value::from(
                    suggestions
                        .iter()
                        .map(|s| serde_json::to_value(s).expect("suggestion serializes"))
                        .collect::<Vec<_>>()
                ),
                "attribution": {"text": ATTRIBUTION_TEXT, "url": ATTRIBUTION_URL},
            }))
            .into_response();
            response
                .headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            response
        }
        Err(SearchError::RateLimited {
            retry_after_seconds,
        }) => error(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "Too many address searches. Try again shortly.",
            Some(retry_after_seconds),
        ),
        Err(SearchError::Unavailable {
            retry_after_seconds,
        }) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "address_search_unavailable",
            "Address suggestions are unavailable.",
            Some(retry_after_seconds),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::AddressSearchRuntime;
    use std::time::Duration;

    #[tokio::test]
    async fn a_lookup_cancelled_during_the_upstream_call_leaves_no_flight_behind() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let silent = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        });
        let runtime =
            AddressSearchRuntime::new(&format!("http://127.0.0.1:{port}"), None, 30, 2).unwrap();

        let cancelled = tokio::time::timeout(
            Duration::from_millis(200),
            runtime.search(
                chrono::Utc::now,
                "203.0.113.1",
                "42 Union Street",
                Some("US"),
            ),
        )
        .await;
        assert!(cancelled.is_err(), "the upstream never answered");
        assert!(
            runtime.flights.lock().unwrap().is_empty(),
            "the cancelled lookup's flight key was removed"
        );
        silent.abort();
    }

    #[tokio::test]
    async fn a_half_open_probe_cancelled_during_the_upstream_call_frees_the_probe() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let silent = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        });
        let runtime =
            AddressSearchRuntime::new(&format!("http://127.0.0.1:{port}"), None, 30, 2).unwrap();
        runtime.inner.lock().unwrap().breaker.open_until =
            Some(chrono::Utc::now() - chrono::Duration::seconds(1));

        let cancelled = tokio::time::timeout(
            Duration::from_millis(200),
            runtime.search(
                chrono::Utc::now,
                "203.0.113.1",
                "42 Union Street",
                Some("US"),
            ),
        )
        .await;
        assert!(cancelled.is_err(), "the upstream never answered");
        let inner = runtime.inner.lock().unwrap();
        assert!(!inner.breaker.probing, "the next lookup may probe");
        assert!(
            inner.breaker.open_until.is_some(),
            "a cancelled probe proves nothing, so the breaker stays half-open"
        );
        drop(inner);
        silent.abort();
    }
}
