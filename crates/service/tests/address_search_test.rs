//! Address autocomplete proxy (`POST /v0/address/suggest`). The proxy's own
//! Photon client is never faked: it runs against a local HTTP double that
//! serves the responses captured from the public instance (see
//! `tests/fixtures/photon_fixtures.fixture.md`).

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::extract::connect_info::MockConnectInfo;
use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::IntoResponse;
use axum::Router;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use sqlx::PgPool;
use tower::util::ServiceExt;

use common::{test_app, TestApp};
use marketplace_service::address_search::{
    normalize, AddressSearchRuntime, PhotonClient, ATTRIBUTION_TEXT, ATTRIBUTION_URL,
    BREAKER_BASE_SECONDS, BREAKER_MAX_SECONDS, CACHE_TTL_SECONDS, FAILURE_RETRY_AFTER_SECONDS,
    PROBE_RETRY_AFTER_SECONDS, THROTTLE_MAX_SECONDS,
};
use marketplace_service::http::build_router;

const US_UNION: &[u8] = include_bytes!("fixtures/photon_us_union.json");
const US_HOUSE: &[u8] = include_bytes!("fixtures/photon_us_house.json");
const DE_HOUSE: &[u8] = include_bytes!("fixtures/photon_de_house.json");
const POI: &[u8] = include_bytes!("fixtures/photon_poi.json");

#[derive(Clone)]
struct Upstream {
    inner: Arc<Mutex<UpstreamInner>>,
}

struct UpstreamInner {
    status: u16,
    body: Vec<u8>,
    retry_after: Option<String>,
    delay_ms: u64,
    fetches: usize,
    queries: Vec<String>,
    headers: Vec<HeaderMap>,
    by_marker: Vec<(String, Canned)>,
}

/// The answer for any query containing a marker word, overriding the
/// default answer.
#[derive(Clone)]
struct Canned {
    status: u16,
    body: Vec<u8>,
    retry_after: Option<String>,
    delay_ms: u64,
}

struct UpstreamDouble {
    state: Upstream,
    base_url: String,
}

impl UpstreamDouble {
    fn set(&self, status: u16, body: &[u8]) {
        let mut guard = self.state.inner.lock().unwrap();
        guard.status = status;
        guard.body = body.to_vec();
    }
    fn set_retry_after(&self, value: Option<&str>) {
        self.state.inner.lock().unwrap().retry_after = value.map(str::to_string);
    }
    fn set_for(
        &self,
        marker: &str,
        status: u16,
        body: &[u8],
        retry_after: Option<&str>,
        delay_ms: u64,
    ) {
        self.state.inner.lock().unwrap().by_marker.push((
            marker.to_string(),
            Canned {
                status,
                body: body.to_vec(),
                retry_after: retry_after.map(str::to_string),
                delay_ms,
            },
        ));
    }
    fn set_delay(&self, delay_ms: u64) {
        self.state.inner.lock().unwrap().delay_ms = delay_ms;
    }
    fn fetches(&self) -> usize {
        self.state.inner.lock().unwrap().fetches
    }
    fn last_query(&self) -> String {
        self.state
            .inner
            .lock()
            .unwrap()
            .queries
            .last()
            .cloned()
            .unwrap_or_default()
    }
    fn last_headers(&self) -> HeaderMap {
        self.state
            .inner
            .lock()
            .unwrap()
            .headers
            .last()
            .cloned()
            .unwrap_or_default()
    }
}

async fn serve_search(
    State(state): State<Upstream>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
    headers: HeaderMap,
) -> axum::response::Response {
    let (status, body, retry_after, delay) = {
        let mut guard = state.inner.lock().unwrap();
        let query = query.unwrap_or_default();
        guard.fetches += 1;
        guard.headers.push(headers);
        let canned = guard
            .by_marker
            .iter()
            .find(|(marker, _)| query.contains(marker.as_str()))
            .map(|(_, canned)| canned.clone());
        guard.queries.push(query);
        match canned {
            Some(canned) => (
                canned.status,
                canned.body,
                canned.retry_after,
                canned.delay_ms,
            ),
            None => (
                guard.status,
                guard.body.clone(),
                guard.retry_after.clone(),
                guard.delay_ms,
            ),
        }
    };
    if delay > 0 {
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
    let mut response = (
        StatusCode::from_u16(status).unwrap(),
        [("content-type", "application/json")],
        body,
    )
        .into_response();
    if let Some(value) = retry_after {
        response
            .headers_mut()
            .insert("retry-after", value.parse().unwrap());
    }
    response
}

async fn spawn_upstream() -> UpstreamDouble {
    let state = Upstream {
        inner: Arc::new(Mutex::new(UpstreamInner {
            status: 200,
            body: US_UNION.to_vec(),
            retry_after: None,
            delay_ms: 0,
            fetches: 0,
            queries: Vec::new(),
            headers: Vec::new(),
            by_marker: Vec::new(),
        })),
    };
    let router = Router::new()
        .route("/api/", axum::routing::get(serve_search))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    UpstreamDouble { state, base_url }
}

struct Harness {
    app: TestApp,
    router: Router,
    upstream: UpstreamDouble,
}

async fn harness_with(pool: PgPool, client_per_minute: u32, upstream_per_second: u32) -> Harness {
    harness_with_runtime(pool, |base_url| {
        AddressSearchRuntime::new(
            base_url,
            Some(axum::http::HeaderName::from_static("x-real-ip")),
            client_per_minute,
            upstream_per_second,
        )
        .unwrap()
    })
    .await
}

async fn harness_with_runtime(
    pool: PgPool,
    runtime: impl FnOnce(&str) -> AddressSearchRuntime,
) -> Harness {
    let upstream = spawn_upstream().await;
    let runtime = runtime(&upstream.base_url);
    let app = test_app(pool).await;
    let state = app
        .state
        .clone()
        .with_address_search(Some(Arc::new(runtime)));
    let router = build_router(state).layer(MockConnectInfo(
        "127.0.0.1:41000".parse::<std::net::SocketAddr>().unwrap(),
    ));
    Harness {
        app,
        router,
        upstream,
    }
}

async fn harness(pool: PgPool) -> Harness {
    harness_with(pool, 30, 2).await
}

async fn suggest_raw(
    router: &Router,
    client: &str,
    body: Vec<u8>,
) -> (StatusCode, HeaderMap, Value) {
    let request = Request::builder()
        .method("POST")
        .uri("/v0/address/suggest")
        .header("content-type", "application/json")
        .header("x-real-ip", client)
        .body(Body::from(body))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, headers, value)
}

async fn suggest(
    router: &Router,
    client: &str,
    query: &str,
    country: Option<&str>,
) -> (StatusCode, HeaderMap, Value) {
    let mut body = json!({ "q": query });
    if let Some(country) = country {
        body["country"] = json!(country);
    }
    suggest_raw(router, client, serde_json::to_vec(&body).unwrap()).await
}

fn line1s(suggestions: &[marketplace_service::address_search::Suggestion]) -> Vec<String> {
    suggestions
        .iter()
        .map(|s| s.address.line1.clone())
        .collect()
}

#[test]
fn street_only_match_keeps_the_typed_house_number() {
    let suggestions = normalize(US_UNION, "42 Union Street New Bedford", Some("US")).unwrap();
    let first = &suggestions[0];
    assert_eq!(first.precision, "street");
    assert_eq!(first.address.line1, "42 Union Street");
    assert_eq!(first.address.city, "New Bedford");
    assert_eq!(first.address.region, "Massachusetts");
    assert_eq!(first.address.postal_code, "02740");
    assert_eq!(first.address.country_code, "US");
    assert_eq!(first.secondary, "New Bedford, Massachusetts 02740");
    assert_eq!(first.id, "w664957911");

    let hotel = &suggestions[1];
    assert_eq!(hotel.precision, "house");
    assert_eq!(hotel.address.line1, "222 Union Street");
    assert_eq!(
        hotel.secondary,
        "New Bedford Harbor Hotel · New Bedford, Massachusetts 02740"
    );
}

#[test]
fn a_typed_number_is_not_attached_to_a_fuzzy_neighbour_street() {
    let suggestions = normalize(US_UNION, "42 Union Street New Bedford", Some("US")).unwrap();
    let rahway = suggestions
        .iter()
        .find(|s| s.address.city == "Rahway")
        .expect("the fuzzy neighbour is still offered");
    assert_eq!(rahway.address.line1, "Bedford Street");
    assert_eq!(rahway.precision, "street");
    let brooklyn = suggestions
        .iter()
        .find(|s| s.address.city == "New York")
        .expect("the Union Street in New York is offered");
    assert_eq!(brooklyn.address.line1, "42 Union Street");
}

#[test]
fn ordinals_and_unmatched_numbers_are_not_house_numbers() {
    let street = |name: &str, query: &str| {
        let body = json!({"features": [{"properties": {
            "type": "street", "name": name, "city": "New York", "state": "New York",
            "postcode": "10001", "countrycode": "US", "osm_type": "W", "osm_id": 7
        }}]})
        .to_string();
        normalize(body.as_bytes(), query, Some("US")).unwrap()[0]
            .address
            .line1
            .clone()
    };
    assert_eq!(street("5th Avenue", "5th Avenue New York"), "5th Avenue");
    assert_eq!(street("42nd Street", "42nd Street New York"), "42nd Street");
    assert_eq!(street("42nd Street", "42 42nd Street"), "42 42nd Street");
    assert_eq!(street("Broadway", "1250 Broadway"), "1250 Broadway");
    assert_eq!(street("Broadway", "1250B Broadway"), "1250B Broadway");
    assert_eq!(street("Broadway", "12-14 Broadway"), "12-14 Broadway");
    assert_eq!(street("Main Street", "1234567 Main Street"), "Main Street");
    assert_eq!(street("Union Street", "42 Uni"), "42 Union Street");
    assert_eq!(street("Union Street", "42"), "Union Street");
}

#[test]
fn without_a_typed_number_a_street_match_fills_the_street_only() {
    let suggestions = normalize(US_UNION, "Union Street New Bedford", Some("US")).unwrap();
    assert_eq!(suggestions[0].address.line1, "Union Street");
    assert_eq!(suggestions[0].precision, "street");
}

#[test]
fn house_matches_collapse_duplicates_of_one_address() {
    let suggestions = normalize(US_HOUSE, "1600 Pennsylvania Ave Washington", Some("US")).unwrap();
    assert_eq!(
        line1s(&suggestions),
        vec![
            "1600 Pennsylvania Avenue Northwest",
            "1600 Pennsylvania Avenue Southeast",
            "1600 Pennsylvania Avenue Northwest",
        ]
    );
    let postcodes: Vec<_> = suggestions
        .iter()
        .map(|s| s.address.postal_code.as_str())
        .collect();
    assert_eq!(postcodes, vec!["20500", "20003", "20006"]);
    assert!(suggestions.iter().all(|s| s.precision == "house"));
}

#[test]
fn street_first_countries_write_street_then_number() {
    let suggestions = normalize(DE_HOUSE, "Unter den Linden 1 Berlin", None).unwrap();
    assert_eq!(
        line1s(&suggestions),
        vec!["Unter den Linden", "Unter den Linden 7"]
    );
    assert_eq!(suggestions[1].address.country_code, "DE");
    assert_eq!(suggestions[1].address.region, "");
    assert_eq!(suggestions[1].address.city, "Berlin");
}

#[test]
fn a_typed_leading_number_uses_the_country_order() {
    let suggestions = normalize(DE_HOUSE, "1 Unter den Linden Berlin", None).unwrap();
    assert_eq!(suggestions[0].address.line1, "Unter den Linden 1");
}

#[test]
fn business_results_fill_their_street_address() {
    let suggestions = normalize(POI, "Blue Bottle Coffee Oakland", None).unwrap();
    assert_eq!(suggestions.len(), 2);
    assert_eq!(suggestions[0].address.line1, "480 9th Street");
    assert_eq!(
        suggestions[0].secondary,
        "Blue Bottle Coffee · Oakland, California 94607"
    );
}

#[test]
fn a_country_filter_drops_other_countries_and_garbage_is_refused() {
    let suggestions = normalize(DE_HOUSE, "Unter den Linden", Some("US")).unwrap();
    assert!(suggestions.is_empty());
    assert!(normalize(b"not json", "abcd", None).is_err());
    assert!(
        normalize(b"{\"features\":[{\"properties\":{}}]}", "abcd", None)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn upstream_configuration_is_validated() {
    assert!(PhotonClient::new("https://photon.komoot.io").is_ok());
    assert!(PhotonClient::new("http://127.0.0.1:2322").is_ok());
    assert!(PhotonClient::new("http://photon.example.com").is_err());
    assert!(PhotonClient::new("https://photon.example.com/?key=1").is_err());
    assert!(PhotonClient::new("not a url").is_err());
    assert!(AddressSearchRuntime::new("https://photon.komoot.io", None, 0, 2).is_err());
    assert!(AddressSearchRuntime::new("https://photon.komoot.io", None, 30, 0).is_err());
}

#[test]
fn the_client_address_comes_only_from_the_configured_header() {
    let peer: std::net::IpAddr = "10.0.0.5".parse().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("x-real-ip", "203.0.113.9".parse().unwrap());
    headers.insert(
        "x-forwarded-for",
        "198.51.100.1, 203.0.113.7".parse().unwrap(),
    );

    let unset = AddressSearchRuntime::new("https://photon.komoot.io", None, 30, 2).unwrap();
    assert_eq!(
        unset.client_address(&headers, Some(peer)),
        "10.0.0.5",
        "without a configured header, forwarding headers are ignored"
    );

    let forwarded = AddressSearchRuntime::new(
        "https://photon.komoot.io",
        Some(axum::http::HeaderName::from_static("x-forwarded-for")),
        30,
        2,
    )
    .unwrap();
    assert_eq!(
        forwarded.client_address(&headers, Some(peer)),
        "203.0.113.7",
        "the last value is the one the trusted edge appended"
    );

    let mut malformed = HeaderMap::new();
    malformed.insert("x-forwarded-for", "203.0.113.7, not-an-ip".parse().unwrap());
    assert_eq!(forwarded.client_address(&malformed, Some(peer)), "10.0.0.5");
    assert_eq!(
        forwarded.client_address(&HeaderMap::new(), Some(peer)),
        "10.0.0.5",
        "a missing header falls back to the socket peer"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn suggestions_reach_the_buyer_with_attribution_and_no_identifying_upstream_headers(
    pool: PgPool,
) {
    let h = harness(pool).await;
    let (status, headers, body) = suggest(
        &h.router,
        "203.0.113.7",
        "42 Union Street New Bedford",
        Some("us"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers["cache-control"], "no-store");
    assert_eq!(body["attribution"]["text"], ATTRIBUTION_TEXT);
    assert_eq!(body["attribution"]["url"], ATTRIBUTION_URL);
    let first = &body["suggestions"][0];
    assert_eq!(first["primary"], "42 Union Street");
    assert_eq!(first["precision"], "street");
    assert_eq!(first["address"]["line1"], "42 Union Street");
    assert_eq!(first["address"]["postal_code"], "02740");
    assert_eq!(first["address"]["country_code"], "US");

    let query = h.upstream.last_query();
    assert!(query.contains("q=42+Union+Street+New+Bedford"), "{query}");
    assert!(query.contains("countrycode=US"), "{query}");
    assert!(
        query.contains("layer=house") && query.contains("layer=street"),
        "{query}"
    );
    assert!(!query.contains("lang="), "{query}");
    let upstream_headers = h.upstream.last_headers();
    for forbidden in [
        "cookie",
        "referer",
        "origin",
        "accept-language",
        "x-forwarded-for",
        "x-real-ip",
        "authorization",
    ] {
        assert!(
            upstream_headers.get(forbidden).is_none(),
            "{forbidden} reached the upstream"
        );
    }
    assert!(upstream_headers["user-agent"]
        .to_str()
        .unwrap()
        .starts_with("pubky-marketplace-service/address-search"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_buyer_response_matches_the_pinned_wire_shape(pool: PgPool) {
    let h = harness(pool).await;
    let (_, _, body) = suggest(
        &h.router,
        "203.0.113.7",
        "42 Union Street New Bedford",
        Some("US"),
    )
    .await;
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/address_suggest_response_us.json");
    if std::env::var("UPDATE_ADDRESS_SUGGEST_FIXTURE").is_ok() {
        let mut rendered = serde_json::to_vec_pretty(&body).unwrap();
        rendered.push(b'\n');
        std::fs::write(&path, rendered).unwrap();
    }
    let expected: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(body, expected);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn repeated_and_reformatted_queries_are_served_from_the_cache(pool: PgPool) {
    let h = harness(pool).await;
    for query in [
        "42 Union Street New Bedford",
        "42  union street   NEW BEDFORD",
        " 42 Union Street New Bedford ",
    ] {
        let (status, _, _) = suggest(&h.router, "203.0.113.7", query, Some("US")).await;
        assert_eq!(status, StatusCode::OK);
    }
    assert_eq!(h.upstream.fetches(), 1);

    let (status, _, _) = suggest(
        &h.router,
        "203.0.113.7",
        "42 Union Street New Bedford",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        h.upstream.fetches(),
        2,
        "the country is part of the cache key"
    );

    h.app.clock.advance_seconds(CACHE_TTL_SECONDS + 1);
    let (status, _, _) = suggest(
        &h.router,
        "203.0.113.7",
        "42 Union Street New Bedford",
        Some("US"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.upstream.fetches(), 3);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_identical_queries_share_one_upstream_call(pool: PgPool) {
    let h = harness(pool).await;
    h.upstream.set_delay(150);
    let mut tasks = Vec::new();
    for _ in 0..5 {
        let router = h.router.clone();
        tasks.push(tokio::spawn(async move {
            suggest(
                &router,
                "203.0.113.7",
                "42 Union Street New Bedford",
                Some("US"),
            )
            .await
        }));
    }
    for task in tasks {
        let (status, _, body) = task.await.unwrap();
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    assert_eq!(h.upstream.fetches(), 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn invalid_requests_are_refused_before_any_lookup(pool: PgPool) {
    let h = harness(pool).await;
    let too_long = "a".repeat(121);
    let cases: Vec<(Vec<u8>, &str)> = vec![
        (br#"{"q":"abc"}"#.to_vec(), "too short"),
        (br#"{"q":"   ab   "}"#.to_vec(), "short after trim"),
        (
            serde_json::to_vec(&json!({"q": too_long})).unwrap(),
            "too long",
        ),
        (
            br#"{"q":"42 Union","country":"USA"}"#.to_vec(),
            "country length",
        ),
        (
            br#"{"q":"42 Union","country":"4X"}"#.to_vec(),
            "country characters",
        ),
        (br#"{"q":"42 Union","lang":"en"}"#.to_vec(), "unknown field"),
        (br#"{"q":"42 Uni\u0000on"}"#.to_vec(), "control character"),
        (br#"{"q":42}"#.to_vec(), "wrong type"),
        (b"not json".to_vec(), "not json"),
        (
            serde_json::to_vec(&json!({"q": "a".repeat(2000)})).unwrap(),
            "oversized body",
        ),
    ];
    for (body, label) in cases {
        let (status, _, value) = suggest_raw(&h.router, "203.0.113.7", body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{label}: {value}");
        assert_eq!(value["error"]["code"], "invalid_request", "{label}");
    }
    assert_eq!(h.upstream.fetches(), 0);

    let request = Request::builder()
        .method("GET")
        .uri("/v0/address/suggest?q=42+Union+Street")
        .body(Body::empty())
        .unwrap();
    let response = h.router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn each_client_has_its_own_limit_that_refills(pool: PgPool) {
    let h = harness_with(pool, 30, 100).await;
    for index in 0..8 {
        let (status, _, body) = suggest(
            &h.router,
            "198.51.100.1",
            &format!("{index}0 Union Street"),
            Some("US"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "request {index}: {body}");
    }
    let (status, headers, body) =
        suggest(&h.router, "198.51.100.1", "99 Union Street", Some("US")).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert_eq!(body["error"]["code"], "rate_limited");
    assert!(
        headers["retry-after"]
            .to_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            >= 1
    );
    assert_eq!(h.upstream.fetches(), 8);

    let (status, _, _) = suggest(&h.router, "198.51.100.2", "99 Union Street", Some("US")).await;
    assert_eq!(status, StatusCode::OK, "another client is unaffected");

    h.app.clock.advance_seconds(4);
    let (status, _, _) = suggest(&h.router, "198.51.100.1", "98 Union Street", Some("US")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "tokens refill at the sustained rate"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn cache_hits_still_count_against_the_client_limit(pool: PgPool) {
    let h = harness(pool).await;
    for _ in 0..8 {
        let (status, _, _) =
            suggest(&h.router, "198.51.100.1", "42 Union Street", Some("US")).await;
        assert_eq!(status, StatusCode::OK);
    }
    let (status, _, _) = suggest(&h.router, "198.51.100.1", "42 Union Street", Some("US")).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(h.upstream.fetches(), 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_upstream_is_capped_across_all_clients(pool: PgPool) {
    let h = harness_with(pool, 30, 1).await;
    let mut statuses = Vec::new();
    for index in 0..4 {
        let (status, headers, _) = suggest(
            &h.router,
            &format!("198.51.100.{}", index + 10),
            &format!("{index}1 Union Street"),
            Some("US"),
        )
        .await;
        if status == StatusCode::SERVICE_UNAVAILABLE {
            assert!(headers.contains_key("retry-after"));
        }
        statuses.push(status);
    }
    assert_eq!(
        statuses,
        vec![
            StatusCode::OK,
            StatusCode::OK,
            StatusCode::OK,
            StatusCode::SERVICE_UNAVAILABLE
        ]
    );
    assert_eq!(h.upstream.fetches(), 3);
}

fn retry_after(headers: &HeaderMap) -> u64 {
    headers["retry-after"].to_str().unwrap().parse().unwrap()
}

/// Fails `count` lookups against a failing upstream, each from its own
/// client and query, and returns the last lookup's `Retry-After`.
async fn fail_lookups(h: &Harness, first: usize, count: usize) -> u64 {
    let mut last = 0;
    for index in first..first + count {
        let (status, headers, body) = suggest(
            &h.router,
            &format!("198.51.100.{}", index % 250 + 1),
            &format!("{index} Union Street"),
            Some("US"),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "lookup {index}: {body}"
        );
        assert_eq!(body["error"]["code"], "address_search_unavailable");
        last = retry_after(&headers);
    }
    last
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn one_failed_upstream_call_fails_only_that_lookup(pool: PgPool) {
    let h = harness(pool).await;
    h.upstream.set(500, b"{}");
    assert_eq!(fail_lookups(&h, 1, 1).await, FAILURE_RETRY_AFTER_SECONDS);

    h.upstream.set(200, US_UNION);
    let (status, _, body) =
        suggest(&h.router, "198.51.100.77", "42 Union Street", Some("US")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the next lookup reaches the upstream at once: {body}"
    );
    assert_eq!(h.upstream.fetches(), 2);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_answer_slower_than_three_seconds_still_reaches_the_buyer(pool: PgPool) {
    let h = harness(pool).await;
    h.upstream.set_delay(3_300);
    let (status, _, body) = suggest(&h.router, "203.0.113.7", "42 Union Street", Some("US")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(!body["suggestions"].as_array().unwrap().is_empty());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn timeouts_open_the_breaker_only_when_they_repeat(pool: PgPool) {
    let h = harness_with_runtime(pool, |base_url| {
        AddressSearchRuntime::new(
            base_url,
            Some(axum::http::HeaderName::from_static("x-real-ip")),
            30,
            100,
        )
        .unwrap()
        .with_upstream_timeout(Duration::from_millis(300))
        .unwrap()
    })
    .await;
    h.upstream.set_delay(600);
    assert_eq!(fail_lookups(&h, 1, 2).await, FAILURE_RETRY_AFTER_SECONDS);
    assert_eq!(fail_lookups(&h, 3, 1).await, BREAKER_BASE_SECONDS as u64);
    assert_eq!(h.upstream.fetches(), 3);

    fail_lookups(&h, 4, 1).await;
    assert_eq!(
        h.upstream.fetches(),
        3,
        "the open breaker sends nothing upstream"
    );

    h.upstream.set_delay(0);
    h.app.clock.advance_seconds(BREAKER_BASE_SECONDS + 1);
    let (status, _, body) =
        suggest(&h.router, "198.51.100.90", "42 Union Street", Some("US")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(h.upstream.fetches(), 4);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_upstream_429_opens_a_short_breaker_and_cached_answers_survive(pool: PgPool) {
    let h = harness(pool).await;
    let (status, _, _) = suggest(&h.router, "203.0.113.7", "42 Union Street", Some("US")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.upstream.fetches(), 1);

    h.upstream.set(429, b"{}");
    let (status, headers, body) =
        suggest(&h.router, "203.0.113.7", "43 Union Street", Some("US")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["error"]["code"], "address_search_unavailable");
    assert_eq!(retry_after(&headers), BREAKER_BASE_SECONDS as u64);
    assert_eq!(h.upstream.fetches(), 2);

    let (status, _, _) = suggest(&h.router, "203.0.113.7", "44 Union Street", Some("US")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        h.upstream.fetches(),
        2,
        "the open breaker sends nothing upstream"
    );

    let (status, _, body) = suggest(&h.router, "203.0.113.7", "42 Union Street", Some("US")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "cached answers are still served: {body}"
    );

    h.app.clock.advance_seconds(BREAKER_BASE_SECONDS + 1);
    h.upstream.set(200, US_UNION);
    let (status, _, _) = suggest(&h.router, "203.0.113.7", "45 Union Street", Some("US")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.upstream.fetches(), 3);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_upstream_429_retry_after_is_honoured_up_to_the_cap(pool: PgPool) {
    let h = harness(pool).await;
    h.upstream.set(429, b"{}");
    h.upstream.set_retry_after(Some("120"));
    assert_eq!(fail_lookups(&h, 1, 1).await, 120);

    h.upstream.set(200, US_UNION);
    h.upstream.set_retry_after(None);
    h.app.clock.advance_seconds(61);
    fail_lookups(&h, 2, 1).await;
    assert_eq!(
        h.upstream.fetches(),
        1,
        "the upstream asked for 120 seconds"
    );

    h.app.clock.advance_seconds(60);
    let (status, _, body) =
        suggest(&h.router, "198.51.100.90", "42 Union Street", Some("US")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(h.upstream.fetches(), 2);

    h.upstream.set(429, b"{}");
    h.upstream.set_retry_after(Some("86400"));
    assert_eq!(fail_lookups(&h, 3, 1).await, THROTTLE_MAX_SECONDS as u64);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_half_open_breaker_probes_once_and_backs_off_until_the_upstream_answers(pool: PgPool) {
    let h = harness_with(pool, 30, 100).await;
    h.upstream.set(500, b"{}");
    assert_eq!(fail_lookups(&h, 1, 3).await, BREAKER_BASE_SECONDS as u64);
    assert_eq!(h.upstream.fetches(), 3);

    let mut cooldown = BREAKER_BASE_SECONDS;
    let mut next = 4;
    let mut fetches = 3;
    while cooldown < BREAKER_MAX_SECONDS {
        h.app.clock.advance_seconds(cooldown - 1);
        fail_lookups(&h, next, 1).await;
        assert_eq!(
            h.upstream.fetches(),
            fetches,
            "still open after {cooldown}s - 1"
        );
        h.app.clock.advance_seconds(2);
        let reopened = fail_lookups(&h, next + 1, 1).await;
        fetches += 1;
        assert_eq!(h.upstream.fetches(), fetches, "one probe after {cooldown}s");
        let doubled = (cooldown * 2).min(BREAKER_MAX_SECONDS);
        assert_eq!(
            reopened, doubled as u64,
            "a failed probe doubles the cooldown"
        );
        cooldown = doubled;
        next += 2;
    }

    h.app.clock.advance_seconds(cooldown + 1);
    h.upstream.set(200, US_UNION);
    let (status, _, body) =
        suggest(&h.router, "198.51.100.200", "42 Union Street", Some("US")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a successful probe closes the breaker: {body}"
    );

    h.upstream.set(500, b"{}");
    assert_eq!(
        fail_lookups(&h, 300, 1).await,
        FAILURE_RETRY_AFTER_SECONDS,
        "a success resets the failure count"
    );
    assert_eq!(
        fail_lookups(&h, 301, 2).await,
        BREAKER_BASE_SECONDS as u64,
        "and the cooldown"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn while_one_lookup_probes_the_others_wait_without_reaching_the_upstream(pool: PgPool) {
    let h = harness_with(pool, 30, 100).await;
    h.upstream.set(500, b"{}");
    fail_lookups(&h, 1, 3).await;
    h.app.clock.advance_seconds(BREAKER_BASE_SECONDS + 1);
    h.upstream.set(200, US_UNION);
    h.upstream.set_delay(400);

    let router = h.router.clone();
    let probe = tokio::spawn(async move {
        suggest(&router, "198.51.100.50", "50 Union Street", Some("US")).await
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    let (status, headers, _) =
        suggest(&h.router, "198.51.100.51", "51 Union Street", Some("US")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(retry_after(&headers), PROBE_RETRY_AFTER_SECONDS);

    let (status, _, body) = probe.await.unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(h.upstream.fetches(), 4);

    let (status, _, _) = suggest(&h.router, "198.51.100.51", "51 Union Street", Some("US")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.upstream.fetches(), 5);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_broken_upstream_answer_is_unavailable_not_empty(pool: PgPool) {
    let h = harness(pool).await;
    h.upstream.set(200, b"<html>not photon</html>");
    let (status, _, body) = suggest(&h.router, "203.0.113.7", "42 Union Street", Some("US")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");

    h.app.clock.advance_seconds(61);
    h.upstream.set(500, b"{}");
    let (status, _, _) = suggest(&h.router, "203.0.113.7", "43 Union Street", Some("US")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_query_the_upstream_rejects_answers_empty_without_tripping_the_breaker(pool: PgPool) {
    let h = harness(pool).await;
    h.upstream.set(400, b"{\"message\":\"bad\"}");
    let (status, _, body) = suggest(&h.router, "203.0.113.7", "42 Union Street", Some("US")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["suggestions"], json!([]));

    h.upstream.set(200, US_UNION);
    let (status, _, body) = suggest(&h.router, "203.0.113.7", "43 Union Street", Some("US")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!body["suggestions"].as_array().unwrap().is_empty());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_route_is_unavailable_when_the_kill_switch_removed_the_runtime(pool: PgPool) {
    let app = test_app(pool).await;
    let router = build_router(app.state.clone().with_address_search(None));
    let (status, _, body) = suggest(&router, "203.0.113.7", "42 Union Street", Some("US")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"]["code"], "address_search_unavailable");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn query_text_and_client_addresses_never_reach_the_logs(pool: PgPool) {
    struct Buffer(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let h = harness(pool).await;
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_ansi(false)
        .with_max_level(tracing::Level::TRACE)
        .with_writer({
            let buffer = buffer.clone();
            move || Buffer(buffer.clone())
        })
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    let marker = "ZQXWV Marker Boulevard";
    let (status, _, _) = suggest(&h.router, "203.0.113.99", marker, Some("US")).await;
    assert_eq!(status, StatusCode::OK);
    h.upstream.set(500, b"{}");
    let (status, _, _) =
        suggest(&h.router, "203.0.113.99", "ZQXWV Second Marker", Some("US")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    drop(guard);
    let logs = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
    assert!(
        logs.contains("/v0/address/suggest"),
        "the request is logged by route: {logs}"
    );
    for secret in ["ZQXWV", "Marker", "203.0.113.99", "Boulevard"] {
        assert!(!logs.contains(secret), "{secret} reached the logs:\n{logs}");
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_success_that_started_before_a_429_does_not_close_the_breaker(pool: PgPool) {
    let h = harness_with(pool, 30, 100).await;
    h.upstream.set(429, b"{}");
    h.upstream.set_retry_after(Some("120"));
    h.upstream.set_for("Slowok", 200, US_UNION, None, 400);

    let router = h.router.clone();
    let slow = tokio::spawn(async move {
        suggest(&router, "198.51.100.40", "10 Slowok Street", Some("US")).await
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        fail_lookups(&h, 1, 1).await,
        120,
        "the 429 opened the breaker"
    );

    let (status, _, body) = slow.await.unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "its own buyer still gets the answer: {body}"
    );

    let (status, headers, _) =
        suggest(&h.router, "198.51.100.41", "11 Union Street", Some("US")).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "the late success did not close it"
    );
    assert!(retry_after(&headers) >= 119);
    h.app.clock.advance_seconds(61);
    fail_lookups(&h, 2, 1).await;
    assert_eq!(
        h.upstream.fetches(),
        2,
        "nothing reached the upstream inside its Retry-After"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_late_429_lengthens_a_shorter_cooldown_already_in_force(pool: PgPool) {
    let h = harness_with(pool, 30, 100).await;
    h.upstream.set(500, b"{}");
    h.upstream
        .set_for("Slowthrottle", 429, b"{}", Some("120"), 400);

    let router = h.router.clone();
    let slow = tokio::spawn(async move {
        suggest(
            &router,
            "198.51.100.40",
            "10 Slowthrottle Street",
            Some("US"),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        fail_lookups(&h, 1, 3).await,
        BREAKER_BASE_SECONDS as u64,
        "three failures opened a short breaker"
    );

    let (status, headers, _) = slow.await.unwrap();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(retry_after(&headers), 120, "the 429 extended it");
    assert_eq!(h.upstream.fetches(), 4);

    h.app.clock.advance_seconds(BREAKER_BASE_SECONDS + 1);
    let remaining = fail_lookups(&h, 4, 1).await;
    assert!(
        remaining >= 120 - (BREAKER_BASE_SECONDS as u64) - 2,
        "{remaining}"
    );
    assert_eq!(
        h.upstream.fetches(),
        4,
        "no probe before the 429's Retry-After"
    );

    h.app.clock.advance_seconds(120);
    h.upstream.set(200, US_UNION);
    let (status, _, body) =
        suggest(&h.router, "198.51.100.90", "42 Union Street", Some("US")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_late_429_with_a_shorter_retry_after_does_not_shorten_the_cooldown(pool: PgPool) {
    let h = harness_with(pool, 30, 100).await;
    h.upstream.set(429, b"{}");
    h.upstream.set_retry_after(Some("120"));
    h.upstream
        .set_for("Slowthrottle", 429, b"{}", Some("5"), 400);

    let router = h.router.clone();
    let slow = tokio::spawn(async move {
        suggest(
            &router,
            "198.51.100.40",
            "10 Slowthrottle Street",
            Some("US"),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(fail_lookups(&h, 1, 1).await, 120);

    let (status, headers, _) = slow.await.unwrap();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(retry_after(&headers) >= 119, "{:?}", headers["retry-after"]);

    h.app.clock.advance_seconds(61);
    fail_lookups(&h, 2, 1).await;
    assert_eq!(h.upstream.fetches(), 2);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_probe_success_does_not_close_a_breaker_a_late_429_reopened(pool: PgPool) {
    let h = harness_with(pool, 30, 100).await;
    h.upstream.set(500, b"{}");
    h.upstream
        .set_for("Slowthrottle", 429, b"{}", Some("120"), 700);
    h.upstream.set_for("Slowok", 200, US_UNION, None, 1_200);

    let router = h.router.clone();
    let throttled = tokio::spawn(async move {
        suggest(
            &router,
            "198.51.100.40",
            "10 Slowthrottle Street",
            Some("US"),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(fail_lookups(&h, 1, 3).await, BREAKER_BASE_SECONDS as u64);
    h.app.clock.advance_seconds(BREAKER_BASE_SECONDS + 1);

    let router = h.router.clone();
    let probe = tokio::spawn(async move {
        suggest(&router, "198.51.100.41", "20 Slowok Street", Some("US")).await
    });
    let (status, headers, _) = throttled.await.unwrap();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(retry_after(&headers), 120);
    let (status, _, body) = probe.await.unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(h.upstream.fetches(), 5);

    let (status, headers, _) =
        suggest(&h.router, "198.51.100.42", "21 Union Street", Some("US")).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "the probe was admitted before the 429 reopened the breaker"
    );
    assert!(retry_after(&headers) >= 119);
    assert_eq!(h.upstream.fetches(), 5);
}
