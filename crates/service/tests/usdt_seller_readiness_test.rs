//! S2: seller USDT readiness. With `USDT_PAYMENTS_ENABLED` on, a seller's own
//! payment config carries `usdt_enabled`, `usdt_setup` and
//! `usdt_setup_action`, and the public config carries `usdt_available`. The
//! readiness comes only from paykit-server's signed `POST /setup/status`, and
//! every answer these tests replay is an exchange captured from a running
//! paykit-server that has `accepted_asset` on `/setup/status`
//! (`tests/fixtures/paykit-server-u4/`). USDT is asked as
//! `{"accepted_asset":"USDT",...}`, never as the `asset` denomination. With
//! the flag off none of it exists.

mod common;

use axum::http::StatusCode;
use common::paykit_server_u4 as u4;
use common::*;
use marketplace_service::config::Config;
use marketplace_service::payment_attempt::MarketplaceAssets;
use marketplace_service::payments::{
    PaykitApi, PaykitClient, PaykitRequestError, SetupAsset, SetupStatus,
};
use serde_json::{json, Value};
use sqlx::PgPool;

const OWN: &str = "/v0/sellers/me/payment-config";
const EPOCH: &str = "1970-01-01T00:00:00.000Z";

/// The flag on and a deployment whose Marketplace prepare lists USDT.
fn usdt_config() -> Config {
    let mut config = usdt_config_btc_only();
    config.paykit_marketplace_assets =
        MarketplaceAssets::parse(Some("BTC,USDT"), PaykitApi::Upstream).expect("assets parse");
    config
}

/// The flag on, but `PAYKIT_MARKETPLACE_ASSETS` left at its default (`BTC`).
fn usdt_config_btc_only() -> Config {
    let mut config = Config::for_tests();
    config.usdt_payments_enabled = true;
    config
}

async fn upstream_app(pool: PgPool) -> (TestApp, FakePaykit) {
    test_app_with_paykit_api_config(pool, usdt_config(), PaykitApi::Upstream).await
}

async fn put(app: &TestApp, token: &str, body: &Value) -> (StatusCode, Value) {
    send(app.router.clone(), "PUT", OWN, Some(token), body).await
}

/// A `PUT` whose answer is read as raw bytes: the framework's refusal of an
/// unreadable body is not JSON.
async fn put_raw(app: &TestApp, token: &str, body: &Value) -> (StatusCode, String) {
    use http_body_util::BodyExt;
    use tower::util::ServiceExt;
    let request = axum::http::Request::builder()
        .method("PUT")
        .uri(OWN)
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"))
        .body(axum::body::Body::from(
            serde_json::to_vec(body).expect("serializes"),
        ))
        .expect("request builds");
    let response = app
        .router
        .clone()
        .oneshot(request)
        .await
        .expect("request executes");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    (status, String::from_utf8(bytes.to_vec()).expect("utf8"))
}

async fn get_own(app: &TestApp, token: &str) -> (StatusCode, Value) {
    send(app.router.clone(), "GET", OWN, Some(token), &Value::Null).await
}

async fn get_public(app: &TestApp, seller: &str) -> Value {
    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("/v0/sellers/{seller}/payment-config"),
        None,
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

/// The seller's USDT answers: what paykit-server says for `asset: "USDT"`
/// and for no asset.
fn replay(paykit: &FakePaykit, seller: &str, usdt: &str, authority: &str) {
    paykit.replay_setup_status(seller, Some("accepted_asset:USDT"), &u4::load(usdt));
    paykit.replay_setup_status(seller, None, &u4::load(authority));
}

fn usdt_fields(body: &Value) -> (Value, Value, Value) {
    let config = &body["payment_config"];
    (
        config["usdt_enabled"].clone(),
        config["usdt_setup"].clone(),
        config["usdt_setup_action"].clone(),
    )
}

fn setup_status_calls(paykit: &FakePaykit) -> Vec<Value> {
    paykit
        .calls()
        .into_iter()
        .filter(|call| call.path == "/setup/status")
        .map(|call| call.body)
        .collect()
}

fn usdt_calls(paykit: &FakePaykit) -> usize {
    setup_status_calls(paykit)
        .iter()
        .filter(|body| body["accepted_asset"] == json!("USDT"))
        .count()
}

// ---------------------------------------------------------------------------
// The captured exchanges
// ---------------------------------------------------------------------------

#[test]
fn every_capture_is_from_a_pinned_server_revision_and_a_closed_signed_body() {
    for fixture in u4::load_all() {
        let expected = if fixture.name == "setup_status_accepted_asset_pre_u4" {
            u4::PRE_U4_REVISION
        } else {
            u4::U4_REVISION
        };
        assert_eq!(fixture.server_revision, expected, "{}", fixture.name);
        assert_eq!(fixture.method, "POST");
        assert_eq!(fixture.path, "/setup/status");
        assert!(!fixture.signature.is_empty(), "{}", fixture.name);
        let body = fixture.request();
        assert_eq!(
            serde_json_canonicalizer::to_string(&body).expect("canonicalizes"),
            fixture.request_body,
            "{} was sent in canonical form",
            fixture.name
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_client_reads_every_captured_answer_as_the_service_contract_says(pool: PgPool) {
    let (_app, paykit) = upstream_app(pool).await;
    let client = PaykitClient::new(&paykit.base_url, TEST_PAYKIT_SIGNING_SEED)
        .expect("client builds")
        .with_api(PaykitApi::Upstream);
    let seller = "capture-seller";
    for fixture in u4::load_all() {
        // The fake answers with the captured status and body for this seller
        // and the captured request's readiness question. A capture whose
        // request the client never sends (a refused asset value, an extra
        // field) is skipped: the client cannot produce that body.
        let question = fixture.question();
        let asset = match question.as_deref() {
            Some("asset:BTC") => Some(SetupAsset::Btc),
            Some("accepted_asset:USDT") => Some(SetupAsset::Usdt),
            _ => None,
        };
        let keys = fixture.request().as_object().expect("object").len();
        if (question.is_some() && asset.is_none()) || keys > 1 + usize::from(asset.is_some()) {
            continue;
        }
        paykit.replay_setup_status(seller, question.as_deref(), &fixture);
        let read = client.upstream_setup_status(seller, asset).await;
        if fixture.status == 200 {
            let expected = match fixture.status_value().as_str() {
                "ready" => SetupStatus::Ready,
                "setup_required" => SetupStatus::SetupRequired,
                "unavailable" => SetupStatus::Unavailable,
                other => panic!("{}: unexpected status {other}", fixture.name),
            };
            assert_eq!(read, Ok(expected), "{}", fixture.name);
        } else {
            assert_eq!(
                read,
                Err(PaykitRequestError::Unavailable),
                "{}",
                fixture.name
            );
        }
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_usdt_question_is_accepted_asset_never_asset_in_the_closed_canonical_bodies_the_captures_show(
    pool: PgPool,
) {
    let (app, paykit) = upstream_app(pool).await;
    let seller = new_actor(&app).await;
    replay(
        &paykit,
        &seller.pubky,
        "setup_status_usdt_without_usdt_config",
        "setup_status_authority_without_usdt_config",
    );
    let (status, _) = get_own(&app, &seller.token).await;
    assert_eq!(status, StatusCode::OK);

    let creator = format!("pubky{}", seller.pubky);
    assert_eq!(
        setup_status_calls(&paykit),
        vec![
            json!({ "accepted_asset": "USDT", "creator": creator }),
            json!({ "creator": creator }),
        ]
    );
    // The same field sets the captured requests carry.
    let captured_keys = |name: &str| -> Vec<String> {
        u4::load(name)
            .request()
            .as_object()
            .expect("object")
            .keys()
            .cloned()
            .collect()
    };
    assert_eq!(
        captured_keys("setup_status_usdt_dual"),
        ["accepted_asset", "creator"]
    );
    assert_eq!(captured_keys("setup_status_btc_dual"), ["asset", "creator"]);
    assert_eq!(captured_keys("setup_status_authority_ready"), ["creator"]);
}

// ---------------------------------------------------------------------------
// Own config: readiness and the action
// ---------------------------------------------------------------------------

async fn own_usdt(pool: PgPool, usdt: &str, authority: &str) -> (Value, FakePaykit) {
    let (app, paykit) = upstream_app(pool).await;
    let seller = new_actor(&app).await;
    replay(&paykit, &seller.pubky, usdt, authority);
    let (status, body) = get_own(&app, &seller.token).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    (body, paykit)
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_ready_seller_is_ready_with_no_action(pool: PgPool) {
    let (body, paykit) = own_usdt(
        pool,
        "setup_status_usdt_dual",
        "setup_status_authority_ready",
    )
    .await;
    assert_eq!(
        usdt_fields(&body),
        (json!(false), json!("ready"), Value::Null)
    );
    assert_eq!(usdt_calls(&paykit), 1);
    assert_eq!(
        setup_status_calls(&paykit).len(),
        1,
        "the authority is looked up only when USDT is not ready"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_new_seller_is_told_to_set_up(pool: PgPool) {
    let (body, _paykit) = own_usdt(
        pool,
        "setup_status_usdt_never_set_up",
        "setup_status_authority_never_set_up",
    )
    .await;
    assert_eq!(
        usdt_fields(&body),
        (json!(false), json!("setup_required"), json!("setup"))
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_seller_with_an_account_but_no_usdt_is_told_to_reconnect(pool: PgPool) {
    // Captured: a seller who approved Bitcoin and USDT, asked of a deployment
    // without `[usdt]`: USDT is setup_required while the authority is ready.
    let (body, paykit) = own_usdt(
        pool,
        "setup_status_usdt_without_usdt_config",
        "setup_status_authority_without_usdt_config",
    )
    .await;
    assert_eq!(
        usdt_fields(&body),
        (json!(false), json!("setup_required"), json!("reconnect"))
    );
    assert_eq!(setup_status_calls(&paykit).len(), 2);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bitcoin_wallet_seller_on_a_deployment_without_usdt_is_told_to_reconnect(pool: PgPool) {
    let (body, _paykit) = own_usdt(
        pool,
        "setup_status_usdt_without_usdt_config_bitcoin_wallet",
        "setup_status_authority_without_usdt_config",
    )
    .await;
    assert_eq!(
        usdt_fields(&body),
        (json!(false), json!("setup_required"), json!("reconnect"))
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unavailable_answer_never_yields_an_action(pool: PgPool) {
    // USDT unavailable: no authority lookup, no action.
    let (body, paykit) = own_usdt(
        pool.clone(),
        "setup_status_usdt_homeserver_down",
        "setup_status_authority_never_set_up",
    )
    .await;
    assert_eq!(
        usdt_fields(&body),
        (json!(false), json!("unavailable"), Value::Null)
    );
    assert_eq!(setup_status_calls(&paykit).len(), 1);

    // USDT setup_required but the authority cannot be read: the action is
    // unknown, so none is offered.
    let (body, paykit) = own_usdt(
        pool,
        "setup_status_usdt_without_usdt_config",
        "setup_status_authority_homeserver_down",
    )
    .await;
    assert_eq!(
        usdt_fields(&body),
        (json!(false), json!("unavailable"), Value::Null)
    );
    assert_eq!(setup_status_calls(&paykit).len(), 2);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unreadable_or_off_contract_answer_is_unavailable_never_ready(pool: PgPool) {
    let (app, paykit) = upstream_app(pool).await;
    let seller = new_actor(&app).await;
    for answer in [
        json!({ "status": "ready", "extra": true }),
        json!({ "status": "maybe" }),
        json!({}),
        json!("ready"),
    ] {
        paykit.set_setup_status_body(answer.clone());
        let (_, body) = get_own(&app, &seller.token).await;
        assert_eq!(
            usdt_fields(&body),
            (json!(false), json!("unavailable"), Value::Null),
            "{answer}"
        );
    }
    paykit.fail_setup_status_with(503);
    let (_, body) = get_own(&app, &seller.token).await;
    assert_eq!(
        usdt_fields(&body),
        (json!(false), json!("unavailable"), Value::Null)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_seller_who_never_saved_gets_the_usdt_fields_without_a_stored_row(pool: PgPool) {
    let (app, paykit) = upstream_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    replay(
        &paykit,
        &seller.pubky,
        "setup_status_usdt_never_set_up",
        "setup_status_authority_never_set_up",
    );
    let (status, body) = get_own(&app, &seller.token).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["payment_config"],
        json!({
            "bitcoin_enabled": false,
            "stripe_payment_link": null,
            "paypal_merchant_email": null,
            "stripe_restricted_key_set": false,
            "updated_at": EPOCH,
            "usdt_enabled": false,
            "usdt_setup": "setup_required",
            "usdt_setup_action": "setup",
        })
    );
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM seller_payment_configs")
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(rows, 0, "reading stores nothing");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn own_reads_are_never_served_from_the_availability_cache(pool: PgPool) {
    let (app, paykit) = upstream_app(pool).await;
    let seller = new_actor(&app).await;
    replay(
        &paykit,
        &seller.pubky,
        "setup_status_usdt_without_usdt_config",
        "setup_status_authority_without_usdt_config",
    );
    let (_, first) = get_own(&app, &seller.token).await;
    assert_eq!(usdt_fields(&first).1, json!("setup_required"));

    // The seller finishes Bitkit's reconnect: the very next own read, with no
    // clock advance, sees it.
    paykit.replay_setup_status(
        &seller.pubky,
        Some("accepted_asset:USDT"),
        &u4::load("setup_status_usdt_after_reconnect"),
    );
    let (_, second) = get_own(&app, &seller.token).await;
    assert_eq!(usdt_fields(&second).1, json!("ready"));
    assert_eq!(usdt_calls(&paykit), 2);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_seller_who_declined_usdt_is_told_to_reconnect_and_is_ready_after_it(pool: PgPool) {
    // Captured on a `[usdt]` deployment: a Bitcoin-only seller who declined
    // the USDT address is `setup_required` for `accepted_asset: "USDT"` (and
    // before a reconnect), with the authority ready, so the action is
    // `reconnect`. After a reconnect that adds the address the same question
    // is `ready`.
    for fixture in [
        "setup_status_usdt_bitcoin_only",
        "setup_status_usdt_before_reconnect",
    ] {
        let (body, paykit) = own_usdt(
            pool.clone(),
            fixture,
            "setup_status_authority_ready_bitcoin_only",
        )
        .await;
        assert_eq!(
            usdt_fields(&body),
            (json!(false), json!("setup_required"), json!("reconnect")),
            "{fixture}"
        );
        assert_eq!(setup_status_calls(&paykit).len(), 2, "{fixture}");
    }
    let (body, _paykit) = own_usdt(
        pool,
        "setup_status_usdt_after_reconnect",
        "setup_status_authority_ready",
    )
    .await;
    assert_eq!(
        usdt_fields(&body),
        (json!(false), json!("ready"), Value::Null)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_paykit_server_without_accepted_asset_fails_closed_to_unavailable(pool: PgPool) {
    // Captured: a server that predates the field refuses the USDT body with
    // `400 invalid_request`. That is a non-2xx answer: USDT is unavailable
    // with no action, no authority lookup follows, and the seller is never
    // offered USDT.
    let (app, paykit) = upstream_app(pool).await;
    let seller = new_actor(&app).await;
    paykit.replay_setup_status(
        &seller.pubky,
        Some("accepted_asset:USDT"),
        &u4::load("setup_status_accepted_asset_pre_u4"),
    );
    paykit.replay_setup_status(
        &seller.pubky,
        None,
        &u4::load("setup_status_authority_ready"),
    );
    put(
        &app,
        &seller.token,
        &with(rails(false), "usdt_enabled", json!(true)),
    )
    .await;
    let (_, own) = get_own(&app, &seller.token).await;
    assert_eq!(
        usdt_fields(&own),
        (json!(true), json!("unavailable"), Value::Null)
    );
    assert_eq!(
        get_public(&app, &seller.pubky).await["usdt_available"],
        json!(false)
    );
    assert!(
        setup_status_calls(&paykit)
            .iter()
            .all(|body| body.get("accepted_asset").is_some()),
        "the authority is never asked after an unanswered USDT question"
    );
}

// ---------------------------------------------------------------------------
// Own config: the stored consent
// ---------------------------------------------------------------------------

fn rails(bitcoin_enabled: bool) -> Value {
    json!({
        "bitcoin_enabled": bitcoin_enabled,
        "paypal_merchant_email": "merchant@example.com",
    })
}

fn with(mut body: Value, key: &str, value: Value) -> Value {
    body[key] = value;
    body
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_omitted_usdt_enabled_leaves_the_stored_consent_unchanged(pool: PgPool) {
    let (app, paykit) = upstream_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    replay(
        &paykit,
        &seller.pubky,
        "setup_status_usdt_dual",
        "setup_status_authority_ready",
    );

    // First save, with consent.
    let (status, body) = put(
        &app,
        &seller.token,
        &with(rails(true), "usdt_enabled", json!(true)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(usdt_fields(&body).0, json!(true));

    // A PayPal or Bitcoin save that does not mention USDT (an older Shop, or
    // one whose gate is off) cannot reset it.
    for saved in [
        rails(true),
        rails(false),
        json!({ "bitcoin_enabled": false }),
    ] {
        let (status, body) = put(&app, &seller.token, &saved).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(usdt_fields(&body).0, json!(true), "{saved}");
        let (_, read) = get_own(&app, &seller.token).await;
        assert_eq!(usdt_fields(&read).0, json!(true), "{saved}");
    }
    // An explicit null is "unchanged" too.
    let (status, body) = put(
        &app,
        &seller.token,
        &with(rails(true), "usdt_enabled", Value::Null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(usdt_fields(&body).0, json!(true));

    // Only an explicit value changes it, and it can be switched back on.
    let (_, body) = put(
        &app,
        &seller.token,
        &with(rails(true), "usdt_enabled", json!(false)),
    )
    .await;
    assert_eq!(usdt_fields(&body).0, json!(false));
    let (_, body) = put(&app, &seller.token, &rails(true)).await;
    assert_eq!(usdt_fields(&body).0, json!(false));
    let (_, body) = put(
        &app,
        &seller.token,
        &with(rails(true), "usdt_enabled", json!(true)),
    )
    .await;
    assert_eq!(usdt_fields(&body).0, json!(true));

    let rows: Vec<(String, bool)> = sqlx::query_as(
        "SELECT option_id, enabled FROM seller_accepted_payment_options WHERE seller_pubky = $1",
    )
    .bind(&seller.pubky)
    .fetch_all(&pool)
    .await
    .expect("option rows");
    assert_eq!(rows, vec![("paykit.usdt.arbitrum-one".to_string(), true)]);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_first_save_that_omits_usdt_stores_no_consent(pool: PgPool) {
    let (app, _paykit) = upstream_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    let (status, body) = put(&app, &seller.token, &rails(true)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(usdt_fields(&body).0, json!(false));
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM seller_accepted_payment_options")
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(rows, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_rails_and_the_consent_are_stored_together(pool: PgPool) {
    let (app, _paykit) = upstream_app(pool.clone()).await;
    let seller = new_actor(&app).await;
    // A non-boolean consent is refused and stores nothing, rails included.
    let (status, _) = put_raw(
        &app,
        &seller.token,
        &with(rails(true), "usdt_enabled", json!("yes")),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let configs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM seller_payment_configs")
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(configs, 0);
    // An unknown field is still refused with the flag on.
    let (status, _) = put_raw(
        &app,
        &seller.token,
        &with(rails(true), "bogus", json!(true)),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn consent_is_stored_per_seller_and_needs_no_readiness(pool: PgPool) {
    let (app, paykit) = upstream_app(pool).await;
    let ready = new_actor(&app).await;
    let not_ready = new_actor(&app).await;
    replay(
        &paykit,
        &ready.pubky,
        "setup_status_usdt_dual",
        "setup_status_authority_ready",
    );
    replay(
        &paykit,
        &not_ready.pubky,
        "setup_status_usdt_without_usdt_config",
        "setup_status_authority_without_usdt_config",
    );
    for actor in [&ready, &not_ready] {
        let (status, _) = put(
            &app,
            &actor.token,
            &with(rails(false), "usdt_enabled", json!(true)),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
    let (_, own) = get_own(&app, &not_ready.token).await;
    assert_eq!(
        usdt_fields(&own),
        (json!(true), json!("setup_required"), json!("reconnect"))
    );
    assert_eq!(
        get_public(&app, &ready.pubky).await["usdt_available"],
        json!(true)
    );
    assert_eq!(
        get_public(&app, &not_ready.pubky).await["usdt_available"],
        json!(false),
        "consent alone never offers USDT"
    );
}

// ---------------------------------------------------------------------------
// Public config
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn usdt_is_available_only_when_consented_and_ready(pool: PgPool) {
    let (app, paykit) = upstream_app(pool).await;
    let seller = new_actor(&app).await;
    replay(
        &paykit,
        &seller.pubky,
        "setup_status_usdt_dual",
        "setup_status_authority_ready",
    );
    paykit.set_claimed(&seller.pubky);

    // Never saved: false, and not even a lookup.
    let public = get_public(&app, &seller.pubky).await;
    assert_eq!(public["usdt_available"], json!(false));
    assert_eq!(usdt_calls(&paykit), 0);

    // Saved without consent: false, and the public read looks nothing up (the
    // save's own answer asked once for the seller's own view).
    put(&app, &seller.token, &rails(true)).await;
    let after_save = usdt_calls(&paykit);
    assert_eq!(
        get_public(&app, &seller.pubky).await["usdt_available"],
        json!(false)
    );
    assert_eq!(usdt_calls(&paykit), after_save);

    // Consent and ready: true.
    put(
        &app,
        &seller.token,
        &with(rails(true), "usdt_enabled", json!(true)),
    )
    .await;
    let public = get_public(&app, &seller.pubky).await;
    assert_eq!(public["usdt_available"], json!(true));
    assert_eq!(public["bitcoin_available"], json!(true));

    // Withdrawn consent: false at once for the next read after the TTL.
    put(
        &app,
        &seller.token,
        &with(rails(true), "usdt_enabled", json!(false)),
    )
    .await;
    assert_eq!(
        get_public(&app, &seller.pubky).await["usdt_available"],
        json!(false)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn usdt_is_never_offered_when_the_deployment_does_not_list_it(pool: PgPool) {
    // The flag is on, the seller consented and Paykit says USDT is ready, but
    // `PAYKIT_MARKETPLACE_ASSETS` is the default `BTC`: the bind would refuse
    // every USDT attempt, so no buyer is offered one, and the public read
    // does not even ask Paykit.
    let (app, paykit) =
        test_app_with_paykit_api_config(pool, usdt_config_btc_only(), PaykitApi::Upstream).await;
    let seller = new_actor(&app).await;
    replay(
        &paykit,
        &seller.pubky,
        "setup_status_usdt_dual",
        "setup_status_authority_ready",
    );
    paykit.set_claimed(&seller.pubky);
    let (status, _) = put(
        &app,
        &seller.token,
        &with(rails(true), "usdt_enabled", json!(true)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let calls_after_save = usdt_calls(&paykit);
    let public = get_public(&app, &seller.pubky).await;
    assert_eq!(public["usdt_available"], json!(false));
    assert_eq!(
        public["bitcoin_available"],
        json!(true),
        "Bitcoin is unaffected"
    );
    assert_eq!(usdt_calls(&paykit), calls_after_save);

    // The seller's own view still reports readiness and consent, so setup and
    // the toggle keep working before the deployment lists USDT.
    let (_, own) = get_own(&app, &seller.token).await;
    assert_eq!(
        usdt_fields(&own),
        (json!(true), json!("ready"), Value::Null)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn listing_usdt_in_the_deployment_is_what_turns_the_offer_on(pool: PgPool) {
    // Same seller, same Paykit answers: only the asset list differs.
    for (config, offered) in [(usdt_config_btc_only(), false), (usdt_config(), true)] {
        let paykit_pool = pool.clone();
        let (app, paykit) =
            test_app_with_paykit_api_config(paykit_pool, config, PaykitApi::Upstream).await;
        let seller = new_actor(&app).await;
        replay(
            &paykit,
            &seller.pubky,
            "setup_status_usdt_dual",
            "setup_status_authority_ready",
        );
        paykit.set_claimed(&seller.pubky);
        put(
            &app,
            &seller.token,
            &with(rails(true), "usdt_enabled", json!(true)),
        )
        .await;
        assert_eq!(
            get_public(&app, &seller.pubky).await["usdt_available"],
            json!(offered)
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_seller_without_a_usdt_setup_is_never_offered_usdt(pool: PgPool) {
    // The shape B7 describes: Bitcoin is ready, USDT is not (a Ring-only
    // seller has no USDT address). Captured on a `[usdt]` deployment: USDT
    // `setup_required` while the seller's authority and Bitcoin are ready.
    let (app, paykit) = upstream_app(pool).await;
    let seller = new_actor(&app).await;
    replay(
        &paykit,
        &seller.pubky,
        "setup_status_usdt_bitcoin_only",
        "setup_status_authority_ready_bitcoin_only",
    );
    paykit.replay_setup_status(
        &seller.pubky,
        Some("asset:BTC"),
        &u4::load("setup_status_btc_bitcoin_only"),
    );
    put(
        &app,
        &seller.token,
        &with(rails(true), "usdt_enabled", json!(true)),
    )
    .await;
    let public = get_public(&app, &seller.pubky).await;
    assert_eq!(public["bitcoin_available"], json!(true));
    assert_eq!(public["usdt_available"], json!(false));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unavailable_status_is_not_offered_and_does_not_start_anything(pool: PgPool) {
    let (app, paykit) = upstream_app(pool).await;
    let seller = new_actor(&app).await;
    put(
        &app,
        &seller.token,
        &with(rails(true), "usdt_enabled", json!(true)),
    )
    .await;
    paykit.replay_setup_status(
        &seller.pubky,
        Some("accepted_asset:USDT"),
        &u4::load("setup_status_usdt_homeserver_down"),
    );
    let before = setup_status_calls(&paykit).len();
    assert_eq!(
        get_public(&app, &seller.pubky).await["usdt_available"],
        json!(false)
    );
    // Only the USDT readiness was asked: no authority lookup, nothing else.
    let new_calls: Vec<Value> = setup_status_calls(&paykit).split_off(before);
    assert_eq!(
        new_calls
            .iter()
            .filter(|body| body["accepted_asset"] == json!("USDT"))
            .count(),
        1
    );
    assert!(
        new_calls
            .iter()
            .all(|body| body.get("asset").is_some() || body.get("accepted_asset").is_some()),
        "no authority-only lookup follows an unavailable USDT answer"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn readiness_is_cached_per_seller_and_asset(pool: PgPool) {
    let (app, paykit) = upstream_app(pool).await;
    let seller = new_actor(&app).await;
    paykit.set_claimed(&seller.pubky);
    replay(
        &paykit,
        &seller.pubky,
        "setup_status_usdt_dual",
        "setup_status_authority_ready",
    );
    put(
        &app,
        &seller.token,
        &with(rails(true), "usdt_enabled", json!(true)),
    )
    .await;

    // The save asked once for the seller's own view; public reads use the cache.
    let count = |field: &str, asset: &str| {
        setup_status_calls(&paykit)
            .iter()
            .filter(|body| body[field] == json!(asset))
            .count()
    };
    let usdt_before = count("accepted_asset", "USDT");
    get_public(&app, &seller.pubky).await;
    get_public(&app, &seller.pubky).await;
    assert_eq!(
        count("accepted_asset", "USDT") - usdt_before,
        1,
        "one USDT lookup for two reads inside the TTL"
    );
    assert_eq!(count("asset", "BTC"), 1, "Bitcoin keeps its own entry");

    // The seller leaves USDT; the public read follows after the TTL.
    paykit.replay_setup_status(
        &seller.pubky,
        Some("accepted_asset:USDT"),
        &u4::load("setup_status_usdt_without_usdt_config"),
    );
    assert_eq!(
        get_public(&app, &seller.pubky).await["usdt_available"],
        json!(true)
    );
    app.clock.advance_seconds(16);
    assert_eq!(
        get_public(&app, &seller.pubky).await["usdt_available"],
        json!(false)
    );
}

// ---------------------------------------------------------------------------
// Flag off and the fork
// ---------------------------------------------------------------------------

const RAIL_KEYS: [&str; 5] = [
    "bitcoin_enabled",
    "paypal_merchant_email",
    "stripe_payment_link",
    "stripe_restricted_key_set",
    "updated_at",
];

fn keys(value: &Value) -> Vec<String> {
    let mut keys: Vec<String> = value.as_object().expect("object").keys().cloned().collect();
    keys.sort_unstable();
    keys
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn with_the_flag_off_nothing_usdt_exists_and_paykit_is_never_asked(pool: PgPool) {
    let (app, paykit) =
        test_app_with_paykit_api_config(pool.clone(), Config::for_tests(), PaykitApi::Upstream)
            .await;
    let seller = new_actor(&app).await;
    paykit.set_claimed(&seller.pubky);

    let (_, body) = get_own(&app, &seller.token).await;
    assert_eq!(body, json!({ "payment_config": null }));

    let (status, body) = put(&app, &seller.token, &rails(true)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(keys(&body["payment_config"]), RAIL_KEYS);
    let (_, body) = get_own(&app, &seller.token).await;
    assert_eq!(keys(&body["payment_config"]), RAIL_KEYS);

    // `usdt_enabled` is an unknown field, refused exactly as any other.
    let (status, refused) = put_raw(
        &app,
        &seller.token,
        &with(rails(true), "usdt_enabled", json!(true)),
    )
    .await;
    let (unknown_status, unknown) = put_raw(
        &app,
        &seller.token,
        &with(rails(true), "bogus", json!(true)),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(status, unknown_status);
    let before_position = |text: &str| text.split(" at line").next().expect("split").to_string();
    assert_eq!(
        before_position(&refused.replace("usdt_enabled", "bogus")),
        before_position(&unknown),
        "the refusal is the framework's own, naming only the field"
    );
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM seller_accepted_payment_options")
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(rows, 0);

    let public = get_public(&app, &seller.pubky).await;
    assert_eq!(
        keys(&public),
        [
            "bitcoin_available",
            "bitcoin_offer_available",
            "paypal_available",
            "stripe_available"
        ]
    );
    for call in setup_status_calls(&paykit) {
        assert_eq!(
            call["asset"],
            json!("BTC"),
            "only Bitcoin readiness is ever asked"
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn on_the_fork_usdt_is_unavailable_and_paykit_is_not_asked_about_it(pool: PgPool) {
    let (app, paykit) = test_app_with_paykit_api_config(pool, usdt_config(), PaykitApi::Fork).await;
    let seller = new_actor(&app).await;
    paykit.set_claimed(&seller.pubky);
    let (status, body) = put(
        &app,
        &seller.token,
        &with(rails(true), "usdt_enabled", json!(true)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        usdt_fields(&body),
        (json!(true), json!("unavailable"), Value::Null)
    );
    let (_, own) = get_own(&app, &seller.token).await;
    assert_eq!(
        usdt_fields(&own),
        (json!(true), json!("unavailable"), Value::Null)
    );
    let public = get_public(&app, &seller.pubky).await;
    assert_eq!(public["usdt_available"], json!(false));
    assert_eq!(
        public["bitcoin_available"],
        json!(true),
        "Bitcoin is unaffected"
    );
    assert!(
        setup_status_calls(&paykit).is_empty(),
        "the fork has no per-asset readiness"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn without_a_paykit_client_usdt_is_unavailable(pool: PgPool) {
    let app = test_app_with_config(pool, usdt_config()).await;
    let seller = new_actor(&app).await;
    let (_, body) = get_own(&app, &seller.token).await;
    assert_eq!(
        usdt_fields(&body),
        (json!(false), json!("unavailable"), Value::Null)
    );
    assert_eq!(
        get_public(&app, &seller.pubky).await["usdt_available"],
        json!(false)
    );
}

#[test]
fn the_fixture_list_matches_the_directory() {
    let mut on_disk: Vec<String> = std::fs::read_dir(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/paykit-server-u4"),
    )
    .expect("fixture directory")
    .filter_map(|entry| {
        let name = entry
            .expect("entry")
            .file_name()
            .into_string()
            .expect("utf8");
        name.strip_suffix(".json").map(str::to_string)
    })
    .collect();
    on_disk.sort_unstable();
    let mut listed: Vec<String> = u4::FIXTURE_NAMES
        .iter()
        .map(|name| name.to_string())
        .collect();
    listed.sort_unstable();
    assert_eq!(on_disk, listed);
}
