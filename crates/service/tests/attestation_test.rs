//! Purchase attestation tests (ADR 0024, trust & reputation Phase 1):
//! issuance inside `review.create`, D2 both-sides amount-band consent,
//! claim shape, offline signature verifiability against the attestor pubky,
//! the idempotent participant-scoped re-fetch, refund annotations,
//! the weekly stat-attestation job,
//! and the deterministic receipt and drop-edition attestation endpoints.

mod common;

use axum::http::StatusCode;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    checkout_command, count, create_paid_order, drop_record_json, execute, indexed_command_id,
    new_actor, order_command, payment_command, register_command, send, sync_drop_command, test_app,
    test_app_with_attestor, test_app_with_homeserver, test_app_with_homeserver_and_attestor,
    test_attestor, ts_after, FakeHomeserver, PaidOrder, TestActor, TestApp,
};
use marketplace_service::clock::Clock;
use marketplace_service::workers::generate_due_stat_attestations;

async fn get(app: &TestApp, uri: &str, token: &str) -> (StatusCode, Value) {
    send(app.router.clone(), "GET", uri, Some(token), &json!(null)).await
}

/// Decodes a z-base-32 pubky to its Ed25519 key bytes — the verifier-side
/// step the design's recipe (§9) requires to work offline.
fn decode_pubky(pubky: &str) -> [u8; 32] {
    const ALPHABET: &str = "ybndrfg8ejkmcpqxot1uwisza345h769";
    let mut accumulator: u64 = 0;
    let mut bits: u32 = 0;
    let mut out = Vec::new();
    for c in pubky.chars() {
        let value = ALPHABET.find(c).expect("z-base-32 char") as u64;
        accumulator = (accumulator << 5) | value;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push((accumulator >> bits) as u8);
        }
    }
    out.truncate(32);
    out.try_into().expect("32 bytes")
}

/// Verifies a compact JWS against its own `iss` claim and returns the
/// decoded claims. This is the third-party recipe: no service call, no key
/// server — the pubky IS the key.
fn verify_jws(jws: &str) -> Value {
    let parts: Vec<&str> = jws.split('.').collect();
    assert_eq!(parts.len(), 3, "compact JWS has three segments");
    let header: Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).expect("header b64")).unwrap();
    assert_eq!(header["alg"], json!("EdDSA"));
    let claims: Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).expect("payload b64")).unwrap();
    let iss = claims
        .get("iss")
        .or_else(|| claims.get("attestor"))
        .and_then(Value::as_str)
        .expect("issuer claim present");
    let key = VerifyingKey::from_bytes(&decode_pubky(iss)).expect("iss decodes to an Ed25519 key");
    let signature = Signature::from_bytes(
        &URL_SAFE_NO_PAD
            .decode(parts[2])
            .expect("signature b64")
            .try_into()
            .expect("64-byte signature"),
    );
    key.verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &signature)
        .expect("signature verifies against the attestor pubky");
    claims
}

/// Drives a paid order to `delivered` and returns the order id.
async fn delivered_order(app: &TestApp, seller: &TestActor, buyer: &TestActor) -> String {
    let order = create_paid_order(app, seller, buyer).await;
    let order_id = order.order_id.clone();
    let (status, body) = execute(
        app,
        &seller.token,
        &order_command(
            "fulfillment.ship",
            &order_id,
            2,
            json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-123" }),
            2_201,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "ship failed: {body}");
    let (status, body) = execute(
        app,
        &buyer.token,
        &order_command(
            "fulfillment.confirm_delivery",
            &order_id,
            3,
            json!({}),
            2_202,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "delivery failed: {body}");
    order_id
}

fn band_consent_command(
    seller_pubky: &str,
    expected_revision: i64,
    allows: bool,
    command_number: u64,
) -> Value {
    json!({
        "version": 1,
        "command_id": indexed_command_id(0x9000, command_number),
        "aggregate_id": format!("seller_settings:{seller_pubky}"),
        "expected_revision": expected_revision,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "attestation.set_band_consent",
        "payload": { "allows_amount_band": allows },
    })
}

#[sqlx::test]
async fn review_create_issues_a_verifiable_attestation_with_the_designed_claims(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer).await;

    let (status, reviewed) = execute(
        &app,
        &buyer.token,
        &order_command(
            "review.create",
            &order_id,
            4,
            json!({ "rating": 5, "text": "Accurate and fast." }),
            2_203,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "review failed: {reviewed}");
    let attestation = &reviewed["result"]["attestation"];
    let jws = attestation["jws"]
        .as_str()
        .expect("attestation jws present");

    // Third-party verification: signature checks against the iss pubky
    // alone, and iss is this deployment's attestor.
    let claims = verify_jws(jws);
    assert_eq!(claims["iss"], json!(test_attestor().pubky()));

    // Exact claim shape (closed world, design §5.3). No amount band: the D2
    // default is not-included.
    let keys: Vec<&str> = claims
        .as_object()
        .expect("claims are an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        vec![
            "v",
            "iss",
            "sub",
            "cpk",
            "role",
            "listing",
            "order_ref",
            "completed_on",
            "iat"
        ]
    );
    assert_eq!(claims["v"], json!(1));
    assert_eq!(claims["sub"], json!(buyer.pubky));
    assert_eq!(claims["cpk"], json!(seller.pubky));
    assert_eq!(claims["role"], json!("buyer_reviewing_seller"));
    assert_eq!(
        claims["listing"],
        json!(format!(
            "pubky://{}/pub/pubky.app/marketplace/v1/listings/boots_01",
            seller.pubky
        ))
    );
    // The delivery happened at the fixed test instant: day granularity only.
    assert_eq!(claims["completed_on"], json!("2026-08-19"));
    let order_ref = claims["order_ref"].as_str().expect("order_ref present");
    assert_eq!(order_ref.len(), 64);
    assert!(order_ref
        .chars()
        .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase()));
    // The order_ref is the salted hash of the order UUID.
    assert_eq!(
        order_ref,
        test_attestor().order_ref(order_id.parse().expect("uuid")),
    );
    // The result claims match what the command result carried.
    assert_eq!(attestation["claims"], claims);

    // The seller's own review gets its own attestation with the mirrored
    // role and bindings.
    let (status, seller_reviewed) = execute(
        &app,
        &seller.token,
        &order_command(
            "review.create",
            &order_id,
            5,
            json!({ "rating": 5, "text": "Great buyer." }),
            2_204,
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "seller review failed: {seller_reviewed}"
    );
    let seller_claims = verify_jws(
        seller_reviewed["result"]["attestation"]["jws"]
            .as_str()
            .expect("seller attestation present"),
    );
    assert_eq!(seller_claims["role"], json!("seller_reviewing_buyer"));
    assert_eq!(seller_claims["sub"], json!(seller.pubky));
    assert_eq!(seller_claims["cpk"], json!(buyer.pubky));
    assert_eq!(seller_claims["order_ref"], claims["order_ref"]);

    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM review_attestations").await,
        2
    );
}

#[sqlx::test]
async fn amount_band_requires_both_sides_consent(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;

    // Case 1: buyer opts in but the seller never consented -> no band.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer).await;
    let (status, reviewed) = execute(
        &app,
        &buyer.token,
        &order_command(
            "review.create",
            &order_id,
            4,
            json!({ "rating": 5, "text": "Great.", "allow_amount_band": true }),
            2_210,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "review failed: {reviewed}");
    assert!(
        reviewed["result"]["attestation"]["claims"]
            .get("amount_band")
            .is_none(),
        "band must be absent without seller consent"
    );

    // Case 2: seller consented but the buyer did not opt in -> no band.
    let seller2 = new_actor(&app).await;
    let buyer2 = new_actor(&app).await;
    let (status, body) = execute(
        &app,
        &seller2.token,
        &band_consent_command(&seller2.pubky, 0, true, 300),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "consent failed: {body}");
    assert_eq!(
        body["result"]["band_consent"]["allows_amount_band"],
        json!(true)
    );
    let order_id2 = delivered_order(&app, &seller2, &buyer2).await;
    let (status, reviewed) = execute(
        &app,
        &buyer2.token,
        &order_command(
            "review.create",
            &order_id2,
            4,
            json!({ "rating": 4, "text": "Fine." }),
            2_211,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "review failed: {reviewed}");
    assert!(
        reviewed["result"]["attestation"]["claims"]
            .get("amount_band")
            .is_none(),
        "band must be absent without buyer opt-in"
    );

    // Case 3: both sides consent -> the log-decade band is present. The
    // fixture order totals 14,796 minor units of USD -> USD:4.
    let seller3 = new_actor(&app).await;
    let buyer3 = new_actor(&app).await;
    let (status, body) = execute(
        &app,
        &seller3.token,
        &band_consent_command(&seller3.pubky, 0, true, 301),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "consent failed: {body}");
    let order_id3 = delivered_order(&app, &seller3, &buyer3).await;
    let (status, reviewed) = execute(
        &app,
        &buyer3.token,
        &order_command(
            "review.create",
            &order_id3,
            4,
            json!({ "rating": 5, "text": "Great.", "allow_amount_band": true }),
            2_212,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "review failed: {reviewed}");
    let claims = verify_jws(reviewed["result"]["attestation"]["jws"].as_str().unwrap());
    assert_eq!(claims["amount_band"], json!("USD:4"));

    // Case 4: a seller who consented can withdraw; later reviews omit the
    // band even when buyers opt in.
    let (status, body) = execute(
        &app,
        &seller3.token,
        &band_consent_command(&seller3.pubky, 1, false, 302),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "withdrawal failed: {body}");
    let (status, body) = get(
        &app,
        &format!("/v1/sellers/{}/band-consent", seller3.pubky),
        &buyer3.token,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["allows_amount_band"], json!(false));
}

#[sqlx::test]
async fn band_consent_endpoint_defaults_to_false_and_conflicts_are_surfaced(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let reader = new_actor(&app).await;

    let (status, body) = get(
        &app,
        &format!("/v1/sellers/{}/band-consent", seller.pubky),
        &reader.token,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["allows_amount_band"], json!(false));

    // Setting consent on someone else's aggregate is refused.
    let (status, body) = execute(
        &app,
        &reader.token,
        &band_consent_command(&seller.pubky, 0, true, 303),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "unexpected: {body}"
    );
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));

    // A stale revision conflicts with the stored one.
    let (status, body) = execute(
        &app,
        &seller.token,
        &band_consent_command(&seller.pubky, 0, true, 304),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "consent failed: {body}");
    let (status, body) = execute(
        &app,
        &seller.token,
        &band_consent_command(&seller.pubky, 0, false, 305),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(body["error"]["current_revision"], json!(1));
}

#[sqlx::test]
async fn attestation_refetch_is_idempotent_and_participant_scoped(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer).await;

    // Before any review: nothing to fetch.
    let (status, _) = get(
        &app,
        &format!("/v1/orders/{order_id}/review-attestation"),
        &buyer.token,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, reviewed) = execute(
        &app,
        &buyer.token,
        &order_command(
            "review.create",
            &order_id,
            4,
            json!({ "rating": 5, "text": "Accurate and fast." }),
            2_220,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "review failed: {reviewed}");
    let issued_jws = reviewed["result"]["attestation"]["jws"]
        .as_str()
        .expect("jws present")
        .to_string();

    // Re-fetch returns the exact same attestation (idempotent, no
    // consumption semantics).
    let (status, fetched) = get(
        &app,
        &format!("/v1/orders/{order_id}/review-attestation"),
        &buyer.token,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "re-fetch failed: {fetched}");
    assert_eq!(fetched["attestation"]["jws"], json!(issued_jws));
    assert_eq!(
        fetched["attestation"]["claims"],
        reviewed["result"]["attestation"]["claims"]
    );

    // The seller has not reviewed: their fetch is 404 (the attestation is
    // per reviewer). A stranger's fetch is likewise 404 without revealing
    // the order exists.
    for outsider in [&seller, &stranger] {
        let (status, body) = get(
            &app,
            &format!("/v1/orders/{order_id}/review-attestation"),
            &outsider.token,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {body}");
    }

    // review.update revises text but echoes the unchanged attestation.
    let (status, updated) = execute(
        &app,
        &buyer.token,
        &order_command(
            "review.update",
            &order_id,
            5,
            json!({ "rating": 4, "text": "Revised after wear." }),
            2_221,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "update failed: {updated}");
    assert_eq!(updated["result"]["attestation"]["jws"], json!(issued_jws));
}

#[sqlx::test]
async fn reviews_still_work_without_an_attestor_and_return_no_attestation(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer).await;

    let (status, reviewed) = execute(
        &app,
        &buyer.token,
        &order_command(
            "review.create",
            &order_id,
            4,
            json!({ "rating": 5, "text": "Accurate and fast." }),
            2_230,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "review failed: {reviewed}");
    assert!(reviewed["result"].get("attestation").is_none());
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM review_attestations").await,
        0
    );
    let (status, _) = get(
        &app,
        &format!("/v1/orders/{order_id}/review-attestation"),
        &buyer.token,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[sqlx::test]
async fn refund_outcomes_annotate_the_order_ref(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let attestor = test_attestor();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer).await;
    let order_ref = attestor.order_ref(order_id.parse().expect("uuid"));

    // The peer-to-peer return flow: the buyer requests, the seller approves
    // and receives, then records the externally evidenced refund — the
    // refund annotates the order_ref (never revokes anything).
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "return.request",
            &order_id,
            4,
            json!({ "reason": "Item not as described", "requested_amount_minor": 13_700 }),
            2_240,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "return request failed: {body}");
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command("return.approve", &order_id, 5, json!({}), 2_241),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "return approve failed: {body}");
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command("return.receive", &order_id, 6, json!({}), 2_242),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "return receive failed: {body}");
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command(
            "refund.record_external",
            &order_id,
            7,
            json!({ "amount_minor": 13_700, "transaction_id": "tx-refund-1" }),
            2_243,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "refund failed: {body}");
    let outcomes: Vec<(String,)> = sqlx::query_as(
        "SELECT outcome FROM attestation_annotations WHERE order_ref = $1 ORDER BY outcome",
    )
    .bind(&order_ref)
    .fetch_all(&app.pool)
    .await
    .expect("annotations readable");
    assert_eq!(outcomes, vec![("refunded".to_string(),)]);
}

#[sqlx::test]
async fn receipt_attestation_is_verifiable_and_binds_the_paid_order_facts(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let paid = create_paid_order(&app, &seller, &buyer).await;

    let (status, body) = get(
        &app,
        &format!("/v1/receipts/{}/attestation", paid.receipt_id),
        &buyer.token,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "fetch failed: {body}");
    let jws = body["receipt_attestation"]["jws"]
        .as_str()
        .expect("receipt attestation jws present");

    // Compact JWS: exactly three base64url segments, and the protected
    // header is byte-exact (the specs-fork verifier contract).
    let parts: Vec<&str> = jws.split('.').collect();
    assert_eq!(parts.len(), 3, "compact JWS has three segments");
    assert_eq!(
        String::from_utf8(URL_SAFE_NO_PAD.decode(parts[0]).expect("header b64")).expect("utf8"),
        r#"{"alg":"EdDSA","typ":"pubky-order-receipt+v1"}"#
    );

    // Third-party recipe: the signature verifies against the iss pubky
    // alone, and iss is this deployment's attestor.
    let claims = verify_jws(jws);
    assert_eq!(claims, body["receipt_attestation"]["claims"]);
    assert_eq!(claims["iss"], json!(test_attestor().pubky()));

    // Exact claim shape and order (closed world).
    let keys: Vec<&str> = claims
        .as_object()
        .expect("claims are an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        vec![
            "v",
            "iss",
            "buyer",
            "seller",
            "order",
            "receipt",
            "total_minor",
            "currency",
            "exponent",
            "paid_at",
            "iat"
        ]
    );
    assert_eq!(claims["v"], json!(1));
    assert_eq!(claims["buyer"], json!(buyer.pubky));
    assert_eq!(claims["seller"], json!(seller.pubky));
    assert_eq!(claims["order"], json!(paid.order_id));
    assert_eq!(claims["receipt"], json!(paid.receipt_id));
    assert_eq!(claims["total_minor"], json!(paid.total_minor));
    assert_eq!(claims["currency"], json!("USD"));
    assert_eq!(claims["exponent"], json!(2));
    // The receipt was issued at the fixed test instant.
    assert_eq!(claims["paid_at"], json!("2026-08-19T22:00:00.000Z"));
    assert_eq!(claims["iat"], json!(app.clock.now().timestamp()));
}

#[sqlx::test]
async fn receipt_attestation_is_byte_identical_across_calls_and_participants(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let paid = create_paid_order(&app, &seller, &buyer).await;
    let uri = format!("/v1/receipts/{}/attestation", paid.receipt_id);

    let (status, first) = get(&app, &uri, &buyer.token).await;
    assert_eq!(status, StatusCode::OK, "buyer fetch failed: {first}");

    // Advancing the clock changes nothing: the claims derive entirely from
    // stored rows, never from `now`.
    app.clock.advance_seconds(86_400);
    let (status, second) = get(&app, &uri, &buyer.token).await;
    assert_eq!(status, StatusCode::OK, "repeat fetch failed: {second}");
    assert_eq!(
        first["receipt_attestation"]["jws"],
        second["receipt_attestation"]["jws"]
    );

    let (status, sellers) = get(&app, &uri, &seller.token).await;
    assert_eq!(status, StatusCode::OK, "seller fetch failed: {sellers}");
    assert_eq!(
        first["receipt_attestation"]["jws"],
        sellers["receipt_attestation"]["jws"]
    );
    assert_eq!(
        first["receipt_attestation"]["claims"],
        sellers["receipt_attestation"]["claims"]
    );
}

#[sqlx::test]
async fn receipt_attestation_is_participant_scoped(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    let paid = create_paid_order(&app, &seller, &buyer).await;

    let (status, foreign) = get(
        &app,
        &format!("/v1/receipts/{}/attestation", paid.receipt_id),
        &stranger.token,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {foreign}");

    // Indistinguishable from a receipt that does not exist.
    let (status, missing) = get(
        &app,
        "/v1/receipts/00000000-0000-4000-8000-0000000000ff/attestation",
        &buyer.token,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {missing}");
    assert_eq!(foreign, missing);
}

#[sqlx::test]
async fn receipt_attestation_for_an_unknown_receipt_is_not_found(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let buyer = new_actor(&app).await;

    let (status, body) = get(
        &app,
        "/v1/receipts/00000000-0000-4000-8000-0000000000ff/attestation",
        &buyer.token,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {body}");
    assert_eq!(body["ok"], json!(false));
    assert_eq!(body["error"]["code"], json!("NOT_FOUND"));
}

#[sqlx::test]
async fn receipt_attestation_without_an_attestor_is_not_found(pool: PgPool) {
    // Mirrors the review-attestation re-fetch on an attestor-less
    // deployment: a participant's fetch for a real receipt is 404.
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let paid = create_paid_order(&app, &seller, &buyer).await;

    let (status, body) = get(
        &app,
        &format!("/v1/receipts/{}/attestation", paid.receipt_id),
        &buyer.token,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {body}");
    assert_eq!(body["ok"], json!(false));
    assert_eq!(body["error"]["code"], json!("NOT_FOUND"));
}

#[sqlx::test]
async fn receipt_attestation_paid_at_matches_the_receipt_projection(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let paid = create_paid_order(&app, &seller, &buyer).await;

    let (status, receipt) = get(
        &app,
        &format!("/v1/receipts/{}", paid.receipt_id),
        &buyer.token,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "receipt fetch failed: {receipt}");
    let (status, attested) = get(
        &app,
        &format!("/v1/receipts/{}/attestation", paid.receipt_id),
        &buyer.token,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "attestation fetch failed: {attested}"
    );

    // The claim and the projection compare equal string-to-string, so the
    // published record and the attested claims never drift.
    assert_eq!(
        attested["receipt_attestation"]["claims"]["paid_at"],
        receipt["issued_at"]
    );
    assert!(receipt["issued_at"].is_string());
}

/// Registers one listing, publishes + syncs a `total`-unit drop over it,
/// checks out one unit as the buyer, and confirms the sandbox payment: a
/// paid DROP order holding edition 1 of `total`.
async fn drop_paid_order(
    app: &TestApp,
    homeserver: &FakeHomeserver,
    seller: &TestActor,
    buyer: &TestActor,
    total: i64,
) -> PaidOrder {
    let (status, body) = execute(app, &seller.token, &register_command(&seller.pubky, total)).await;
    assert_eq!(status, StatusCode::OK, "register fixture failed: {body}");
    homeserver.put_drop_record(
        &seller.pubky,
        "winter_drop",
        drop_record_json(
            &seller.pubky,
            "winter_drop",
            1,
            &["boots_01"],
            &ts_after(0),
            None,
            total,
            1,
        ),
    );
    let (status, body) = execute(
        app,
        &buyer.token,
        &sync_drop_command(&seller.pubky, "winter_drop", 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "drop sync fixture failed: {body}");
    let (status, body) = execute(app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_eq!(status, StatusCode::OK, "checkout fixture failed: {body}");
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id present")
        .to_string();
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id present")
        .to_string();
    let total_minor = body["result"]["orders"][0]["total"]["amount_minor"]
        .as_i64()
        .expect("order total present");
    let (status, body) = execute(
        app,
        &buyer.token,
        &payment_command(&payment_id, 1, "confirmed", 1, 1_050),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "payment fixture failed: {body}");
    let receipt_id = body["result"]["receipt"]["id"]
        .as_str()
        .expect("receipt id present")
        .to_string();
    PaidOrder {
        order_id,
        payment_id,
        receipt_id,
        total_minor,
    }
}

#[sqlx::test]
async fn edition_attestation_is_verifiable_and_binds_the_drop_order_facts(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver_and_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    let paid = drop_paid_order(&app, &homeserver, &seller, &buyer, 3).await;
    let uri = format!("/v1/receipts/{}/edition-attestation", paid.receipt_id);

    let (status, body) = get(&app, &uri, &buyer.token).await;
    assert_eq!(status, StatusCode::OK, "fetch failed: {body}");
    let jws = body["edition_attestation"]["jws"]
        .as_str()
        .expect("edition attestation jws present");

    // Compact JWS: exactly three base64url segments, and the protected
    // header is byte-exact (the specs-fork verifier contract for
    // `pubky-drop-edition+v1`).
    let parts: Vec<&str> = jws.split('.').collect();
    assert_eq!(parts.len(), 3, "compact JWS has three segments");
    assert_eq!(
        String::from_utf8(URL_SAFE_NO_PAD.decode(parts[0]).expect("header b64")).expect("utf8"),
        r#"{"alg":"EdDSA","typ":"pubky-drop-edition+v1"}"#
    );

    // Third-party recipe: the signature verifies against the iss pubky
    // alone, and iss is this deployment's attestor.
    let claims = verify_jws(jws);
    assert_eq!(claims, body["edition_attestation"]["claims"]);
    assert_eq!(claims["iss"], json!(test_attestor().pubky()));

    // Exact claim shape and order (closed world). `drop` is the DROP ID,
    // never the aggregate id; `iat` is the receipt's stored creation
    // instant, never wall clock.
    let keys: Vec<&str> = claims
        .as_object()
        .expect("claims are an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        vec!["v", "iss", "buyer", "seller", "drop", "edition", "of", "receipt", "iat"]
    );
    assert_eq!(claims["v"], json!(1));
    assert_eq!(claims["buyer"], json!(buyer.pubky));
    assert_eq!(claims["seller"], json!(seller.pubky));
    assert_eq!(claims["drop"], json!("winter_drop"));
    assert_eq!(claims["edition"], json!(1));
    assert_eq!(claims["of"], json!(3));
    assert_eq!(claims["receipt"], json!(paid.receipt_id));
    assert_eq!(claims["iat"], json!(app.clock.now().timestamp()));

    // Buyer and seller fetch the byte-identical JWS, and advancing the
    // clock changes nothing: every claim derives from stored rows.
    let (status, sellers) = get(&app, &uri, &seller.token).await;
    assert_eq!(status, StatusCode::OK, "seller fetch failed: {sellers}");
    assert_eq!(sellers["edition_attestation"]["jws"], json!(jws));
    assert_eq!(sellers["edition_attestation"]["claims"], claims);
    app.clock.advance_seconds(86_400);
    let (status, later) = get(&app, &uri, &buyer.token).await;
    assert_eq!(status, StatusCode::OK, "repeat fetch failed: {later}");
    assert_eq!(later["edition_attestation"]["jws"], json!(jws));

    // A stranger's fetch is 404, indistinguishable from a receipt that
    // does not exist — and both carry the receipt read's body.
    let (status, foreign) = get(&app, &uri, &stranger.token).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {foreign}");
    let (status, missing) = get(
        &app,
        "/v1/receipts/00000000-0000-4000-8000-0000000000ff/edition-attestation",
        &buyer.token,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {missing}");
    assert_eq!(foreign, missing);
    assert_eq!(
        foreign["error"]["message"],
        json!("The receipt was not found.")
    );
}

#[sqlx::test]
async fn edition_attestation_for_a_non_drop_order_is_not_found(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let paid = create_paid_order(&app, &seller, &buyer).await;

    // The receipt itself reads fine...
    let (status, body) = get(
        &app,
        &format!("/v1/receipts/{}", paid.receipt_id),
        &buyer.token,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "receipt read failed: {body}");

    // ...but a non-drop order has no edition to attest: 404 with the
    // receipt read's body.
    let (status, body) = get(
        &app,
        &format!("/v1/receipts/{}/edition-attestation", paid.receipt_id),
        &buyer.token,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {body}");
    assert_eq!(body["ok"], json!(false));
    assert_eq!(body["error"]["code"], json!("NOT_FOUND"));
    assert_eq!(
        body["error"]["message"],
        json!("The receipt was not found.")
    );
}

#[sqlx::test]
async fn edition_attestation_without_an_attestor_is_not_found(pool: PgPool) {
    // A real paid drop order on an attestor-less deployment: the fetch is
    // 404 with the receipt read's body, like the other attestation reads.
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let paid = drop_paid_order(&app, &homeserver, &seller, &buyer, 3).await;

    let (status, body) = get(
        &app,
        &format!("/v1/receipts/{}/edition-attestation", paid.receipt_id),
        &buyer.token,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {body}");
    assert_eq!(body["ok"], json!(false));
    assert_eq!(body["error"]["code"], json!("NOT_FOUND"));
    assert_eq!(
        body["error"]["message"],
        json!("The receipt was not found.")
    );
}

#[sqlx::test]
async fn stat_attestation_job_signs_weekly_seller_stats(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let attestor = test_attestor();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    delivered_order(&app, &seller, &buyer).await;

    let now = app.clock.now();
    let signed = generate_due_stat_attestations(&app.pool, &attestor, now)
        .await
        .expect("stat job runs");
    assert_eq!(signed, 1);

    let (body, jws): (Value, String) =
        sqlx::query_as("SELECT body, jws FROM seller_stat_attestations WHERE seller_pubky = $1")
            .bind(&seller.pubky)
            .fetch_one(&app.pool)
            .await
            .expect("stat row exists");

    // The signed body verifies against the attestor pubky and carries the
    // D3 minimal set as bands/per-mille, never raw counts or amounts.
    let claims = verify_jws(&jws);
    assert_eq!(claims, body);
    assert_eq!(body["v"], json!(1));
    assert_eq!(body["attestor"], json!(attestor.pubky()));
    assert_eq!(body["seller"], json!(seller.pubky));
    assert_eq!(body["ordersCompletedBand"], json!("0"));
    assert_eq!(body["medianTimeToShipHours"], json!(0));
    assert_eq!(body["completionRatePermille"], json!(1000));
    assert_eq!(body["period"]["to"], json!("2026-08-19"));

    // Re-running within the weekly cadence signs nothing new.
    let signed_again = generate_due_stat_attestations(&app.pool, &attestor, now)
        .await
        .expect("stat job runs");
    assert_eq!(signed_again, 0);
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM seller_stat_attestations").await,
        1
    );
}
