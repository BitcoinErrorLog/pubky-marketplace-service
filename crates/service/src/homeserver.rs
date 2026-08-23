//! Homeserver listing-record fetch for `listing.sync`.
//!
//! The canonical listing record lives on a seller-owned homeserver path
//! (`/pub/pubky.app/marketplace/v1/listings/{listing_id}`, addressed with a
//! `pubky-host` header naming the seller). Because only the seller can write
//! that path, a record fetched this way is seller-authorized by construction
//! — it substitutes for the `actor == seller_pubky` check that
//! `listing.register` enforces, which is what lets ANY authenticated user
//! trigger the sync.
//!
//! The record is parsed leniently (camelCase, unknown and null fields
//! tolerated): the record contract belongs to the client, and this service
//! extracts only the handful of fields registration needs.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use chrono::{DateTime, Utc};
use marketplace_domain::commands::{AuctionTerms, RegisterListingPayload, SaleFormat};
use marketplace_domain::money::Money;
use marketplace_domain::ValidationIssue;
use serde::Deserialize;
use serde_json::Value;

/// Environment variable naming the homeserver base URL used to fetch
/// canonical seller-signed listing records. Required: the service refuses to
/// start without it.
pub const ENV_HOMESERVER_URL: &str = "HOMESERVER_URL";

const FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// One listing-record fetch result. Transport failures, non-2xx responses
/// other than a definitive 404, and non-JSON bodies are `Unavailable`
/// (retriable: the caller may succeed once the homeserver recovers). A 404
/// is definitive: the seller's homeserver has no such record.
#[derive(Debug, Clone, PartialEq)]
pub enum HomeserverFetchOutcome {
    Found(Value),
    NotFound,
    Unavailable,
}

/// The homeserver record fetch (listings and drops). The trait exists so
/// integration tests can stand in a local listener; production only ever
/// constructs [`HttpHomeserverClient`].
pub trait HomeserverListingClient: Send + Sync + 'static {
    fn fetch_listing<'a>(
        &'a self,
        seller_pubky: &'a str,
        listing_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>>;

    /// Fetches the seller-signed drop record at
    /// `/pub/pubky.app/marketplace/v1/drops/{drop_id}` — the same path
    /// ownership doctrine as listings: only the seller can write it, so the
    /// record's provenance substitutes for a seller actor check.
    fn fetch_drop<'a>(
        &'a self,
        seller_pubky: &'a str,
        drop_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>>;
}

/// The production client: a real
/// `GET {base}/pub/pubky.app/marketplace/v1/listings/{listing_id}` with a
/// `pubky-host: {seller_pubky}` header.
pub struct HttpHomeserverClient {
    base_url: String,
    http: reqwest::Client,
}

impl HttpHomeserverClient {
    pub fn new(base_url: &str) -> anyhow::Result<Self> {
        let base_url = base_url.trim_end_matches('/').to_string();
        if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
            anyhow::bail!("{ENV_HOMESERVER_URL} must be an http(s) URL");
        }
        let http = reqwest::Client::builder().timeout(FETCH_TIMEOUT).build()?;
        Ok(Self { base_url, http })
    }

    async fn fetch_inner(&self, seller_pubky: &str, path: &str) -> HomeserverFetchOutcome {
        let url = format!("{}{path}", self.base_url);
        let response = self
            .http
            .get(url)
            .header("pubky-host", seller_pubky)
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(_) => {
                tracing::warn!("homeserver record fetch transport failure");
                return HomeserverFetchOutcome::Unavailable;
            }
        };
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return HomeserverFetchOutcome::NotFound;
        }
        if !status.is_success() {
            tracing::warn!(status = %status, "homeserver record fetch rejected");
            return HomeserverFetchOutcome::Unavailable;
        }
        match response.json::<Value>().await {
            Ok(record) => HomeserverFetchOutcome::Found(record),
            Err(_) => {
                tracing::warn!("homeserver record fetch returned a non-JSON body");
                HomeserverFetchOutcome::Unavailable
            }
        }
    }
}

impl HomeserverListingClient for HttpHomeserverClient {
    fn fetch_listing<'a>(
        &'a self,
        seller_pubky: &'a str,
        listing_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async move {
            self.fetch_inner(
                seller_pubky,
                &format!("/pub/pubky.app/marketplace/v1/listings/{listing_id}"),
            )
            .await
        })
    }

    fn fetch_drop<'a>(
        &'a self,
        seller_pubky: &'a str,
        drop_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async move {
            self.fetch_inner(
                seller_pubky,
                &format!("/pub/pubky.app/marketplace/v1/drops/{drop_id}"),
            )
            .await
        })
    }
}

// ---------------------------------------------------------------------------
// Lenient record parsing. The homeserver record is client-owned camelCase
// JSON; unknown fields are ignored and optional fields tolerate null.
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RecordMoney {
    amount_minor: i64,
    currency: String,
    exponent: i32,
}

impl RecordMoney {
    fn into_money(self) -> Money {
        Money {
            amount_minor: self.amount_minor,
            currency: self.currency,
            exponent: self.exponent,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RecordMedia {
    #[serde(default)]
    content_hash: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RecordVariant {
    #[serde(default)]
    quantity: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RecordSale {
    format: String,
    #[serde(default)]
    unit_price: Option<RecordMoney>,
    #[serde(default)]
    starting_price: Option<RecordMoney>,
    #[serde(default)]
    starts_at: Option<DateTime<Utc>>,
    #[serde(default)]
    ends_at: Option<DateTime<Utc>>,
    #[serde(default)]
    minimum_increment: Option<RecordMoney>,
    #[serde(default)]
    reserve_price: Option<RecordMoney>,
    #[serde(default)]
    anti_sniping_window_seconds: Option<i64>,
    #[serde(default)]
    anti_sniping_extension_seconds: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListingRecord {
    #[serde(default)]
    title: Option<String>,
    revision: i64,
    #[serde(default)]
    media: Vec<RecordMedia>,
    #[serde(default)]
    variants: Vec<RecordVariant>,
    sale: RecordSale,
}

/// Derives the registration payload from a fetched record, mirroring the
/// reference client's `registerListing` field mapping EXACTLY: the title,
/// `listingRevision = record.revision`, `contentHash = media[0].contentHash`,
/// quantity as the sum of ALL variants' quantities (enabled or not), the
/// unit price from `sale.unitPrice` (fixed price) or `sale.startingPrice`
/// (auction), and — for auctions — the terms from the sale object.
///
/// Returns `None` when the record cannot be interpreted as a listing record
/// at all; field-level invariants are left to
/// `validate_register_listing_payload`, which the sync handler runs on the
/// result.
pub fn registration_payload_from_record(
    seller_pubky: &str,
    listing_id: &str,
    record: &Value,
) -> Option<RegisterListingPayload> {
    let record: ListingRecord = serde_json::from_value(record.clone()).ok()?;
    let sale_format = match record.sale.format.as_str() {
        "fixed_price" => SaleFormat::FixedPrice,
        "auction" => SaleFormat::Auction,
        _ => return None,
    };
    let unit_price = match sale_format {
        SaleFormat::FixedPrice => record.sale.unit_price,
        SaleFormat::Auction => record.sale.starting_price,
    }?
    .into_money();
    let auction_terms = if sale_format == SaleFormat::Auction {
        Some(AuctionTerms {
            starts_at: record.sale.starts_at?,
            ends_at: record.sale.ends_at?,
            minimum_increment: record.sale.minimum_increment?.into_money(),
            reserve_price: record.sale.reserve_price.map(RecordMoney::into_money),
            anti_sniping_window_seconds: record.sale.anti_sniping_window_seconds?,
            anti_sniping_extension_seconds: record.sale.anti_sniping_extension_seconds?,
        })
    } else {
        None
    };
    Some(RegisterListingPayload {
        seller_pubky: seller_pubky.to_string(),
        listing_id: listing_id.to_string(),
        title: record
            .title
            .filter(|title| !title.trim().is_empty())
            .unwrap_or_else(|| "Marketplace item".to_string()),
        listing_revision: record.revision,
        content_hash: record
            .media
            .first()
            .and_then(|media| media.content_hash.clone())
            .unwrap_or_default(),
        quantity: record.variants.iter().map(|variant| variant.quantity).sum(),
        unit_price,
        sale_format,
        auction_terms,
    })
}

// ---------------------------------------------------------------------------
// Drop record parsing. Unlike listing records (parsed leniently), the drop
// record contract is fixed (ADR-0026): unknown fields are REJECTED so a
// record from a newer, incompatible schema can never be half-interpreted
// into enforcement terms the seller did not sign.
// ---------------------------------------------------------------------------

/// The seller-signed drop record at
/// `/pub/pubky.app/marketplace/v1/drops/{drop_id}` (camelCase on the wire).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DropRecord {
    pub schema_version: i64,
    pub record_type: String,
    pub owner_pubky: String,
    pub revision: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub drop_id: String,
    pub title: String,
    #[serde(default)]
    pub description: String,
    /// Display media, opaque to this service; only the count is bounded.
    #[serde(default)]
    pub media: Vec<Value>,
    pub format: String,
    pub starts_at: DateTime<Utc>,
    #[serde(default)]
    pub ends_at: Option<DateTime<Utc>>,
    pub listing_ids: Vec<String>,
    pub total_quantity: i64,
    pub per_buyer_limit: i64,
    pub stock_display: String,
}

fn record_issue(path: &str, message: &str) -> ValidationIssue {
    ValidationIssue {
        path: path.to_string(),
        message: message.to_string(),
    }
}

/// Parses and validates a fetched drop record against the fixed ADR-0026
/// contract. `None`-style interpretation failures (not a drop record at all,
/// unknown fields, missing required fields) surface as a parse issue;
/// field-level violations are reported individually. Issues never echo
/// record values.
pub fn validated_drop_record(
    seller_pubky: &str,
    drop_id: &str,
    record: &Value,
) -> Result<DropRecord, Vec<ValidationIssue>> {
    let record: DropRecord = serde_json::from_value(record.clone()).map_err(|_| {
        vec![record_issue(
            "record",
            "Expected a schema-version-1 drop record with no unknown fields",
        )]
    })?;

    let mut issues = Vec::new();
    if record.schema_version != 1 {
        issues.push(record_issue("record.schemaVersion", "Expected version 1"));
    }
    if record.record_type != "drop" {
        issues.push(record_issue("record.recordType", "Expected \"drop\""));
    }
    if record.owner_pubky != seller_pubky {
        issues.push(record_issue(
            "record.ownerPubky",
            "Expected the seller the record was fetched from",
        ));
    }
    if record.drop_id != drop_id {
        issues.push(record_issue(
            "record.dropId",
            "Expected the drop id the record was fetched for",
        ));
    }
    if record.revision < 1 {
        issues.push(record_issue(
            "record.revision",
            "Expected a positive record revision",
        ));
    }
    let title_chars = record.title.trim().chars().count();
    if !(1..=120).contains(&title_chars) {
        issues.push(record_issue(
            "record.title",
            "Expected between 1 and 120 characters",
        ));
    }
    if record.description.chars().count() > 2_000 {
        issues.push(record_issue(
            "record.description",
            "Expected at most 2000 characters",
        ));
    }
    if record.media.len() > 10 {
        issues.push(record_issue(
            "record.media",
            "Expected at most 10 media entries",
        ));
    }
    if record.format != "fcfs" {
        issues.push(record_issue("record.format", "Expected \"fcfs\""));
    }
    if let Some(ends_at) = record.ends_at {
        if ends_at <= record.starts_at {
            issues.push(record_issue(
                "record.endsAt",
                "Expected the drop end to follow its start",
            ));
        }
    }
    if record.listing_ids.is_empty() || record.listing_ids.len() > 20 {
        issues.push(record_issue(
            "record.listingIds",
            "Expected between 1 and 20 listing ids",
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for (index, listing_id) in record.listing_ids.iter().enumerate() {
        if !marketplace_domain::commands::is_valid_entity_id(listing_id) {
            issues.push(record_issue(
                &format!("record.listingIds.{index}"),
                "Expected a path-safe commerce identifier",
            ));
        }
        if !seen.insert(listing_id.clone()) {
            issues.push(record_issue(
                "record.listingIds",
                "Listing ids must be unique",
            ));
        }
    }
    if !(1..=1_000_000).contains(&record.total_quantity) {
        issues.push(record_issue(
            "record.totalQuantity",
            "Expected a quantity between 1 and 1000000",
        ));
    }
    if !(1..=100).contains(&record.per_buyer_limit) {
        issues.push(record_issue(
            "record.perBuyerLimit",
            "Expected a limit between 1 and 100",
        ));
    } else if record.per_buyer_limit > record.total_quantity {
        issues.push(record_issue(
            "record.perBuyerLimit",
            "Expected a limit no greater than the total quantity",
        ));
    }
    if !matches!(record.stock_display.as_str(), "exact" | "bands" | "hidden") {
        issues.push(record_issue(
            "record.stockDisplay",
            "Expected exact, bands, or hidden",
        ));
    }

    if issues.is_empty() {
        Ok(record)
    } else {
        Err(issues)
    }
}

/// Builds the production client from the environment. `HOMESERVER_URL` is
/// required: `listing.sync` cannot work without it, and running without the
/// sync path would silently re-open the dead-end this command exists to fix.
pub fn client_from_env() -> anyhow::Result<HttpHomeserverClient> {
    let url = std::env::var(ENV_HOMESERVER_URL)
        .map_err(|_| anyhow::anyhow!("{ENV_HOMESERVER_URL} must be set"))?;
    HttpHomeserverClient::new(&url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn record_json() -> Value {
        json!({
            "recordType": "listing",
            "schemaVersion": 1,
            "title": "Pokemon / Snorlax",
            "revision": 1,
            "location": { "countryCode": "US", "region": null },
            "media": [
                { "id": "m1", "contentHash": "f".repeat(64), "unknownField": true },
                { "id": "m2", "contentHash": "9".repeat(64) },
            ],
            "variants": [
                { "id": "variant_1", "enabled": true, "quantity": 1, "sku": null },
                { "id": "variant_2", "enabled": false, "quantity": 2 },
            ],
            "sale": {
                "acceptsOffers": true,
                "format": "fixed_price",
                "unitPrice": { "amountMinor": 6_900, "currency": "USD", "exponent": 2 },
            },
        })
    }

    #[test]
    fn derives_the_registration_payload_like_the_client() {
        let seller = "y".repeat(52);
        let payload = registration_payload_from_record(&seller, "listing_01", &record_json())
            .expect("record parses");
        assert_eq!(payload.title, "Pokemon / Snorlax");
        assert_eq!(payload.listing_revision, 1);
        assert_eq!(payload.content_hash, "f".repeat(64));
        // The client sums ALL variants' quantities, enabled or not.
        assert_eq!(payload.quantity, 3);
        assert_eq!(payload.unit_price.amount_minor, 6_900);
        assert_eq!(payload.sale_format, SaleFormat::FixedPrice);
        assert!(payload.auction_terms.is_none());
    }

    #[test]
    fn tolerates_nulls_and_unknown_fields_and_defaults_a_missing_title() {
        let mut record = record_json();
        record["title"] = json!(null);
        record["someFutureField"] = json!({ "nested": true });
        let payload = registration_payload_from_record(&"y".repeat(52), "l", &record)
            .expect("lenient parse succeeds");
        assert_eq!(payload.title, "Marketplace item");
    }

    #[test]
    fn derives_auction_terms_from_the_sale_object() {
        let mut record = record_json();
        record["sale"] = json!({
            "format": "auction",
            "startingPrice": { "amountMinor": 4_500, "currency": "USD", "exponent": 2 },
            "startsAt": "2026-08-19T22:00:00.000Z",
            "endsAt": "2026-08-19T22:10:00.000Z",
            "minimumIncrement": { "amountMinor": 500, "currency": "USD", "exponent": 2 },
            "reservePrice": null,
            "antiSnipingWindowSeconds": 60,
            "antiSnipingExtensionSeconds": 120,
        });
        let payload = registration_payload_from_record(&"y".repeat(52), "l", &record)
            .expect("auction record parses");
        assert_eq!(payload.sale_format, SaleFormat::Auction);
        assert_eq!(payload.unit_price.amount_minor, 4_500);
        let terms = payload.auction_terms.expect("terms derived");
        assert_eq!(terms.minimum_increment.amount_minor, 500);
        assert!(terms.reserve_price.is_none());
        assert_eq!(terms.anti_sniping_window_seconds, 60);
    }

    #[test]
    fn refuses_records_it_cannot_interpret() {
        let mut no_sale = record_json();
        no_sale.as_object_mut().unwrap().remove("sale");
        assert!(registration_payload_from_record(&"y".repeat(52), "l", &no_sale).is_none());

        let mut bad_format = record_json();
        bad_format["sale"]["format"] = json!("raffle");
        assert!(registration_payload_from_record(&"y".repeat(52), "l", &bad_format).is_none());

        let mut auction_without_terms = record_json();
        auction_without_terms["sale"]["format"] = json!("auction");
        assert!(
            registration_payload_from_record(&"y".repeat(52), "l", &auction_without_terms)
                .is_none()
        );
    }

    fn drop_record_json(seller: &str) -> Value {
        json!({
            "schemaVersion": 1,
            "recordType": "drop",
            "ownerPubky": seller,
            "revision": 1,
            "createdAt": "2026-08-19T21:00:00.000Z",
            "updatedAt": "2026-08-19T21:00:00.000Z",
            "dropId": "winter_drop",
            "title": "Winter capsule",
            "description": "Ten pairs, first come first served.",
            "media": [{ "id": "m1", "contentHash": "f".repeat(64) }],
            "format": "fcfs",
            "startsAt": "2026-08-19T22:10:00.000Z",
            "endsAt": "2026-08-19T23:10:00.000Z",
            "listingIds": ["boots_01", "boots_02"],
            "totalQuantity": 10,
            "perBuyerLimit": 2,
            "stockDisplay": "exact",
        })
    }

    #[test]
    fn validates_a_canonical_drop_record() {
        let seller = "y".repeat(52);
        let record = validated_drop_record(&seller, "winter_drop", &drop_record_json(&seller))
            .expect("canonical record validates");
        assert_eq!(record.revision, 1);
        assert_eq!(record.listing_ids, vec!["boots_01", "boots_02"]);
        assert_eq!(record.total_quantity, 10);
        assert_eq!(record.per_buyer_limit, 2);
    }

    #[test]
    fn drop_records_reject_unknown_fields_and_field_violations() {
        let seller = "y".repeat(52);

        let mut unknown = drop_record_json(&seller);
        unknown["someFutureField"] = json!(true);
        let issues =
            validated_drop_record(&seller, "winter_drop", &unknown).expect_err("unknown rejected");
        assert_eq!(issues[0].path, "record");

        let mut wrong_owner = drop_record_json(&seller);
        wrong_owner["ownerPubky"] = json!("o".repeat(52));
        let issues = validated_drop_record(&seller, "winter_drop", &wrong_owner)
            .expect_err("wrong owner rejected");
        assert!(issues.iter().any(|i| i.path == "record.ownerPubky"));

        let mut ends_before_start = drop_record_json(&seller);
        ends_before_start["endsAt"] = json!("2026-08-19T22:00:00.000Z");
        let issues = validated_drop_record(&seller, "winter_drop", &ends_before_start)
            .expect_err("inverted schedule rejected");
        assert!(issues.iter().any(|i| i.path == "record.endsAt"));

        let mut over_limit = drop_record_json(&seller);
        over_limit["totalQuantity"] = json!(1);
        let issues = validated_drop_record(&seller, "winter_drop", &over_limit)
            .expect_err("limit above total rejected");
        assert!(issues.iter().any(|i| i.path == "record.perBuyerLimit"));

        let mut duplicate_listings = drop_record_json(&seller);
        duplicate_listings["listingIds"] = json!(["boots_01", "boots_01"]);
        let issues = validated_drop_record(&seller, "winter_drop", &duplicate_listings)
            .expect_err("duplicate listing ids rejected");
        assert!(issues.iter().any(|i| i.path == "record.listingIds"));

        let mut bad_display = drop_record_json(&seller);
        bad_display["stockDisplay"] = json!("teaser");
        let issues = validated_drop_record(&seller, "winter_drop", &bad_display)
            .expect_err("unknown stock display rejected");
        assert!(issues.iter().any(|i| i.path == "record.stockDisplay"));

        // An open-ended schedule (no endsAt) is valid.
        let mut open_ended = drop_record_json(&seller);
        open_ended.as_object_mut().unwrap().remove("endsAt");
        validated_drop_record(&seller, "winter_drop", &open_ended)
            .expect("open-ended schedule validates");
    }

    #[test]
    fn http_client_requires_an_http_base_url() {
        assert!(HttpHomeserverClient::new("ftp://homeserver.example").is_err());
        assert!(HttpHomeserverClient::new("https://homeserver.example/").is_ok());
    }
}
