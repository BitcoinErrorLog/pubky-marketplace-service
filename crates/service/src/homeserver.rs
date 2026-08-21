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

/// The homeserver listing-record fetch. The trait exists so integration
/// tests can stand in a local listener; production only ever constructs
/// [`HttpHomeserverClient`].
pub trait HomeserverListingClient: Send + Sync + 'static {
    fn fetch_listing<'a>(
        &'a self,
        seller_pubky: &'a str,
        listing_id: &'a str,
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

    async fn fetch_inner(&self, seller_pubky: &str, listing_id: &str) -> HomeserverFetchOutcome {
        let url = format!(
            "{}/pub/pubky.app/marketplace/v1/listings/{listing_id}",
            self.base_url
        );
        let response = self
            .http
            .get(url)
            .header("pubky-host", seller_pubky)
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(_) => {
                tracing::warn!("homeserver listing fetch transport failure");
                return HomeserverFetchOutcome::Unavailable;
            }
        };
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return HomeserverFetchOutcome::NotFound;
        }
        if !status.is_success() {
            tracing::warn!(status = %status, "homeserver listing fetch rejected");
            return HomeserverFetchOutcome::Unavailable;
        }
        match response.json::<Value>().await {
            Ok(record) => HomeserverFetchOutcome::Found(record),
            Err(_) => {
                tracing::warn!("homeserver listing fetch returned a non-JSON body");
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
        Box::pin(self.fetch_inner(seller_pubky, listing_id))
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

    #[test]
    fn http_client_requires_an_http_base_url() {
        assert!(HttpHomeserverClient::new("ftp://homeserver.example").is_err());
        assert!(HttpHomeserverClient::new("https://homeserver.example/").is_ok());
    }
}
