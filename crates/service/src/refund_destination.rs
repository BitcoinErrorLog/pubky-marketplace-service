//! The refund destination of a USDT order: the Arbitrum One address the buyer
//! confirmed for the seller's refund, and the rules a recorded USDT refund
//! follows.
//!
//! The marketplace never holds funds and does not assume the address a USDT
//! payment came from can take a refund (the buyer may have paid from an
//! exchange). The buyer confirms an address on the order
//! (`refund.confirm_destination`), the seller sends the refund from their own
//! wallet, and records the Arbitrum transaction hash. Nothing here verifies
//! the refund on Arbitrum: it is a record of what the seller said they did.
//!
//! Everything in this module applies to orders paid with the `usdt` method
//! only; an order of any other method never reads or writes it.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sha3::{Digest, Keccak256};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::clock::format_timestamp;
use crate::model::OrderRow;

/// The `orders.payment_method` of an order paid in USDT.
pub const USDT_PAYMENT_METHOD: &str = "usdt";
/// What a USDT refund is sent in.
pub const REFUND_ASSET: &str = "USDT";
/// Where a USDT refund settles.
pub const REFUND_NETWORK: &str = "arbitrum-one";
/// The only address source today: the buyer typed it.
pub const SOURCE_BUYER_ENTERED: &str = "buyer_entered";

/// Whether the order was paid with USDT.
pub fn is_usdt_order(order: &OrderRow) -> bool {
    order.payment_method.as_deref() == Some(USDT_PAYMENT_METHOD)
}

fn hex_body(address: &str) -> Option<&str> {
    let body = address.strip_prefix("0x")?;
    (body.len() == 40 && body.bytes().all(|byte| byte.is_ascii_hexdigit())).then_some(body)
}

/// The EIP-55 mixed-case form of 40 hex digits: a letter is upper-case when
/// the matching nibble of the Keccak-256 of the lower-case digits is 8 or
/// more.
fn eip55_checksum(body: &str) -> String {
    let lower = body.to_ascii_lowercase();
    let digest = Keccak256::digest(lower.as_bytes());
    lower
        .char_indices()
        .map(|(index, digit)| {
            let nibble = if index % 2 == 0 {
                digest[index / 2] >> 4
            } else {
                digest[index / 2] & 0x0f
            };
            if digit.is_ascii_alphabetic() && nibble >= 8 {
                digit.to_ascii_uppercase()
            } else {
                digit
            }
        })
        .collect()
}

/// Whether `input` is an Arbitrum One address: `0x` and 40 hex digits. An
/// all-lower-case or all-upper-case address carries no checksum and is
/// accepted; a mixed-case one must match its EIP-55 checksum, because a
/// wrong-case digit is how a typo is caught.
pub fn is_valid_address(input: &str) -> bool {
    let Some(body) = hex_body(input) else {
        return false;
    };
    let has_lower = body.bytes().any(|byte| byte.is_ascii_lowercase());
    let has_upper = body.bytes().any(|byte| byte.is_ascii_uppercase());
    !(has_lower && has_upper) || body == eip55_checksum(body)
}

/// Whether `input` is an Arbitrum transaction hash: `0x` and 64 lower-case
/// hex digits.
pub fn is_transaction_hash(input: &str) -> bool {
    input.strip_prefix("0x").is_some_and(|body| {
        body.len() == 64
            && body
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

/// The buyer-confirmed destination of one order.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RefundDestinationRow {
    pub order_id: Uuid,
    pub buyer_pubky: String,
    pub asset: String,
    pub network: String,
    pub address: String,
    pub address_source: String,
    pub confirmed_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

const DESTINATION_COLUMNS: &str =
    "order_id, buyer_pubky, asset, network, address, address_source, confirmed_at, updated_at";

impl RefundDestinationRow {
    /// The participant projection: the address, where it settles, who chose
    /// it, and when the buyer last confirmed it. Never in a public
    /// projection and never logged.
    pub fn view(&self) -> Value {
        json!({
            "address": self.address,
            "network": self.network,
            "asset": self.asset,
            "source": self.address_source,
            "confirmed_at": format_timestamp(self.confirmed_at),
        })
    }
}

/// The order's destination, if the buyer confirmed one.
pub async fn fetch(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
) -> Result<Option<RefundDestinationRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {DESTINATION_COLUMNS} FROM order_refund_destinations WHERE order_id = $1"
    ))
    .bind(order_id)
    .fetch_optional(&mut **tx)
    .await
}

/// Stores the buyer's address, replacing an earlier one. The caller holds
/// the order row lock.
pub async fn upsert(
    tx: &mut Transaction<'_, Postgres>,
    order_id: Uuid,
    buyer_pubky: &str,
    address: &str,
    now: DateTime<Utc>,
) -> Result<RefundDestinationRow, sqlx::Error> {
    sqlx::query_as(&format!(
        "INSERT INTO order_refund_destinations \
         (order_id, buyer_pubky, asset, network, address, address_source, confirmed_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $7) \
         ON CONFLICT (order_id) DO UPDATE SET address = EXCLUDED.address, \
         address_source = EXCLUDED.address_source, confirmed_at = EXCLUDED.confirmed_at, \
         updated_at = EXCLUDED.updated_at \
         RETURNING {DESTINATION_COLUMNS}"
    ))
    .bind(order_id)
    .bind(buyer_pubky)
    .bind(REFUND_ASSET)
    .bind(REFUND_NETWORK)
    .bind(address)
    .bind(SOURCE_BUYER_ENTERED)
    .bind(now)
    .fetch_one(&mut **tx)
    .await
}

/// Stamps `refund_destination` onto the participant views of USDT orders:
/// the destination, or `null` until the buyer confirms one. Orders of every
/// other method get no key, so their projections are unchanged. The views
/// are matched to orders by `id`.
pub async fn attach_to_views(
    pool: &PgPool,
    orders: &[OrderRow],
    views: &mut [Value],
) -> Result<(), sqlx::Error> {
    let usdt_ids: Vec<Uuid> = orders
        .iter()
        .filter(|order| is_usdt_order(order))
        .map(|order| order.id)
        .collect();
    if usdt_ids.is_empty() {
        return Ok(());
    }
    let rows: Vec<RefundDestinationRow> = sqlx::query_as(&format!(
        "SELECT {DESTINATION_COLUMNS} FROM order_refund_destinations WHERE order_id = ANY($1)"
    ))
    .bind(&usdt_ids)
    .fetch_all(pool)
    .await?;
    let destinations: HashMap<Uuid, Value> =
        rows.iter().map(|row| (row.order_id, row.view())).collect();
    for view in views {
        let Some(id) = view["id"].as_str().and_then(|id| Uuid::parse_str(id).ok()) else {
            continue;
        };
        if usdt_ids.contains(&id) {
            view["refund_destination"] = destinations.get(&id).cloned().unwrap_or(Value::Null);
        }
    }
    Ok(())
}

/// The USDT amount the order was quoted, in order minor units (cents for a
/// USD order): what a refund of the whole payment is. `None` when the order
/// carries no USDT terms or the quote is not a whole number of order units.
pub fn quoted_refund_amount_minor(order: &OrderRow) -> Option<i64> {
    let sent = order.payment_amount_minor?;
    let sent_exponent = i32::from(order.payment_exponent?);
    let steps = u32::try_from(sent_exponent.checked_sub(order.exponent)?).ok()?;
    let divisor = 10_i64.checked_pow(steps)?;
    (sent % divisor == 0).then_some(sent / divisor)
}

#[cfg(test)]
mod tests {
    use super::*;

    // The EIP-55 reference vectors (EIP-55, "Test Cases").
    const CHECKSUMMED: [&str; 8] = [
        "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed",
        "0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359",
        "0xdbF03B407c01E7cD3CBea99509d93f8DDDC8C6FB",
        "0xD1220A0cf47c7B9Be7A2E6BA89F429762e7b9aDb",
        "0x52908400098527886E0F7030069857D2E4169EE7",
        "0x8617E340B3D01FA5F11F306F4090FD50E238070D",
        "0xde709f2102306220921060314715629080e2fb77",
        "0x27b1fdb04752bbc536007a920d24acb045561c26",
    ];

    #[test]
    fn eip55_reference_vectors_validate() {
        for address in CHECKSUMMED {
            assert!(is_valid_address(address), "{address}");
        }
    }

    #[test]
    fn a_wrong_case_digit_in_a_mixed_case_address_is_refused() {
        for address in &CHECKSUMMED[..6] {
            for (index, character) in address.char_indices().skip(2) {
                if !character.is_ascii_alphabetic() {
                    continue;
                }
                let flipped = if character.is_ascii_uppercase() {
                    character.to_ascii_lowercase()
                } else {
                    character.to_ascii_uppercase()
                };
                let mut typo = address.to_string();
                typo.replace_range(index..=index, &flipped.to_string());
                assert!(!is_valid_address(&typo), "{typo}");
            }
        }
    }

    #[test]
    fn single_case_addresses_carry_no_checksum() {
        assert!(is_valid_address(
            "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed"
        ));
        assert!(is_valid_address(
            "0x5AAEB6053F3E94C9B9A09F33669435E7EF1BEAED"
        ));
        assert!(is_valid_address(&format!("0x{}", "1".repeat(40))));
    }

    #[test]
    fn malformed_addresses_are_refused() {
        for address in [
            "",
            "0x",
            "5aaeb6053f3e94c9b9a09f33669435e7ef1beaed",
            "0X5aaeb6053f3e94c9b9a09f33669435e7ef1beaed",
            "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beae",
            "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaedd",
            "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaeg",
            " 0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed",
            "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed\n",
            "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq",
        ] {
            assert!(!is_valid_address(address), "{address:?}");
        }
    }

    #[test]
    fn transaction_hashes_are_66_lower_case_hex() {
        assert!(is_transaction_hash(&format!("0x{}", "ab12".repeat(16))));
        for hash in [
            String::new(),
            "0x".to_string(),
            "ab12".repeat(16),
            format!("0x{}", "ab12".repeat(15)),
            format!("0x{}", "ab12".repeat(17)),
            format!("0x{}", "AB12".repeat(16)),
            format!("0x{}g", "ab12".repeat(15) + "ab1"),
            format!(" 0x{}", "ab12".repeat(16)),
        ] {
            assert!(!is_transaction_hash(&hash), "{hash:?}");
        }
    }
}
