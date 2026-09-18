use serde_json::Value;

pub const FORBIDDEN_RESERVE_KEYS: [&str; 4] =
    ["reserve_price", "reservePrice", "reserve_met", "reserveMet"];
pub const SELLER_ONLY_RESERVE_METADATA_KEYS: [&str; 2] =
    ["reserve_record_revision", "last_reserve_command_id"];

pub fn is_reserve_audience_key(key: &str) -> bool {
    FORBIDDEN_RESERVE_KEYS.contains(&key) || SELLER_ONLY_RESERVE_METADATA_KEYS.contains(&key)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForbiddenReserveKey;

impl std::fmt::Display for ForbiddenReserveKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a forbidden reserve field is present")
    }
}

impl std::error::Error for ForbiddenReserveKey {}

/// Checks raw JSON before normalization or typed parsing. Objects nested in
/// arrays are traversed, and a forbidden key is rejected regardless of its
/// value (including null or false).
pub fn ensure_reserve_free(value: &Value) -> Result<(), ForbiddenReserveKey> {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                if is_reserve_audience_key(key) {
                    return Err(ForbiddenReserveKey);
                }
                ensure_reserve_free(value)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                ensure_reserve_free(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// HTTP command-result audience guard. Seller reserve fields are legal only
/// as direct members of an object whose `seller_pubky` exactly matches the
/// authenticated actor. Nested reserve fields and camelCase aliases are
/// always forbidden.
pub fn ensure_actor_reserve_audience(
    value: &Value,
    actor: &str,
) -> Result<(), ForbiddenReserveKey> {
    match value {
        Value::Object(object) => {
            let seller_object = object.get("seller_pubky").and_then(Value::as_str) == Some(actor);
            for (key, value) in object {
                if is_reserve_audience_key(key)
                    && !(seller_object
                        && matches!(
                            key.as_str(),
                            "reserve_price"
                                | "reserve_met"
                                | "reserve_record_revision"
                                | "last_reserve_command_id"
                        ))
                {
                    return Err(ForbiddenReserveKey);
                }
                ensure_actor_reserve_audience(value, actor)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                ensure_actor_reserve_audience(value, actor)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ensure_actor_reserve_audience, ensure_reserve_free};
    use serde_json::json;

    #[test]
    fn recursive_guard_rejects_every_spelling_and_value_shape() {
        for value in [
            json!({"reserve_price": null}),
            json!({"nested": {"reservePrice": false}}),
            json!({"items": [{"reserve_met": null}]}),
            json!([{"deep": [{"reserveMet": false}]}]),
            json!({"reserve_record_revision": 1}),
            json!({"nested": {"last_reserve_command_id": null}}),
        ] {
            assert!(ensure_reserve_free(&value).is_err());
        }
        ensure_reserve_free(&json!({"reservation": {"price": null}}))
            .expect("unrelated public keys remain valid");
    }

    #[test]
    fn actor_guard_allows_only_direct_matching_seller_fields() {
        ensure_actor_reserve_audience(
            &json!({
                "seller_pubky": "seller",
                "reserve_price": null,
                "reserve_met": false,
                "reserve_record_revision": 1,
                "last_reserve_command_id": null,
                "auction": {"status": "sold"}
            }),
            "seller",
        )
        .expect("matching seller projection is allowed");
        for bad in [
            json!({"seller_pubky": "other", "reserve_price": null}),
            json!({"seller_pubky": "seller", "auction": {"reserve_met": false}}),
            json!({"seller_pubky": "seller", "reservePrice": null}),
            json!({"seller_pubky": "other", "reserve_record_revision": 1}),
            json!({"seller_pubky": "seller", "nested": {"last_reserve_command_id": null}}),
        ] {
            assert!(ensure_actor_reserve_audience(&bad, "seller").is_err());
        }
    }
}
