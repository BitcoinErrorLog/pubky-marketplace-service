//! Strict typed wire mirror of the upstream Locks content-lock schema and
//! identity derivation, pinned to `pubky/locks@ba49a777`.
//!
//! Upstream validates a fetched content-lock document by decoding it into
//! the typed `ContentLock` — every nested type is
//! `#[serde(deny_unknown_fields)]` with required version, resources,
//! criteria, lock logic, access policy, lock-server configuration, and
//! timestamp — and then hashes the TYPED value's canonical serialization
//! (`locks-sdk/src/discovery.rs:37-53`;
//! `locks-core/src/lock_policy.rs:190-215`). Hashing the raw fetched JSON
//! instead would accept documents upstream rejects (an attacker chooses
//! both the document and its advertised content address, so no BLAKE3
//! preimage is needed). Every struct below mirrors its upstream counterpart
//! field-for-field, including `skip_serializing_if`/default behavior, with
//! the upstream citation in its doc comment. The Paykit v1 policy
//! invariants (`locks-core/src/lock_policy.rs:146-188,380-425`) — which
//! upstream enforces when a content lock is created and when a payment
//! submission is validated
//! (`locks-service/src/application/use_cases/create_content_lock.rs:86`,
//! `.../validate_paykit_payment_submission.rs:47`) — are mirrored with the
//! same rejection vocabulary.

use std::collections::BTreeMap;

use base32::Alphabet;
use chrono::{DateTime, FixedOffset};
use marketplace_domain::commands::{canonical_lock_resource, LOCKS_CONTENT_LOCK_PREFIX};
use pubky_common::crypto::PublicKey;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Mirrors `pubky/locks@ba49a777:locks-core/src/ids.rs:249-327`
/// (`CreatorPubky`/`LockServerPubky`) and `:510-514`
/// (`parse_pubky_identity`): a Pubky public key parsed from either the
/// `pubky<z32>` or bare-z32 form and normalized to the `pubky<z32>`
/// rendering, which is also its serialized form. Equality is key identity,
/// exactly as upstream compares `pubky::PublicKey` values.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PubkyIdentity(String);

impl PubkyIdentity {
    pub fn parse(value: &str) -> Option<Self> {
        PublicKey::try_from(value)
            .ok()
            .map(|key| Self(key.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PubkyIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for PubkyIdentity {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for PubkyIdentity {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).ok_or_else(|| serde::de::Error::custom("invalid Pubky identity"))
    }
}

/// Mirrors `pubky/locks@ba49a777:locks-core/src/ids.rs:173-215`
/// (`GuardedResourceHash`): a 32-byte hash carried as Crockford base32.
/// Deserialization accepts any Crockford decoding of exactly 32 bytes;
/// serialization is the canonical uppercase encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GuardedResourceHash([u8; 32]);

impl Serialize for GuardedResourceHash {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&base32::encode(Alphabet::Crockford, &self.0))
    }
}

impl<'de> Deserialize<'de> for GuardedResourceHash {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        let decoded = base32::decode(Alphabet::Crockford, &value)
            .filter(|decoded| decoded.len() == 32)
            .ok_or_else(|| {
                serde::de::Error::custom("guarded resource hash is not valid Crockford base32")
            })?;
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&decoded);
        Ok(Self(bytes))
    }
}

/// Mirrors the `time::serde::rfc3339` serde adapter upstream applies to
/// `ContentLock.created_at`
/// (`pubky/locks@ba49a777:locks-core/src/lock_policy.rs:56-58`): parsing is
/// strict RFC 3339 (uppercase `T`, `Z` or numeric offset), and formatting
/// renders UTC as `Z` with fractional seconds trimmed of trailing zeros
/// (absent when zero), matching `time`'s well-known Rfc3339 behavior.
mod rfc3339 {
    use chrono::{DateTime, FixedOffset, SecondsFormat, Timelike};
    use serde::{Deserialize, Serialize};

    pub fn serialize<S: serde::Serializer>(
        value: &DateTime<FixedOffset>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let rendered = if value.offset().local_minus_utc() == 0 {
            let utc = value.naive_utc();
            if utc.nanosecond() == 0 {
                utc.format("%Y-%m-%dT%H:%M:%SZ").to_string()
            } else {
                let fraction = format!(".{:09}", utc.nanosecond());
                format!(
                    "{}{}Z",
                    utc.format("%Y-%m-%dT%H:%M:%S"),
                    fraction.trim_end_matches('0')
                )
            }
        } else if value.nanosecond() == 0 {
            value.to_rfc3339_opts(SecondsFormat::Secs, false)
        } else {
            let fraction = format!(".{:09}", value.nanosecond());
            format!(
                "{}{}{}",
                value.format("%Y-%m-%dT%H:%M:%S"),
                fraction.trim_end_matches('0'),
                value.offset()
            )
        };
        rendered.serialize(serializer)
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<DateTime<FixedOffset>, D::Error> {
        let value = String::deserialize(deserializer)?;
        let bytes = value.as_bytes();
        // `time`'s Rfc3339 parser is strict ABNF: uppercase `T` separator,
        // uppercase `Z`, full second precision. chrono is more lenient, so
        // pin the shape before delegating.
        let strict_shape = bytes.len() >= 20
            && bytes[4] == b'-'
            && bytes[7] == b'-'
            && bytes[10] == b'T'
            && bytes[13] == b':'
            && bytes[16] == b':'
            && !value.ends_with('z');
        if !strict_shape {
            return Err(serde::de::Error::custom("invalid RFC 3339 timestamp"));
        }
        DateTime::parse_from_rfc3339(&value).map_err(serde::de::Error::custom)
    }
}

/// Mirrors `pubky/locks@ba49a777:locks-core/src/lock_policy.rs:217-229`
/// (`GuardedResource`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct GuardedResource {
    pub path: String,
    pub hash: GuardedResourceHash,
    pub content_type: String,
    pub size: u64,
}

/// Mirrors `pubky/locks@ba49a777:locks-core/src/lock_policy.rs:267-277`
/// (`SecondaryGuardedResource`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SecondaryGuardedResource {
    pub hash: GuardedResourceHash,
    pub content_type: String,
    pub size: u64,
}

/// Mirrors `pubky/locks@ba49a777:locks-core/src/lock_policy.rs:306-314`
/// (`VerifierType`): the kebab-case wire vocabulary is closed; an unknown
/// verifier fails the decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum VerifierType {
    DevStatic,
    PaykitPayment,
}

/// Mirrors `pubky/locks@ba49a777:locks-core/src/lock_policy.rs:358-368`
/// (`Criterion`): `params` stays an opaque `Value`, verbatim in the typed
/// serialization, exactly as upstream carries it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Criterion {
    pub criterion_id: String,
    pub verifier_type: VerifierType,
    pub params: Value,
}

/// Mirrors `pubky/locks@ba49a777:locks-core/src/lock_policy.rs:428-442`
/// (`LockLogic`): internally tagged on `type` (`all`/`any`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", deny_unknown_fields)]
pub enum LockLogic {
    All { criteria: Vec<String> },
    Any { criteria: Vec<String> },
}

/// Mirrors `pubky/locks@ba49a777:locks-core/src/lock_policy.rs:444-450`
/// (`AccessPolicy`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct AccessPolicy {
    pub requested_credential_ttl_seconds: u64,
}

/// Mirrors `pubky/locks@ba49a777:locks-core/src/lock_policy.rs:452-459`
/// (`LockServerConfig`): the `override` key is required and may be null.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct LockServerConfig {
    #[serde(rename = "override")]
    pub override_: Option<PubkyIdentity>,
}

/// Mirrors `pubky/locks@ba49a777:locks-core/src/lock_policy.rs:34-59`
/// (`ContentLock`): required fields, closed field set, and identical
/// omission/default behavior (`primary_resource` omitted when null,
/// `secondary_resources` omitted when empty), so the typed canonical
/// serialization is byte-identical to upstream's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ContentLock {
    pub version: u16,
    pub creator: PubkyIdentity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_resource: Option<GuardedResource>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secondary_resources: BTreeMap<String, SecondaryGuardedResource>,
    pub criteria: Vec<Criterion>,
    pub lock_logic: LockLogic,
    pub access_policy: AccessPolicy,
    pub lock_server: LockServerConfig,
    #[serde(with = "rfc3339")]
    pub created_at: DateTime<FixedOffset>,
}

impl ContentLock {
    /// Mirrors `pubky/locks@ba49a777:locks-core/src/lock_policy.rs:190-198`
    /// (`canonical_json_bytes`/`canonical_json_string`): RFC 8785 canonical
    /// bytes of the TYPED value.
    pub fn canonical_json_bytes(&self) -> Option<Vec<u8>> {
        serde_json_canonicalizer::to_vec(self).ok()
    }

    /// Mirrors `pubky/locks@ba49a777:locks-core/src/lock_policy.rs:200-209`
    /// (`lock_hash`/`lock_id`) and `locks-core/src/ids.rs:57-60`
    /// (`LockId::from_hash`): BLAKE3 over the typed canonical bytes,
    /// Crockford-base32 uppercase.
    pub fn lock_id(&self) -> Option<String> {
        let canonical = self.canonical_json_bytes()?;
        Some(base32::encode(
            Alphabet::Crockford,
            blake3::hash(&canonical).as_bytes(),
        ))
    }

    /// Mirrors `pubky/locks@ba49a777:locks-core/src/lock_policy.rs:211-214`
    /// (`content_lock_path`) and `locks-core/src/ids.rs:329-358`
    /// (`ContentLockPath` display): `/pub/locks.app/<lock_id>.json`.
    pub fn content_lock_path(&self) -> Option<String> {
        Some(format!("/pub/locks.app/{}.json", self.lock_id()?))
    }

    /// Mirrors
    /// `pubky/locks@ba49a777:locks-core/src/lock_policy.rs:146-188`
    /// (`validate_paykit_payment_v1_policy`): when any criterion is
    /// `paykit-payment`, it must be the ONLY criterion, its params must be
    /// exactly `{recipient_pubky, amount, asset}`, the recipient must be
    /// the lock creator, and the lock logic must reference exactly that
    /// criterion.
    pub fn validate_paykit_payment_v1_policy(&self) -> Result<(), PaykitPaymentPolicyRejection> {
        let Some(payment_criterion) = self
            .criteria
            .iter()
            .find(|criterion| criterion.verifier_type == VerifierType::PaykitPayment)
        else {
            return Ok(());
        };
        if self.criteria.len() != 1 {
            return Err(PaykitPaymentPolicyRejection::MustBeOnlyCriterion);
        }
        validate_paykit_payment_params(&payment_criterion.params)
            .map_err(PaykitPaymentPolicyRejection::InvalidParams)?;
        let recipient = payment_criterion
            .params
            .get("recipient_pubky")
            .and_then(Value::as_str)
            .and_then(PubkyIdentity::parse)
            .ok_or(PaykitPaymentPolicyRejection::InvalidParams(
                PaykitPaymentParamsRejection::InvalidRecipientPubky,
            ))?;
        if recipient != self.creator {
            return Err(PaykitPaymentPolicyRejection::RecipientMustMatchCreator);
        }
        let logic_criteria = match &self.lock_logic {
            LockLogic::All { criteria } | LockLogic::Any { criteria } => criteria,
        };
        if logic_criteria.len() != 1 || logic_criteria[0] != payment_criterion.criterion_id {
            return Err(PaykitPaymentPolicyRejection::InvalidLockLogic {
                criterion_id: payment_criterion.criterion_id.clone(),
            });
        }
        Ok(())
    }
}

/// Mirrors
/// `pubky/locks@ba49a777:locks-core/src/lock_policy.rs:325-339`
/// (`PaykitPaymentParamsValidationError`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaykitPaymentParamsRejection {
    NotObject,
    MissingField(&'static str),
    UnknownField(String),
    InvalidRecipientPubky,
    InvalidAmount,
    InvalidAsset,
}

/// Mirrors
/// `pubky/locks@ba49a777:locks-core/src/lock_policy.rs:341-356`
/// (`PaykitPaymentPolicyValidationError`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaykitPaymentPolicyRejection {
    MustBeOnlyCriterion,
    InvalidParams(PaykitPaymentParamsRejection),
    RecipientMustMatchCreator,
    InvalidLockLogic { criterion_id: String },
}

/// Mirrors
/// `pubky/locks@ba49a777:locks-core/src/lock_policy.rs:380-425`
/// (`validate_paykit_payment_params`): exactly the three known fields,
/// recipient a valid Pubky key, amount a positive decimal integer string,
/// asset a non-empty string.
fn validate_paykit_payment_params(params: &Value) -> Result<(), PaykitPaymentParamsRejection> {
    let object = params
        .as_object()
        .ok_or(PaykitPaymentParamsRejection::NotObject)?;
    for key in object.keys() {
        if !matches!(key.as_str(), "recipient_pubky" | "amount" | "asset") {
            return Err(PaykitPaymentParamsRejection::UnknownField(key.clone()));
        }
    }
    let recipient_pubky = object
        .get("recipient_pubky")
        .and_then(Value::as_str)
        .ok_or(PaykitPaymentParamsRejection::MissingField(
            "recipient_pubky",
        ))?;
    if PubkyIdentity::parse(recipient_pubky).is_none() {
        return Err(PaykitPaymentParamsRejection::InvalidRecipientPubky);
    }
    let amount = object
        .get("amount")
        .ok_or(PaykitPaymentParamsRejection::MissingField("amount"))?
        .as_str()
        .ok_or(PaykitPaymentParamsRejection::InvalidAmount)?;
    if amount.is_empty()
        || !amount.bytes().all(|byte| byte.is_ascii_digit())
        || !amount.bytes().any(|byte| byte != b'0')
    {
        return Err(PaykitPaymentParamsRejection::InvalidAmount);
    }
    let asset = object
        .get("asset")
        .ok_or(PaykitPaymentParamsRejection::MissingField("asset"))?
        .as_str()
        .ok_or(PaykitPaymentParamsRejection::InvalidAsset)?;
    if asset.is_empty() {
        return Err(PaykitPaymentParamsRejection::InvalidAsset);
    }
    Ok(())
}

/// The marketplace-side rejection vocabulary of
/// [`validate_content_lock_value`], mirroring the upstream error mapping
/// (`pubky/locks@ba49a777:locks-sdk/src/discovery.rs:41-51`):
/// `InvalidDocument` ↔ `LocksSdkError::InvalidDiscoveryResponse`,
/// `CreatorMismatch` ↔ `ContentLockCreatorMismatch`, `PathMismatch` ↔
/// `ContentLockPathMismatch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentLockIdentityRejection {
    /// The advertised resource itself is not a canonical
    /// `pubky<creator>/pub/locks.app/<lock_id>.json`.
    InvalidResource,
    /// The document does not decode into the strict typed `ContentLock`.
    InvalidDocument,
    /// The document's creator is not the resource's creator.
    CreatorMismatch,
    /// The typed canonical serialization does not derive the advertised
    /// lock id.
    PathMismatch,
}

/// One parsed addressed lock resource: the normalized creator identity and
/// the normalized (uppercase Crockford) lock id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockResourceParts {
    pub creator: PubkyIdentity,
    pub lock_id: String,
}

/// Mirrors `pubky/locks@ba49a777:locks-core/src/ids.rs:461-484`
/// (`PubkyLockResource::from_str`), `:379-396`
/// (`ContentLockPath::from_str`), and `:486-508` (`parse_crockford_id`):
/// the lock id must be 52 Crockford characters decoding to 32 bytes
/// (normalized to canonical uppercase), and the creator must be a valid
/// Pubky key. The service additionally accepts the Shop client's
/// `pubky://` addressing of a Locks policy URI: the exact
/// exact `pubky://` prefix is canonicalized away first, and the
/// locks-core-mirroring checks below then apply — strict and unchanged —
/// to the bare remainder.
pub fn parse_lock_resource_typed(resource: &str) -> Option<LockResourceParts> {
    let canonical = canonical_lock_resource(resource)?;
    let (creator, path) = canonical.split_at(canonical.find(LOCKS_CONTENT_LOCK_PREFIX)?);
    let creator = PubkyIdentity::parse(creator)?;
    let lock_id = path
        .strip_prefix(LOCKS_CONTENT_LOCK_PREFIX)?
        .strip_suffix(".json")?;
    Some(LockResourceParts {
        creator,
        lock_id: lock_id.to_string(),
    })
}

/// Mirrors `pubky/locks@ba49a777:locks-sdk/src/discovery.rs:37-53`
/// (`validate_content_lock_value`): strict typed decode first, creator
/// comparison by key identity, then the content-address check against the
/// TYPED serialization's derived path.
pub fn validate_content_lock_value(
    document: &Value,
    expected_resource: &str,
) -> Result<ContentLock, ContentLockIdentityRejection> {
    let expected = parse_lock_resource_typed(expected_resource)
        .ok_or(ContentLockIdentityRejection::InvalidResource)?;
    let content_lock: ContentLock = serde_json::from_value(document.clone())
        .map_err(|_| ContentLockIdentityRejection::InvalidDocument)?;
    if content_lock.creator != expected.creator {
        return Err(ContentLockIdentityRejection::CreatorMismatch);
    }
    let actual_path = content_lock
        .content_lock_path()
        .ok_or(ContentLockIdentityRejection::InvalidDocument)?;
    if actual_path != format!("/pub/locks.app/{}.json", expected.lock_id) {
        return Err(ContentLockIdentityRejection::PathMismatch);
    }
    Ok(content_lock)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CREATOR: &str = "7jfgaa9nutjyixzikb7tgmsf9gkwq7iqz498zr1nd5ig1fng4esy";
    /// The typed-derived lock id of the pinned positive upstream vector
    /// (`5Z4F...json`, rendered by `pubky/locks@ba49a777`).
    const POSITIVE_LOCK_ID: &str = "5Z4FC0QEAFTTTE1DFH7DNZW2HVVPTDNJY5MERMD3Y0CQKH1P2SM0";

    fn fixture(name: &str) -> Value {
        let bytes = match name {
            "negative-malformed-schema.json" => {
                include_bytes!("../tests/fixtures/locks/negative-malformed-schema.json").as_slice()
            }
            "negative-unknown-top-level.json" => {
                include_bytes!("../tests/fixtures/locks/negative-unknown-top-level.json").as_slice()
            }
            "negative-unknown-paykit-param.json" => {
                include_bytes!("../tests/fixtures/locks/negative-unknown-paykit-param.json")
                    .as_slice()
            }
            "negative-invalid-lock-logic.json" => {
                include_bytes!("../tests/fixtures/locks/negative-invalid-lock-logic.json")
                    .as_slice()
            }
            "typed-reserialisation.json" => {
                include_bytes!("../tests/fixtures/locks/typed-reserialisation.json").as_slice()
            }
            _ => panic!("unknown fixture {name}"),
        };
        serde_json::from_slice(bytes)
            .unwrap_or_else(|error| panic!("fixture {name} parses: {error}"))
    }

    fn resource(lock_id: &str) -> String {
        format!("{CREATOR}/pub/locks.app/{lock_id}.json")
    }

    /// Every negative below was generated from the pinned positive vector
    /// and shown rejected by `pubky/locks@ba49a777` in the read-only
    /// reference clone (throwaway test, since removed); the upstream error
    /// variant is quoted on each assertion.

    // Upstream: `LocksSdkError::InvalidDiscoveryResponse`
    // (`locks-sdk/src/discovery.rs:41-42` — the strict typed decode fails
    // because `version` is a required field).
    #[test]
    fn malformed_schema_is_rejected_like_upstream() {
        assert_eq!(
            validate_content_lock_value(
                &fixture("negative-malformed-schema.json"),
                &resource(POSITIVE_LOCK_ID)
            ),
            Err(ContentLockIdentityRejection::InvalidDocument)
        );
    }

    // Upstream: `LocksSdkError::InvalidDiscoveryResponse` (the top-level
    // `lock_id` field violates `deny_unknown_fields`).
    #[test]
    fn an_unknown_top_level_field_is_rejected_like_upstream() {
        assert_eq!(
            validate_content_lock_value(
                &fixture("negative-unknown-top-level.json"),
                &resource(POSITIVE_LOCK_ID)
            ),
            Err(ContentLockIdentityRejection::InvalidDocument)
        );
    }

    // Upstream: discovery ACCEPTS this document at its own typed path, and
    // the Paykit v1 policy then rejects it with
    // `PaykitPaymentPolicyValidationError::InvalidParams(PaykitPaymentParamsValidationError::UnknownField("exponent"))`
    // (`locks-core/src/lock_policy.rs:380-425`).
    #[test]
    fn an_unknown_paykit_param_is_rejected_like_upstream() {
        const TYPED_LOCK_ID: &str = "RMFVM3N8PM1P6MZYCAKXRW3JXP6H3CDT1N4YPDS7NVJVQYYH4KAG";
        let lock = validate_content_lock_value(
            &fixture("negative-unknown-paykit-param.json"),
            &resource(TYPED_LOCK_ID),
        )
        .expect("the document is identity-valid at its own typed path");
        assert_eq!(
            lock.validate_paykit_payment_v1_policy(),
            Err(PaykitPaymentPolicyRejection::InvalidParams(
                PaykitPaymentParamsRejection::UnknownField("exponent".to_string())
            ))
        );
    }

    // Upstream: `PaykitPaymentPolicyValidationError::InvalidLockLogic {
    // criterion_id: "paykit" }` (`locks-core/src/lock_policy.rs:174-184` —
    // the sole payment criterion must be the sole `lock_logic` member).
    #[test]
    fn an_invalid_lock_logic_is_rejected_like_upstream() {
        const TYPED_LOCK_ID: &str = "4WP45JR79KYWWJ0EAAZ2ZSWYDRKAPGBQRE8C5FY308QPZ4ARCW30";
        let lock = validate_content_lock_value(
            &fixture("negative-invalid-lock-logic.json"),
            &resource(TYPED_LOCK_ID),
        )
        .expect("the document is identity-valid at its own typed path");
        assert_eq!(
            lock.validate_paykit_payment_v1_policy(),
            Err(PaykitPaymentPolicyRejection::InvalidLockLogic {
                criterion_id: "paykit".to_string()
            })
        );
    }

    // Typed-reserialisation: the document carries an optional field at its
    // default (`"secondary_resources": {}`), so its RAW canonical bytes
    // differ from its TYPED canonical bytes. Upstream decodes, then hashes
    // the typed form — and so does the mirror: the typed path is accepted
    // (it is the positive vector's id), the raw-derived path is not.
    #[test]
    fn the_mirror_hashes_the_typed_serialization_not_the_raw_bytes() {
        let document = fixture("typed-reserialisation.json");
        let raw_canonical =
            serde_json_canonicalizer::to_vec(&document).expect("raw canonical bytes");
        let raw_lock_id =
            base32::encode(Alphabet::Crockford, blake3::hash(&raw_canonical).as_bytes());
        assert_ne!(
            raw_lock_id, POSITIVE_LOCK_ID,
            "the raw canonical bytes must derive a DIFFERENT id"
        );
        let lock = validate_content_lock_value(&document, &resource(POSITIVE_LOCK_ID))
            .expect("the typed serialization derives the advertised path");
        assert_eq!(lock.lock_id().as_deref(), Some(POSITIVE_LOCK_ID));
        assert_eq!(
            validate_content_lock_value(&document, &resource(&raw_lock_id)),
            Err(ContentLockIdentityRejection::PathMismatch),
            "the raw-canonical content address is NOT the typed identity"
        );
    }

    // The Shop client addresses the lock as
    // `pubky://<creator>/pub/locks.app/<id>.json`: the service strips the
    // exact scheme prefix and the strict mirrored checks then see the bare
    // form, so both spellings parse to the same typed parts.
    #[test]
    fn the_typed_parse_accepts_the_shop_pubky_scheme_form() {
        let bare = resource(POSITIVE_LOCK_ID);
        let addressed = format!("pubky://{bare}");
        let bare_parts = parse_lock_resource_typed(&bare).expect("bare form parses");
        let addressed_parts = parse_lock_resource_typed(&addressed).expect("pubky:// form parses");
        assert_eq!(addressed_parts, bare_parts);
        assert_eq!(addressed_parts.lock_id, POSITIVE_LOCK_ID);
    }

    #[test]
    fn the_typed_parse_canonicalizes_a_lowercase_z32_lock_id() {
        let lowercase = resource(&POSITIVE_LOCK_ID.to_ascii_lowercase());
        let parts = parse_lock_resource_typed(&lowercase)
            .expect("a lowercase Crockford spelling identifies the same lock");
        assert_eq!(parts.lock_id, POSITIVE_LOCK_ID);
    }

    #[test]
    fn the_typed_parse_rejects_a_wrong_scheme() {
        assert_eq!(
            parse_lock_resource_typed(&format!("https://{}", resource(POSITIVE_LOCK_ID))),
            None,
            "only the exact pubky:// prefix is accepted"
        );
    }

    #[test]
    fn validation_accepts_both_canonical_resource_spellings() {
        let document = fixture("typed-reserialisation.json");
        let bare = resource(POSITIVE_LOCK_ID);
        let addressed = format!("pubky://{bare}");
        for expected in [&bare, &addressed] {
            let lock = validate_content_lock_value(&document, expected)
                .expect("the same document is valid under both spellings");
            assert_eq!(lock.lock_id().as_deref(), Some(POSITIVE_LOCK_ID));
        }
    }

    #[test]
    fn validation_rejects_a_wrong_path_under_the_pubky_scheme_spelling() {
        let document = fixture("typed-reserialisation.json");
        const WRONG_LOCK_ID: &str = "RMFVM3N8PM1P6MZYCAKXRW3JXP6H3CDT1N4YPDS7NVJVQYYH4KAG";
        assert_eq!(
            validate_content_lock_value(&document, &format!("pubky://{}", resource(WRONG_LOCK_ID))),
            Err(ContentLockIdentityRejection::PathMismatch),
            "a wrong id is still a path mismatch under the pubky:// spelling"
        );
    }

    #[test]
    fn validation_rejects_a_creator_mismatch_under_the_pubky_scheme_spelling() {
        let mut document = fixture("typed-reserialisation.json");
        document["creator"] = Value::String(format!("pubky{}", "y".repeat(52)));
        assert_eq!(
            validate_content_lock_value(
                &document,
                &format!("pubky://{}", resource(POSITIVE_LOCK_ID)),
            ),
            Err(ContentLockIdentityRejection::CreatorMismatch),
        );
    }
}
