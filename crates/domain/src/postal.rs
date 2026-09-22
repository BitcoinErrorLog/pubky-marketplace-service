//! Canonical postal-region rules for `delivery_address.region`.
//!
//! The machine-readable table is `contracts/postal-region-rules.json`. Shop
//! currently duplicates it in `src/libs/commerce/postal-address.ts`; the
//! follow-up is to consume this file so the client and this service cannot
//! drift. Region is required only where a postal system uses a subdivision.
//! US/CA/AU store ISO 3166-2 suffixes (`NY`, `ON`, `NSW`). Shop sends those
//! codes; full names are accepted for one release and normalized to the
//! suffix, with a deprecation log line (no address values).

use serde::Deserialize;
use std::collections::HashMap;
use std::sync::OnceLock;

const POSTAL_REGION_RULES_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../contracts/postal-region-rules.json"
));

#[derive(Debug, Clone, Deserialize)]
struct Subdivision {
    code: String,
    name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct CountryOverride {
    #[serde(default)]
    region_required: bool,
    #[serde(default)]
    region_label: Option<String>,
    #[serde(default)]
    subdivisions: Vec<Subdivision>,
}

#[derive(Debug, Clone, Deserialize)]
struct DefaultRule {
    region_required: bool,
    region_label: String,
}

#[derive(Debug, Deserialize)]
struct PostalRegionRulesFile {
    version: u32,
    region_max_chars: usize,
    default: DefaultRule,
    countries: HashMap<String, CountryOverride>,
}

#[derive(Debug, Clone)]
pub struct PostalCountryRule {
    pub region_required: bool,
    pub region_label: String,
    pub subdivisions: Vec<(String, String)>,
}

struct PostalRegionRules {
    region_max_chars: usize,
    default: PostalCountryRule,
    countries: HashMap<String, PostalCountryRule>,
}

fn rules() -> &'static PostalRegionRules {
    static RULES: OnceLock<PostalRegionRules> = OnceLock::new();
    RULES.get_or_init(|| {
        let file: PostalRegionRulesFile = serde_json::from_str(POSTAL_REGION_RULES_JSON)
            .expect("contracts/postal-region-rules.json must parse");
        assert_eq!(file.version, 1, "unsupported postal-region-rules version");
        let default = PostalCountryRule {
            region_required: file.default.region_required,
            region_label: file.default.region_label.clone(),
            subdivisions: Vec::new(),
        };
        let countries = file
            .countries
            .into_iter()
            .map(|(code, override_rule)| {
                let rule = PostalCountryRule {
                    region_required: override_rule.region_required,
                    region_label: override_rule
                        .region_label
                        .unwrap_or_else(|| file.default.region_label.clone()),
                    subdivisions: override_rule
                        .subdivisions
                        .into_iter()
                        .map(|item| (item.code, item.name))
                        .collect(),
                };
                (code, rule)
            })
            .collect();
        PostalRegionRules {
            region_max_chars: file.region_max_chars,
            default,
            countries,
        }
    })
}

pub fn region_max_chars() -> usize {
    rules().region_max_chars
}

pub fn postal_country_rule(country_code: &str) -> PostalCountryRule {
    let country = country_code.trim().to_ascii_uppercase();
    rules()
        .countries
        .get(&country)
        .cloned()
        .unwrap_or_else(|| rules().default.clone())
}

pub fn is_region_required(country_code: &str) -> bool {
    postal_country_rule(country_code).region_required
}

/// Normalize a region for storage.
///
/// Empty is allowed only when the country does not require a subdivision.
/// Closed-list countries (US/CA/AU) accept the ISO suffix, or the full name
/// for one release (normalized to the suffix). Never returns the input when
/// a closed list applies — unknown values are `Err`.
pub fn canonicalize_region(country_code: &str, region: &str) -> Result<String, &'static str> {
    let trimmed = region.trim();
    let rule = postal_country_rule(country_code);
    let max = region_max_chars();
    if trimmed.chars().count() > max {
        return Err("Expected at most 100 characters");
    }
    if trimmed.is_empty() {
        if rule.region_required {
            return Err("Expected between 1 and 100 characters");
        }
        return Ok(String::new());
    }
    if rule.subdivisions.is_empty() {
        return Ok(trimmed.to_string());
    }
    let upper = trimmed.to_ascii_uppercase();
    if let Some((code, _)) = rule
        .subdivisions
        .iter()
        .find(|(code, _)| code.eq_ignore_ascii_case(&upper))
    {
        return Ok(code.clone());
    }
    let lower = trimmed.to_ascii_lowercase();
    if let Some((code, _)) = rule
        .subdivisions
        .iter()
        .find(|(_, name)| name.to_ascii_lowercase() == lower)
    {
        tracing::warn!(
            country = %country_code.trim().to_ascii_uppercase(),
            "delivery_address.region used a subdivision name; ISO 3166-2 suffix is the contract (names accepted for one release)"
        );
        return Ok(code.clone());
    }
    Err("Expected an ISO 3166-2 subdivision suffix")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_parses_and_matches_shop_required_set() {
        let required: Vec<_> = {
            let rules = rules();
            let mut codes: Vec<_> = rules
                .countries
                .iter()
                .filter(|(_, rule)| rule.region_required)
                .map(|(code, _)| code.as_str())
                .collect();
            codes.sort_unstable();
            codes
        };
        assert_eq!(
            required,
            [
                "AE", "AR", "AU", "BD", "BR", "CA", "CL", "CN", "CO", "EG", "ID", "IN", "KR", "MX",
                "MY", "NG", "PE", "PH", "PK", "SA", "TH", "TR", "US", "VN", "ZA"
            ]
        );
        assert!(!is_region_required("GB"));
        assert!(!is_region_required("DE"));
        assert!(!is_region_required("JP"));
        assert!(!is_region_required("NL"));
        assert!(!is_region_required("FR"));
        assert_eq!(postal_country_rule("US").region_label, "State");
        assert_eq!(postal_country_rule("CA").region_label, "Province");
        assert_eq!(postal_country_rule("JP").region_label, "Prefecture");
        assert_eq!(postal_country_rule("US").subdivisions.len(), 59);
        assert_eq!(postal_country_rule("CA").subdivisions.len(), 13);
        assert_eq!(postal_country_rule("AU").subdivisions.len(), 8);
        assert_eq!(region_max_chars(), 100);
    }

    #[test]
    fn closed_list_accepts_codes_and_names() {
        assert_eq!(canonicalize_region("US", "NY").unwrap(), "NY");
        assert_eq!(canonicalize_region("us", " ma ").unwrap(), "MA");
        assert_eq!(canonicalize_region("US", "Massachusetts").unwrap(), "MA");
        assert_eq!(canonicalize_region("CA", "ON").unwrap(), "ON");
        assert_eq!(canonicalize_region("CA", "Ontario").unwrap(), "ON");
        assert_eq!(canonicalize_region("AU", "NSW").unwrap(), "NSW");
        assert_eq!(canonicalize_region("AU", "New South Wales").unwrap(), "NSW");
        assert_eq!(
            canonicalize_region("US", "XX").unwrap_err(),
            "Expected an ISO 3166-2 subdivision suffix"
        );
        assert_eq!(
            canonicalize_region("US", "").unwrap_err(),
            "Expected between 1 and 100 characters"
        );
    }

    #[test]
    fn optional_countries_allow_empty_region() {
        assert_eq!(canonicalize_region("GB", "").unwrap(), "");
        assert_eq!(canonicalize_region("GB", "  ").unwrap(), "");
        assert_eq!(canonicalize_region("DE", "Bayern").unwrap(), "Bayern");
        assert_eq!(canonicalize_region("JP", "").unwrap(), "");
        assert_eq!(canonicalize_region("BR", "SP").unwrap(), "SP");
        assert_eq!(
            canonicalize_region("BR", "").unwrap_err(),
            "Expected between 1 and 100 characters"
        );
        assert_eq!(
            canonicalize_region("MX", "").unwrap_err(),
            "Expected between 1 and 100 characters"
        );
        assert_eq!(
            canonicalize_region("IN", "").unwrap_err(),
            "Expected between 1 and 100 characters"
        );
    }
}
