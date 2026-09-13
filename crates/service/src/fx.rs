//! Production FX reference handling for USD-at-bind.
//!
//! The feed is deliberately narrow: only Blocktank's BTCUSD ticker is
//! accepted. The source contract is the live response captured in
//! `tests/fixtures/blocktank_fx_rates_btc.json` on 2026-09-13T16:19:07Z.
//! Paykit receives only the resulting satoshi amount; its amount contract is
//! pinned at paykit-server@67a03896b87b3df9bab78aba81ac4cfe93fca5de:
//! paykit-server/src/application/create_payment_request.rs.

use std::cmp::Ordering;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;

pub const FX_SOURCE: &str = "blocktank";
pub const FX_URL: &str = "https://api1.blocktank.to/api/fx/rates/btc";
pub const RATE_MIN_USD: u128 = 10_000;
pub const RATE_MAX_USD: u128 = 1_000_000;
pub const MAX_DEVIATION_BPS: u128 = 500;
pub const MAX_QUOTE_SATS: u128 = 100_000_000;
pub const MIN_QUOTE_SATS: u128 = 1_000;
pub const MAX_RATE_AGE_SECS: i64 = 300;
pub const MAX_FUTURE_SKEW_SECS: i64 = 60;
pub const REFERENCE_WINDOW_SECS: i64 = 3_600;
pub const REFERENCE_MIN_SAMPLES: usize = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decimal {
    pub mantissa: u128,
    pub scale: u32,
}

impl Decimal {
    pub fn parse(value: &str) -> Option<Self> {
        let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
        if fraction.len() > 38 || whole.len() > 38 {
            return None;
        }
        if whole.is_empty()
            || !whole.chars().all(|c| c.is_ascii_digit())
            || !fraction.chars().all(|c| c.is_ascii_digit())
        {
            return None;
        }
        let digits = format!("{whole}{fraction}");
        let mantissa = digits.parse().ok()?;
        Some(Self {
            mantissa,
            scale: fraction.len().try_into().ok()?,
        })
    }

    fn cmp(&self, other: &Self) -> Ordering {
        let scale = self.scale.max(other.scale);
        let left = self.mantissa * 10u128.pow(scale - self.scale);
        let right = other.mantissa * 10u128.pow(scale - other.scale);
        left.cmp(&right)
    }

    fn in_bounds(&self) -> bool {
        let min = Self {
            mantissa: RATE_MIN_USD,
            scale: 0,
        };
        let max = Self {
            mantissa: RATE_MAX_USD,
            scale: 0,
        };
        self.cmp(&min) != Ordering::Less && self.cmp(&max) != Ordering::Greater
    }

    pub fn to_string_value(&self) -> String {
        if self.scale == 0 {
            return self.mantissa.to_string();
        }
        let scale = self.scale as usize;
        let digits = format!("{:0width$}", self.mantissa, width = scale + 1);
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    }
}

#[derive(Debug, Deserialize)]
struct Feed {
    tickers: Vec<Ticker>,
}

#[derive(Debug, Deserialize)]
struct Ticker {
    symbol: String,
    #[serde(rename = "lastPrice")]
    last_price: String,
    base: String,
    quote: String,
    #[serde(rename = "lastUpdatedAt")]
    last_updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateSample {
    pub rate: Decimal,
    pub fetched_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FxError {
    Malformed,
    MissingReference,
    Stale,
    Future,
    OutOfBounds,
    DeviationExceeded,
    Overflow,
    BelowMinimum,
    CapExceeded,
}

pub fn parse_feed(body: &[u8], observed_at: DateTime<Utc>) -> Result<RateSample, FxError> {
    let feed: Feed = serde_json::from_slice(body).map_err(|_| FxError::Malformed)?;
    let ticker = feed
        .tickers
        .iter()
        .filter(|ticker| ticker.symbol == "BTCUSD" && ticker.base == "BTC" && ticker.quote == "USD")
        .collect::<Vec<_>>();
    if ticker.len() != 1 {
        return Err(FxError::Malformed);
    }
    let ticker = ticker[0];
    let updated_at =
        DateTime::from_timestamp_millis(ticker.last_updated_at).ok_or(FxError::Malformed)?;
    let age = observed_at.signed_duration_since(updated_at).num_seconds();
    if age > MAX_RATE_AGE_SECS {
        return Err(FxError::Stale);
    }
    if age < -MAX_FUTURE_SKEW_SECS {
        return Err(FxError::Future);
    }
    let rate = Decimal::parse(&ticker.last_price).ok_or(FxError::Malformed)?;
    if !rate.in_bounds() {
        return Err(FxError::OutOfBounds);
    }
    Ok(RateSample {
        rate,
        fetched_at: updated_at,
    })
}

pub fn median(mut rates: Vec<Decimal>) -> Option<Decimal> {
    if rates.len() < REFERENCE_MIN_SAMPLES {
        return None;
    }
    rates.sort_by(Decimal::cmp);
    Some(rates[rates.len() / 2].clone())
}

pub fn within_deviation(rate: &Decimal, reference: &Decimal) -> bool {
    let scale = rate.scale.max(reference.scale);
    let rate_value = rate.mantissa * 10u128.pow(scale - rate.scale);
    let reference_value = reference.mantissa * 10u128.pow(scale - reference.scale);
    let difference = rate_value.abs_diff(reference_value);
    difference * 10_000 <= reference_value * MAX_DEVIATION_BPS
}

/// Converts fiat minor units into satoshis with checked integer arithmetic:
/// ceil(total_minor * 100_000_000 / (10^exponent * BTCUSD)).
pub fn quote_sats(total_minor: i64, exponent: i32, rate: &Decimal) -> Result<u64, FxError> {
    let total = u128::try_from(total_minor).map_err(|_| FxError::Overflow)?;
    let exponent = u32::try_from(exponent).map_err(|_| FxError::Overflow)?;
    let fiat_scale = 10u128.checked_pow(exponent).ok_or(FxError::Overflow)?;
    let rate_scale = 10u128.checked_pow(rate.scale).ok_or(FxError::Overflow)?;
    let numerator = total
        .checked_mul(100_000_000)
        .and_then(|value| value.checked_mul(rate_scale))
        .ok_or(FxError::Overflow)?;
    let denominator = fiat_scale
        .checked_mul(rate.mantissa)
        .ok_or(FxError::Overflow)?;
    if denominator == 0 {
        return Err(FxError::Overflow);
    }
    let sats = numerator
        .checked_add(denominator - 1)
        .ok_or(FxError::Overflow)?
        / denominator;
    if sats == 0 {
        return Err(FxError::BelowMinimum);
    }
    if sats < MIN_QUOTE_SATS {
        return Err(FxError::BelowMinimum);
    }
    if sats > MAX_QUOTE_SATS {
        return Err(FxError::CapExceeded);
    }
    u64::try_from(sats).map_err(|_| FxError::Overflow)
}

pub fn reference_cutoff(now: DateTime<Utc>) -> DateTime<Utc> {
    now - Duration::seconds(REFERENCE_WINDOW_SECS)
}

pub async fn quote_usd(
    pool: &sqlx::PgPool,
    total_minor: i64,
    exponent: i32,
    now: DateTime<Utc>,
) -> Result<(Decimal, RateSample, u64), FxError> {
    let current = fetch_current(now).await?;
    let rates: Vec<(String,)> = sqlx::query_as(
        "SELECT rate::text FROM fx_rate_samples \
         WHERE currency = 'USD' AND accepted_at >= $1 ORDER BY accepted_at",
    )
    .bind(reference_cutoff(now))
    .fetch_all(pool)
    .await
    .map_err(|_| FxError::MissingReference)?;
    let median_rate = median(
        rates
            .into_iter()
            .filter_map(|(rate,)| Decimal::parse(&rate))
            .collect(),
    )
    .ok_or(FxError::MissingReference)?;
    if !within_deviation(&current.rate, &median_rate) {
        return Err(FxError::DeviationExceeded);
    }
    let sats = quote_sats(total_minor, exponent, &current.rate)?;
    Ok((current.rate.clone(), current, sats))
}

pub async fn fetch_current(now: DateTime<Utc>) -> Result<RateSample, FxError> {
    let response = reqwest::Client::builder()
        .timeout(StdDuration::from_secs(3))
        .build()
        .map_err(|_| FxError::Malformed)?
        .get(FX_URL)
        .send()
        .await
        .map_err(|_| FxError::Malformed)?;
    let body = response.bytes().await.map_err(|_| FxError::Malformed)?;
    parse_feed(&body, now)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_fixture_selects_only_btcusd() {
        let now = "2026-09-13T16:21:00Z".parse().unwrap();
        let sample = parse_feed(
            include_bytes!("../tests/fixtures/blocktank_fx_rates_btc.json"),
            now,
        )
        .expect("captured production response parses");
        assert_eq!(sample.rate.to_string_value(), "77197");
    }

    #[test]
    fn quote_uses_checked_integer_ceiling_and_minimum() {
        let rate = Decimal::parse("10000").unwrap();
        assert_eq!(quote_sats(1_000_000, 2, &rate), Ok(100_000_000));
        assert_eq!(
            quote_sats(1, 2, &rate),
            Err(FxError::BelowMinimum),
            "small fiat principals are rejected without uplift"
        );
        assert_eq!(
            quote_sats(10_000_000_000, 2, &rate),
            Err(FxError::CapExceeded)
        );
    }

    #[test]
    fn quote_usd_two_decimal_matrix_uses_exact_rational_ceiling() {
        let rate = Decimal::parse("100000").unwrap();
        assert_eq!(quote_sats(99, 2, &rate), Err(FxError::BelowMinimum));
        assert_eq!(quote_sats(1_000, 2, &rate), Ok(10_000));
        assert_eq!(quote_sats(1_001, 2, &rate), Ok(10_010));
        assert_eq!(quote_sats(12_500, 2, &rate), Ok(125_000));
        assert_eq!(quote_sats(12_501, 2, &rate), Ok(125_010));
    }

    #[test]
    fn quote_rejects_zero_negative_and_overflow_inputs() {
        let rate = Decimal::parse("77197").unwrap();
        assert_eq!(quote_sats(0, 2, &rate), Err(FxError::BelowMinimum));
        assert_eq!(quote_sats(-1, 2, &rate), Err(FxError::Overflow));
        assert_eq!(
            quote_sats(i64::MAX, i32::MAX, &rate),
            Err(FxError::Overflow)
        );
    }

    #[test]
    fn quote_rejects_zero_and_three_decimal_currency_exponents_at_bind_boundary() {
        let rate = Decimal::parse("77197").unwrap();
        assert_eq!(quote_sats(1_000, 0, &rate), Ok(1_295_388));
        assert_eq!(quote_sats(100, 3, &rate), Err(FxError::BelowMinimum));
    }

    #[test]
    fn deviation_boundaries_are_inclusive() {
        let median = Decimal::parse("100000").unwrap();
        assert!(within_deviation(
            &Decimal::parse("105000").unwrap(),
            &median
        ));
        assert!(within_deviation(&Decimal::parse("95000").unwrap(), &median));
        assert!(!within_deviation(
            &Decimal::parse("105001").unwrap(),
            &median
        ));
        assert!(!within_deviation(
            &Decimal::parse("94999").unwrap(),
            &median
        ));
    }

    #[test]
    fn malformed_or_duplicate_btcusd_is_rejected() {
        let now = "2026-09-13T16:21:00Z".parse().unwrap();
        let body = br#"{"tickers":[
            {"symbol":"BTCUSD","lastPrice":"77178","base":"BTC","quote":"USD","lastUpdatedAt":1789316268233},
            {"symbol":"BTCUSD","lastPrice":"77179","base":"BTC","quote":"USD","lastUpdatedAt":1789316268233}
        ]}"#;
        assert_eq!(parse_feed(body, now), Err(FxError::Malformed));
    }

    #[test]
    fn feed_rejects_missing_stale_future_zero_and_out_of_band_samples() {
        let now: DateTime<Utc> = "2026-09-13T16:21:00Z".parse().unwrap();
        let body = |price: &str, timestamp: i64| {
            format!(
                r#"{{"tickers":[{{"symbol":"BTCUSD","lastPrice":"{price}","base":"BTC","quote":"USD","lastUpdatedAt":{timestamp}}}]}}"#
            )
        };
        let current = now.timestamp_millis();
        assert_eq!(
            parse_feed(br#"{"tickers":[]}"#, now),
            Err(FxError::Malformed)
        );
        assert_eq!(
            parse_feed(body("77197", current - 301_000).as_bytes(), now),
            Err(FxError::Stale)
        );
        assert_eq!(
            parse_feed(body("77197", current + 61_000).as_bytes(), now),
            Err(FxError::Future)
        );
        assert_eq!(
            parse_feed(body("0", current).as_bytes(), now),
            Err(FxError::OutOfBounds)
        );
        assert_eq!(
            parse_feed(body("1000001", current).as_bytes(), now),
            Err(FxError::OutOfBounds)
        );
    }
}
