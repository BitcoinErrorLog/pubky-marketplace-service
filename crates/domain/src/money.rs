use serde::{Deserialize, Serialize};

/// Largest integer the TypeScript contracts accept (`Number.MAX_SAFE_INTEGER`).
pub const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

/// Integer-minor-unit money, identical to the `commerceMoneySchema` wire shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Money {
    pub amount_minor: i64,
    pub currency: String,
    pub exponent: i32,
}

impl Money {
    pub fn with_amount(&self, amount_minor: i64) -> Money {
        Money {
            amount_minor,
            currency: self.currency.clone(),
            exponent: self.exponent,
        }
    }

    /// Two amounts are comparable only when currency and exponent match.
    pub fn same_asset(&self, other: &Money) -> bool {
        self.currency == other.currency && self.exponent == other.exponent
    }
}
