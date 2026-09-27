//! A current sealing key plus an optional previous key: the dual-key read
//! window every rotating [`crate::seal`] family uses. Seals always use the
//! current key; opens try the current key, then the previous one. Each
//! family is its own type parameter, so one family's keys cannot be passed
//! where another's are expected.

use std::marker::PhantomData;

use crate::seal::{self, KEY_LEN};

/// The environment names and wording of one rotating key family.
pub trait KeyFamily {
    const CURRENT_ENV: &'static str;
    const PREVIOUS_ENV: &'static str;
    /// Used in errors: "... under the configured {DESCRIPTION} key".
    const DESCRIPTION: &'static str;
    /// The redacted `Debug` form's type name.
    const DEBUG_NAME: &'static str;
}

pub struct RotatingKeys<F: KeyFamily> {
    current: [u8; KEY_LEN],
    previous: Option<[u8; KEY_LEN]>,
    family: PhantomData<F>,
}

impl<F: KeyFamily> std::fmt::Debug for RotatingKeys<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}(<redacted>)", F::DEBUG_NAME)
    }
}

impl<F: KeyFamily> RotatingKeys<F> {
    pub fn from_hex(current_hex: &str, previous_hex: Option<&str>) -> anyhow::Result<Self> {
        let current = seal::parse_key(F::CURRENT_ENV, current_hex)?;
        let previous = previous_hex
            .map(|hex| seal::parse_key(F::PREVIOUS_ENV, hex))
            .transpose()?;
        if previous.as_ref() == Some(&current) {
            anyhow::bail!(
                "{} and {} must be distinct keys",
                F::CURRENT_ENV,
                F::PREVIOUS_ENV
            );
        }
        Ok(Self {
            current,
            previous,
            family: PhantomData,
        })
    }

    /// Reads the family's two environment variables. `None` means the
    /// family is off; a previous key without a current key is refused.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        let current = std::env::var(F::CURRENT_ENV).ok();
        let previous = std::env::var(F::PREVIOUS_ENV).ok();
        let Some(current) = current else {
            if previous.is_some() {
                anyhow::bail!("{} requires {}", F::PREVIOUS_ENV, F::CURRENT_ENV);
            }
            return Ok(None);
        };
        Self::from_hex(&current, previous.as_deref()).map(Some)
    }

    pub fn has_previous(&self) -> bool {
        self.previous.is_some()
    }

    pub fn seal(&self, aad: &[u8], plaintext: &[u8]) -> Vec<u8> {
        seal::seal(&self.current, aad, plaintext)
    }

    pub fn open(&self, aad: &[u8], sealed: &[u8]) -> anyhow::Result<Vec<u8>> {
        if let Ok(plaintext) = seal::open(&self.current, aad, sealed) {
            return Ok(plaintext);
        }
        if let Some(previous) = &self.previous {
            return seal::open(previous, aad, sealed);
        }
        Err(anyhow::anyhow!(
            "ciphertext did not authenticate under the configured {} key",
            F::DESCRIPTION
        ))
    }

    pub(crate) fn opens_under_current(&self, aad: &[u8], sealed: &[u8]) -> bool {
        seal::open(&self.current, aad, sealed).is_ok()
    }

    pub(crate) fn open_under_previous(&self, aad: &[u8], sealed: &[u8]) -> anyhow::Result<Vec<u8>> {
        let Some(previous) = &self.previous else {
            anyhow::bail!("no previous {} key configured", F::DESCRIPTION);
        };
        seal::open(previous, aad, sealed)
    }

    /// Both configured keys, for distinctness checks against other sealing
    /// keys. Never logged or serialized.
    pub(crate) fn key_material(&self) -> impl Iterator<Item = &[u8; KEY_LEN]> {
        std::iter::once(&self.current).chain(self.previous.iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Probe;
    impl KeyFamily for Probe {
        const CURRENT_ENV: &'static str = "ROTATING_KEYS_TEST_KEY";
        const PREVIOUS_ENV: &'static str = "ROTATING_KEYS_TEST_KEY_PREVIOUS";
        const DESCRIPTION: &'static str = "probe";
        const DEBUG_NAME: &'static str = "ProbeKeys";
    }

    const CURRENT: &str = "8888888888888888888888888888888888888888888888888888888888888888";
    const PREVIOUS: &str = "9999999999999999999999999999999999999999999999999999999999999999";

    #[test]
    fn seals_under_current_and_opens_with_previous_fallback() {
        let old = RotatingKeys::<Probe>::from_hex(PREVIOUS, None).expect("old");
        let rotated = RotatingKeys::<Probe>::from_hex(CURRENT, Some(PREVIOUS)).expect("rotated");
        let current_only = RotatingKeys::<Probe>::from_hex(CURRENT, None).expect("current");

        let legacy = old.seal(b"aad", b"secret");
        assert_eq!(rotated.open(b"aad", &legacy).expect("fallback"), b"secret");
        assert!(!rotated.opens_under_current(b"aad", &legacy));
        current_only
            .open(b"aad", &legacy)
            .expect_err("no fallback without a previous key");

        let fresh = rotated.seal(b"aad", b"secret");
        assert!(current_only.opens_under_current(b"aad", &fresh));
        rotated
            .open(b"other", &fresh)
            .expect_err("different associated data fails");
    }

    #[test]
    fn keys_parse_fail_closed_and_debug_is_redacted() {
        RotatingKeys::<Probe>::from_hex("zz", None).expect_err("non-hex key rejected");
        RotatingKeys::<Probe>::from_hex(CURRENT, Some(CURRENT)).expect_err("equal keys rejected");
        let keys = RotatingKeys::<Probe>::from_hex(CURRENT, Some(PREVIOUS)).expect("keys parse");
        let debug = format!("{keys:?}");
        assert_eq!(debug, "ProbeKeys(<redacted>)");
        assert_eq!(keys.key_material().count(), 2);
    }
}
