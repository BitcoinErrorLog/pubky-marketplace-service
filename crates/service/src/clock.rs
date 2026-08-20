use std::sync::Mutex;

use chrono::{DateTime, SecondsFormat, Utc};

/// Server time authority. `issued_at` on commands is diagnostic only; every
/// deadline (reservation expiry, challenge/session TTL) uses this clock.
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> DateTime<Utc>;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Deterministic clock for tests, mirroring the injectable `now` of the
/// TypeScript prototype suite.
pub struct AdjustableClock {
    now: Mutex<DateTime<Utc>>,
}

impl AdjustableClock {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            now: Mutex::new(now),
        }
    }

    pub fn set(&self, now: DateTime<Utc>) {
        *self.now.lock().expect("clock lock poisoned") = now;
    }

    pub fn advance_seconds(&self, seconds: i64) {
        let mut guard = self.now.lock().expect("clock lock poisoned");
        *guard += chrono::Duration::seconds(seconds);
    }
}

impl Clock for AdjustableClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().expect("clock lock poisoned")
    }
}

/// Canonical wire timestamp format: RFC 3339 with milliseconds and `Z`.
pub fn format_timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Millis, true)
}
