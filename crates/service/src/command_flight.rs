//! Per-process single flight for one actor + command id.
//!
//! A command that reads the seller's homeserver before its executor
//! transaction enters a flight first. A concurrent duplicate waits for the
//! first submission to finish, then finds its stored result instead of
//! repeating the read. The wait is bounded by the first submission's own
//! homeserver timeout and transaction. The map holds only keys with a
//! submission holding or waiting for the flight.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use uuid::Uuid;

type FlightKey = (String, Uuid);

#[derive(Debug)]
struct Slot {
    lock: Arc<AsyncMutex<()>>,
    entrants: usize,
}

#[derive(Clone, Debug, Default)]
pub struct CommandFlights {
    slots: Arc<Mutex<HashMap<FlightKey, Slot>>>,
}

/// Held for the rest of the submission. Dropping it, including when the
/// submission is cancelled while waiting, releases the flight and removes
/// the key once no other submission holds or waits for it.
pub struct CommandFlight {
    guard: Option<OwnedMutexGuard<()>>,
    key: FlightKey,
    slots: Arc<Mutex<HashMap<FlightKey, Slot>>>,
}

impl CommandFlights {
    pub async fn enter(&self, actor: &str, command_id: Uuid) -> CommandFlight {
        let key = (actor.to_string(), command_id);
        let lock = {
            let mut slots = self.slots.lock().expect("command flight lock");
            let slot = slots.entry(key.clone()).or_insert_with(|| Slot {
                lock: Arc::new(AsyncMutex::new(())),
                entrants: 0,
            });
            slot.entrants += 1;
            slot.lock.clone()
        };
        let mut flight = CommandFlight {
            guard: None,
            key,
            slots: self.slots.clone(),
        };
        flight.guard = Some(lock.lock_owned().await);
        flight
    }

    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.slots.lock().expect("command flight lock").len()
    }
}

impl Drop for CommandFlight {
    fn drop(&mut self) {
        self.guard.take();
        let mut slots = self.slots.lock().expect("command flight lock");
        if let Some(slot) = slots.get_mut(&self.key) {
            slot.entrants -= 1;
            if slot.entrants == 0 {
                slots.remove(&self.key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CommandFlights;
    use std::time::Duration;
    use uuid::Uuid;

    #[tokio::test]
    async fn a_duplicate_waits_for_the_first_submission_and_other_keys_do_not() {
        let flights = CommandFlights::default();
        let command = Uuid::from_u128(1);
        let first = flights.enter("actor", command).await;

        let waiting = tokio::spawn({
            let flights = flights.clone();
            async move {
                let _flight = flights.enter("actor", command).await;
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiting.is_finished(), "the duplicate waits");

        let other_actor = flights.enter("other", command).await;
        let other_command = flights.enter("actor", Uuid::from_u128(2)).await;
        drop((other_actor, other_command));

        drop(first);
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("the duplicate proceeds once the first finishes")
            .expect("duplicate task");
        assert_eq!(flights.tracked(), 0);
    }

    #[tokio::test]
    async fn a_cancelled_waiter_leaves_no_key_behind() {
        let flights = CommandFlights::default();
        let command = Uuid::from_u128(3);
        let first = flights.enter("actor", command).await;
        let cancelled =
            tokio::time::timeout(Duration::from_millis(20), flights.enter("actor", command)).await;
        assert!(cancelled.is_err(), "the waiter was still waiting");
        drop(first);
        assert_eq!(flights.tracked(), 0);
    }
}
