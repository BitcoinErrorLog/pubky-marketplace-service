# Upstream Paykit inventory hold boundary

Status: accepted consumer design for H1 (`pubky/paykit-server#26`).

## Invariant

Marketplace must not publish an upstream Paykit payment request after inventory has been released. One bind creates one fixed inventory deadline. Activation retries never move that deadline.

Fork Paykit behavior is unchanged: its prepare request already carries Marketplace's hold deadline. This design applies only to `PAYKIT_SERVER_API=upstream`.

## Bound

Upstream prepare returns its authoritative `prepare_expires_at`. Paykit atomically refuses the first activation when its database `clock_timestamp() >= prepare_expires_at`. A successful first activation fixes:

`payment_deadline = Paykit activation clock + requested payment_window_seconds`

Marketplace accepts that deadline only when its remaining TTL is no more than
`PAYKIT_MAX_PREPARE_TTL_SECONDS` (default 900, closed range 60–86400), plus the
same 60-second cross-service clock-skew allowance. Operators must align this
setting with Paykit's `prepare_ttl_seconds`. A longer response is treated as
contract corruption: Marketplace rolls back the bind and inventory hold and
best-effort voids the prepared invoice.

Marketplace therefore fixes the original hold after successful prepare to:

`prepare_expires_at + bitcoin_payment_window_seconds + 60 seconds`

The extra 60 seconds is a bounded clock/reaper margin. It is not sent to Paykit and does not alter Paykit's immutable payment window. It only prevents Marketplace's clock or expiry sweep from releasing stock at the producer's latest valid payment deadline.

Marketplace persists this deadline in the same transaction as the prepared invoice and activation outbox row. Activation success records Paykit's returned deadline but never rewrites the inventory deadline. Every activation response must fit inside the original hold.

## Activation guard and races

Before each signed upstream activation, worker locks payment then order and requires all of:

- order remains `pending_payment`;
- activation remains `preparing`;
- inventory remains held;
- original hold has not elapsed;
- Marketplace time plus 60-second skew margin is before authoritative `prepare_expires_at`.

A failed guard never sends activation. Existing prepared-invoice release handling records attempt for reconciliation, releases inventory atomically, and leaves producer prepare reaper/void handling to close unpublished invoice.

Pre-call check is defense in depth, not cross-service atomicity. H1 relies on Paykit's reviewed atomic activation transaction: it locks preparation, samples database time, refuses at/after `prepare_expires_at`, and creates publication outbox work only in same transaction as activation. Since Marketplace hold extends beyond producer's latest possible payment deadline, local expiry cannot race a valid publication.

Activation transport failure or lost response leaves fixed original hold unchanged and activation row retryable. It never extends hold. Existing `paykit_superseded_attempts` records attempts released after ambiguous delivery and polls signed status; confirmed money follows late-money/manual-review path. Marketplace never blindly voids a known active invoice.

## Timing assumptions and fail-closed checks

Accepted producer is reviewed Paykit commit `cce84b127febdc0411cd709df2b7cc913f6c4726` (tree `faa5a49144fc27667360d15f6a0a306e58188c18`). Consumer assumes:

1. `prepare_expires_at` is producer database time and immutable on replay.
2. First activation is rejected when producer DB time is at or past that timestamp.
3. Successful first activation returns immutable `activated_at` and `payment_deadline`, with `payment_deadline = activated_at + requested payment window`.
4. Activation and publication-outbox insertion commit atomically.
5. Worst tolerated Marketplace/Paykit clock plus expiry-scheduling skew is 60 seconds.

Consumer rejects a prepare whose remaining TTL is already inside margin or exceeds the configured maximum plus margin. With the largest permitted config, remaining preparation time is bounded to 86,460 seconds; with the default it is bounded to 960 seconds. The final hold adds only the configured payment window and one further 60-second expiry-scheduling margin. Consumer treats an activation response outside persisted bounds as contract corruption: it must not silently change Paykit terms or extend original hold. Such corruption requires operator alert and fresh producer/consumer review; 60 seconds is explicit operational assumption, not proof against unbounded clock faults.
