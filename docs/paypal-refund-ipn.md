# PayPal refund and reversal IPN contract

`POST /v0/paypal/ipn` records a seller's PayPal refund, a buyer's PayPal
reversal (chargeback or dispute), and PayPal's cancellation of a reversal, on
the order they concern. No participant acts. The service never moves money:
PayPal already moved it, and the service records the verified fact.

Nothing verified is discarded. A postback-verified refund-class notification
with a `txn_id` is either recorded on its order, in every order state, or
held in `gateway_refund_inbox`. PayPal gets its 200 only after one of those
writes commits; a database failure answers 500 so PayPal retries.

## Inputs

PayPal sends a new `txn_id` for each refund, reversal, or canceled reversal.
`parent_txn_id` is the `txn_id` of the original payment.

| `payment_status` | Meaning | `mc_gross` |
| --- | --- | --- |
| `Refunded` | The merchant refunded, fully or partially | negative: the amount returned to the buyer |
| `Reversed` | A chargeback or other reversal took the money back | negative: the amount taken back |
| `Canceled_Reversal` | The reversal was undone; the money went back to the seller | positive: the amount restored |

## Which payment and receiver are trusted

`orders.fiat_transaction_ref` is not trusted for refund matching. The buyer
writes it through `POST /v0/orders/{id}/fiat/mark-paid`.

Refunds match `orders.paypal_txn_id` only. It is written from a
postback-verified `Completed` IPN whose receiver, currency, amount, and
`custom` order id all passed the payment checks, and nothing else writes it.
The same write stores the receiver snapshot: the seller's configured PayPal
email that the payment matched (`paypal_receiver_email`, lowercased) and
PayPal's `receiver_id` account id when the IPN carries one
(`paypal_receiver_id`). The first verified payment is kept; a unique index
lets one PayPal payment pay one order. The write happens in every payment
state, so a `Completed` IPN that arrives after the seller confirmed receipt
by hand still stores it.

A refund-class notification must name the snapshot receiver: the same
`receiver_id`, or `receiver_email`/`business` equal to the snapshot email. The
seller's current payment configuration is not consulted, so changing or
clearing the PayPal email after the sale does not strand later refunds.

`paypal_txn_id` and the snapshot are not projected. `fiat_transaction_ref`
keeps its meaning and projection.

Migration `0039` backfills `paypal_txn_id` from `fiat_transaction_ref` for
gateway-verified payments. The receiver that payment matched was never
stored, and the seller's current configuration is not evidence of it, so a
backfilled payment has no receiver snapshot and its refund-class
notifications are held as `receiver_unverified`. It backfills only when:

- the order's event log has no `order.fiat_payment_reported` after its
  `receipt.issued` (the event sequence decides, not timestamps);
- with a buyer report on the order, the value has PayPal's 17-character
  `[A-Z0-9]` transaction id shape (a buyer-typed value survives when the
  verifying IPN carried no usable `txn_id`);
- no other gateway-verified order carries the same reference.

Production preflight (2026-09-24, read-only): 69 order rows (256 kB), one
backfill candidate, no buyer report on it, and a configured email. The index
builds hold their lock for milliseconds.

## Serialization

Everything keyed by one PayPal payment runs under one transaction-scoped
advisory lock on that payment's `txn_id`
(`pg_advisory_xact_lock(hashtextextended(txn_id, 6361))`), taken before the
payment row and then the order row, so the lock order on both paths is
advisory, payment, order:

- A verified `Completed` IPN settles in one transaction: the payment lock,
  the payment and order rows, confirmation (or late-money or manual-review
  handling), the payment id and receiver snapshot, and re-evaluation of every
  held `unknown_parent` row naming the payment.
- A refund-class IPN takes the lock for its `parent_txn_id`, then resolves,
  validates, and records or holds itself in one transaction, locking the
  payment row before the order row.

A refund therefore never sees a payment id before that payment is settled,
and a refund held while the payment settles is either seen by the settlement
or sees the settled payment.

## Resolution and the inbox

1. `payment_status` is not one of the three, or `txn_id` is missing or not
   1–64 printable ASCII characters: nothing can key a record. PayPal always
   sends a `txn_id`; the notification is logged and acknowledged.
2. `txn_id` already recorded on an order or held in the inbox: no-op.
3. `parent_txn_id` missing or malformed: inbox, `missing_parent`.
4. No order owns `parent_txn_id` yet: inbox, `unknown_parent`. The parent
   payment's settlement re-evaluates the row in its own transaction: it is
   applied and marked resolved, or it takes the reason a later check now
   gives. A row with any reason other than `unknown_parent` is terminal and
   is not replayed; it waits for the seller's review.
5. `custom` names an order other than the owner: inbox, `custom_mismatch`.
6. The payment has no receiver snapshot (backfilled): inbox,
   `receiver_unverified`. Otherwise, a receiver that does not match the
   snapshot: inbox, `receiver_mismatch`.
7. `mc_currency` differs from the order currency: inbox, `currency_mismatch`.
8. `mc_gross` has the wrong sign, is zero, or lacks exactly the currency's
   fraction digits: inbox, `amount_invalid`.
9. Otherwise: recorded on the order.

Re-evaluating or re-delivering a held notification moves the unresolved row
to the new reason, keeping the known order when the new evaluation names
none. An inbox row keeps the fields needed to apply it (`payment_status`, ids,
`custom`, `mc_gross`, `mc_currency`, `receiver_email`, `business`,
`receiver_id`, `reason_code`) and nothing about the payer. An unresolved row
naming an order projects `gateway_refund_unmatched: true` on that order.

## Recording on the order

Under the order row lock, one `order_gateway_refunds` row per `txn_id`. Totals
over the order's rows:

- `refunded` = sum of `Refunded`; `reversed` = sum of `Reversed`;
  `restored` = sum of `Canceled_Reversal`;
- outstanding reversal = `max(reversed − restored, 0)`;
- effective refund = `refunded` + outstanding reversal.

`external_refund` is the running effective refund, capped at the order total:
`{ amount_minor, transaction_id (latest refund or reversal), recorded_at }`,
or null when the effective refund returns to zero. When `external_refund` was
written by `refund.record_external` or a manual review (its transaction id is
not a ledger row), it is left as the seller recorded it and the order is
flagged for review instead.

### Refunded and Reversed

- Effective refund below the total: a partial refund. The order keeps its
  state (event `refund.recorded_partial`).
- Effective refund reaching the total, from a state holding a confirmed
  payment (`paid`, `ready_for_pickup`, `shipped`, `delivered`, `completed`,
  `return_received`, `cancel_requested`, `cancelled`, `return_requested`,
  `return_approved`): the order moves to `refunded_external` (server trigger
  `paypal_refund`, event `refund.recorded_external`, attestor annotation
  `refunded`). An open return request moves to `refunded`. The ledger row
  keeps the replaced order and return states.
- Effective refund reaching the total from `pending_payment`, `processing`,
  `closed`, or an already manual `refunded_external`; or exceeding the total:
  recorded, state unchanged, `gateway_refund_review_at` set.
- `Reversed` also sets `payment_reversed_at` (first outstanding reversal),
  but only while a reversal is outstanding: a `Canceled_Reversal` delivered
  before its `Reversed` nets it to zero, and the flag stays clear.

An open cancel or return request is resolved by the refund that reaches the
total: the seller refunded the buyer, which is what either request asks for.

### Canceled_Reversal

- Restores up to the outstanding reversal; never offsets `Refunded` money.
- Clears `payment_reversed_at` when no reversal remains outstanding, and sets
  `payment_reversal_cancelled_at`.
- If the order is `refunded_external` because of a recorded row and the
  effective refund is now below the total, the order returns to the state
  that row replaced (server trigger `paypal_reversal_cancelled`), the return
  request to its replaced state, and the attestor annotation
  `refund_reversal_cancelled` supersedes `refunded`.
- Event `refund.reversal_cancelled`; notification
  `payment_reversal_cancelled` to both participants.
- With nothing reversed, or on a manual `refunded_external`: recorded,
  `gateway_refund_review_at` set.

## No `refunded_partial` state

A partial refund leaves the order in its current state and records the running
sum on `external_refund`. There is no `refunded_partial` state.

- The Shop treats `refunded_partial` as terminal. A seller who refunds
  shipping on a `paid` order still has to ship it.
- The deployed Shop already renders "Refunded $X of $Y" whenever
  `externalRefund.amountMinor` is below `total.amountMinor`, in any state.
- A new state changes the `orders.state` CHECK and the Shop's order enum.

## Manual record alongside PayPal partials

`refund.record_external` (seller, from `return_received` or `cancelled`)
accepts a record when `external_refund` is null, or when it holds the running
PayPal sum and the recorded amount is at least that sum. The seller can close
a return at the PayPal amount or record money returned outside PayPal. The
record replaces `external_refund`; later PayPal refunds on that order are
recorded and flagged for review. A record below the PayPal sum, or a second
manual record, is refused.

## Stock

A refund never restocks. A refunded order may have shipped or been handed
over, and the service cannot know whether the unit came back. Stock returns
to a listing only through cancellation, as today. While a reversal is
outstanding, the pickup retention purge keeps the order's pinned pickup
snapshot, because a canceled reversal can reopen the order.

## Reputation

The reputation worker counts a refund in `terminated_badly` only when the
order has a `refund.recorded_external` event and is still
`refunded_external`. A canceled reversal that reopened the order removes the
penalty. `refund.recorded_partial` never counts.

## Notifications and events

The actor is `paypal-ipn`, not a participant, so both buyer and seller are
notified.

| Record | Event kind | Notification |
| --- | --- | --- |
| Partial, or recorded for review | `refund.recorded_partial` | `refund_recorded` |
| Full | `refund.recorded_external` | `refund_recorded` |
| Canceled reversal | `refund.reversal_cancelled` | `payment_reversal_cancelled` |

The Shop quarantines an unknown notification type per row; it needs
`payment_reversal_cancelled` copy to display it.

## State machine contract

- Order: `paypal_refund` on `paid | ready_for_pickup | shipped | delivered |
  completed | return_received | cancel_requested | cancelled |
  return_requested | return_approved → refunded_external`;
  `paypal_reversal_cancelled` on `refunded_external →` each of those states.
- Return: `paypal_refund` on `requested | approved | received → refunded`;
  `paypal_reversal_cancelled` on `refunded →` each of those.
- No state is added.

## Projection

| Field | Meaning |
| --- | --- |
| `external_refund` | running effective refund, or the seller's manual record |
| `payment_reversed_at` | set while a reversal is outstanding |
| `payment_reversal_cancelled_at` | latest canceled reversal |
| `gateway_refund_review_at` | first refund notification that could not be reflected automatically |
| `gateway_refund_unmatched` | an inbox row naming this order is unresolved |

All five go to both participants.

## Contract table

Every row answers PayPal with HTTP 200 unless it says 5xx. PayPal retries only
non-2xx. Log lines carry the order id and a static outcome at most, never an
email, amount, or transaction id.

### Refunded and Reversed by order state

| Input or state | Deployed client sends / current behaviour | New behaviour | UI copy or state | Test name |
| --- | --- | --- | --- | --- |
| `Refunded`, `paid`, sum < total | 200, dropped | Recorded; state `paid`; revision +1; `external_refund` = running sum; `refund.recorded_partial`; `refund_recorded` to both | Shop: "Refunded $X of $Y" | `partial_refund_ipn_keeps_the_state_and_records_the_amount` |
| `Refunded`, `paid`, sum = total | 200, dropped | `refunded_external`; `refund.recorded_external`; attestor `refunded`; `refund_recorded` to both | "Refund recorded from external evidence: {txn}" | `full_refund_ipn_moves_a_paid_order_to_refunded_external` |
| `Refunded`, `ready_for_pickup`, `shipped`, `delivered`, `completed` | 200, dropped | As `paid` | As `paid` | `refund_ipn_is_recorded_in_every_paid_state` |
| `Refunded`, `return_received` | 200, dropped | As `paid`; full also moves the return to `refunded` | return line "refunded" | `full_refund_ipn_resolves_a_received_return` |
| `Refunded`, `cancel_requested`, `cancelled` | 200, dropped (previous revision: refused) | Partial recorded, state kept; full moves to `refunded_external` | As `paid` | `refund_ipn_is_recorded_in_every_paid_state` |
| `Refunded`, `return_requested`, `return_approved` | 200, dropped (previous revision: refused) | Partial recorded, state and return kept; full moves to `refunded_external` and the return to `refunded` | As `paid` | `refund_ipn_is_recorded_in_every_paid_state` |
| `Refunded`, full, `pending_payment`, `processing`, `closed` | 200, dropped | Recorded; state kept; `gateway_refund_review_at` set | review flag | `full_refund_sources_are_exactly_the_paypal_refund_edges` (unit: these states are outside the transition gate) |
| `Refunded`, `refunded_external` written by `refund.record_external` | 200, dropped | Recorded; `external_refund` unchanged; `gateway_refund_review_at` set | review flag | `refund_ipn_on_a_manually_refunded_order_is_recorded_for_review` |
| Running sum exceeds the total | 200, dropped | Recorded; `external_refund` capped at the total; `gateway_refund_review_at` set | review flag | `refund_ipn_exceeding_the_total_is_recorded_and_flagged` |
| Refund races a cancel approval on the order lock | n/a | Recorded whichever commits first; approval may lose with 409 | As the state rows | `a_refund_racing_a_cancel_approval_is_recorded` |
| Refund races `return.approve` or `return.receive` | n/a | Recorded whichever commits first; a full refund resolves the return | return "refunded" | `a_refund_racing_each_return_step_is_recorded` |
| Two refunds arrive together | n/a | Both recorded under the lock; the second completes the refund | "Refund recorded from external evidence: {txn}" | `two_simultaneous_partial_refunds_both_record` |
| Refund finds no payment and is being held while the payment settles | stranded as `unknown_parent` (round 2) | The payment lock serializes them; the settlement applies the held refund | refunded once paid | `a_refund_racing_its_payment_settlement_is_never_stranded` |
| Full refund arrives while the payment is confirming | recorded for review on a `pending_payment` order, then left `paid` (round 2) | The refund waits for the settlement and moves the paid order to `refunded_external` | "Refund recorded from external evidence: {txn}" | `a_full_refund_racing_payment_confirmation_moves_the_paid_order` |
| `Reversed`, any state | 200, dropped | As `Refunded` in that state, plus `payment_reversed_at` | `payment_reversed_at` on the order | `reversed_ipn_records_the_refund_and_flags_the_order` |

### Canceled_Reversal

| Input or state | Deployed client sends / current behaviour | New behaviour | UI copy or state | Test name |
| --- | --- | --- | --- | --- |
| After a full reversal moved the order to `refunded_external` | 200, dropped (previous revision: penalty and flag kept) | Order back to the replaced state; `payment_reversed_at` null; `payment_reversal_cancelled_at` set; `external_refund` null; attestor `refund_reversal_cancelled`; `refund.reversal_cancelled`; `payment_reversal_cancelled` to both | prior state restored | `canceled_reversal_restores_the_order_and_clears_the_flag` |
| After a partial refund plus a reversal reached the total from `return_received` | 200, dropped | Order back to `return_received`, return back to `received`; `external_refund` = the refund only | "Refunded $X of $Y" | `canceled_reversal_restores_the_order_and_clears_the_flag` |
| Nothing reversed on the order | 200, dropped | Recorded; state and `external_refund` unchanged; `gateway_refund_review_at` set | review flag | `canceled_reversal_restores_the_order_and_clears_the_flag` |
| `Canceled_Reversal` delivered before its `Reversed` | `payment_reversed_at` stuck set, blocking the pickup purge (round 2) | Both recorded; the reversal nets to zero; `payment_reversed_at` stays null; review flag from the early cancellation | review flag | `a_canceled_reversal_before_its_reversal_leaves_nothing_outstanding` |
| Reputation after a canceled reversal | full reversal counted in `terminated_badly` | Not counted; a standing reversal still is | completion rate | `a_canceled_reversal_removes_the_reputation_penalty` |

### Other payment statuses

| Input or state | Deployed client sends / current behaviour | New behaviour | UI copy or state | Test name |
| --- | --- | --- | --- | --- |
| `Completed`, all checks pass, payment `awaiting_entitlement` | Confirms the order; `fiat_transaction_ref` = `txn_id` | Unchanged, plus `paypal_txn_id` and the receiver snapshot, then waiting inbox rows applied | `fiat_verification` `gateway-notified` | `a_verified_completed_ipn_pays_the_order_without_any_participant` |
| `Completed`, all checks pass, payment already `confirmed` | No effect | `paypal_txn_id` and snapshot when unset | unchanged (`seller-attested`) | `completed_ipn_after_seller_confirmation_arms_refund_matching` |
| `Completed`, a second verified payment for the same order | No effect | `paypal_txn_id` unchanged | unchanged | `completed_ipn_after_seller_confirmation_arms_refund_matching` |
| `Completed`, any check fails | 200, dropped | Unchanged; nothing written | unchanged | `an_ipn_that_does_not_match_server_held_facts_is_dropped` |
| `Pending`, `Denied`, `Expired`, `Failed`, `Voided`, `Processed`, `Created`, any other | 200, dropped | Unchanged | unchanged | `an_ipn_that_does_not_match_server_held_facts_is_dropped` |

### Validation and the inbox

| Input or state | Deployed client sends / current behaviour | New behaviour | UI copy or state | Test name |
| --- | --- | --- | --- | --- |
| Postback `INVALID` | 200, dropped | Unchanged: not verified, nothing written | unchanged | `a_refund_ipn_that_fails_postback_validation_is_dropped` |
| Postback unreachable | 503 | Unchanged (PayPal retries) | unchanged | `an_unreachable_verifier_asks_paypal_to_retry_a_refund` |
| Database failure while recording | n/a | 500; the transaction rolls back; the retry records | unchanged, then recorded | `refund_ipn_database_failure_asks_paypal_to_retry` |
| `txn_id` missing, empty, over 64 characters, or not printable ASCII | 200, dropped | Logged and acknowledged; no key to record under | unchanged | `refund_ipn_transaction_ids` |
| `parent_txn_id` missing or malformed | 200, dropped | Inbox `missing_parent` | `gateway_refund_unmatched` when `custom` names the order | `refund_ipn_transaction_ids` |
| No order owns `parent_txn_id` | 200, dropped | Inbox `unknown_parent` | `gateway_refund_unmatched` when `custom` names the order | `refund_ipn_with_unknown_parent_is_held_in_the_inbox` |
| Refund before its payment's `Completed` IPN | 200, dropped | Inbox `unknown_parent`, applied in the payment's settlement transaction | refunded once paid | `a_refund_that_arrives_before_its_payment_is_applied_when_the_payment_lands` |
| Held refund fails a later check once its payment settles | reason stayed `unknown_parent`, replayed on every payment retry (round 2) | Row moves to the real reason (for example `receiver_mismatch`) and is terminal: not replayed | `gateway_refund_unmatched` | `a_held_refund_is_re_evaluated_when_its_payment_lands` |
| `parent_txn_id` equals a buyer-reported `fiat_transaction_ref` (no gateway payment) | 200, dropped | Inbox `unknown_parent`; order untouched | `gateway_refund_unmatched` | `a_buyer_reported_reference_never_matches_a_refund` |
| `custom` names an order other than the parent's owner | 200, dropped | Inbox `custom_mismatch`; both orders untouched | `gateway_refund_unmatched` on the owner | `refund_ipn_whose_custom_order_does_not_own_the_parent_is_held` |
| `custom` absent, parent owned | 200, dropped | Recorded on the owner | as the state rows | `refund_ipn_without_custom_resolves_by_parent` |
| Seller changed or cleared the configured PayPal email after the sale | 200, dropped (previous revision) | Recorded: matched against the snapshot | as the state rows | `refunds_validate_against_the_receiver_snapshot_not_current_config` |
| PayPal account email renamed, same `receiver_id` | 200, dropped | Recorded: matched by account id | as the state rows | `refunds_validate_against_the_receiver_snapshot_not_current_config` |
| Receiver matches neither snapshot email nor account id | 200, dropped | Inbox `receiver_mismatch` | `gateway_refund_unmatched` | `refund_ipn_to_another_receiver_is_held` |
| `mc_currency` differs | 200, dropped | Inbox `currency_mismatch` | `gateway_refund_unmatched` | `refund_ipn_with_currency_mismatch_is_held` |
| `mc_gross` wrong sign, zero, malformed, wrong precision | 200, dropped | Inbox `amount_invalid` | `gateway_refund_unmatched` | `refund_ipn_with_a_non_negative_or_malformed_gross_is_held` |
| Same `txn_id` again (recorded or held) | 200, dropped | 200; no row, revision, event, or notification | unchanged | `duplicate_refund_ipn_is_a_no_op` |
| Two partial refunds summing to the total | 200, dropped twice | First keeps the state; second moves to `refunded_external` | "Refunded $X of $Y", then "Refund recorded from external evidence: {second txn}" | `two_partial_refund_ipns_sum_to_a_full_refund` |

### Interactions and migration

| Input or state | Deployed client sends / current behaviour | New behaviour | UI copy or state | Test name |
| --- | --- | --- | --- | --- |
| `refund.record_external` after PayPal partials, amount ≥ PayPal sum | `INVALID_STATE` (previous revision) | Accepted; `refunded_external`; return `refunded`; `external_refund` = the record | "Refunded $X of $Y" or full copy | `a_manual_record_closes_a_return_alongside_ipn_partials` |
| `refund.record_external` after PayPal partials, amount < PayPal sum | n/a | `INVALID_STATE` "The external refund cannot be recorded." | Shop toasts the sentence | `a_manual_record_closes_a_return_alongside_ipn_partials` |
| Second `refund.record_external` on a manual record | `INVALID_STATE` | Unchanged | Shop toasts the sentence | `a_manual_record_closes_a_return_alongside_ipn_partials` |
| Full refund of a stocked listing | n/a | No restock | unchanged listing | `a_refunded_order_does_not_restock` |
| Pickup snapshot purge while a reversal is outstanding | purged once terminal with refund evidence | Kept until `payment_reversed_at` clears | pickup reveal survives a canceled reversal | `clear_retention_and_terminal_purge` |
| Partial refund, then the buyer's review completes the order | n/a | `delivered` to `completed`; `external_refund` carried | "Refunded $X of $Y" | `partial_refund_ipn_keeps_the_state_and_records_the_amount` |
| Backfill, buyer report after the receipt at the same instant | n/a | Not backfilled (event sequence) | n/a | `migration_0039_backfills_only_gateway_verified_payment_ids` |
| Backfill, buyer report before a payment IPN without `txn_id` (buyer value survives) | n/a | Not backfilled (not PayPal-shaped) | n/a | `migration_0039_backfills_only_gateway_verified_payment_ids` |
| Backfill, a reference on two gateway orders | n/a | Neither backfilled | n/a | `migration_0039_skips_a_reference_shared_by_two_orders` |
| `paypal_txn_id` on two orders, or an account id without its email | n/a | Refused by the unique index and CHECK | n/a | `migration_0039_constraints_hold` |
| Backfilled payment id (no receiver snapshot) | receiver copied from current configuration (round 2) | Receiver unresolved; its refunds held as `receiver_unverified` | `gateway_refund_unmatched` | `a_refund_on_a_backfilled_payment_is_held_as_receiver_unverified` |
