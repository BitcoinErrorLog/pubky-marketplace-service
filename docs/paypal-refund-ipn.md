# PayPal refund and reversal IPN contract

`POST /v0/paypal/ipn` records a seller's PayPal refund, and a buyer's PayPal
reversal (chargeback or dispute), on the order it refunds. No participant acts.
The service never moves money: PayPal already moved it, and the service
records the verified fact.

## Inputs

PayPal sends a new `txn_id` for each refund or reversal. `parent_txn_id` is the
`txn_id` of the original payment. `mc_gross` is negative: the amount returned
to the buyer, in the payment currency. `payment_status` is `Refunded` for a
merchant refund (full or partial) and `Reversed` for a chargeback or other
reversal. `Canceled_Reversal` means a reversal was undone and the funds came
back to the seller.

## Which transaction id is trusted

`orders.fiat_transaction_ref` is not trusted for refund matching. The buyer
writes it through `POST /v0/orders/{id}/fiat/mark-paid`, and a buyer report
after a gateway confirmation overwrites the gateway's value.

Refunds match `orders.paypal_txn_id` only. It is written from a postback-verified
`Completed` IPN whose receiver, currency, amount, and `custom` order id all
passed the payment checks, and nothing else writes it. The first verified
payment id is kept (`COALESCE`). It is written in every payment state, so a
`Completed` IPN that arrives after the seller already confirmed receipt still
stores the id. Migration `0039` backfills it from `fiat_transaction_ref` for
orders whose payment was gateway-verified and whose buyer report, if any, is
not later than the receipt.

`paypal_txn_id` is not projected. `fiat_transaction_ref` keeps its current
meaning and projection.

## Order resolution

1. `custom` is an order UUID: that order, and its `paypal_txn_id` must equal
   `parent_txn_id`.
2. `custom` is absent or not a UUID: the one PayPal order whose
   `paypal_txn_id` equals `parent_txn_id`. Zero or several matches drop the
   notification.

## Amounts

The refund amount is `-mc_gross` in minor units, parsed strictly under the
order's exponent (the fraction carries exactly the exponent's digits, as for
`Completed`). A refund is recorded once per refund `txn_id`
(`order_gateway_refunds` primary key). The order's `external_refund.amount_minor`
is the sum of every recorded refund and reversal on the order.

- Sum below the order total: a partial refund. The order keeps its state.
- Sum equal to the order total: a full refund. The order moves to
  `refunded_external`.
- Sum above the order total: nothing is recorded.

`external_refund` keeps its existing shape: `amount_minor` (the running sum),
`transaction_id` (the latest refund `txn_id`), `recorded_at` (the latest
record).

## No `refunded_partial` state

A partial refund leaves the order in its current state and records the running
sum on `external_refund`. There is no `refunded_partial` state.

- The Shop treats `refunded_partial` as terminal: completed tab, next actor
  `none`, payment terminal, no pickup reveal. A seller who refunds shipping on
  a `paid` order still has to ship it. A terminal state would strand that
  order, and every fulfillment edge would need a copy out of
  `refunded_partial`.
- The deployed Shop already renders "Refunded $X of $Y" whenever
  `externalRefund.amountMinor` is below `total.amountMinor`, in any state.
- A new order state changes the `orders.state` CHECK, the order enum in
  `contracts/state-machines.json`, and the Shop's vendored `orderStateSchema`
  and client transition table. Keeping the state set unchanged needs none of
  that.

## Reversal flag

A recorded `Reversed` notification sets `orders.payment_reversed_at` (first
reversal instant, never cleared) and projects it as `payment_reversed_at` to
both participants. The buyer opened the dispute, so it is not a secret from
the buyer. Both participants also receive `refund_recorded`.

`Canceled_Reversal` records nothing. A reversal that PayPal later cancels does
not undo `refunded_external`, because `refunded_external` has no outgoing edge.
The seller sees the returned funds in PayPal.

## Stock

A refund never restocks. A refunded order may have shipped or been handed
over, and the service cannot know whether the unit came back. Stock returns to
a listing only through cancellation (`order.cancel_approve` and the unilateral
buyer exits), as today. A seller who got the unit back raises the quantity on
the listing record.

## Notifications, events, reputation

The actor is `paypal-ipn`, not a participant. As with a gateway-confirmed
payment, both the buyer and the seller receive the notification.

| Record | Event kind | Notification | Reputation (`terminated_badly`) |
| --- | --- | --- | --- |
| Partial | `refund.recorded_partial` | `refund_recorded` to buyer and seller | not counted |
| Full | `refund.recorded_external` | `refund_recorded` to buyer and seller | counted, as for `refund.record_external` |

A full refund also writes the attestor annotation `refunded`, as
`refund.record_external` does.

## State machine contract

The order machine gains the server trigger `paypal_refund` on
`paid`, `ready_for_pickup`, `shipped`, `delivered`, `completed`, and
`return_received` to `refunded_external`. The return machine gains it on
`received` to `refunded`. No state is added. The Shop's vendored
`contracts/state-machines.json` and its client transition table need the five
new order edges when the Shop next re-vendors the artifact; order parsing is
unaffected because `refunded_external` is already a known state.

## Contract table

Every row answers PayPal with HTTP 200 unless it says 5xx. PayPal retries
only non-2xx.

"Drop" means nothing is written and an `info`/`warn` log line names the
reason. Log lines carry the order id at most, never an email, amount, or
transaction id.

### Refunded and Reversed by order state

| Input or state | Deployed client sends / current behaviour | New behaviour | UI copy or state | Test name |
| --- | --- | --- | --- | --- |
| `Refunded`, `paid`, sum < total | 200, dropped ("not a completed payment") | Record; state `paid`; revision +1; `external_refund` = running sum; event `refund.recorded_partial`; `refund_recorded` to buyer and seller | Shop: "Refunded $X of $Y" | `partial_refund_ipn_keeps_the_state_and_records_the_amount` |
| `Refunded`, `paid`, sum = total | 200, dropped | Record; state `refunded_external`; event `refund.recorded_external`; attestor `refunded`; `refund_recorded` to both | Shop: "Refund recorded from external evidence: {txn}" | `full_refund_ipn_moves_a_paid_order_to_refunded_external` |
| `Refunded`, `ready_for_pickup` | 200, dropped | As `paid` (partial keeps `ready_for_pickup`; full to `refunded_external`, which ends the pickup reveal) | As `paid` | `refund_ipn_applies_in_every_allowed_state` |
| `Refunded`, `shipped` | 200, dropped | As `paid` | As `paid` | `refund_ipn_applies_in_every_allowed_state` |
| `Refunded`, `delivered` | 200, dropped | As `paid` | As `paid` | `refund_ipn_applies_in_every_allowed_state` |
| `Refunded`, `completed` | 200, dropped | As `paid` | As `paid` | `refund_ipn_applies_in_every_allowed_state` |
| `Refunded`, `return_received` | 200, dropped | As `paid`; a full refund also moves `return_request.state` to `refunded` | As `paid`; return line "refunded" | `full_refund_ipn_resolves_a_received_return` |
| `Refunded`, `pending_payment` | 200, dropped | Drop (state refused) | unchanged | `refundable_states_are_exactly_the_paypal_refund_edges` |
| `Refunded`, `processing` | 200, dropped (state unreachable) | Drop (state refused) | unchanged | `refundable_states_are_exactly_the_paypal_refund_edges` |
| `Refunded`, `cancel_requested` | 200, dropped | Drop (state refused) | unchanged | `refund_ipn_in_a_refused_state_records_nothing` |
| `Refunded`, `cancelled` | 200, dropped | Drop (state refused); the seller records it with `refund.record_external` | unchanged | `refund_ipn_in_a_refused_state_records_nothing` |
| `Refunded`, `return_requested` | 200, dropped | Drop (state refused) | unchanged | `refund_ipn_in_a_refused_state_records_nothing` |
| `Refunded`, `return_approved` | 200, dropped | Drop (state refused) | unchanged | `refund_ipn_in_a_refused_state_records_nothing` |
| `Refunded`, `refunded_external` | 200, dropped | Drop (already refunded) | unchanged | `refund_ipn_in_a_refused_state_records_nothing` |
| `Refunded`, `closed` | 200, dropped (state unreachable) | Drop (state refused) | unchanged | `refundable_states_are_exactly_the_paypal_refund_edges` |
| `Reversed`, any allowed state | 200, dropped | As `Refunded` in that state, plus `payment_reversed_at` set once | `payment_reversed_at` on the order (Shop ignores the key today); `refund_recorded` to the seller | `reversed_ipn_records_the_refund_and_flags_the_order` |
| `Reversed`, any refused state | 200, dropped | Drop (state refused); no flag | unchanged | `refund_ipn_in_a_refused_state_records_nothing` |

### Other payment statuses

| Input or state | Deployed client sends / current behaviour | New behaviour | UI copy or state | Test name |
| --- | --- | --- | --- | --- |
| `Completed`, all checks pass, payment `awaiting_entitlement` | Confirms the order; `fiat_transaction_ref` = `txn_id` | Unchanged, plus `paypal_txn_id` = `txn_id` | `fiat_verification` `gateway-notified` | `a_verified_completed_ipn_pays_the_order_without_any_participant` |
| `Completed`, all checks pass, payment already `confirmed` (seller confirmed first) | No effect | `paypal_txn_id` = `txn_id` when unset; nothing else | unchanged (`seller-attested`) | `completed_ipn_after_seller_confirmation_arms_refund_matching` |
| `Completed`, all checks pass, payment `expired` or `manual_review` | Late-money fork or no effect | Unchanged, plus `paypal_txn_id` when unset (the same write runs before the confirmation in every payment state) | unchanged | `completed_ipn_after_seller_confirmation_arms_refund_matching` (the `confirmed` case of that write) |
| `Completed`, any check fails | 200, dropped | Unchanged; `paypal_txn_id` not written | unchanged | `an_ipn_that_does_not_match_server_held_facts_is_dropped` |
| `Canceled_Reversal`, any state | 200, dropped | 200, dropped; `payment_reversed_at` and `external_refund` unchanged | unchanged | `canceled_reversal_ipn_changes_nothing` |
| `Pending`, `Denied`, `Expired`, `Failed`, `Voided`, `Processed`, `Created`, any other | 200, dropped | Unchanged | unchanged | `an_ipn_that_does_not_match_server_held_facts_is_dropped` |

### Refunded and Reversed validation

| Input or state | Deployed client sends / current behaviour | New behaviour | UI copy or state | Test name |
| --- | --- | --- | --- | --- |
| Postback `INVALID` | 200, dropped | Unchanged | unchanged | `a_refund_ipn_that_fails_postback_validation_is_dropped` |
| Postback unreachable | 503 | Unchanged (PayPal retries) | unchanged | `an_unreachable_verifier_asks_paypal_to_retry_a_refund` |
| Payment methods disabled | 200, dropped | Unchanged | unchanged | existing startup gate (no route change) |
| `txn_id` or `parent_txn_id` missing, empty, over 64 characters, or not printable ASCII | 200, dropped | Drop | unchanged | `refund_ipn_with_malformed_transaction_ids_is_dropped` |
| `custom` names an order whose `paypal_txn_id` differs from `parent_txn_id` | 200, dropped | Drop | unchanged | `refund_ipn_whose_custom_order_does_not_own_the_parent_is_dropped` |
| `custom` names no existing order | 200, dropped | Drop | unchanged | `refund_ipn_with_unknown_parent_is_dropped` |
| `custom` absent, `parent_txn_id` matches no order | 200, dropped | Drop ("unknown parent") | unchanged | `refund_ipn_with_unknown_parent_is_dropped` |
| `custom` absent, `parent_txn_id` matches one order | 200, dropped | Resolve to that order, then as the state rows | as the state rows | `refund_ipn_without_custom_resolves_by_parent` |
| `parent_txn_id` equals a buyer-reported `fiat_transaction_ref` on a seller-attested order (no gateway id) | 200, dropped | Drop ("unknown parent") | unchanged | `a_buyer_reported_reference_never_matches_a_refund` |
| Refund IPN arrives before the order's `Completed` IPN is recorded (the payment IPN still in PayPal's retry queue) | 200, dropped | Drop (the order has no gateway id yet). Not retried with a 5xx: PayPal can disable IPN for an account whose endpoint keeps failing, and a seller-attested order never gains a gateway id | unchanged | `a_buyer_reported_reference_never_matches_a_refund` (same resolution: `custom` order without a gateway id) |
| Order not PayPal-bound | 200, dropped | Drop. Unreachable through resolution: only a PayPal-bound order's `Completed` IPN writes `paypal_txn_id` | unchanged | `refund_ipn_with_unknown_parent_is_dropped` (resolution) |
| `receiver_email` and `business` both differ from the seller's configured PayPal email | 200, dropped | Drop | unchanged | `refund_ipn_to_another_receiver_is_dropped` |
| `mc_currency` differs from the order currency | 200, dropped | Drop | unchanged | `refund_ipn_with_currency_mismatch_is_dropped` |
| `mc_gross` zero, positive, malformed, or wrong precision | 200, dropped | Drop | unchanged | `refund_ipn_with_a_non_negative_or_malformed_gross_is_dropped` |
| Running sum would exceed the order total | 200, dropped | Drop; earlier records stay | unchanged | `refund_ipn_exceeding_the_remaining_total_is_dropped` |
| Same refund `txn_id` again (PayPal retry or duplicate) | 200, dropped | 200; no row, revision, event, or notification | unchanged | `duplicate_refund_ipn_is_a_no_op` |
| Two partial refunds summing to the total | 200, dropped twice | First keeps the state (sum partial); second moves to `refunded_external` with the sum | "Refunded $X of $Y", then "Refund recorded from external evidence: {second txn}" | `two_partial_refund_ipns_sum_to_a_full_refund` |
| Database failure while recording | n/a | 500 (PayPal retries); the transaction rolls back | unchanged | `refund_ipn_database_failure_asks_paypal_to_retry` |

### Interactions

| Input or state | Deployed client sends / current behaviour | New behaviour | UI copy or state | Test name |
| --- | --- | --- | --- | --- |
| Seller sends `refund.record_external` after a partial IPN refund (order in `return_received`) | Accepted when `external_refund` is null | Refused `INVALID_STATE` "The external refund cannot be recorded." (existing one-record rule); further refunds arrive by IPN | Shop toasts the sentence | `manual_record_after_a_partial_ipn_refund_is_refused` |
| Full refund of a stocked listing | n/a | No restock: listing quantities unchanged | unchanged listing | `a_refunded_order_does_not_restock` |
| Partial refund, then the buyer's review completes the delivered order | n/a | `delivered` to `completed` unchanged; `external_refund` carried | "Refunded $X of $Y" on the completed order | `partial_refund_ipn_keeps_the_state_and_records_the_amount` |
| Reputation window containing a partial refund | n/a | `refund.recorded_partial` is not a terminal refund | none | `partial_refund_ipn_keeps_the_state_and_records_the_amount` |
