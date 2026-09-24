# Released Paykit attempts

An order carries one set of Paykit pins: the Bitcoin attempt it is bound to. An attempt is released when:

- the activation worker voids the bind;
- a buyer cancels a preparing request; or
- a preparing order's hold expires.

A released invoice can still receive money, for example when an activation committed at Paykit but its response was lost. Every released attempt is recorded in `paykit_superseded_attempts` (migration 0042) and polled by its own reference until it closes.

## States

| State | Meaning | Who acts |
| --- | --- | --- |
| `watching` | No terminal answer yet. | The poller. |
| `closed_unpaid` | No money by 24 h after the attempt's expiry. | Nobody. |
| `late_money` | Confirmed money took the late-money path: the order completed late, entered `manual_review` (`late_settlement`, `refund_required`, or `amount_mismatch`), or was cancelled with `refund_required`. The paid attempt, with its quote, is the order's attempt of record. | The seller, through the existing manual-review resolution, when the payment is in `manual_review`. |
| `needs_review` | Money the order cannot take, or a detection that never confirmed. See `review_reason`. | An operator. |
| `resolved` | An operator recorded the outcome. | Nobody. |

`review_reason` values:

- `payment_settled`: money confirmed on a released attempt after the order's payment was already confirmed, resolved, or under review. The buyer paid twice.
- `other_rail`: money confirmed on a released Bitcoin attempt while another rail owns the payment: the order is bound to Stripe or PayPal, or the payment is managed by Locks (or any adapter other than `paykit` or `sandbox`). The order and payment are left untouched.
- `detected_unconfirmed`: money was detected on the released attempt but did not confirm within 7 days.

## Polling cadence

A released attempt is first checked on the next Paykit poll pass. After each check, the interval to the next one doubles, starting from `PAYKIT_POLL_SECONDS` and capped at one hour. An attempt with no detection closes on the first check at least 24 h after its expiry. A detection keeps the attempt watched for up to 7 days, after which it moves to `needs_review` with an `ALERT`.

## Alerts

- `ALERT money detected on a released paykit attempt`: a detection was recorded. No action yet.
- `ALERT money confirmed on a released paykit attempt …` (`code=paykit_released_attempt_paid_after_settlement`): a `payment_settled` or `other_rail` entry was created.
- `ALERT money detected on a released paykit attempt never confirmed` (`code=paykit_released_attempt_detected_unconfirmed`).

## Operator path

`paykit-attempts-admin` runs against the marketplace database. It needs `DATABASE_URL`, and `PAYKIT_ADMIN_OPERATOR` for resolutions.

```bash
# Everything waiting for review, one JSON object per line: order, invoice,
# seller, buyer, reason, invoice total, frozen observation (txid, observed
# sats, confirmations), and when it was held.
cargo run -p marketplace-service --bin paykit-attempts-admin -- list

# The buyer's extra payment was returned: record the external refund reference.
PAYKIT_ADMIN_OPERATOR=<operator> cargo run -p marketplace-service --bin paykit-attempts-admin -- \
  resolve <order_id> <invoice_id> refunded '<refund reference>'

# No refund is due (for example the detected transaction never settled).
PAYKIT_ADMIN_OPERATOR=<operator> cargo run -p marketplace-service --bin paykit-attempts-admin -- \
  resolve <order_id> <invoice_id> dismissed '<reason>'
```

For each entry:

1. Read the frozen observation (`txid`, `observed_sats`) and confirm the transaction on-chain against the invoice total.
2. `payment_settled` and `other_rail`: the seller holds funds the order cannot take. Ask the seller to return them to the buyer, then record the refund reference.
3. `detected_unconfirmed`: if the transaction was dropped or replaced, dismiss with the reason. If it has since confirmed, the buyer's money is real: handle it as `payment_settled`.

A resolution is recorded once. Resolving an entry that is not waiting for review fails, and the row is left unchanged.
