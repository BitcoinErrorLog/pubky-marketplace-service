# PayPal IPN fixtures

Raw `application/x-www-form-urlencoded` bodies as PayPal posts them to
`notify_url` for a `_xclick` Buy Now payment of 137.00 USD to
`merchant@example.com`, and for its refunds and reversals. The tests read a
file, replace `{{ORDER_ID}}` in `custom` with the fixture order id, and post
the body unchanged otherwise. The service echoes the exact body back to the
IPN validation double, as it does to PayPal.

The field names, their order, and value formats follow PayPal's IPN variable
reference and the IPN simulator templates for Web Accept (`Completed`),
`Refunded`, `Reversed`, and `Canceled_Reversal`. Values are synthetic:
sandbox-style ids and `example.com` addresses. They are not captured from a
live account.

| File | `payment_status` | `mc_gross` | `txn_id` | `parent_txn_id` | `reason_code` |
| --- | --- | --- | --- | --- | --- |
| `completed.ipn` | `Completed` | `137.00` | `7XP31449AB123456C` | none | none |
| `refund-full.ipn` | `Refunded` | `-137.00` | `2WF58163VJ0384519` | `7XP31449AB123456C` | `refund` |
| `refund-partial-1.ipn` | `Refunded` | `-40.00` | `3KA71027LM5520841` | `7XP31449AB123456C` | `refund` |
| `refund-partial-2.ipn` | `Refunded` | `-97.00` | `4LB82138MN6631952` | `7XP31449AB123456C` | `refund` |
| `reversal-full.ipn` | `Reversed` | `-137.00` | `5MC93249NP7742063` | `7XP31449AB123456C` | `chargeback` |
| `canceled-reversal.ipn` | `Canceled_Reversal` | `137.00` | `6ND04350PQ8853174` | `7XP31449AB123456C` | `other` |

A refund or reversal carries a new `txn_id`, the original payment's
`txn_id` as `parent_txn_id`, the original `custom`, and a negative
`mc_gross` and `mc_fee`. It has no `txn_type`. The two partial refunds sum to
the payment's gross.
