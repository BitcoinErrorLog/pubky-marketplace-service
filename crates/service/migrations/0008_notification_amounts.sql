-- Additive: optional monetary context on delivered notifications.
--
-- An amount rides a notification only when the recipient already sees that
-- exact figure in a role-scoped projection they can read (the offer amount
-- on the offer projection, an auction's visible price on the listing
-- projection) — never anything address- or payment-bearing (ADR-0019 §8).
-- Shape matches the projections' money JSON: {amount_minor, currency,
-- exponent}. Rows delivered before this column existed stay NULL and keep
-- rendering without an amount.

ALTER TABLE notifications ADD COLUMN amount JSONB;
