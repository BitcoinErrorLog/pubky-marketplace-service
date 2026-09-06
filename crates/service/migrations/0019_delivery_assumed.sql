-- Delivery autocomplete (ADR-0019): there is no carrier tracking feed, so a
-- `shipped` order is marked `delivered` on server time once
-- DELIVERY_ASSUME_DAYS has elapsed since the ship timestamp. The
-- `delivery_assumed` flag lets the projection say "marked delivered
-- automatically after N days; tell us if it hasn't arrived" instead of
-- implying the buyer confirmed receipt.
--
-- Forward-only and additive: existing rows default to FALSE and remain
-- valid; only the server-time `delivery_assume` transition ever sets TRUE
-- (a buyer-confirmed delivery via `fulfillment.confirm_delivery` keeps
-- FALSE). Idempotent so a partially applied deploy can simply re-run.
--
-- There is NO DOWN migration.

ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_assumed BOOLEAN NOT NULL DEFAULT FALSE;
