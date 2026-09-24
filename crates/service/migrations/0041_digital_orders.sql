-- Digital orders (digital-delivery-design.md §3.6, §4.2).
--
-- The instant a digital order was delivered: at confirmation for an order
-- whose every line is released on the order page, or when the seller marks
-- a manual delivery. The auto-complete sweep counts from it, as it counts
-- from a shipment's `delivered_at` or a pickup handover.
--
-- Additive and idempotent. There is NO DOWN migration.

ALTER TABLE orders ADD COLUMN IF NOT EXISTS digital_delivered_at TIMESTAMPTZ;
