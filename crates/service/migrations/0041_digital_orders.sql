-- Digital orders (digital-delivery-design.md §3.6, §4.2).
--
-- The instant a digital order was delivered: at confirmation for an order
-- whose every line is released on the order page, or when the seller marks
-- a manual delivery. The auto-complete sweep counts from it, as it counts
-- from a shipment's `delivered_at` or a pickup handover. And the token
-- buckets that bound the buyer's download read.
--
-- Additive and idempotent. There is NO DOWN migration.

ALTER TABLE orders ADD COLUMN IF NOT EXISTS digital_delivered_at TIMESTAMPTZ;

-- Token buckets for the buyer download read, keyed `buyer:{pubky}` and
-- `order:{id}`. One row per buyer and per order that ever downloaded.
CREATE TABLE IF NOT EXISTS digital_read_rate_limits (
    bucket TEXT PRIMARY KEY,
    tokens DOUBLE PRECISION NOT NULL CHECK (tokens >= 0),
    updated_at TIMESTAMPTZ NOT NULL
);
