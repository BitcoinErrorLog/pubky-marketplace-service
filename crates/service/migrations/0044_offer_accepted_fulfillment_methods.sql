-- Offers on pickup listings (pubky-marketplace#57).
--
-- An accepted award records the physical fulfillment methods (`shipping`,
-- `pickup`) its seller-signed listing snapshot published, next to the
-- shipping price taken from the same snapshot. Award checkout authorizes the
-- buyer's fulfillment choice from this column, never from the mutable
-- `listings.fulfillment_methods` row, so the method and the price always come
-- from one snapshot.
--
-- NULL is every award accepted before this migration: shipping only, as
-- those awards were priced. A snapshot that does not publish shipping priced
-- shipping at zero, which the second constraint pins.
--
-- `accepted_fulfillment` (0025) stays the legacy `shipping` marker; nothing
-- reads it, and this column is the authority.
--
-- Additive and idempotent. There is NO DOWN migration.

ALTER TABLE offers ADD COLUMN IF NOT EXISTS accepted_fulfillment_methods TEXT[];

ALTER TABLE offers DROP CONSTRAINT IF EXISTS offers_accepted_fulfillment_methods_check;
ALTER TABLE offers
    ADD CONSTRAINT offers_accepted_fulfillment_methods_check CHECK (
        accepted_fulfillment_methods IS NULL OR (
            cardinality(accepted_fulfillment_methods) BETWEEN 1 AND 2
            AND accepted_fulfillment_methods <@ ARRAY['shipping', 'pickup']::TEXT[]
        )
    ) NOT VALID;
ALTER TABLE offers VALIDATE CONSTRAINT offers_accepted_fulfillment_methods_check;

ALTER TABLE offers DROP CONSTRAINT IF EXISTS offers_accepted_pickup_only_ships_free_check;
ALTER TABLE offers
    ADD CONSTRAINT offers_accepted_pickup_only_ships_free_check CHECK (
        accepted_fulfillment_methods IS NULL
        OR 'shipping' = ANY (accepted_fulfillment_methods)
        OR accepted_shipping_minor = 0
    ) NOT VALID;
ALTER TABLE offers VALIDATE CONSTRAINT offers_accepted_pickup_only_ships_free_check;
