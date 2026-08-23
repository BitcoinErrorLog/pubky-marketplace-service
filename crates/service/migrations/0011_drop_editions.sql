-- Drops layer 2 (ADR-0026): editions and post-end listing release.

-- The order's edition inside its drop: the value `paid_quantity` reached
-- when THIS order's payment confirmed, assigned exactly once under the drop
-- row lock in the confirmation transaction. 1-based and gapless over paid
-- orders; NULL for non-drop orders and for drop orders not yet paid.
ALTER TABLE orders ADD COLUMN edition INTEGER
    CHECK (edition IS NULL OR edition >= 1);

-- A binding the seller released after the drop ended
-- (`drop.release_listings`): gating no longer considers it at all, so the
-- listing sells again as ordinary open inventory. Distinct from `active`
-- (which only encodes "the drop is announced or live"): an ended binding
-- stays in gating consideration until it is released.
ALTER TABLE drop_listings ADD COLUMN released BOOLEAN NOT NULL DEFAULT false;
