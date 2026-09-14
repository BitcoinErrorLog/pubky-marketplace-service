-- Offer award snapshots and the additive conversion boundary.
ALTER TABLE offers
    ADD COLUMN variant_id TEXT,
    ADD COLUMN terms_listing_revision BIGINT,
    ADD COLUMN terms_listing_record_sha256 TEXT,
    ADD COLUMN terms_snapshot JSONB,
    ADD COLUMN award_id UUID UNIQUE,
    ADD COLUMN accepted_at TIMESTAMPTZ,
    ADD COLUMN award_expires_at TIMESTAMPTZ,
    ADD COLUMN reservation_id UUID UNIQUE,
    ADD COLUMN accepted_unit_price_minor BIGINT,
    ADD COLUMN accepted_currency TEXT,
    ADD COLUMN accepted_exponent INTEGER,
    ADD COLUMN accepted_quantity BIGINT,
    ADD COLUMN accepted_listing_aggregate_id TEXT,
    ADD COLUMN accepted_listing_title TEXT,
    ADD COLUMN accepted_listing_revision BIGINT,
    ADD COLUMN accepted_listing_record_sha256 TEXT,
    ADD COLUMN accepted_variant_id TEXT,
    ADD COLUMN accepted_variant_sku TEXT,
    ADD COLUMN accepted_variant_options JSONB,
    ADD COLUMN accepted_shipping_minor BIGINT,
    ADD COLUMN accepted_subtotal_minor BIGINT,
    ADD COLUMN accepted_total_minor BIGINT,
    ADD COLUMN accepted_fulfillment TEXT,
    ADD COLUMN converted_order_id UUID UNIQUE,
    ADD COLUMN converted_at TIMESTAMPTZ,
    ADD COLUMN expiry_reason TEXT;

ALTER TABLE offers
    DROP CONSTRAINT offers_state_check;

UPDATE offers
SET state = 'expired', expiry_reason = 'legacy_unconvertible', updated_at = updated_at
WHERE state = 'accepted';

ALTER TABLE offers
    ADD CONSTRAINT offers_state_check CHECK (
        state IN ('pending', 'countered', 'accepted', 'converted', 'rejected',
                  'withdrawn', 'expired')
    ),
    ADD CONSTRAINT offers_expiry_reason_check CHECK (
        expiry_reason IS NULL OR expiry_reason IN
        ('negotiation_window', 'award_window', 'legacy_unconvertible')
    ),
    ADD CONSTRAINT offers_award_snapshot_check CHECK (
        state NOT IN ('accepted', 'converted') OR (
            award_id IS NOT NULL AND accepted_at IS NOT NULL
            AND award_expires_at IS NOT NULL AND reservation_id IS NOT NULL
            AND accepted_unit_price_minor > 0 AND accepted_currency IS NOT NULL
            AND accepted_exponent BETWEEN 0 AND 18 AND accepted_quantity > 0
            AND accepted_listing_aggregate_id IS NOT NULL
            AND accepted_listing_revision > 0
            AND accepted_listing_record_sha256 IS NOT NULL
            AND accepted_variant_id IS NOT NULL
            AND accepted_shipping_minor >= 0 AND accepted_subtotal_minor > 0
            AND accepted_total_minor > 0 AND accepted_fulfillment = 'shipping'
        )
    ),
    ADD CONSTRAINT offers_converted_snapshot_check CHECK (
        state <> 'converted' OR (converted_order_id IS NOT NULL AND converted_at IS NOT NULL)
    );

ALTER TABLE reservations
    ADD COLUMN offer_award_id UUID UNIQUE;

ALTER TABLE reservations
    ADD CONSTRAINT reservations_offer_award_fk
    FOREIGN KEY (offer_award_id) REFERENCES offers (award_id)
    DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE offers
    ADD CONSTRAINT offers_reservation_fk
    FOREIGN KEY (reservation_id) REFERENCES reservations (id)
    DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE orders
    ADD COLUMN offer_award_id UUID UNIQUE,
    ADD COLUMN priced_from TEXT NOT NULL DEFAULT 'listing';

ALTER TABLE orders
    ADD CONSTRAINT orders_priced_from_check CHECK (priced_from IN ('listing', 'offer'));

ALTER TABLE orders
    ADD CONSTRAINT orders_offer_award_fk
    FOREIGN KEY (offer_award_id) REFERENCES offers (award_id);

ALTER TABLE payments
    ADD COLUMN merchandise_amount_minor BIGINT,
    ADD COLUMN merchandise_currency TEXT,
    ADD COLUMN merchandise_exponent INTEGER;

ALTER TABLE receipts
    ADD COLUMN merchandise_total_minor BIGINT,
    ADD COLUMN merchandise_currency TEXT,
    ADD COLUMN merchandise_exponent INTEGER;

ALTER TABLE receipts
    ADD CONSTRAINT receipts_merchandise_money_check CHECK (
        (merchandise_total_minor IS NULL AND merchandise_currency IS NULL
            AND merchandise_exponent IS NULL)
        OR (merchandise_total_minor >= 0 AND merchandise_currency IS NOT NULL
            AND merchandise_exponent BETWEEN 0 AND 18)
    );

CREATE INDEX offers_award_expiry_idx
    ON offers (award_expires_at)
    WHERE state = 'accepted';

