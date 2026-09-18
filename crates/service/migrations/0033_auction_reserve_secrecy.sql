-- Secret auction reserve authority. Every auction has exactly one row,
-- including auctions whose reserve is explicitly null. The public
-- `listings.auction` JSONB document never stores the reserve or reserve_met.

CREATE TABLE listing_auction_reserves (
    listing_aggregate_id TEXT PRIMARY KEY REFERENCES listings (aggregate_id),
    listing_revision BIGINT NOT NULL CHECK (listing_revision > 0),
    record_revision BIGINT NOT NULL CHECK (record_revision > 0),
    reserve_amount_minor BIGINT,
    reserve_currency TEXT,
    reserve_exponent INTEGER,
    last_command_id UUID NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT listing_auction_reserves_money_presence CHECK (
        (reserve_amount_minor IS NULL
            AND reserve_currency IS NULL
            AND reserve_exponent IS NULL)
        OR
        (reserve_amount_minor IS NOT NULL
            AND reserve_currency IS NOT NULL
            AND reserve_exponent IS NOT NULL)
    ),
    CONSTRAINT listing_auction_reserves_amount_bounds CHECK (
        reserve_amount_minor IS NULL
        OR reserve_amount_minor BETWEEN 1 AND 9007199254740991
    ),
    CONSTRAINT listing_auction_reserves_currency_format CHECK (
        reserve_currency IS NULL
        OR reserve_currency ~ '^[A-Z][A-Z0-9]{2,11}$'
    ),
    CONSTRAINT listing_auction_reserves_exponent_bounds CHECK (
        reserve_exponent IS NULL
        OR reserve_exponent BETWEEN 0 AND 18
    )
);

COMMENT ON TABLE listing_auction_reserves IS
    'Seller and trusted-service-only auction reserve authority; never serialized publicly.';
