-- FX-at-bind is additive. Existing orders remain NULL and retain their
-- existing fiat fields. Rates are accepted only from Blocktank's BTCUSD
-- ticker; the source contract is pinned by the live fixture.
CREATE TABLE fx_rate_samples (
    id BIGSERIAL PRIMARY KEY,
    currency CHAR(3) NOT NULL,
    rate NUMERIC NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    accepted_at TIMESTAMPTZ NOT NULL,
    sample_bucket TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    CONSTRAINT fx_rate_samples_currency_check CHECK (currency = 'USD'),
    CONSTRAINT fx_rate_samples_rate_check CHECK (rate BETWEEN 10000 AND 1000000),
    CONSTRAINT fx_rate_samples_source_check CHECK (source = 'blocktank'),
    CONSTRAINT fx_rate_samples_one_per_bucket UNIQUE (currency, sample_bucket)
);

ALTER TABLE orders
    ADD COLUMN bitcoin_quote_rate NUMERIC,
    ADD COLUMN bitcoin_quote_source TEXT,
    ADD COLUMN bitcoin_quote_fetched_at TIMESTAMPTZ,
    ADD COLUMN bitcoin_quoted_sats BIGINT,
    ADD COLUMN bitcoin_quote_expires_at TIMESTAMPTZ,
    ADD COLUMN bitcoin_quote_currency CHAR(3),
    ADD COLUMN bitcoin_quote_exponent SMALLINT,
    ADD COLUMN bitcoin_quote_spread_bps INTEGER,
    ADD COLUMN paykit_observed_sats BIGINT;

ALTER TABLE orders ADD CONSTRAINT orders_fx_quote_check CHECK (
    (bitcoin_quote_rate IS NULL AND bitcoin_quote_source IS NULL
     AND bitcoin_quote_fetched_at IS NULL AND bitcoin_quoted_sats IS NULL
     AND bitcoin_quote_expires_at IS NULL AND bitcoin_quote_currency IS NULL
     AND bitcoin_quote_exponent IS NULL AND bitcoin_quote_spread_bps IS NULL)
    OR (bitcoin_quote_rate > 0 AND bitcoin_quote_source = 'blocktank'
        AND bitcoin_quote_fetched_at IS NOT NULL AND bitcoin_quoted_sats > 0
        AND bitcoin_quote_expires_at = paykit_expires_at
        AND bitcoin_quote_currency = currency
        AND bitcoin_quote_exponent = exponent
        AND bitcoin_quote_spread_bps >= 0
        AND bitcoin_quoted_sats <= 100000000)
);

ALTER TABLE orders ADD CONSTRAINT orders_paykit_observed_sats_check CHECK (
    paykit_observed_sats IS NULL
    OR (paykit_observed_sats > 0 AND paykit_total_sats IS NOT NULL)
);

CREATE OR REPLACE FUNCTION reject_paykit_observed_sats_rewrite()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.paykit_observed_sats IS NOT NULL
       AND NEW.paykit_observed_sats IS DISTINCT FROM OLD.paykit_observed_sats THEN
        RAISE EXCEPTION 'paykit_observed_sats is single-assignment';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER orders_paykit_observed_sats_immutable
    BEFORE UPDATE OF paykit_observed_sats ON orders
    FOR EACH ROW EXECUTE FUNCTION reject_paykit_observed_sats_rewrite();

CREATE INDEX fx_rate_samples_accepted_at
    ON fx_rate_samples (currency, accepted_at);
