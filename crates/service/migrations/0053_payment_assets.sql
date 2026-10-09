-- The asset model behind USDT payments (`USDT_PAYMENTS_ENABLED`): additive,
-- idempotent, directly rerunnable.
--
-- `orders.payment_method` stays the buyer-facing label and gains `usdt`.
-- What the buyer sends and where it settles live in five new columns that
-- are NULL on every order that is not paid in USDT:
--
--   payment_asset         what the buyer sends (`USDT`)
--   payment_network       where it settles (`arbitrum-one`)
--   payment_amount_minor  the amount sent, in units of 10^-payment_exponent
--   payment_exponent      decimals of that amount (6 for USDT0)
--   payment_quote_basis   how the amount was derived from the price (`parity`)
--
-- The order's `currency` and `total_minor` stay the price of record. The
-- columns are all set or all NULL, and the amount is positive. There is NO
-- CHECK on the values: a later asset, network or quote basis is a new value,
-- not a migration.
--
-- `orders.paykit_asset` (0052) is a different fact: what the upstream Paykit
-- request is DENOMINATED in (`BTC` today), not what the buyer sends.
--
-- `seller_accepted_payment_options` holds a seller's Shop-level consent to
-- accept an option beyond Bitcoin (`bitcoin_enabled` stays where it is). Its
-- only option today is USDT0 on Arbitrum One.
--
-- Both orders constraints are NOT VALID: they are enforced for every new and
-- updated row without scanning the table under a lock, and existing rows
-- already satisfy them. Nullable columns without defaults: no table rewrite,
-- and a binary built before this file lists its `orders` columns, so it
-- ignores them. There is NO DOWN migration.

SET LOCAL lock_timeout = '10s';

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_payment_method_check;
ALTER TABLE orders ADD CONSTRAINT orders_payment_method_check
    CHECK (payment_method IN ('bitcoin', 'stripe', 'paypal', 'usdt')) NOT VALID;

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS payment_asset TEXT,
    ADD COLUMN IF NOT EXISTS payment_network TEXT,
    ADD COLUMN IF NOT EXISTS payment_amount_minor BIGINT,
    ADD COLUMN IF NOT EXISTS payment_exponent SMALLINT,
    ADD COLUMN IF NOT EXISTS payment_quote_basis TEXT;

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_payment_asset_terms_check;
ALTER TABLE orders ADD CONSTRAINT orders_payment_asset_terms_check
    CHECK (
        (payment_asset IS NULL) = (payment_network IS NULL)
        AND (payment_asset IS NULL) = (payment_amount_minor IS NULL)
        AND (payment_asset IS NULL) = (payment_exponent IS NULL)
        AND (payment_asset IS NULL) = (payment_quote_basis IS NULL)
        AND (payment_amount_minor IS NULL OR payment_amount_minor > 0)
        AND (payment_exponent IS NULL OR payment_exponent >= 0)
    ) NOT VALID;

CREATE TABLE IF NOT EXISTS seller_accepted_payment_options (
    seller_pubky TEXT NOT NULL,
    option_id TEXT NOT NULL CHECK (option_id IN ('paykit.usdt.arbitrum-one')),
    enabled BOOLEAN NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (seller_pubky, option_id)
);
