-- Seller shipping configuration (Shippo integration): the seller's own
-- Shippo API token sealed with the runtime cipher (never returned by any
-- read), and the ship-from address labels are purchased against.
CREATE TABLE shipping_configs (
    seller_pubky TEXT PRIMARY KEY,
    shippo_api_key_ciphertext BYTEA,
    ship_from JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

-- A purchased shipping label on an order: SELLER-ONLY data (the label PDF
-- embeds the buyer's delivery address, ADR-0019 §8), stored here and served
-- exclusively through the seller-scoped label endpoints — never in the
-- shared order projection.
ALTER TABLE orders ADD COLUMN shipping_label JSONB;
