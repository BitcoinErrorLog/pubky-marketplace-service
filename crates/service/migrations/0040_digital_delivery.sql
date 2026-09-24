-- Digital delivery (digital-delivery-design.md §4.4).
--
-- Adds: `digital` to the listing and order fulfillment vocabularies; the
-- listing's public delivery kind; the per-listing monotonic version counter;
-- the three sealed families (deliverable versions, per-order pins, buyer
-- delivery emails) under DIGITAL_DELIVERY_ENCRYPTION_KEY; the buyer access
-- log; and the refusal-audit command kinds for the new commands.
--
-- Forward-only and additive: every statement is idempotent (IF NOT EXISTS /
-- DROP ... IF EXISTS then re-add / ON CONFLICT DO NOTHING), so a partially
-- applied deploy can re-run. Sealed rows are XChaCha20-Poly1305 with a fresh
-- nonce per seal and associated data naming the row's owners (§4.1); no
-- plaintext column exists. Every sealed table has a BIGSERIAL id so the
-- boot probe and the re-seal job page it by key.
--
-- There is NO DOWN migration.

-- === Fulfillment vocabularies =============================================

ALTER TABLE listings DROP CONSTRAINT IF EXISTS listings_fulfillment_methods_check;
ALTER TABLE listings
    ADD CONSTRAINT listings_fulfillment_methods_check CHECK (
        cardinality(fulfillment_methods) BETWEEN 1 AND 3
        AND fulfillment_methods <@ ARRAY['shipping', 'pickup', 'digital']::TEXT[]
    ) NOT VALID;
ALTER TABLE listings VALIDATE CONSTRAINT listings_fulfillment_methods_check;

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_fulfillment_check;
ALTER TABLE orders
    ADD CONSTRAINT orders_fulfillment_check
    CHECK (fulfillment IN ('shipping', 'pickup', 'digital')) NOT VALID;
ALTER TABLE orders VALIDATE CONSTRAINT orders_fulfillment_check;

-- === Listing: public delivery kind ========================================
-- What the listing page and Checkout show ("Instant download · PDF ·
-- 12 MB"). Written only by digital_delivery.set / .clear; NULL when the
-- listing has no current deliverable.

ALTER TABLE listings ADD COLUMN IF NOT EXISTS digital_delivery_kind TEXT;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS digital_delivery_content_type TEXT;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS digital_delivery_size_bytes BIGINT;
ALTER TABLE listings DROP CONSTRAINT IF EXISTS listings_digital_delivery_kind_check;
ALTER TABLE listings
    ADD CONSTRAINT listings_digital_delivery_kind_check CHECK (
        digital_delivery_kind IS NULL
        OR digital_delivery_kind IN ('file', 'link', 'text', 'email', 'message')
    ) NOT VALID;
ALTER TABLE listings VALIDATE CONSTRAINT listings_digital_delivery_kind_check;

-- === Per-listing version counter ==========================================
-- Survives digital_delivery.clear so versions never restart: a version is
-- part of the associated data and of the homeserver path, and a restarted
-- sequence would let a new upload collide with a pinned one.

CREATE TABLE IF NOT EXISTS listing_digital_counters (
    listing_aggregate_id TEXT PRIMARY KEY,
    seller_pubky TEXT NOT NULL,
    last_version BIGINT NOT NULL DEFAULT 0 CHECK (last_version >= 0),
    cleared_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL
);

-- === Sealed family 1: deliverable versions ================================
-- AAD: digital-version/v1|{listing_aggregate_id}|{deliverable_id}|{version}|{kind}.
-- The current version has superseded_at NULL (at most one per listing).
-- ciphertext_blake3 / size_bytes / content_type describe the homeserver
-- ciphertext of a file version and are NULL for other kinds.

CREATE TABLE IF NOT EXISTS listing_digital_versions (
    id BIGSERIAL PRIMARY KEY,
    listing_aggregate_id TEXT NOT NULL,
    seller_pubky TEXT NOT NULL,
    deliverable_id TEXT NOT NULL CHECK (deliverable_id ~ '^[0-9a-f]{32}$'),
    version BIGINT NOT NULL CHECK (version > 0),
    kind TEXT NOT NULL CHECK (kind IN ('file', 'link', 'text', 'email', 'message')),
    payload_ciphertext BYTEA NOT NULL,
    ciphertext_blake3 TEXT CHECK (ciphertext_blake3 IS NULL OR ciphertext_blake3 ~ '^[0-9a-f]{64}$'),
    size_bytes BIGINT CHECK (size_bytes IS NULL OR size_bytes > 0),
    content_type TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    superseded_at TIMESTAMPTZ,
    UNIQUE (listing_aggregate_id, version),
    CHECK ((kind = 'file') = (ciphertext_blake3 IS NOT NULL AND size_bytes IS NOT NULL
                              AND content_type IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS listing_digital_versions_one_current
    ON listing_digital_versions (listing_aggregate_id) WHERE superseded_at IS NULL;

-- === Sealed family 2: order pins ==========================================
-- AAD: digital-pin/v1|{order_id}|{line_index}|{listing_aggregate_id}|{deliverable_id}|{version}.
-- The version payload re-sealed per instant order line inside the receipt
-- transaction; the buyer's download reads only this row.

CREATE TABLE IF NOT EXISTS order_digital_pins (
    id BIGSERIAL PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders (id),
    line_index INTEGER NOT NULL CHECK (line_index >= 0),
    listing_aggregate_id TEXT NOT NULL,
    deliverable_id TEXT NOT NULL CHECK (deliverable_id ~ '^[0-9a-f]{32}$'),
    version BIGINT NOT NULL CHECK (version > 0),
    kind TEXT NOT NULL CHECK (kind IN ('file', 'link', 'text')),
    payload_ciphertext BYTEA NOT NULL,
    confirming_adapter TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (order_id, line_index)
);
CREATE INDEX IF NOT EXISTS order_digital_pins_listing_version
    ON order_digital_pins (listing_aggregate_id, version);

-- === Buyer access log =====================================================
-- One row per successful buyer read of a pinned line: the delivery evidence
-- ("first opened 14:02 · opened 3 times"). No IP address or client detail.

CREATE TABLE IF NOT EXISTS order_digital_access (
    id BIGSERIAL PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders (id),
    line_index INTEGER NOT NULL CHECK (line_index >= 0),
    accessed_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS order_digital_access_order_line
    ON order_digital_access (order_id, line_index);

-- === Sealed family 3: buyer delivery emails ===============================
-- AAD: buyer-email/v1|{order_id}|{buyer_pubky}. One row per order with an
-- email-kind line. The purge deletes the ciphertext and keeps emailed_at
-- and purged_at.

CREATE TABLE IF NOT EXISTS order_delivery_emails (
    id BIGSERIAL PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE REFERENCES orders (id),
    buyer_pubky TEXT NOT NULL,
    email_ciphertext BYTEA,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    emailed_at TIMESTAMPTZ,
    purged_at TIMESTAMPTZ,
    CHECK (email_ciphertext IS NOT NULL OR purged_at IS NOT NULL)
);

-- === Refusal-audit command kinds ==========================================
-- The catalog is append-only (0032's trigger forbids UPDATE and DELETE).

INSERT INTO command_refusal_command_kinds (id, name) VALUES
  (35, 'set_digital_delivery'), (36, 'clear_digital_delivery')
ON CONFLICT (id) DO NOTHING;
