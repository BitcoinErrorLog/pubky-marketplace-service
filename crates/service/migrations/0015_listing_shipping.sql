-- Real seller-signed shipping replaces the prototype's flat $12 fixture:
-- the flat shipping charged once per order line for this listing, in the
-- listing currency's minor units (0 = free / not configured). Populated
-- from the seller-signed record's shippingOptions at registration/sync;
-- existing rows heal on their next listing.sync.
ALTER TABLE listings ADD COLUMN shipping_minor BIGINT NOT NULL DEFAULT 0;
