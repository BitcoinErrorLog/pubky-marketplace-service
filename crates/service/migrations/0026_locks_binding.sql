-- Seller-authored Locks metadata and immutable payment preparation facts.
-- All fields are additive so existing inventory and correlations remain valid.

ALTER TABLE listings
    ADD COLUMN digital_lock_policy_uri TEXT,
    ADD COLUMN digital_lock_criterion_id TEXT,
    ADD CONSTRAINT listings_digital_lock_pair CHECK (
        (digital_lock_policy_uri IS NULL) = (digital_lock_criterion_id IS NULL)
    );

ALTER TABLE payment_locks_correlations
    ADD COLUMN expected_resource_ciphertext BYTEA,
    ADD COLUMN expected_resource_hash TEXT,
    ADD COLUMN criterion_id TEXT,
    ADD COLUMN expected_reader_pubky TEXT,
    ADD COLUMN expected_recipient_pubky TEXT,
    ADD COLUMN client_reference_ciphertext BYTEA,
    ADD COLUMN preparation_state TEXT NOT NULL DEFAULT 'registered'
        CHECK (preparation_state IN ('prepared', 'registered')),
    ADD COLUMN binding_outcome TEXT;

ALTER TABLE payment_locks_correlations
    ADD CONSTRAINT payment_locks_prepared_fields CHECK (
        preparation_state <> 'prepared'
        OR (
            expected_resource_ciphertext IS NOT NULL
            AND expected_resource_hash IS NOT NULL
            AND criterion_id IS NOT NULL
            AND expected_reader_pubky IS NOT NULL
            AND expected_recipient_pubky IS NOT NULL
            AND client_reference_ciphertext IS NOT NULL
            AND bundle_id_ciphertext IS NULL
            AND bundle_lookup_token IS NULL
        )
    );

ALTER TABLE payment_locks_correlations
    ALTER COLUMN bundle_id_ciphertext DROP NOT NULL,
    ALTER COLUMN bundle_lookup_token DROP NOT NULL;
