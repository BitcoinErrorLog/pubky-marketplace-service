-- The immutable checkout-time Locks authority snapshot (Sol Wave 1A review
-- round 2, P1-4). DESIGN §§3.1–3.2 require the order's expected lock to be
-- fixed AT CHECKOUT: the listing rows are mutable seller state (an
-- equal-revision sync may heal their digital_lock columns), so
-- `payment.prepare_locks` must never re-read them. Checkout writes one
-- private, non-projected row per payment carrying the seller-authoritative
-- lock exactly as the buyer's cart saw it: the canonical resource sealed
-- with the payment id as associated data (it must never appear in
-- plaintext at rest), its hash for equality, the seller-authored criterion,
-- and the expected economics (amount/asset/exponent from the immutable
-- payment snapshot, recipient the seller, reader the buyer). Prepare reads
-- ONLY this row; a payment without one (legacy orders, zero or multiple
-- distinct locks at checkout) is refused statically.

CREATE TABLE payment_locks_checkout_snapshots (
    payment_id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    expected_resource_ciphertext BYTEA NOT NULL,
    expected_resource_hash TEXT NOT NULL,
    criterion_id TEXT NOT NULL,
    amount_minor BIGINT NOT NULL,
    asset TEXT NOT NULL,
    exponent INTEGER NOT NULL,
    expected_reader_pubky TEXT NOT NULL,
    expected_recipient_pubky TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
