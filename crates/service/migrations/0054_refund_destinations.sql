-- The buyer-confirmed refund destination of a USDT order (the USDT refund
-- flow): additive, idempotent, directly rerunnable.
--
-- A USDT payment is refunded by the seller from their own wallet to an
-- Arbitrum One address the buyer confirmed. The address of the payment is
-- not assumed to take a refund (the buyer may have paid from an exchange),
-- so the buyer confirms one on the order. One row per order, replaced by the
-- buyer until a refund is recorded (`refund.confirm_destination`).
--
--   address         `0x` + 40 hex digits, as the buyer entered it. When the
--                   input was mixed-case its EIP-55 checksum was verified in
--                   code; the database checks only the shape.
--   address_source  `buyer_entered` is the only source today. The payment's
--                   own sending address is not known to the service, so there
--                   is no `payment_address` source until paykit-server can
--                   supply it; a later migration widens the CHECK then.
--   confirmed_at    when the buyer last confirmed the current address.
--
-- A refund recorded on a USDT order copies the address into the order's
-- `external_refund`, so a later row change cannot rewrite what was recorded.
--
-- `refund.confirm_destination` is a new refusal-audit command kind; the
-- catalog is append-only (0032's trigger forbids UPDATE and DELETE). There is
-- NO DOWN migration.

SET LOCAL lock_timeout = '10s';

CREATE TABLE IF NOT EXISTS order_refund_destinations (
    order_id UUID PRIMARY KEY REFERENCES orders (id),
    buyer_pubky TEXT NOT NULL,
    asset TEXT NOT NULL CHECK (asset = 'USDT'),
    network TEXT NOT NULL CHECK (network = 'arbitrum-one'),
    address TEXT NOT NULL CHECK (address ~ '^0x[0-9a-fA-F]{40}$'),
    address_source TEXT NOT NULL CHECK (address_source IN ('buyer_entered')),
    confirmed_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

INSERT INTO command_refusal_command_kinds (id, name) VALUES
  (39, 'confirm_refund_destination')
ON CONFLICT (id) DO NOTHING;
