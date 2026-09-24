-- Manual digital delivery (digital-delivery-design.md §4.3, §6 F13). Numbered 0043:
-- 0042 is reserved for the Paykit attempt fix (PR #42).
--
-- The instant the seller marked an order's message-delivered lines
-- delivered (email lines stamp `order_delivery_emails.emailed_at`), and
-- the refusal-audit command kinds for `order.set_delivery_email` and
-- `fulfillment.deliver_digital`.
--
-- Additive and idempotent. There is NO DOWN migration.

ALTER TABLE orders ADD COLUMN IF NOT EXISTS digital_message_delivered_at TIMESTAMPTZ;

INSERT INTO command_refusal_command_kinds (id, name) VALUES
  (37, 'set_delivery_email'), (38, 'deliver_digital')
ON CONFLICT (id) DO NOTHING;
