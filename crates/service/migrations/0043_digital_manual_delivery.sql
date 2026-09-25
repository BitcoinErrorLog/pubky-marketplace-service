-- Manual digital delivery (digital-delivery-design.md §4.3, §6 F13, F17).
--
-- The instant the seller marked an order's message-delivered lines
-- delivered (email lines stamp `order_delivery_emails.emailed_at`), the
-- instant an order last entered a terminal state (`ended_at`, the clock the
-- buyer-email purge runs on), and the refusal-audit command kinds for
-- `order.set_delivery_email` and `fulfillment.deliver_digital`.
--
-- `ended_at` is stamped by trigger from the transition's `updated_at`
-- whenever `state` changes into `completed`, `cancelled`,
-- `refunded_external` or `closed`, and cleared when an order leaves those
-- states (late completion of a cancelled order). A later write that keeps
-- the state, such as a review on a completed order, leaves it alone.
-- Orders already in a terminal state are backfilled from `updated_at`.
--
-- Additive and idempotent. There is NO DOWN migration.

ALTER TABLE orders ADD COLUMN IF NOT EXISTS digital_message_delivered_at TIMESTAMPTZ;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS ended_at TIMESTAMPTZ;

UPDATE orders SET ended_at = updated_at
WHERE ended_at IS NULL AND state IN ('completed', 'cancelled', 'refunded_external', 'closed');

CREATE OR REPLACE FUNCTION stamp_order_ended_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.state IN ('completed', 'cancelled', 'refunded_external', 'closed') THEN
    IF TG_OP = 'INSERT' OR NEW.state IS DISTINCT FROM OLD.state OR OLD.ended_at IS NULL THEN
      NEW.ended_at := NEW.updated_at;
    ELSE
      NEW.ended_at := OLD.ended_at;
    END IF;
  ELSE
    NEW.ended_at := NULL;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS orders_stamp_ended_at ON orders;
CREATE TRIGGER orders_stamp_ended_at BEFORE INSERT OR UPDATE ON orders
  FOR EACH ROW EXECUTE FUNCTION stamp_order_ended_at();

INSERT INTO command_refusal_command_kinds (id, name) VALUES
  (37, 'set_delivery_email'), (38, 'deliver_digital')
ON CONFLICT (id) DO NOTHING;
