-- Whether the Bitcoin payment request reached the buyer's wallet: the
-- delivery state paykit-server reports on the status poll
-- (`paykit_delivery_state`), normalized. NULL until the first poll after
-- activation; reset by every new bind. Additive and rerunnable.
--   pending    paykit is still establishing the link or sending
--   delivered  the request and endpoint were sent over the Encrypted Link
--   failed     paykit gave up (e.g. the wallet never answered the link)
ALTER TABLE orders ADD COLUMN IF NOT EXISTS paykit_delivery_state TEXT;
ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_paykit_delivery_state_check;
ALTER TABLE orders ADD CONSTRAINT orders_paykit_delivery_state_check
    CHECK (paykit_delivery_state IN ('pending', 'delivered', 'failed'));
