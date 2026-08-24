-- The VISIBLE auction price right after this bid was applied (proxy-bid
-- second-price semantics), recorded so bid history can be published without
-- ever exposing any bidder's secret proxy maximum. NULL on bids placed
-- before this column existed — history shows those without an amount rather
-- than inventing one.
ALTER TABLE bids ADD COLUMN visible_amount_minor BIGINT;
