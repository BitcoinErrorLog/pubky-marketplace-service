-- no-transaction
CREATE INDEX CONCURRENTLY listings_rule_probe_idx ON listings (updated_at);
CREATE INDEX CONCURRENTLY listings_rule_probe_two_idx ON listings (title);
