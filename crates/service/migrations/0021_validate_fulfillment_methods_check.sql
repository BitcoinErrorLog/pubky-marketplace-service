-- Validate `listings_fulfillment_methods_check`.
--
-- Migration 0020 added this constraint NOT VALID but shipped without the
-- matching VALIDATE (the two orders checks in the same file validate
-- in-line). 0020 is already applied on staging, so the constraint must NOT
-- be edited in place there: the validation lands as this follow-up
-- migration instead. Until it runs, the check is enforced only for NEW
-- and UPDATED rows; validating here scans existing rows once (they all
-- satisfy the check — the column defaults to '{shipping}' and every write
-- goes through command validation) and marks the constraint fully trusted.
--
-- Idempotent: validating an already-validated constraint is a no-op.
-- There is NO DOWN migration.

ALTER TABLE listings VALIDATE CONSTRAINT listings_fulfillment_methods_check;
