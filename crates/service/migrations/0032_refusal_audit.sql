-- Privacy-bounded refusal audit. This migration is additive: it must never
-- alter the prior Locks success-only outcome history.

CREATE TABLE command_refusal_surface_kinds (
  id SMALLINT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE CHECK (name ~ '^[a-z0-9_]{1,64}$')
);
CREATE TABLE command_refusal_command_kinds (
  id SMALLINT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE CHECK (name ~ '^[a-z0-9_]{1,64}$')
);
CREATE TABLE command_refusal_kinds (
  id SMALLINT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE CHECK (name ~ '^[a-z0-9_]{1,64}$')
);

INSERT INTO command_refusal_surface_kinds (id, name) VALUES
  (1, 'v1_command'), (2, 'bitcoin_manual_resolve');
INSERT INTO command_refusal_command_kinds (id, name) VALUES
  (0, 'invalid_envelope'), (1, 'register_listing'), (2, 'sync_listing'),
  (3, 'sync_drop'), (4, 'cancel_drop'), (5, 'release_drop_listings'),
  (6, 'reserve_inventory'), (7, 'create_checkout'), (8, 'create_offer'),
  (9, 'counter_offer'), (10, 'accept_offer'), (11, 'offer_checkout'),
  (12, 'reject_offer'), (13, 'withdraw_offer'), (14, 'place_bid'),
  (15, 'close_auction'), (16, 'advance_sandbox_payment'), (17, 'prepare_locks'),
  (18, 'register_locks'), (19, 'request_cancellation'), (20, 'approve_cancellation'),
  (21, 'ship_order'), (22, 'confirm_delivery'), (23, 'set_pickup_details'),
  (24, 'clear_pickup_details'), (25, 'mark_ready_for_pickup'), (26, 'confirm_pickup'),
  (27, 'request_return'), (28, 'approve_return'), (29, 'receive_return'),
  (30, 'record_external_refund'), (31, 'create_review'), (32, 'update_review'),
  (33, 'set_band_consent'), (34, 'manual_resolve');
INSERT INTO command_refusal_kinds (id, name) VALUES
  (1, 'invalid_envelope'), (2, 'invalid_command'), (3, 'unauthorized'),
  (4, 'not_found'), (5, 'revision_conflict'), (6, 'idempotency_conflict'),
  (7, 'insufficient_inventory'), (8, 'invariant_violation'), (9, 'offer_expired'),
  (10, 'invalid_state'), (11, 'auction_closed'), (12, 'bid_too_low'),
  (13, 'upstream_unavailable'), (14, 'award_expired'), (15, 'award_already_converted'),
  (16, 'award_quantity_mismatch'), (17, 'award_variant_mismatch'),
  (18, 'award_listing_changed'), (19, 'award_hold_missing'),
  (20, 'manual_resolve_confirmation_observation_mismatch'),
  (21, 'manual_resolve_confirmation_effects_failed'),
  (22, 'manual_resolve_invalid_reason'), (23, 'manual_resolve_invalid_idempotency_key'),
  (24, 'manual_resolve_invalid_outcome'), (25, 'manual_resolve_invalid_refund_reference'),
  (26, 'manual_resolve_not_order_seller'), (27, 'manual_resolve_order_not_found'),
  (28, 'manual_resolve_order_not_awaiting_confirmation'),
  (29, 'manual_resolve_not_applicable'), (30, 'manual_resolve_missing_pin'),
  (31, 'manual_resolve_conflict'), (32, 'manual_resolve_already_resolved'),
  (33, 'manual_resolve_not_in_review'), (34, 'manual_resolve_stock_unavailable');

CREATE OR REPLACE FUNCTION public.forbid_refusal_audit_catalog_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'refusal audit catalogs are immutable'; END $$;
CREATE TRIGGER command_refusal_surface_kinds_immutable BEFORE UPDATE OR DELETE ON command_refusal_surface_kinds
FOR EACH ROW EXECUTE FUNCTION public.forbid_refusal_audit_catalog_mutation();
CREATE TRIGGER command_refusal_command_kinds_immutable BEFORE UPDATE OR DELETE ON command_refusal_command_kinds
FOR EACH ROW EXECUTE FUNCTION public.forbid_refusal_audit_catalog_mutation();
CREATE TRIGGER command_refusal_kinds_immutable BEFORE UPDATE OR DELETE ON command_refusal_kinds
FOR EACH ROW EXECUTE FUNCTION public.forbid_refusal_audit_catalog_mutation();

CREATE TABLE command_refusal_audit_buckets (
  bucket_start TIMESTAMPTZ NOT NULL,
  surface_kind SMALLINT NOT NULL REFERENCES command_refusal_surface_kinds(id),
  command_kind SMALLINT NOT NULL REFERENCES command_refusal_command_kinds(id),
  refusal_kind SMALLINT NOT NULL REFERENCES command_refusal_kinds(id),
  actor_key_epoch SMALLINT NOT NULL CHECK (actor_key_epoch > 0),
  actor_tag BYTEA NOT NULL CHECK (octet_length(actor_tag) = 16),
  occurrence_count BIGINT NOT NULL CHECK (occurrence_count > 0),
  count_saturated BOOLEAN NOT NULL DEFAULT FALSE,
  first_occurred_at TIMESTAMPTZ NOT NULL,
  last_occurred_at TIMESTAMPTZ NOT NULL,
  sample_command_tag BYTEA CHECK (sample_command_tag IS NULL OR octet_length(sample_command_tag) = 16),
  command_id_present BOOLEAN NOT NULL,
  PRIMARY KEY (bucket_start, surface_kind, command_kind, refusal_kind, actor_key_epoch, actor_tag),
  CHECK (date_trunc('hour', bucket_start) = bucket_start),
  CHECK (first_occurred_at >= bucket_start),
  CHECK (last_occurred_at >= first_occurred_at),
  CHECK (last_occurred_at < bucket_start + interval '1 hour')
);
CREATE INDEX command_refusal_audit_time_idx ON command_refusal_audit_buckets (bucket_start DESC);
CREATE INDEX command_refusal_audit_reason_idx ON command_refusal_audit_buckets (refusal_kind, bucket_start DESC);
CREATE INDEX command_refusal_audit_actor_idx ON command_refusal_audit_buckets (actor_key_epoch, actor_tag, bucket_start DESC);

CREATE TABLE command_refusal_audit_bucket_limits (
  bucket_start TIMESTAMPTZ PRIMARY KEY,
  admitted_rows INTEGER NOT NULL CHECK (admitted_rows BETWEEN 0 AND 10000),
  overflow_count BIGINT NOT NULL DEFAULT 0 CHECK (overflow_count >= 0),
  overflow_saturated BOOLEAN NOT NULL DEFAULT FALSE,
  CHECK (date_trunc('hour', bucket_start) = bucket_start)
);
CREATE TABLE command_refusal_audit_access_buckets (
  bucket_start TIMESTAMPTZ NOT NULL,
  session_role NAME NOT NULL,
  function_kind SMALLINT NOT NULL CHECK (function_kind IN (1, 2)),
  filter_class SMALLINT NOT NULL CHECK (filter_class BETWEEN 0 AND 7),
  result_size_band SMALLINT NOT NULL CHECK (result_size_band BETWEEN 0 AND 7),
  occurrence_count BIGINT NOT NULL CHECK (occurrence_count > 0),
  count_saturated BOOLEAN NOT NULL DEFAULT FALSE,
  PRIMARY KEY (bucket_start, session_role, function_kind, filter_class, result_size_band),
  CHECK (date_trunc('day', bucket_start) = bucket_start)
);
CREATE TABLE command_refusal_audit_access_limits (
  bucket_start TIMESTAMPTZ PRIMARY KEY,
  admitted_rows INTEGER NOT NULL CHECK (admitted_rows BETWEEN 0 AND 1000),
  overflow_count BIGINT NOT NULL DEFAULT 0 CHECK (overflow_count >= 0),
  overflow_saturated BOOLEAN NOT NULL DEFAULT FALSE,
  CHECK (date_trunc('day', bucket_start) = bucket_start)
);

DO $roles$
DECLARE role_name name;
BEGIN
  FOREACH role_name IN ARRAY ARRAY[
    'marketplace_refusal_audit_owner'::name,
    'marketplace_refusal_audit_writer'::name,
    'marketplace_refusal_audit_aggregate'::name,
    'marketplace_refusal_audit_raw'::name
  ] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = role_name) THEN
      EXECUTE pg_catalog.format('CREATE ROLE %I NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT -1', role_name);
    END IF;
  END LOOP;
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'marketplace_refusal_audit_retention') THEN
    CREATE ROLE marketplace_refusal_audit_retention LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 1;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'marketplace_refusal_audit_writer_login') THEN
    CREATE ROLE marketplace_refusal_audit_writer_login LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 2;
  END IF;
END $roles$;

DO $bootstrap$
DECLARE migration_login name := current_user;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'marketplace_refusal_audit_owner') THEN
    RAISE EXCEPTION 'marketplace_refusal_audit_owner missing';
  END IF;
  EXECUTE pg_catalog.format('GRANT marketplace_refusal_audit_owner TO %I', migration_login);
END $bootstrap$;
GRANT CREATE ON SCHEMA public TO marketplace_refusal_audit_owner;

CREATE OR REPLACE FUNCTION public.purge_refusal_audit(cutoff timestamptz, batch_size integer)
RETURNS integer LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE deleted_count integer;
BEGIN
  IF batch_size < 1 OR batch_size > 500 OR cutoff <> date_trunc('hour', now() - interval '30 days') THEN
    RAISE EXCEPTION 'invalid refusal audit purge bound';
  END IF;
  WITH doomed AS (
    SELECT ctid FROM public.command_refusal_audit_buckets WHERE bucket_start < cutoff
    ORDER BY bucket_start LIMIT batch_size
  ) DELETE FROM public.command_refusal_audit_buckets WHERE ctid IN (SELECT ctid FROM doomed);
  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  DELETE FROM public.command_refusal_audit_bucket_limits WHERE bucket_start < cutoff;
  DELETE FROM public.command_refusal_audit_access_buckets WHERE bucket_start < cutoff;
  DELETE FROM public.command_refusal_audit_access_limits WHERE bucket_start < cutoff;
  RETURN deleted_count;
END $$;

CREATE OR REPLACE FUNCTION public.operator_refusal_audit_summary(
  from_hour timestamptz, to_hour timestamptz, surface_filter smallint,
  command_filter smallint, refusal_filter smallint, page_size integer, cursor jsonb
) RETURNS TABLE(bucket_start timestamptz, surface_kind smallint, command_kind smallint,
  refusal_kind smallint, occurrence_count bigint, overflow_count bigint)
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  IF date_trunc('hour', from_hour) <> from_hour OR date_trunc('hour', to_hour) <> to_hour
     OR to_hour <= from_hour OR to_hour - from_hour > interval '30 days'
     OR page_size NOT BETWEEN 1 AND 100 THEN RAISE EXCEPTION 'invalid audit query bound'; END IF;
  RETURN QUERY SELECT b.bucket_start, b.surface_kind, b.command_kind, b.refusal_kind,
    b.occurrence_count, COALESCE(l.overflow_count, 0)
  FROM public.command_refusal_audit_buckets b
  LEFT JOIN public.command_refusal_audit_bucket_limits l USING (bucket_start)
  WHERE b.bucket_start >= from_hour AND b.bucket_start < to_hour
    AND (surface_filter IS NULL OR b.surface_kind = surface_filter)
    AND (command_filter IS NULL OR b.command_kind = command_filter)
    AND (refusal_filter IS NULL OR b.refusal_kind = refusal_filter)
  ORDER BY b.bucket_start, b.surface_kind, b.command_kind, b.refusal_kind LIMIT page_size;
END $$;

CREATE OR REPLACE FUNCTION public.operator_refusal_audit_buckets(
  from_hour timestamptz, to_hour timestamptz, surface_filter smallint,
  command_filter smallint, refusal_filter smallint, page_size integer, cursor jsonb
) RETURNS TABLE(bucket_start timestamptz, surface_kind smallint, command_kind smallint,
  refusal_kind smallint, actor_key_epoch smallint, actor_tag bytea, occurrence_count bigint,
  sample_command_tag bytea, command_id_present boolean)
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  IF date_trunc('hour', from_hour) <> from_hour OR date_trunc('hour', to_hour) <> to_hour
     OR to_hour <= from_hour OR to_hour - from_hour > interval '30 days'
     OR page_size NOT BETWEEN 1 AND 100 THEN RAISE EXCEPTION 'invalid audit query bound'; END IF;
  RETURN QUERY SELECT b.bucket_start, b.surface_kind, b.command_kind, b.refusal_kind,
    b.actor_key_epoch, b.actor_tag, b.occurrence_count, b.sample_command_tag, b.command_id_present
  FROM public.command_refusal_audit_buckets b
  WHERE b.bucket_start >= from_hour AND b.bucket_start < to_hour
    AND (surface_filter IS NULL OR b.surface_kind = surface_filter)
    AND (command_filter IS NULL OR b.command_kind = command_filter)
    AND (refusal_filter IS NULL OR b.refusal_kind = refusal_filter)
  ORDER BY b.bucket_start, b.surface_kind, b.command_kind, b.refusal_kind, b.actor_key_epoch, b.actor_tag
  LIMIT page_size;
END $$;

ALTER FUNCTION public.operator_refusal_audit_summary(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb) OWNER TO marketplace_refusal_audit_owner;
ALTER FUNCTION public.operator_refusal_audit_summary(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb) SET search_path = pg_catalog;
ALTER FUNCTION public.operator_refusal_audit_buckets(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb) OWNER TO marketplace_refusal_audit_owner;
ALTER FUNCTION public.operator_refusal_audit_buckets(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb) SET search_path = pg_catalog;
ALTER FUNCTION public.purge_refusal_audit(timestamptz,integer) OWNER TO marketplace_refusal_audit_owner;
ALTER FUNCTION public.purge_refusal_audit(timestamptz,integer) SET search_path = pg_catalog;

REVOKE ALL ON command_refusal_surface_kinds, command_refusal_command_kinds,
  command_refusal_kinds, command_refusal_audit_buckets,
  command_refusal_audit_bucket_limits, command_refusal_audit_access_buckets,
  command_refusal_audit_access_limits FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.operator_refusal_audit_summary(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.operator_refusal_audit_buckets(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.purge_refusal_audit(timestamptz,integer) FROM PUBLIC;
GRANT SELECT ON command_refusal_surface_kinds, command_refusal_command_kinds, command_refusal_kinds TO marketplace_refusal_audit_writer;
GRANT SELECT, INSERT, UPDATE ON command_refusal_audit_buckets, command_refusal_audit_bucket_limits TO marketplace_refusal_audit_writer;
GRANT marketplace_refusal_audit_writer TO marketplace_refusal_audit_writer_login;
GRANT EXECUTE ON FUNCTION public.operator_refusal_audit_summary(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb) TO marketplace_refusal_audit_aggregate, marketplace_refusal_audit_raw;
GRANT EXECUTE ON FUNCTION public.operator_refusal_audit_buckets(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb) TO marketplace_refusal_audit_raw;
GRANT EXECUTE ON FUNCTION public.purge_refusal_audit(timestamptz,integer) TO marketplace_refusal_audit_retention;
GRANT SELECT, INSERT, UPDATE, DELETE ON command_refusal_audit_buckets, command_refusal_audit_bucket_limits, command_refusal_audit_access_buckets, command_refusal_audit_access_limits TO marketplace_refusal_audit_owner;
REVOKE CREATE ON SCHEMA public FROM marketplace_refusal_audit_owner;
DO $bootstrap$
DECLARE migration_login name := current_user;
BEGIN EXECUTE pg_catalog.format('REVOKE marketplace_refusal_audit_owner FROM %I', migration_login); END $bootstrap$;
