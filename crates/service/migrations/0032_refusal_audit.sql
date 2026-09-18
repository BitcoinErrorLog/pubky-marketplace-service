-- Privacy-bounded refusal audit. This migration is additive: it must never
-- alter the prior Locks success-only outcome history.

-- A direct re-run starts after the objects have already transferred to the
-- NOLOGIN owner. Re-establish only the same transactional temporary
-- membership used by the first-run bootstrap, before touching those objects.
DO $rerun_bootstrap$
DECLARE migration_login name := current_user;
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles
    WHERE rolname = 'marketplace_refusal_audit_owner'
  ) THEN
    IF EXISTS (
      SELECT 1 FROM pg_catalog.pg_roles
      WHERE rolname = 'marketplace_refusal_audit_owner'
        AND (rolcanlogin OR rolinherit OR rolsuper OR rolcreatedb OR
             rolcreaterole OR rolreplication OR rolbypassrls OR rolconnlimit <> -1)
    ) THEN
      RAISE EXCEPTION 'marketplace_refusal_audit_owner exists with unsafe attributes';
    END IF;
    EXECUTE pg_catalog.format(
      'GRANT marketplace_refusal_audit_owner TO %I', migration_login
    );
  END IF;
END $rerun_bootstrap$;

CREATE TABLE IF NOT EXISTS command_refusal_surface_kinds (
  id SMALLINT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE CHECK (name ~ '^[a-z0-9_]{1,64}$')
);
CREATE TABLE IF NOT EXISTS command_refusal_command_kinds (
  id SMALLINT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE CHECK (name ~ '^[a-z0-9_]{1,64}$')
);
CREATE TABLE IF NOT EXISTS command_refusal_kinds (
  id SMALLINT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE CHECK (name ~ '^[a-z0-9_]{1,64}$')
);

INSERT INTO command_refusal_surface_kinds (id, name) VALUES
  (1, 'v1_command'), (2, 'bitcoin_manual_resolve')
ON CONFLICT (id) DO NOTHING;
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
  (33, 'set_band_consent'), (34, 'manual_resolve')
ON CONFLICT (id) DO NOTHING;
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
  (33, 'manual_resolve_not_in_review'), (34, 'manual_resolve_stock_unavailable'),
  (35, 'bid_wrong_asset'), (36, 'bid_seller_forbidden'),
  (37, 'bid_not_auction'), (38, 'bid_listing_not_found'),
  (39, 'locks_identity_mismatch'), (40, 'locks_upstream_unavailable')
ON CONFLICT (id) DO NOTHING;

DO $catalog_parity$
BEGIN
  IF (SELECT jsonb_object_agg(id::text, name ORDER BY id) FROM command_refusal_surface_kinds)
       <> '{"1":"v1_command","2":"bitcoin_manual_resolve"}'::jsonb
     OR (SELECT count(*) FROM command_refusal_command_kinds) <> 35
     OR (SELECT count(*) FROM command_refusal_kinds) <> 40 THEN
    RAISE EXCEPTION 'refusal audit catalog parity violation';
  END IF;
END $catalog_parity$;

CREATE OR REPLACE FUNCTION public.forbid_refusal_audit_catalog_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'refusal audit catalogs are immutable'; END $$;
DROP TRIGGER IF EXISTS command_refusal_surface_kinds_immutable ON command_refusal_surface_kinds;
CREATE TRIGGER command_refusal_surface_kinds_immutable BEFORE UPDATE OR DELETE ON command_refusal_surface_kinds
FOR EACH ROW EXECUTE FUNCTION public.forbid_refusal_audit_catalog_mutation();
DROP TRIGGER IF EXISTS command_refusal_command_kinds_immutable ON command_refusal_command_kinds;
CREATE TRIGGER command_refusal_command_kinds_immutable BEFORE UPDATE OR DELETE ON command_refusal_command_kinds
FOR EACH ROW EXECUTE FUNCTION public.forbid_refusal_audit_catalog_mutation();
DROP TRIGGER IF EXISTS command_refusal_kinds_immutable ON command_refusal_kinds;
CREATE TRIGGER command_refusal_kinds_immutable BEFORE UPDATE OR DELETE ON command_refusal_kinds
FOR EACH ROW EXECUTE FUNCTION public.forbid_refusal_audit_catalog_mutation();

CREATE TABLE IF NOT EXISTS command_refusal_audit_buckets (
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
CREATE INDEX IF NOT EXISTS command_refusal_audit_time_idx ON command_refusal_audit_buckets (bucket_start DESC);
CREATE INDEX IF NOT EXISTS command_refusal_audit_reason_idx ON command_refusal_audit_buckets (refusal_kind, bucket_start DESC);
CREATE INDEX IF NOT EXISTS command_refusal_audit_actor_idx ON command_refusal_audit_buckets (actor_key_epoch, actor_tag, bucket_start DESC);

CREATE TABLE IF NOT EXISTS command_refusal_audit_bucket_limits (
  bucket_start TIMESTAMPTZ PRIMARY KEY,
  admitted_rows INTEGER NOT NULL CHECK (admitted_rows BETWEEN 0 AND 10000),
  overflow_count BIGINT NOT NULL DEFAULT 0 CHECK (overflow_count >= 0),
  overflow_saturated BOOLEAN NOT NULL DEFAULT FALSE,
  CHECK (date_trunc('hour', bucket_start) = bucket_start)
);
CREATE TABLE IF NOT EXISTS command_refusal_audit_access_buckets (
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
CREATE TABLE IF NOT EXISTS command_refusal_audit_access_limits (
  bucket_start TIMESTAMPTZ PRIMARY KEY,
  admitted_rows INTEGER NOT NULL CHECK (admitted_rows BETWEEN 0 AND 1000),
  overflow_count BIGINT NOT NULL DEFAULT 0 CHECK (overflow_count >= 0),
  overflow_saturated BOOLEAN NOT NULL DEFAULT FALSE,
  CHECK (date_trunc('day', bucket_start) = bucket_start)
);
CREATE TABLE IF NOT EXISTS command_refusal_audit_loss_gap (
  id BOOLEAN PRIMARY KEY CHECK (id),
  pending_loss_count BIGINT NOT NULL CHECK (pending_loss_count >= 0),
  updated_at TIMESTAMPTZ NOT NULL
);

DO $roles$
DECLARE
  role_name name;
  expected_login boolean;
  expected_limit integer;
BEGIN
  FOREACH role_name IN ARRAY ARRAY[
    'marketplace_refusal_audit_owner'::name,
    'marketplace_refusal_audit_writer'::name,
    'marketplace_refusal_audit_aggregate'::name,
    'marketplace_refusal_audit_raw'::name,
    'marketplace_refusal_audit_retention'::name,
    'marketplace_refusal_audit_writer_login'::name
  ] LOOP
    expected_login := role_name IN (
      'marketplace_refusal_audit_retention',
      'marketplace_refusal_audit_writer_login'
    );
    expected_limit := CASE role_name
      WHEN 'marketplace_refusal_audit_retention' THEN 1
      WHEN 'marketplace_refusal_audit_writer_login' THEN 2
      ELSE -1
    END;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = role_name) THEN
      EXECUTE pg_catalog.format(
        'CREATE ROLE %I %s NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT %s',
        role_name,
        CASE WHEN expected_login THEN 'LOGIN' ELSE 'NOLOGIN' END,
        expected_limit
      );
    ELSIF EXISTS (
      SELECT 1 FROM pg_catalog.pg_roles
      WHERE rolname = role_name
        AND (
          rolcanlogin <> expected_login OR rolinherit OR rolsuper OR
          rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls OR
          rolconnlimit <> expected_limit
        )
    ) THEN
      RAISE EXCEPTION '% exists with unsafe attributes', role_name;
    END IF;
  END LOOP;
END $roles$;

DO $role_memberships$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_catalog.pg_auth_members m
    JOIN pg_catalog.pg_roles member ON member.oid = m.member
    JOIN pg_catalog.pg_roles parent ON parent.oid = m.roleid
    WHERE member.rolname = 'marketplace_refusal_audit_writer_login'
      AND parent.rolname <> 'marketplace_refusal_audit_writer'
  ) OR EXISTS (
    SELECT 1 FROM pg_catalog.pg_auth_members m
    JOIN pg_catalog.pg_roles member ON member.oid = m.member
    WHERE member.rolname = 'marketplace_refusal_audit_retention'
  ) OR EXISTS (
    SELECT 1 FROM pg_catalog.pg_auth_members m
    JOIN pg_catalog.pg_roles member ON member.oid = m.member
    WHERE member.rolname IN (
      'marketplace_refusal_audit_owner',
      'marketplace_refusal_audit_writer',
      'marketplace_refusal_audit_aggregate',
      'marketplace_refusal_audit_raw'
    )
  ) THEN
    RAISE EXCEPTION 'refusal audit machine login has excess role membership';
  END IF;
END $role_memberships$;

DO $bootstrap$
DECLARE migration_login name := current_user;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'marketplace_refusal_audit_owner') THEN
    RAISE EXCEPTION 'marketplace_refusal_audit_owner missing';
  END IF;
  EXECUTE pg_catalog.format('GRANT marketplace_refusal_audit_owner TO %I', migration_login);
END $bootstrap$;
GRANT CREATE ON SCHEMA public TO marketplace_refusal_audit_owner;

CREATE OR REPLACE FUNCTION public.purge_refusal_audit(reference_time timestamptz, batch_size integer)
RETURNS integer LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  cutoff timestamptz;
  deleted_count integer := 0;
  batch_count integer;
  target text;
BEGIN
  -- PostgreSQL is the authoritative purge clock. Accept the current or
  -- immediately previous service-clock hour so crossing an hour boundary
  -- cannot abort a valid pass; derive the privacy cutoff here.
  IF batch_size < 1 OR batch_size > 500
     OR date_trunc('hour', reference_time) <> reference_time
     OR reference_time > date_trunc('hour', clock_timestamp())
     OR reference_time < date_trunc('hour', clock_timestamp()) - interval '1 hour' THEN
    RAISE EXCEPTION 'invalid refusal audit purge bound';
  END IF;
  cutoff := date_trunc('hour', clock_timestamp()) - interval '30 days';

  -- At most two oldest-first registered-table batches per tick. Repeated
  -- ticks resume from whichever privacy-bearing table has the oldest row.
  FOR target IN
    SELECT table_name FROM (
      SELECT 'command_refusal_audit_buckets'::text AS table_name, min(bucket_start) AS oldest
        FROM public.command_refusal_audit_buckets WHERE bucket_start < cutoff
      UNION ALL
      SELECT 'command_refusal_audit_bucket_limits', min(bucket_start)
        FROM public.command_refusal_audit_bucket_limits WHERE bucket_start < cutoff
      UNION ALL
      SELECT 'command_refusal_audit_access_buckets', min(bucket_start)
        FROM public.command_refusal_audit_access_buckets WHERE bucket_start < cutoff
      UNION ALL
      SELECT 'command_refusal_audit_access_limits', min(bucket_start)
        FROM public.command_refusal_audit_access_limits WHERE bucket_start < cutoff
    ) registered
    WHERE oldest IS NOT NULL
    ORDER BY oldest, table_name
    LIMIT 2
  LOOP
    CASE target
      WHEN 'command_refusal_audit_buckets' THEN
        WITH doomed AS (
          SELECT ctid FROM public.command_refusal_audit_buckets
          WHERE bucket_start < cutoff ORDER BY bucket_start, ctid LIMIT batch_size
        )
        DELETE FROM public.command_refusal_audit_buckets
        WHERE ctid IN (SELECT ctid FROM doomed);
      WHEN 'command_refusal_audit_bucket_limits' THEN
        WITH doomed AS (
          SELECT ctid FROM public.command_refusal_audit_bucket_limits
          WHERE bucket_start < cutoff ORDER BY bucket_start, ctid LIMIT batch_size
        )
        DELETE FROM public.command_refusal_audit_bucket_limits
        WHERE ctid IN (SELECT ctid FROM doomed);
      WHEN 'command_refusal_audit_access_buckets' THEN
        WITH doomed AS (
          SELECT ctid FROM public.command_refusal_audit_access_buckets
          WHERE bucket_start < cutoff ORDER BY bucket_start, ctid LIMIT batch_size
        )
        DELETE FROM public.command_refusal_audit_access_buckets
        WHERE ctid IN (SELECT ctid FROM doomed);
      WHEN 'command_refusal_audit_access_limits' THEN
        WITH doomed AS (
          SELECT ctid FROM public.command_refusal_audit_access_limits
          WHERE bucket_start < cutoff ORDER BY bucket_start, ctid LIMIT batch_size
        )
        DELETE FROM public.command_refusal_audit_access_limits
        WHERE ctid IN (SELECT ctid FROM doomed);
      ELSE
        RAISE EXCEPTION 'unregistered refusal audit retention table';
    END CASE;
    GET DIAGNOSTICS batch_count = ROW_COUNT;
    deleted_count := deleted_count + batch_count;
  END LOOP;
  RETURN deleted_count;
END $$;

CREATE OR REPLACE FUNCTION public.record_refusal_audit_access(
  requested_function_kind smallint,
  requested_filter_class smallint,
  requested_result_size integer
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  day_start timestamptz := date_trunc('day', clock_timestamp());
  size_band smallint := CASE
    WHEN requested_result_size = 0 THEN 0
    WHEN requested_result_size = 1 THEN 1
    WHEN requested_result_size <= 10 THEN 2
    WHEN requested_result_size <= 50 THEN 3
    ELSE 4
  END;
  updated bigint;
  admitted integer;
BEGIN
  IF requested_function_kind NOT IN (1, 2)
     OR requested_filter_class NOT BETWEEN 0 AND 7
     OR requested_result_size NOT BETWEEN 0 AND 100 THEN
    RAISE EXCEPTION 'invalid refusal audit access descriptor';
  END IF;
  UPDATE public.command_refusal_audit_access_buckets
     SET occurrence_count = CASE WHEN occurrence_count = 9223372036854775807
            THEN occurrence_count ELSE occurrence_count + 1 END,
         count_saturated = count_saturated OR occurrence_count = 9223372036854775807
   WHERE bucket_start = day_start AND session_role = session_user
     AND function_kind = requested_function_kind
     AND filter_class = requested_filter_class AND result_size_band = size_band;
  GET DIAGNOSTICS updated = ROW_COUNT;
  IF updated = 0 THEN
    PERFORM pg_advisory_xact_lock(hashtextextended(day_start::text, 33));
    UPDATE public.command_refusal_audit_access_buckets
       SET occurrence_count = CASE WHEN occurrence_count = 9223372036854775807
              THEN occurrence_count ELSE occurrence_count + 1 END,
           count_saturated = count_saturated OR occurrence_count = 9223372036854775807
     WHERE bucket_start = day_start AND session_role = session_user
       AND function_kind = requested_function_kind
       AND filter_class = requested_filter_class AND result_size_band = size_band;
    GET DIAGNOSTICS updated = ROW_COUNT;
    IF updated = 0 THEN
      INSERT INTO public.command_refusal_audit_access_limits(bucket_start, admitted_rows)
      VALUES (day_start, 0) ON CONFLICT (bucket_start) DO NOTHING;
      SELECT admitted_rows INTO admitted
        FROM public.command_refusal_audit_access_limits
       WHERE bucket_start = day_start FOR UPDATE;
      IF admitted >= 1000 THEN
        UPDATE public.command_refusal_audit_access_limits
           SET overflow_count = CASE WHEN overflow_count = 9223372036854775807
                  THEN overflow_count ELSE overflow_count + 1 END,
               overflow_saturated = overflow_saturated OR overflow_count = 9223372036854775807
         WHERE bucket_start = day_start;
        RETURN false;
      END IF;
      INSERT INTO public.command_refusal_audit_access_buckets(
        bucket_start, session_role, function_kind, filter_class,
        result_size_band, occurrence_count
      ) VALUES (
        day_start, session_user, requested_function_kind,
        requested_filter_class, size_band, 1
      );
      UPDATE public.command_refusal_audit_access_limits
         SET admitted_rows = admitted_rows + 1 WHERE bucket_start = day_start;
    END IF;
  END IF;
  RETURN true;
END $$;

CREATE OR REPLACE FUNCTION public.operator_refusal_audit_summary(
  from_hour timestamptz, to_hour timestamptz, surface_filter smallint,
  command_filter smallint, refusal_filter smallint, page_size integer, cursor jsonb
) RETURNS TABLE(bucket_start timestamptz, surface_kind smallint, command_kind smallint,
  refusal_kind smallint, occurrence_count bigint, overflow_count bigint)
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  cursor_bucket timestamptz;
  cursor_surface smallint;
  cursor_command smallint;
  cursor_refusal smallint;
  result_count integer;
  filter_class smallint :=
    (CASE WHEN surface_filter IS NULL THEN 0 ELSE 1 END)
    + (CASE WHEN command_filter IS NULL THEN 0 ELSE 2 END)
    + (CASE WHEN refusal_filter IS NULL THEN 0 ELSE 4 END);
BEGIN
  IF date_trunc('hour', from_hour) <> from_hour OR date_trunc('hour', to_hour) <> to_hour
     OR to_hour <= from_hour OR to_hour - from_hour > interval '30 days'
     OR page_size NOT BETWEEN 1 AND 100
     OR (surface_filter IS NOT NULL AND NOT EXISTS (
          SELECT 1 FROM public.command_refusal_surface_kinds WHERE id = surface_filter))
     OR (command_filter IS NOT NULL AND NOT EXISTS (
          SELECT 1 FROM public.command_refusal_command_kinds WHERE id = command_filter))
     OR (refusal_filter IS NOT NULL AND NOT EXISTS (
          SELECT 1 FROM public.command_refusal_kinds WHERE id = refusal_filter))
     THEN RAISE EXCEPTION 'invalid audit query bound'; END IF;
  IF cursor IS NOT NULL THEN
    IF jsonb_typeof(cursor) <> 'object'
       OR (SELECT count(*) FROM jsonb_object_keys(cursor)) <> 4
       OR NOT cursor ?& ARRAY['bucket_start','surface_kind','command_kind','refusal_kind'] THEN
      RAISE EXCEPTION 'invalid audit cursor';
    END IF;
    BEGIN
      cursor_bucket := (cursor->>'bucket_start')::timestamptz;
      cursor_surface := (cursor->>'surface_kind')::smallint;
      cursor_command := (cursor->>'command_kind')::smallint;
      cursor_refusal := (cursor->>'refusal_kind')::smallint;
    EXCEPTION WHEN OTHERS THEN RAISE EXCEPTION 'invalid audit cursor';
    END;
  END IF;
  SELECT count(*)::integer INTO result_count FROM (
    SELECT b.bucket_start, b.surface_kind, b.command_kind, b.refusal_kind
    FROM public.command_refusal_audit_buckets b
    WHERE b.bucket_start >= from_hour AND b.bucket_start < to_hour
      AND (surface_filter IS NULL OR b.surface_kind = surface_filter)
      AND (command_filter IS NULL OR b.command_kind = command_filter)
      AND (refusal_filter IS NULL OR b.refusal_kind = refusal_filter)
      AND (cursor IS NULL OR (b.bucket_start, b.surface_kind, b.command_kind, b.refusal_kind)
          > (cursor_bucket, cursor_surface, cursor_command, cursor_refusal))
    GROUP BY b.bucket_start, b.surface_kind, b.command_kind, b.refusal_kind
    ORDER BY b.bucket_start, b.surface_kind, b.command_kind, b.refusal_kind
    LIMIT page_size
  ) bounded;
  IF NOT public.record_refusal_audit_access(1::smallint, filter_class, result_count) THEN
    RETURN;
  END IF;
  RETURN QUERY SELECT b.bucket_start, b.surface_kind, b.command_kind, b.refusal_kind,
    SUM(b.occurrence_count)::bigint, COALESCE(MAX(l.overflow_count), 0)::bigint
  FROM public.command_refusal_audit_buckets b
  LEFT JOIN public.command_refusal_audit_bucket_limits l USING (bucket_start)
  WHERE b.bucket_start >= from_hour AND b.bucket_start < to_hour
    AND (surface_filter IS NULL OR b.surface_kind = surface_filter)
    AND (command_filter IS NULL OR b.command_kind = command_filter)
    AND (refusal_filter IS NULL OR b.refusal_kind = refusal_filter)
    AND (cursor IS NULL OR (b.bucket_start, b.surface_kind, b.command_kind, b.refusal_kind)
        > (cursor_bucket, cursor_surface, cursor_command, cursor_refusal))
  GROUP BY b.bucket_start, b.surface_kind, b.command_kind, b.refusal_kind
  ORDER BY b.bucket_start, b.surface_kind, b.command_kind, b.refusal_kind LIMIT page_size;
END $$;

CREATE OR REPLACE FUNCTION public.operator_refusal_audit_buckets(
  from_hour timestamptz, to_hour timestamptz, surface_filter smallint,
  command_filter smallint, refusal_filter smallint, page_size integer, cursor jsonb
) RETURNS TABLE(bucket_start timestamptz, surface_kind smallint, command_kind smallint,
  refusal_kind smallint, actor_key_epoch smallint, actor_tag bytea, occurrence_count bigint,
  sample_command_tag bytea, command_id_present boolean)
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  cursor_bucket timestamptz;
  cursor_surface smallint;
  cursor_command smallint;
  cursor_refusal smallint;
  cursor_epoch smallint;
  cursor_actor bytea;
  result_count integer;
  filter_class smallint :=
    (CASE WHEN surface_filter IS NULL THEN 0 ELSE 1 END)
    + (CASE WHEN command_filter IS NULL THEN 0 ELSE 2 END)
    + (CASE WHEN refusal_filter IS NULL THEN 0 ELSE 4 END);
BEGIN
  IF date_trunc('hour', from_hour) <> from_hour OR date_trunc('hour', to_hour) <> to_hour
     OR to_hour <= from_hour OR to_hour - from_hour > interval '30 days'
     OR page_size NOT BETWEEN 1 AND 100
     OR (surface_filter IS NOT NULL AND NOT EXISTS (
          SELECT 1 FROM public.command_refusal_surface_kinds WHERE id = surface_filter))
     OR (command_filter IS NOT NULL AND NOT EXISTS (
          SELECT 1 FROM public.command_refusal_command_kinds WHERE id = command_filter))
     OR (refusal_filter IS NOT NULL AND NOT EXISTS (
          SELECT 1 FROM public.command_refusal_kinds WHERE id = refusal_filter))
     THEN RAISE EXCEPTION 'invalid audit query bound'; END IF;
  IF cursor IS NOT NULL THEN
    IF jsonb_typeof(cursor) <> 'object'
       OR (SELECT count(*) FROM jsonb_object_keys(cursor)) <> 6
       OR NOT cursor ?& ARRAY['bucket_start','surface_kind','command_kind','refusal_kind',
                              'actor_key_epoch','actor_tag'] THEN
      RAISE EXCEPTION 'invalid audit cursor';
    END IF;
    BEGIN
      cursor_bucket := (cursor->>'bucket_start')::timestamptz;
      cursor_surface := (cursor->>'surface_kind')::smallint;
      cursor_command := (cursor->>'command_kind')::smallint;
      cursor_refusal := (cursor->>'refusal_kind')::smallint;
      cursor_epoch := (cursor->>'actor_key_epoch')::smallint;
      cursor_actor := decode(cursor->>'actor_tag', 'hex');
      IF octet_length(cursor_actor) <> 16 THEN RAISE EXCEPTION 'invalid actor tag'; END IF;
    EXCEPTION WHEN OTHERS THEN RAISE EXCEPTION 'invalid audit cursor';
    END;
  END IF;
  SELECT count(*)::integer INTO result_count FROM (
    SELECT 1 FROM public.command_refusal_audit_buckets b
    WHERE b.bucket_start >= from_hour AND b.bucket_start < to_hour
      AND (surface_filter IS NULL OR b.surface_kind = surface_filter)
      AND (command_filter IS NULL OR b.command_kind = command_filter)
      AND (refusal_filter IS NULL OR b.refusal_kind = refusal_filter)
      AND (cursor IS NULL OR (b.bucket_start, b.surface_kind, b.command_kind, b.refusal_kind,
                              b.actor_key_epoch, b.actor_tag)
          > (cursor_bucket, cursor_surface, cursor_command, cursor_refusal,
             cursor_epoch, cursor_actor))
    ORDER BY b.bucket_start, b.surface_kind, b.command_kind, b.refusal_kind,
             b.actor_key_epoch, b.actor_tag
    LIMIT page_size
  ) bounded;
  IF NOT public.record_refusal_audit_access(2::smallint, filter_class, result_count) THEN
    RETURN;
  END IF;
  RETURN QUERY SELECT b.bucket_start, b.surface_kind, b.command_kind, b.refusal_kind,
    b.actor_key_epoch, b.actor_tag, b.occurrence_count, b.sample_command_tag, b.command_id_present
  FROM public.command_refusal_audit_buckets b
  WHERE b.bucket_start >= from_hour AND b.bucket_start < to_hour
    AND (surface_filter IS NULL OR b.surface_kind = surface_filter)
    AND (command_filter IS NULL OR b.command_kind = command_filter)
    AND (refusal_filter IS NULL OR b.refusal_kind = refusal_filter)
    AND (cursor IS NULL OR (b.bucket_start, b.surface_kind, b.command_kind, b.refusal_kind,
                            b.actor_key_epoch, b.actor_tag)
        > (cursor_bucket, cursor_surface, cursor_command, cursor_refusal,
           cursor_epoch, cursor_actor))
  ORDER BY b.bucket_start, b.surface_kind, b.command_kind, b.refusal_kind, b.actor_key_epoch, b.actor_tag
  LIMIT page_size;
END $$;

ALTER FUNCTION public.operator_refusal_audit_summary(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb) OWNER TO marketplace_refusal_audit_owner;
ALTER FUNCTION public.operator_refusal_audit_summary(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb) SET search_path = pg_catalog;
ALTER FUNCTION public.operator_refusal_audit_buckets(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb) OWNER TO marketplace_refusal_audit_owner;
ALTER FUNCTION public.operator_refusal_audit_buckets(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb) SET search_path = pg_catalog;
ALTER FUNCTION public.purge_refusal_audit(timestamptz,integer) OWNER TO marketplace_refusal_audit_owner;
ALTER FUNCTION public.purge_refusal_audit(timestamptz,integer) SET search_path = pg_catalog;
ALTER FUNCTION public.record_refusal_audit_access(smallint,smallint,integer) OWNER TO marketplace_refusal_audit_owner;
ALTER FUNCTION public.record_refusal_audit_access(smallint,smallint,integer) SET search_path = pg_catalog;
ALTER FUNCTION public.forbid_refusal_audit_catalog_mutation() OWNER TO marketplace_refusal_audit_owner;

ALTER TABLE command_refusal_surface_kinds OWNER TO marketplace_refusal_audit_owner;
ALTER TABLE command_refusal_command_kinds OWNER TO marketplace_refusal_audit_owner;
ALTER TABLE command_refusal_kinds OWNER TO marketplace_refusal_audit_owner;
ALTER TABLE command_refusal_audit_buckets OWNER TO marketplace_refusal_audit_owner;
ALTER TABLE command_refusal_audit_bucket_limits OWNER TO marketplace_refusal_audit_owner;
ALTER TABLE command_refusal_audit_access_buckets OWNER TO marketplace_refusal_audit_owner;
ALTER TABLE command_refusal_audit_access_limits OWNER TO marketplace_refusal_audit_owner;
ALTER TABLE command_refusal_audit_loss_gap OWNER TO marketplace_refusal_audit_owner;

REVOKE ALL ON command_refusal_surface_kinds, command_refusal_command_kinds,
  command_refusal_kinds, command_refusal_audit_buckets,
  command_refusal_audit_bucket_limits, command_refusal_audit_access_buckets,
  command_refusal_audit_access_limits, command_refusal_audit_loss_gap FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.operator_refusal_audit_summary(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.operator_refusal_audit_buckets(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.purge_refusal_audit(timestamptz,integer) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.record_refusal_audit_access(smallint,smallint,integer) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.forbid_refusal_audit_catalog_mutation() FROM PUBLIC;
REVOKE ALL ON command_refusal_surface_kinds, command_refusal_command_kinds,
  command_refusal_kinds, command_refusal_audit_buckets,
  command_refusal_audit_bucket_limits, command_refusal_audit_access_buckets,
  command_refusal_audit_access_limits, command_refusal_audit_loss_gap
  FROM marketplace_refusal_audit_writer, marketplace_refusal_audit_writer_login,
       marketplace_refusal_audit_retention, marketplace_refusal_audit_aggregate,
       marketplace_refusal_audit_raw;
REVOKE ALL ON FUNCTION public.operator_refusal_audit_summary(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb)
  FROM marketplace_refusal_audit_writer, marketplace_refusal_audit_writer_login,
       marketplace_refusal_audit_retention, marketplace_refusal_audit_aggregate,
       marketplace_refusal_audit_raw;
REVOKE ALL ON FUNCTION public.operator_refusal_audit_buckets(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb)
  FROM marketplace_refusal_audit_writer, marketplace_refusal_audit_writer_login,
       marketplace_refusal_audit_retention, marketplace_refusal_audit_aggregate,
       marketplace_refusal_audit_raw;
REVOKE ALL ON FUNCTION public.purge_refusal_audit(timestamptz,integer)
  FROM marketplace_refusal_audit_writer, marketplace_refusal_audit_writer_login,
       marketplace_refusal_audit_retention, marketplace_refusal_audit_aggregate,
       marketplace_refusal_audit_raw;
REVOKE ALL ON FUNCTION public.record_refusal_audit_access(smallint,smallint,integer)
  FROM marketplace_refusal_audit_writer, marketplace_refusal_audit_writer_login,
       marketplace_refusal_audit_retention, marketplace_refusal_audit_aggregate,
       marketplace_refusal_audit_raw;
REVOKE ALL ON FUNCTION public.forbid_refusal_audit_catalog_mutation()
  FROM marketplace_refusal_audit_writer, marketplace_refusal_audit_writer_login,
       marketplace_refusal_audit_retention, marketplace_refusal_audit_aggregate,
       marketplace_refusal_audit_raw;
GRANT SELECT ON command_refusal_surface_kinds, command_refusal_command_kinds, command_refusal_kinds TO marketplace_refusal_audit_writer;
GRANT SELECT, INSERT, UPDATE ON command_refusal_audit_buckets, command_refusal_audit_bucket_limits TO marketplace_refusal_audit_writer;
GRANT SELECT, INSERT, UPDATE ON command_refusal_audit_loss_gap TO marketplace_refusal_audit_writer;
GRANT marketplace_refusal_audit_writer TO marketplace_refusal_audit_writer_login;
-- The login is deliberately NOINHERIT. Direct runtime grants are therefore
-- the least-authority mechanism; membership remains an auditable capability
-- relationship but is not relied on to make DML work.
GRANT SELECT ON command_refusal_surface_kinds, command_refusal_command_kinds, command_refusal_kinds TO marketplace_refusal_audit_writer_login;
GRANT SELECT, INSERT, UPDATE ON command_refusal_audit_buckets, command_refusal_audit_bucket_limits TO marketplace_refusal_audit_writer_login;
GRANT SELECT, INSERT, UPDATE ON command_refusal_audit_loss_gap TO marketplace_refusal_audit_writer_login;
GRANT EXECUTE ON FUNCTION public.operator_refusal_audit_summary(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb) TO marketplace_refusal_audit_aggregate, marketplace_refusal_audit_raw;
GRANT EXECUTE ON FUNCTION public.operator_refusal_audit_buckets(timestamptz,timestamptz,smallint,smallint,smallint,integer,jsonb) TO marketplace_refusal_audit_raw;
GRANT EXECUTE ON FUNCTION public.purge_refusal_audit(timestamptz,integer) TO marketplace_refusal_audit_retention;
GRANT SELECT, INSERT, UPDATE, DELETE ON command_refusal_audit_buckets, command_refusal_audit_bucket_limits, command_refusal_audit_access_buckets, command_refusal_audit_access_limits, command_refusal_audit_loss_gap TO marketplace_refusal_audit_owner;
REVOKE CREATE ON SCHEMA public FROM marketplace_refusal_audit_owner;
DO $bootstrap$
DECLARE migration_login name := current_user;
BEGIN EXECUTE pg_catalog.format('REVOKE marketplace_refusal_audit_owner FROM %I', migration_login); END $bootstrap$;
