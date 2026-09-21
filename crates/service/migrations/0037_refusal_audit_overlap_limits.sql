-- Additive overlap limits for two healthcheck-overlapping replicas.
-- 0032 is byte-immutable and created marketplace_refusal_audit_retention
-- with CONNECTION LIMIT 1. Each replica's retention pool is max 1; 1+1=2.
-- Writer login stays at CONNECTION LIMIT 2 (0032). Do not reconnect a
-- binary whose migration set omits this version after it is applied.

ALTER ROLE marketplace_refusal_audit_retention CONNECTION LIMIT 2;

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
    'marketplace_refusal_audit_writer_login'::name,
    'marketplace_refusal_audit_admin_login'::name
  ] LOOP
    expected_login := role_name IN (
      'marketplace_refusal_audit_retention',
      'marketplace_refusal_audit_writer_login',
      'marketplace_refusal_audit_admin_login'
    );
    expected_limit := CASE role_name
      WHEN 'marketplace_refusal_audit_retention' THEN 2
      WHEN 'marketplace_refusal_audit_writer_login' THEN 2
      WHEN 'marketplace_refusal_audit_admin_login' THEN 1
      ELSE -1
    END;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = role_name) THEN
      RAISE EXCEPTION '% missing', role_name;
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
    WHERE member.rolname = 'marketplace_refusal_audit_admin_login'
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
