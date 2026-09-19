# Refusal-audit erasure and key retirement

`refusal-audit-admin` is a non-HTTP operator binary. It has no route or server
listener. Run it only from the controlled service environment.

## Identity and configuration inventory

Provision a distinct `marketplace_refusal_audit_admin_login` credential and
set `REFUSAL_AUDIT_ADMIN_DATABASE_URL`. The URL must use `postgres` or
`postgresql`, include that exact username, a non-empty password, host, and
database. The binary verifies the live PostgreSQL session identity, exact
role attributes, absence of memberships, its narrow table/function ACLs, and
absence of domain-table mutation or audit-reader authority before doing work.

The key inventory is the same strict two-epoch set used by the service:

- `REFUSAL_AUDIT_HMAC_ROOT_B64`
- `REFUSAL_AUDIT_HMAC_KEY_EPOCH`
- optional, paired `REFUSAL_AUDIT_HMAC_PREVIOUS_ROOT_B64`
- optional, paired `REFUSAL_AUDIT_HMAC_PREVIOUS_KEY_EPOCH`

Never put a key, password, actor identity, or derived tag in argv or logs.

## Account erasure

Pass the authenticated actor identity as the only stdin line:

```sh
printf '%s\n' "$ACTOR_PUBKY" |
  cargo run -p marketplace-service --bin refusal-audit-admin -- erase-actor
```

The command first checks that every database epoch is represented by the
loaded active/previous key pair, derives both tags in-process, and deletes
only matching refusal buckets. Its output does not include the actor, tags,
keys, or affected-row count.

## Previous-key destruction and second rotation

```sh
cargo run -p marketplace-service --bin refusal-audit-admin -- destroy-previous
```

This requires a configured previous key. PostgreSQL must contain no rows for
that epoch. If the epoch ever held rows, the database-maintained epoch
inventory must also show that its final row was removed at least 30 days ago.
That interval is the maximum configured replica and backup lifetime. The
gate is computed from PostgreSQL state and `clock_timestamp()`; it is not a
request boolean. The command invokes the database epoch check, second-rotation
safety check, and in-process previous-key zeroization. Remove the previous
root from the controlled secret configuration before starting the service
with the next two-epoch pair.

The admin login can select/delete refusal buckets and read the epoch
inventory, and can execute only the storage-safety function. It cannot write
domain tables, add audit rows, purge, or use aggregate/raw operator reads.
