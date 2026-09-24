#!/usr/bin/env python3
"""Staging hold-smoke for marketplace-service cutover.

Canonical copy: BitcoinErrorLog/pubky-marketplace-service
`scripts/release/hold-smoke.py`. The release skill points here; do not keep
a second live copy under .evidence/.

Exercises checkout.create with the deployed Shop v0.6.23 wire shape
(shop-v0.6.23 @ 5f9f20ff7cb3caa06174c4a308a7abc79d60ddd2, checkout wire
unchanged since v0.6.22): the v0.6.17 keys
plus the line's `variant_id`, which the Shop sends for every line whose
listing variant it resolved. Hold semantics are Option B:

  (a) shipping listing + full delivery_address, region as free text
      ("California"), ISO suffix ("CA"), and empty for PT;
  (b) pickup-only (no delivery_address);
  (c) two unheld checkout.create 200s, then bind exclusivity (first
      POST /v0/orders/{id}/payment-method holds; loser 409 HOLDING_COPY);
  (d) hold TTL equals live FIAT_PAYMENT_WINDOW_SECONDS (exact-key read).

Any HTTP 4xx other than the expected bind 409 is STOP. Never production.
Every created order is cancelled and verified cancelled at the end.

    MARKETPLACE_URL=https://staging-api.pubky.app
    RAILWAY_PROJECT_ID=c991d768-4a3c-42ea-b5ed-eaa22d4916ed
    RAILWAY_ENVIRONMENT=c67a6435-bb23-453b-9169-764bfa0312e1
    RAILWAY_DATABASE_SERVICE=Postgres
    python3 scripts/release/hold-smoke.py

    python3 scripts/release/hold-smoke.py --self-check
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

HOLDING_COPY = (
    "Another buyer's payment is holding this item. If it isn't completed in time, the item restocks."
)
TTL_SLACK_SECONDS = 45.0
TTL_OVER_SECONDS = 5.0
FIAT_WINDOW_DEFAULT = 600

SHOP_TAG = "shop-v0.6.23"
SHOP_SHA = "5f9f20ff7cb3caa06174c4a308a7abc79d60ddd2"
FIXTURE_NAME = "shop-v0.6.23-checkout.create.json"

STAGING_URL = "https://staging-api.pubky.app"
STAGING_PROJECT = "c991d768-4a3c-42ea-b5ed-eaa22d4916ed"
STAGING_ENVIRONMENT = "c67a6435-bb23-453b-9169-764bfa0312e1"
STAGING_SERVICE = "3e98e363-f9a7-4711-b668-d714acee6e32"
PRODUCTION_PROJECT = "75faa4fe-466c-4277-977f-1d8e4e31df8c"
PRODUCTION_HOST_MARKERS = (
    "marketplace-service-production-ce23",
    "shop.pubky.app",
    "-ce23.up.railway.app",
)

NODE = "/Users/johncarvalho/.nvm/versions/node/v22.14.0/bin/node"
PUBKY_CWD = "/Users/johncarvalho/work/.deps/pubky-app-ci-green"

# Shop v0.6.17 shipping address object after toSnakeCaseWire. Region values
# are the three classes the 2026-09-22 hold-smoke missed (NY/pickup-only).
SHIPPING_ADDRESSES: dict[str, dict[str, str]] = {
    "california_free_text": {
        "name": "Alice Buyer",
        "line1": "1 Market Street",
        "line2": "",
        "city": "San Francisco",
        "region": "California",
        "postal_code": "94105",
        "country_code": "US",
    },
    "ca_iso_suffix": {
        "name": "Alice Buyer",
        "line1": "1 Market Street",
        "line2": "",
        "city": "San Francisco",
        "region": "CA",
        "postal_code": "94105",
        "country_code": "US",
    },
    "pt_empty_region": {
        "name": "Alice Buyer",
        "line1": "Rua Augusta 1",
        "line2": "",
        "city": "Lisboa",
        "region": "",
        "postal_code": "1100-053",
        "country_code": "PT",
    },
}

# Service canonicalize_region after hotfix #25 stores California as CA.
# Pre-#23 stored the free-text value as-is. Either proves the 422 is gone;
# HTTP 422 on this payload is STOP.
EXPECTED_STORED_REGION = {
    "california_free_text": {"CA", "California"},
    "ca_iso_suffix": {"CA"},
    "pt_empty_region": {""},
}

ENVELOPE_KEYS = {
    "version",
    "command_id",
    "aggregate_id",
    "expected_revision",
    "issued_at",
    "kind",
    "payload",
}
SHIPPING_PAYLOAD_KEYS = {"lines", "delivery_address", "guarantee_policy_version"}
PICKUP_PAYLOAD_KEYS = {"lines", "guarantee_policy_version"}
ADDRESS_KEYS = {"name", "line1", "line2", "city", "region", "postal_code", "country_code"}
LINE_KEYS = {"listing_aggregate_id", "expected_revision", "quantity", "variant_id", "fulfillment"}
# The variant id production Shop sent for a single-variant listing (order
# c7e700de, 2026-09-23). `variant_options` rides only a variant that has
# options, which the smoke listings do not.
SHOP_VARIANT_ID = "variant_1"


def refuse_production(url: str, project: str) -> None:
    lowered = url.lower()
    if project == PRODUCTION_PROJECT:
        raise SystemExit("HOLD_SMOKE=STOP production project id is forbidden")
    if "ce23" in lowered or any(marker in lowered for marker in PRODUCTION_HOST_MARKERS):
        raise SystemExit(f"HOLD_SMOKE=STOP production host is forbidden: {url}")
    if project != STAGING_PROJECT:
        raise SystemExit(f"HOLD_SMOKE=STOP expected staging project {STAGING_PROJECT}, got {project}")
    if url.rstrip("/") != STAGING_URL:
        raise SystemExit(f"HOLD_SMOKE=STOP expected {STAGING_URL}, got {url}")


def issued_at() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def bind_body(method: str) -> dict[str, str]:
    """Shop payment-method bind: `{ "method": "paypal" | "stripe" | "bitcoin" }`."""
    return {"method": method}


def ttl_matches_window(ttl: float, window: int, *, slack: float = TTL_SLACK_SECONDS) -> bool:
    return (window - slack) <= ttl <= (window + TTL_OVER_SECONDS)


def checkout_body(
    listing: dict[str, Any],
    command_id: str,
    *,
    fulfillment: str,
    address: dict[str, str] | None,
) -> dict[str, Any]:
    """Shop v0.6.23 checkout.create after toSnakeCaseWire."""
    line: dict[str, Any] = {
        "listing_aggregate_id": listing["aggregate_id"],
        "expected_revision": listing["server_revision"],
        "quantity": 1,
        "variant_id": SHOP_VARIANT_ID,
        "fulfillment": fulfillment,
    }
    payload: dict[str, Any] = {
        "lines": [line],
        "guarantee_policy_version": 1,
    }
    if fulfillment == "shipping":
        if address is None:
            raise RuntimeError("shipping checkout requires delivery_address")
        payload["delivery_address"] = dict(address)
    elif address is not None:
        raise RuntimeError("pickup-only checkout must omit delivery_address")
    return {
        "version": 1,
        "command_id": command_id,
        "aggregate_id": f"checkout:{command_id}",
        "expected_revision": 0,
        "issued_at": issued_at(),
        "kind": "checkout.create",
        "payload": payload,
    }


def cancel_body(order_id: str, revision: int, reason: str) -> dict[str, Any]:
    return {
        "version": 1,
        "command_id": str(uuid.uuid4()),
        "aggregate_id": f"order:{order_id}",
        "expected_revision": revision,
        "issued_at": issued_at(),
        "kind": "order.cancel_request",
        "payload": {"order_id": order_id, "reason": reason},
    }


def assert_shape(body: dict[str, Any], *, shipping: bool) -> None:
    missing = ENVELOPE_KEYS - body.keys()
    extra = body.keys() - ENVELOPE_KEYS
    if missing or extra:
        raise RuntimeError(f"envelope keys mismatch missing={missing} extra={extra}")
    if body["kind"] != "checkout.create":
        raise RuntimeError(f"kind {body['kind']!r}")
    payload = body["payload"]
    expected_payload = SHIPPING_PAYLOAD_KEYS if shipping else PICKUP_PAYLOAD_KEYS
    if set(payload) != expected_payload:
        raise RuntimeError(f"payload keys {set(payload)} != {expected_payload}")
    line = payload["lines"][0]
    if set(line) != LINE_KEYS:
        raise RuntimeError(f"line keys {set(line)} != {LINE_KEYS}")
    if shipping:
        if set(payload["delivery_address"]) != ADDRESS_KEYS:
            raise RuntimeError(f"address keys {set(payload['delivery_address'])}")
        if line["fulfillment"] != "shipping":
            raise RuntimeError("shipping line fulfillment")
    else:
        if "delivery_address" in payload:
            raise RuntimeError("pickup must omit delivery_address")
        if line["fulfillment"] != "pickup":
            raise RuntimeError("pickup line fulfillment")
    dumped = json.dumps(body)
    if '"region": "NY"' in dumped:
        raise RuntimeError("hard-coded NY region is the class that hid the 422")


def self_check() -> None:
    listing = {
        "aggregate_id": "listing:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "server_revision": 3,
        "fulfillment_methods": "{shipping}",
    }
    pickup_listing = {**listing, "fulfillment_methods": "{pickup}"}
    california = checkout_body(
        listing,
        str(uuid.uuid4()),
        fulfillment="shipping",
        address=SHIPPING_ADDRESSES["california_free_text"],
    )
    assert_shape(california, shipping=True)
    if california["payload"]["delivery_address"]["region"] != "California":
        raise RuntimeError("California free-text region missing from payload")
    ca = checkout_body(
        listing, str(uuid.uuid4()), fulfillment="shipping", address=SHIPPING_ADDRESSES["ca_iso_suffix"]
    )
    assert_shape(ca, shipping=True)
    if ca["payload"]["delivery_address"]["region"] != "CA":
        raise RuntimeError("CA ISO suffix missing from payload")
    pt = checkout_body(
        listing, str(uuid.uuid4()), fulfillment="shipping", address=SHIPPING_ADDRESSES["pt_empty_region"]
    )
    assert_shape(pt, shipping=True)
    if pt["payload"]["delivery_address"]["region"] != "":
        raise RuntimeError("PT empty region missing from payload")
    if pt["payload"]["delivery_address"]["country_code"] != "PT":
        raise RuntimeError("PT country_code")
    chatter = 'Output format is unaligned.\nField separator is "|".\n'
    if sql_scalar(chatter + "region=\n") != "region=":
        raise RuntimeError("empty stored region marker must survive psql chatter")
    if sql_scalar(chatter + "region=CA\n") != "region=CA":
        raise RuntimeError("CA stored region marker")
    pickup = checkout_body(pickup_listing, str(uuid.uuid4()), fulfillment="pickup", address=None)
    assert_shape(pickup, shipping=False)
    if bind_body("paypal") != {"method": "paypal"}:
        raise RuntimeError("paypal bind body")
    if bind_body("stripe") != {"method": "stripe"}:
        raise RuntimeError("stripe bind body")
    if not ttl_matches_window(590.0, 600):
        raise RuntimeError("ttl slack around fiat default")
    if ttl_matches_window(500.0, 600):
        raise RuntimeError("ttl far below window must fail")
    if parse_fiat_window("") != FIAT_WINDOW_DEFAULT:
        raise RuntimeError("empty printenv must use fiat default 600")
    if parse_fiat_window("600") != 600:
        raise RuntimeError("exact-key 600")
    if parse_fiat_window("900") != 900:
        raise RuntimeError("exact-key 900")
    fixture_path = Path(__file__).with_name(FIXTURE_NAME)
    fixture = json.loads(fixture_path.read_text())
    if fixture["captured_from"]["shop_tag"] != SHOP_TAG:
        raise RuntimeError("fixture tag drift")
    if fixture["captured_from"]["shop_sha"] != SHOP_SHA:
        raise RuntimeError("fixture sha drift")
    print(
        json.dumps(
            {
                "shop_tag": SHOP_TAG,
                "shop_sha": SHOP_SHA,
                "california_region": california["payload"]["delivery_address"]["region"],
                "ca_region": ca["payload"]["delivery_address"]["region"],
                "pt_region": pt["payload"]["delivery_address"]["region"],
                "pickup_has_address": "delivery_address" in pickup["payload"],
                "bind_paypal": bind_body("paypal"),
                "ttl_590_vs_600": ttl_matches_window(590.0, 600),
            },
            indent=2,
            sort_keys=True,
        )
    )
    print("HOLD_SMOKE=SELF_CHECK_PASS")


def sql(statement: str) -> str:
    project = os.environ["RAILWAY_PROJECT_ID"]
    environment = os.environ["RAILWAY_ENVIRONMENT"]
    db_service = os.environ.get("RAILWAY_DATABASE_SERVICE", "Postgres")
    refuse_production(os.environ["MARKETPLACE_URL"], project)
    env = os.environ.copy()
    env["PATH"] = "/opt/homebrew/opt/postgresql@17/bin:" + env.get("PATH", "")
    env["PGCONNECT_TIMEOUT"] = "15"
    for key in ("RAILWAY_PROJECT_ID", "RAILWAY_ENVIRONMENT_ID", "RAILWAY_SERVICE_ID"):
        env.pop(key, None)
    payload = (
        "\\set ON_ERROR_STOP on\n"
        "\\pset tuples_only on\n"
        "\\pset format unaligned\n"
        "\\pset fieldsep '|'\n"
        + statement
        + "\n\\q\n"
    )
    result = subprocess.run(
        ["railway", "connect", db_service, "--ssh", "-p", project, "-e", environment],
        input=payload,
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )
    filtered = []
    for line in (result.stdout or "").splitlines():
        lower = line.lower()
        if "postgres://" in lower or "password" in lower:
            continue
        if "config as code" in lower or "migrate:" in lower or "existing files" in lower:
            continue
        if "ssh tunnel" in lower:
            continue
        filtered.append(line)
    if result.returncode != 0:
        err = "\n".join(
            ln
            for ln in (result.stderr or "").splitlines()
            if "postgres://" not in ln.lower() and "password" not in ln.lower()
        )
        raise RuntimeError(f"sql failed rc={result.returncode} err={err[:400]!r} out={filtered!r}")
    return "\n".join(filtered)


def railway_ssh(argv: list[str]) -> str:
    """Run one command on the staging marketplace-service instance.

    Used only for an exact-key `printenv FIAT_PAYMENT_WINDOW_SECONDS`. Never
    list or dump the rest of the environment.
    """
    project = os.environ["RAILWAY_PROJECT_ID"]
    environment = os.environ["RAILWAY_ENVIRONMENT"]
    refuse_production(os.environ["MARKETPLACE_URL"], project)
    env = os.environ.copy()
    env["PATH"] = "/opt/homebrew/bin:" + env.get("PATH", "")
    for key in ("RAILWAY_PROJECT_ID", "RAILWAY_ENVIRONMENT_ID", "RAILWAY_SERVICE_ID"):
        env.pop(key, None)
    result = subprocess.run(
        [
            "railway",
            "ssh",
            "-p",
            project,
            "-e",
            environment,
            "-s",
            STAGING_SERVICE,
            "--",
            *argv,
        ],
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )
    filtered = []
    for line in (result.stdout or "").splitlines():
        lower = line.lower()
        if "postgres://" in lower or "password" in lower:
            continue
        if "config as code" in lower or "migrate:" in lower:
            continue
        if "ssh tunnel" in lower or "connected to" in lower:
            continue
        filtered.append(line)
    if result.returncode != 0:
        # GNU printenv exits 1 when the named key is unset; that is the
        # empty-window case (code default 600), not an SSH failure.
        if (
            result.returncode == 1
            and len(argv) == 2
            and argv[0] == "printenv"
            and not filtered
        ):
            return ""
        err = "\n".join(
            ln
            for ln in (result.stderr or "").splitlines()
            if "postgres://" not in ln.lower() and "password" not in ln.lower()
        )
        raise RuntimeError(f"ssh failed rc={result.returncode} err={err[:400]!r} out={filtered!r}")
    return "\n".join(filtered)


def parse_fiat_window(raw: str) -> int:
    """Parse an exact-key `printenv FIAT_PAYMENT_WINDOW_SECONDS` payload.

    Empty output means the process is on the service default (600). Staging
    IaC `preserve()`s the key and does not inject it when unset.
    """
    token = sql_scalar(raw) if "\n" in raw else raw.strip()
    if not token:
        return FIAT_WINDOW_DEFAULT
    try:
        window = int(token)
    except ValueError as error:
        raise RuntimeError(
            f"FIAT_PAYMENT_WINDOW_SECONDS not an integer ({len(token)} chars)"
        ) from error
    if window < 60:
        raise RuntimeError(f"FIAT_PAYMENT_WINDOW_SECONDS {window} is below the 60s floor")
    return window


def fiat_window_seconds() -> int:
    """Exact-key read of FIAT_PAYMENT_WINDOW_SECONDS from the running service."""
    return parse_fiat_window(railway_ssh(["printenv", "FIAT_PAYMENT_WINDOW_SECONDS"]))


def api(base: str, method: str, path: str, bearer: str | None, body: Any = None) -> tuple[int, dict | str]:
    headers: dict[str, str] = {}
    data = None
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    if body is not None:
        data = json.dumps(body, separators=(",", ":")).encode()
        headers["content-type"] = "application/json"
    request = Request(base + path, method=method, headers=headers, data=data)
    try:
        response = urlopen(request, timeout=30)
        raw = response.read()
        parsed = json.loads(raw) if raw else {}
        return response.status, parsed
    except HTTPError as error:
        raw = error.read()
        try:
            parsed = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            parsed = raw.decode("utf-8", "replace")
        return error.code, parsed


def stop_on_unexpected_4xx(status: int, body: dict | str, *, allowed: set[int], case: str) -> None:
    if status in allowed:
        return
    if 400 <= status < 500:
        raise RuntimeError(f"STOP unexpected 4xx {status} case={case} body={body!r}")
    raise RuntimeError(f"STOP http {status} case={case} body={body!r}")


def z32_pubky() -> str:
    script = (
        "const {Keypair}=require('@synonymdev/pubky');"
        "const k=Keypair.random();"
        "process.stdout.write(k.publicKey.z32());"
    )
    result = subprocess.run(
        [NODE, "-e", script],
        cwd=PUBKY_CWD,
        capture_output=True,
        text=True,
        check=True,
    )
    token = result.stdout.strip()
    if len(token) != 52:
        raise RuntimeError(f"z32 length {len(token)}")
    return token


def quote(value: str) -> str:
    return value.replace("'", "''")


def insert_session(pubky: str) -> tuple[str, str]:
    bearer = base64.urlsafe_b64encode(os.urandom(32)).rstrip(b"=").decode()
    session_id = str(uuid.uuid4())
    token_hash = hashlib.sha256(
        base64.urlsafe_b64decode(bearer + "=" * (-len(bearer) % 4))
    ).hexdigest()
    sql(
        "INSERT INTO auth_sessions "
        "(token_hash,session_id,pubky,capabilities,created_at,expires_at,last_used_at) VALUES "
        f"(decode('{token_hash}','hex'),'{session_id}','{quote(pubky)}',"
        "'/:rw',now(),now()+interval '15 minutes',now());"
    )
    return bearer, session_id


def parse_listing_rows(raw: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in raw.splitlines():
        token = line.strip().strip('"')
        if token.count("|") < 4:
            continue
        parts = [p.strip().strip('"') for p in token.split("|")]
        if len(parts) < 5:
            continue
        bind_method = parts[5] if len(parts) > 5 else ""
        rows.append(
            {
                "aggregate_id": parts[0],
                "seller_pubky": parts[1],
                "server_revision": int(parts[2]),
                "available_quantity": int(parts[3]),
                "fulfillment_methods": parts[4],
                "bind_method": bind_method,
            }
        )
    return rows


def pick_listing(
    *, shipping: bool, exclude: set[str] | None = None, require_bind: bool = False
) -> dict[str, Any]:
    if shipping:
        clause = "AND l.fulfillment_methods::text LIKE '%shipping%'"
        label = "shipping"
    else:
        clause = (
            "AND l.fulfillment_methods::text LIKE '%pickup%' "
            "AND l.fulfillment_methods::text NOT LIKE '%shipping%'"
        )
        label = "pickup-only"
    excluded = ""
    if exclude:
        ids = ",".join(f"'{quote(item)}'" for item in sorted(exclude))
        excluded = f"AND l.aggregate_id NOT IN ({ids}) "
    raw = sql(
        "SELECT l.aggregate_id || '|' || l.seller_pubky || '|' || l.server_revision::text "
        "|| '|' || l.available_quantity::text || '|' || l.fulfillment_methods::text "
        "|| '|' || CASE "
        "WHEN c.paypal_merchant_email IS NOT NULL AND btrim(c.paypal_merchant_email) <> '' THEN 'paypal' "
        "WHEN c.stripe_payment_link IS NOT NULL AND btrim(c.stripe_payment_link) <> '' THEN 'stripe' "
        "ELSE '' END "
        "FROM listings l "
        "LEFT JOIN seller_payment_configs c ON c.seller_pubky = l.seller_pubky "
        "WHERE l.sale_format='fixed_price' AND l.state='available' AND l.available_quantity=1 "
        "AND l.reserved_quantity=0 "
        f"{clause} {excluded}"
        "ORDER BY (CASE "
        "WHEN c.paypal_merchant_email IS NOT NULL AND btrim(c.paypal_merchant_email) <> '' THEN 0 "
        "WHEN c.stripe_payment_link IS NOT NULL AND btrim(c.stripe_payment_link) <> '' THEN 1 "
        "ELSE 2 END), l.updated_at DESC NULLS LAST LIMIT 5;"
    )
    rows = parse_listing_rows(raw)
    if require_bind:
        rows = [row for row in rows if row.get("bind_method") in {"paypal", "stripe"}]
    if not rows:
        suffix = " with paypal/stripe" if require_bind else ""
        raise RuntimeError(f"no qty-1 available {label} listing{suffix}\n{raw!r}")
    listing = rows[0]
    methods = listing["fulfillment_methods"]
    if shipping and "shipping" not in methods:
        raise RuntimeError(f"picked listing is not shipping: {methods!r}")
    if not shipping and ("pickup" not in methods or "shipping" in methods):
        raise RuntimeError(f"picked listing is not pickup-only: {methods!r}")
    return listing


def restore_listing_methods(aggregate_id: str, methods: str) -> None:
    raw = sql(
        "UPDATE listings SET fulfillment_methods = "
        f"'{quote(methods)}'::text[], updated_at = now() "
        f"WHERE aggregate_id='{quote(aggregate_id)}' "
        "RETURNING fulfillment_methods::text;"
    )
    restored = sql_scalar(raw)
    if restored != methods:
        raise RuntimeError(f"restore fulfillment_methods {restored!r} != {methods!r}")


def ensure_pickup_listing(exclude: set[str]) -> tuple[dict[str, Any], dict[str, str] | None]:
    """Return a pickup-only qty-1 listing.

    Staging today has shipping-only rows. If no pickup-only listing exists,
    temporarily retarget a second shipping listing to `{pickup}` and restore
    the original methods in the caller's finally.
    """
    try:
        return pick_listing(shipping=False, exclude=exclude, require_bind=True), None
    except RuntimeError as error:
        if "no qty-1 available pickup-only listing" not in str(error):
            raise
    donor = pick_listing(shipping=True, exclude=exclude, require_bind=True)
    original = donor["fulfillment_methods"]
    raw = sql(
        "UPDATE listings SET fulfillment_methods = '{pickup}'::text[], updated_at = now() "
        f"WHERE aggregate_id='{quote(donor['aggregate_id'])}' "
        "AND state='available' AND available_quantity=1 AND reserved_quantity=0 "
        "AND fulfillment_methods::text NOT LIKE '%pickup%' "
        "RETURNING aggregate_id || '|' || seller_pubky || '|' || server_revision::text "
        "|| '|' || available_quantity::text || '|' || fulfillment_methods::text;"
    )
    rows = parse_listing_rows(raw)
    if not rows:
        raise RuntimeError(f"failed to retarget donor listing for pickup\n{raw!r}")
    listing = rows[0]
    listing["bind_method"] = donor.get("bind_method", "")
    if "pickup" not in listing["fulfillment_methods"] or "shipping" in listing["fulfillment_methods"]:
        restore_listing_methods(donor["aggregate_id"], original)
        raise RuntimeError(f"donor is not pickup-only after retarget: {listing!r}")
    return listing, {"aggregate_id": donor["aggregate_id"], "methods": original}


def sql_scalar(raw: str) -> str:
    skip = {
        "output format is unaligned.",
        'field separator is "|".',
    }
    for line in raw.splitlines():
        token = line.strip().strip('"')
        if not token or token.lower() in skip:
            continue
        return token
    raise RuntimeError(f"scalar unparseable {raw!r}")


def stored_region_of(order_id: str) -> str:
    """Read orders.delivery_address.region, including the empty PT value.

    psql tuples_only unaligned prints nothing for '' so sql_scalar cannot
    see it. Prefix a non-empty marker; a missing row still raises.
    """
    token = sql_scalar(
        sql(
            "SELECT 'region=' || coalesce(delivery_address->>'region','') "
            f"FROM orders WHERE id='{quote(order_id)}'::uuid;"
        )
    )
    if not token.startswith("region="):
        raise RuntimeError(f"stored region marker missing {token!r}")
    return token[len("region=") :]


def parse_hold_row(raw: str) -> dict[str, Any]:
    for line in raw.splitlines():
        token = line.strip().strip('"')
        if token.count("|") < 3:
            continue
        cols = [c.strip() for c in token.split("|")]
        return {
            "stock_held": cols[0] in {"t", "true"},
            "hold_source": None if cols[1] in {"", "null"} else cols[1],
            "hold_expires_at": None if cols[2] in {"", "null"} else cols[2],
            "state": cols[3],
        }
    raise RuntimeError(f"hold row unparseable {raw!r}")


def order_revision(order_id: str) -> int:
    return int(sql_scalar(sql(f"SELECT revision::text FROM orders WHERE id='{quote(order_id)}'::uuid;")))


def refresh_listing(aggregate_id: str) -> dict[str, Any]:
    raw = sql(
        "SELECT l.aggregate_id || '|' || l.seller_pubky || '|' || l.server_revision::text "
        "|| '|' || l.available_quantity::text || '|' || l.fulfillment_methods::text "
        "|| '|' || CASE "
        "WHEN c.paypal_merchant_email IS NOT NULL AND btrim(c.paypal_merchant_email) <> '' THEN 'paypal' "
        "WHEN c.stripe_payment_link IS NOT NULL AND btrim(c.stripe_payment_link) <> '' THEN 'stripe' "
        "ELSE '' END "
        "FROM listings l "
        "LEFT JOIN seller_payment_configs c ON c.seller_pubky = l.seller_pubky "
        f"WHERE l.aggregate_id='{quote(aggregate_id)}';"
    )
    rows = parse_listing_rows(raw)
    if not rows:
        raise RuntimeError(f"listing missing after restock {aggregate_id}")
    return rows[0]


class Smoke:
    def __init__(self) -> None:
        self.base = os.environ["MARKETPLACE_URL"].rstrip("/")
        self.project = os.environ["RAILWAY_PROJECT_ID"]
        refuse_production(self.base, self.project)
        environment = os.environ.get("RAILWAY_ENVIRONMENT", STAGING_ENVIRONMENT)
        os.environ["RAILWAY_ENVIRONMENT"] = environment
        if environment not in {STAGING_ENVIRONMENT, "production"}:
            raise SystemExit(f"HOLD_SMOKE=STOP unexpected RAILWAY_ENVIRONMENT {environment}")
        self.sessions: list[str] = []
        self.created_orders: list[dict[str, Any]] = []
        self.cases: list[dict[str, Any]] = []
        self.fiat_window: int | None = None

    def load_fiat_window(self) -> int:
        if self.fiat_window is None:
            self.fiat_window = fiat_window_seconds()
        return self.fiat_window

    def command(self, token: str, body: dict[str, Any], *, allowed: set[int], case: str) -> tuple[int, dict | str]:
        status, resp = api(self.base, "POST", "/v1/commands", token, body)
        stop_on_unexpected_4xx(status, resp, allowed=allowed, case=case)
        return status, resp

    def cancel_order(self, token: str, order_id: str, reason: str) -> dict[str, Any]:
        revision = order_revision(order_id)
        status, resp = self.command(
            token,
            cancel_body(order_id, revision, reason),
            allowed={200, 409},
            case=f"cancel:{order_id}",
        )
        ok = isinstance(resp, dict) and resp.get("ok") is True
        if status == 409 and isinstance(resp, dict):
            message = ((resp.get("error") or {}).get("message") or "")
            if "no longer be cancelled" in message:
                cols_raw = sql(
                    "SELECT stock_held::text, coalesce(hold_source,''), "
                    "coalesce(hold_expires_at::text,''), state "
                    f"FROM orders WHERE id='{quote(order_id)}'::uuid;"
                )
                hold = parse_hold_row(cols_raw)
                if hold["state"] == "cancelled" and not hold["stock_held"]:
                    return {"http": status, "state": hold["state"], "stock_held": False, "already_cancelled": True}
        if status != 200 or not ok:
            raise RuntimeError(f"cancel failed {status} {resp!r}")
        cols_raw = sql(
            "SELECT stock_held::text, coalesce(hold_source,''), "
            "coalesce(hold_expires_at::text,''), state "
            f"FROM orders WHERE id='{quote(order_id)}'::uuid;"
        )
        hold = parse_hold_row(cols_raw)
        if hold["state"] != "cancelled":
            raise RuntimeError(f"order {order_id} state {hold['state']!r} after cancel")
        if hold["stock_held"]:
            raise RuntimeError(f"order {order_id} still stock_held after cancel")
        return {"http": status, "state": hold["state"], "stock_held": hold["stock_held"]}

    def order_hold(self, order_id: str) -> dict[str, Any]:
        hold_raw = sql(
            "SELECT stock_held::text, coalesce(hold_source,''), "
            "coalesce(hold_expires_at::text,''), state "
            f"FROM orders WHERE id='{quote(order_id)}'::uuid;"
        )
        return parse_hold_row(hold_raw)

    def bind_payment(
        self, token: str, order_id: str, method: str, *, case: str
    ) -> tuple[int, dict | str]:
        body = bind_body(method)
        status, resp = api(
            self.base,
            "POST",
            f"/v0/orders/{order_id}/payment-method",
            token,
            body,
        )
        stop_on_unexpected_4xx(status, resp, allowed={200, 409}, case=case)
        return status, resp

    def two_buyer(
        self,
        listing: dict[str, Any],
        *,
        fulfillment: str,
        address: dict[str, str] | None,
        case: str,
        expected_stored_region: set[str] | None,
    ) -> dict[str, Any]:
        method = listing.get("bind_method") or ""
        if method not in {"paypal", "stripe"}:
            raise RuntimeError(f"{case}: listing has no paypal/stripe bind method {listing!r}")
        window = self.load_fiat_window()
        buyer_a = z32_pubky()
        buyer_b = z32_pubky()
        if buyer_a == listing["seller_pubky"] or buyer_b == listing["seller_pubky"]:
            raise RuntimeError("buyer collides with seller")
        token_a, sid_a = insert_session(buyer_a)
        token_b, sid_b = insert_session(buyer_b)
        self.sessions.extend([sid_a, sid_b])
        cmd_a = str(uuid.uuid4())
        cmd_b = str(uuid.uuid4())
        body_a = checkout_body(listing, cmd_a, fulfillment=fulfillment, address=address)
        body_b = checkout_body(listing, cmd_b, fulfillment=fulfillment, address=address)
        assert_shape(body_a, shipping=fulfillment == "shipping")
        status_a, resp_a = self.command(token_a, body_a, allowed={200}, case=f"{case}.first_create")
        status_b, resp_b = self.command(token_b, body_b, allowed={200}, case=f"{case}.second_create")
        first_ok = isinstance(resp_a, dict) and resp_a.get("ok") is True
        second_ok = isinstance(resp_b, dict) and resp_b.get("ok") is True
        if not first_ok or not second_ok:
            raise RuntimeError(
                f"{case}: both checkouts must succeed unheld first={status_a} {resp_a!r} "
                f"second={status_b} {resp_b!r}"
            )
        order_a = resp_a["result"]["orders"][0]["id"]  # type: ignore[index]
        order_b = resp_b["result"]["orders"][0]["id"]  # type: ignore[index]
        self.created_orders.append({"id": order_a, "token": token_a, "case": case})
        self.created_orders.append({"id": order_b, "token": token_b, "case": case})
        hold_a = self.order_hold(order_a)
        hold_b = self.order_hold(order_b)
        if hold_a["stock_held"] or hold_b["stock_held"]:
            raise RuntimeError(f"{case}: checkout must not hold a={hold_a!r} b={hold_b!r}")
        if hold_a["hold_source"] or hold_b["hold_source"]:
            raise RuntimeError(f"{case}: checkout hold_source a={hold_a!r} b={hold_b!r}")
        bind_a_status, bind_a = self.bind_payment(token_a, order_a, method, case=f"{case}.first_bind")
        bind_b_status, bind_b = self.bind_payment(token_b, order_b, method, case=f"{case}.second_bind")
        if bind_a_status == 200 and bind_b_status == 200:
            raise RuntimeError(f"{case}: both binds succeeded")
        if bind_a_status == 200:
            winner_order, winner_token = order_a, token_a
            loser_status, loser_body = bind_b_status, bind_b
        elif bind_b_status == 200:
            winner_order, winner_token = order_b, token_b
            loser_status, loser_body = bind_a_status, bind_a
        else:
            raise RuntimeError(
                f"{case}: no bind winner first={bind_a_status} {bind_a!r} second={bind_b_status} {bind_b!r}"
            )
        loser_message = (loser_body.get("error") or {}).get("message") if isinstance(loser_body, dict) else None
        if loser_status != 409:
            raise RuntimeError(f"{case}: bind loser http {loser_status} body={loser_body!r}")
        if loser_message != HOLDING_COPY:
            raise RuntimeError(f"{case}: bind loser message {loser_message!r}")
        hold = self.order_hold(winner_order)
        if not hold["stock_held"] or hold["hold_source"] != "bind":
            raise RuntimeError(f"{case}: hold after bind {hold!r}")
        ttl = float(
            sql_scalar(
                sql(
                    "SELECT extract(epoch from (hold_expires_at - now())) "
                    f"FROM orders WHERE id='{quote(winner_order)}'::uuid;"
                )
            )
        )
        if not ttl_matches_window(ttl, window):
            raise RuntimeError(f"{case}: ttl {ttl} window={window}")
        stored_region = None
        if expected_stored_region is not None:
            stored_region = stored_region_of(winner_order)
            if stored_region not in expected_stored_region:
                raise RuntimeError(
                    f"{case}: stored region {stored_region!r} not in {sorted(expected_stored_region)!r}"
                )
        cancel = self.cancel_order(winner_token, winner_order, f"hold-smoke {case} restock")
        loser_order = order_b if winner_order == order_a else order_a
        loser_token = token_b if winner_order == order_a else token_a
        self.cancel_order(loser_token, loser_order, f"hold-smoke {case} loser")
        listing_after = refresh_listing(listing["aggregate_id"])
        if listing_after["available_quantity"] != 1:
            raise RuntimeError(
                f"{case}: listing qty {listing_after['available_quantity']} after cancel"
            )
        return {
            "case": case,
            "listing": listing["aggregate_id"],
            "shop_tag": SHOP_TAG,
            "shop_sha": SHOP_SHA,
            "fulfillment": fulfillment,
            "delivery_address": address,
            "bind_method": method,
            "fiat_window_seconds": window,
            "first_http": status_a,
            "second_http": status_b,
            "first_bind_http": bind_a_status,
            "second_bind_http": bind_b_status,
            "loser_http": loser_status,
            "loser_message": loser_message,
            "winner_order_id": winner_order,
            "hold": hold,
            "ttl_seconds": ttl,
            "stored_region": stored_region,
            "cancel": cancel,
            "listing_qty_after": listing_after["available_quantity"],
        }

    def one_buyer(
        self,
        listing: dict[str, Any],
        *,
        fulfillment: str,
        address: dict[str, str] | None,
        case: str,
        expected_stored_region: set[str] | None,
    ) -> dict[str, Any]:
        buyer = z32_pubky()
        if buyer == listing["seller_pubky"]:
            raise RuntimeError("buyer collides with seller")
        token, sid = insert_session(buyer)
        self.sessions.append(sid)
        body = checkout_body(listing, str(uuid.uuid4()), fulfillment=fulfillment, address=address)
        assert_shape(body, shipping=fulfillment == "shipping")
        status, resp = self.command(token, body, allowed={200}, case=case)
        ok = isinstance(resp, dict) and resp.get("ok") is True
        if not ok:
            raise RuntimeError(f"{case}: not ok {status} {resp!r}")
        order_id = resp["result"]["orders"][0]["id"]  # type: ignore[index]
        self.created_orders.append({"id": order_id, "token": token, "case": case})
        hold = self.order_hold(order_id)
        if hold["stock_held"] or hold["hold_source"]:
            raise RuntimeError(f"{case}: checkout must not hold {hold!r}")
        stored_region = None
        if expected_stored_region is not None:
            stored_region = stored_region_of(order_id)
            if stored_region not in expected_stored_region:
                raise RuntimeError(
                    f"{case}: stored region {stored_region!r} not in {sorted(expected_stored_region)!r}"
                )
        cancel = self.cancel_order(token, order_id, f"hold-smoke {case} restock")
        listing_after = refresh_listing(listing["aggregate_id"])
        if listing_after["available_quantity"] != 1:
            raise RuntimeError(f"{case}: listing qty after cancel {listing_after['available_quantity']}")
        return {
            "case": case,
            "listing": listing["aggregate_id"],
            "shop_tag": SHOP_TAG,
            "shop_sha": SHOP_SHA,
            "fulfillment": fulfillment,
            "delivery_address": address,
            "http": status,
            "order_id": order_id,
            "hold": hold,
            "stored_region": stored_region,
            "cancel": cancel,
            "listing_qty_after": listing_after["available_quantity"],
        }

    def verify_all_cancelled(self) -> list[dict[str, Any]]:
        verified = []
        for row in self.created_orders:
            raw = sql(
                "SELECT stock_held::text, coalesce(hold_source,''), "
                "coalesce(hold_expires_at::text,''), state "
                f"FROM orders WHERE id='{quote(row['id'])}'::uuid;"
            )
            hold = parse_hold_row(raw)
            if hold["state"] != "cancelled" or hold["stock_held"]:
                raise RuntimeError(f"leftover order {row['id']} hold={hold!r}")
            verified.append({"id": row["id"], "case": row["case"], "state": hold["state"]})
        return verified

    def cleanup_sessions(self) -> None:
        for sid in self.sessions:
            try:
                sql(f"DELETE FROM auth_sessions WHERE session_id='{quote(sid)}'::uuid;")
            except Exception as error:  # noqa: BLE001 — cleanup must not hide the smoke result
                print(f"session_cleanup_error={error}", file=sys.stderr)

    def run(self) -> dict[str, Any]:
        pickup_restore: dict[str, str] | None = None
        try:
            shipping = pick_listing(shipping=True, require_bind=True)
            pickup, pickup_restore = ensure_pickup_listing({shipping["aggregate_id"]})
            self.cases.append(
                self.two_buyer(
                    shipping,
                    fulfillment="shipping",
                    address=SHIPPING_ADDRESSES["california_free_text"],
                    case="shipping_california_free_text",
                    expected_stored_region=EXPECTED_STORED_REGION["california_free_text"],
                )
            )
            shipping = refresh_listing(shipping["aggregate_id"])
            self.cases.append(
                self.one_buyer(
                    shipping,
                    fulfillment="shipping",
                    address=SHIPPING_ADDRESSES["ca_iso_suffix"],
                    case="shipping_ca_iso_suffix",
                    expected_stored_region=EXPECTED_STORED_REGION["ca_iso_suffix"],
                )
            )
            shipping = refresh_listing(shipping["aggregate_id"])
            pickup = refresh_listing(pickup["aggregate_id"])
            self.cases.append(
                self.two_buyer(
                    pickup,
                    fulfillment="pickup",
                    address=None,
                    case="pickup_two_buyer",
                    expected_stored_region=None,
                )
            )
            shipping = refresh_listing(shipping["aggregate_id"])
            self.cases.append(
                self.one_buyer(
                    shipping,
                    fulfillment="shipping",
                    address=SHIPPING_ADDRESSES["pt_empty_region"],
                    case="shipping_pt_empty_region",
                    expected_stored_region=EXPECTED_STORED_REGION["pt_empty_region"],
                )
            )
            verified = self.verify_all_cancelled()
            summary = {
                "shop_tag": SHOP_TAG,
                "shop_sha": SHOP_SHA,
                "marketplace_url": self.base,
                "railway_project": self.project,
                "fiat_window_seconds": self.fiat_window,
                "pickup_methods_retargeted": pickup_restore is not None,
                "cases": self.cases,
                "orders_verified_cancelled": verified,
            }
            print(json.dumps(summary, indent=2, sort_keys=True, default=str))
            print("HOLD_SMOKE=PASS")
            return summary
        except Exception:
            for row in list(self.created_orders):
                try:
                    self.cancel_order(row["token"], row["id"], "hold-smoke failure cleanup")
                except Exception as error:  # noqa: BLE001
                    print(f"cancel_cleanup_error={row['id']}:{error}", file=sys.stderr)
            print(
                json.dumps(
                    {
                        "hold_smoke": "STOP",
                        "shop_tag": SHOP_TAG,
                        "shop_sha": SHOP_SHA,
                        "marketplace_url": self.base,
                        "cases_completed": self.cases,
                        "created_orders": [{"id": row["id"], "case": row["case"]} for row in self.created_orders],
                    },
                    indent=2,
                    sort_keys=True,
                    default=str,
                )
            )
            print("HOLD_SMOKE=STOP")
            raise
        finally:
            if pickup_restore is not None:
                try:
                    restore_listing_methods(pickup_restore["aggregate_id"], pickup_restore["methods"])
                except Exception as error:  # noqa: BLE001
                    print(f"pickup_restore_error={error}", file=sys.stderr)
            self.cleanup_sessions()


def main() -> None:
    parser = argparse.ArgumentParser(description="Staging hold-smoke (never production).")
    parser.add_argument("--self-check", action="store_true", help="Validate payload shape without hitting staging.")
    args = parser.parse_args()
    if args.self_check:
        self_check()
        return
    for required in ("MARKETPLACE_URL", "RAILWAY_PROJECT_ID"):
        if required not in os.environ:
            raise SystemExit(f"HOLD_SMOKE=STOP missing {required}")
    os.environ.setdefault("RAILWAY_ENVIRONMENT", STAGING_ENVIRONMENT)
    os.environ.setdefault("RAILWAY_DATABASE_SERVICE", "Postgres")
    Smoke().run()


if __name__ == "__main__":
    main()
