"""RFC 0078 battery 11b and 11c: lake reads after a credential they were given has expired.

Both run on one vending stack (``tests/support/lakekeeper.py``) configured for short lives:

* **11c, the catalog token.** The tenant principals get 1 s access tokens, the shortest
  Keycloak issues. Lakekeeper still accepts a token for 60 s past its ``exp``, so the scenario
  waits 75 s, checks that a token fetched at the start is now refused, and reads again.
* **11b, the vended storage credential.** The warehouses ask for 1 s credentials; the store
  issues 900 s ones, its floor. The scenario reads at t0, waits until the credential vended
  last has expired, and reads again. Every routed read drops the secrets it was vended
  (EXECUTION-LOG D-025, D-047), so a read after expiry always starts from a fresh ``LoadTable``.
  Two legs on a plain session show what the drop is for: a statement held open across expiry,
  and a statement after expiry on a session that kept its first secret.

Each scenario runs once per module; each test asserts one of its outcomes. The 11b scenario
waits about 16 minutes, so the module is ``lake_slow``: excluded from ``just test``, run by
``just lake-slow`` and the scheduled ``lake-expiry`` workflow.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
from collections.abc import Awaitable, Iterator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import UUID

import boto3
import duckdb
import httpx
import pyarrow as pa
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from forze.application.contracts.secrets import SecretRef
from forze_duckdb import LakeTenantCatalog, RoutedDuckDbClient
from tests.support.lakekeeper import (
    BUCKET,
    OAUTH2_SCOPE,
    LakeTenant,
    LakeVendingStack,
    lake_vending_stack,
)

pytestmark = [pytest.mark.integration, pytest.mark.lake_slow, pytest.mark.timeout(1800)]

_A = UUID("aaaaaaaa-0000-0000-0000-000000000001")
_TOKEN_LEEWAY = 60
"""Seconds Lakekeeper accepts a token past its ``exp`` (EXECUTION-LOG D-050)."""
_MARGIN = 15
_ORDERS = "SELECT count(*) AS n FROM lake.gold.orders"
_STATUS = re.compile(r"'status': ([A-Za-z_0-9]+)")
_EVENTS = ("gold", "events")
_EVENT_FILES, _EVENT_ROWS = 24, 50_000
"""Enough data files that a paused stream has requests left to make after the pause."""
_ON_DUCKDB_2 = int(duckdb.__version__.split(".", 1)[0]) >= 2

# ----------------------- #


def _seed_events(lake: LakeVendingStack, tenant: LakeTenant) -> None:
    schema = pa.schema([("id", pa.int64()), ("pad", pa.string())])
    table = lake.admin_catalog(tenant).create_table(_EVENTS, schema=schema)

    for f in range(_EVENT_FILES):
        ids = pa.array(range(f * _EVENT_ROWS, (f + 1) * _EVENT_ROWS), pa.int64())
        pad = pa.array([os.urandom(24).hex() for _ in range(_EVENT_ROWS)], pa.string())
        table.append(pa.table({"id": ids, "pad": pad}, schema=schema))


@pytest.fixture(scope="module")
def lake(docker_available: None) -> Iterator[LakeVendingStack]:
    with lake_vending_stack(tenant_token_lifespan=1, sts_token_validity_seconds=1) as stack:
        _seed_events(stack, stack.tenants["tenant-a"])
        yield stack


class _Principals:
    """The secrets backend: tenant A's principal."""

    def __init__(self, tenant: LakeTenant) -> None:
        self.principal = tenant.principal

    async def resolve_str(self, ref: SecretRef) -> str:
        p = self.principal
        return json.dumps({"client_id": p.client_id, "client_secret": p.client_secret})

    async def exists(self, ref: SecretRef) -> bool:
        return True


def _routed(lake: LakeVendingStack) -> RoutedDuckDbClient:
    a = lake.tenants["tenant-a"]
    return RoutedDuckDbClient(
        secrets=_Principals(a),
        secret_ref_for_tenant=lambda tenant: SecretRef(path=str(tenant)),
        tenant_provider=lambda: _A,
        catalog_for_tenant=lambda tenant: LakeTenantCatalog(
            uri=lake.catalog_uri,
            warehouse=a.warehouse,
            oauth2_server_uri=lake.token_uri,
            oauth2_scope=OAUTH2_SCOPE,
            storage_prefix=f"s3://{BUCKET}/{a.key_prefix}/",
        ),
    )


async def _dynamic(client: RoutedDuckDbClient) -> list[Any]:
    return await client.run_dynamic(
        "SELECT count(*) AS n FROM orders",
        row_probe=2,
        timeout=timedelta(seconds=30),
        use="lake.gold",
    )


async def _analytics(client: RoutedDuckDbClient) -> list[Any]:
    return (await client.run_query(_ORDERS)).rows


async def _attempt(call: Awaitable[Any]) -> Any:
    try:
        return await call

    except Exception as error:
        return error


def _attempt_sync(call: Any) -> Any:
    try:
        return call()

    except Exception as error:
        return error


def _catalog_status(lake: LakeVendingStack, tenant: LakeTenant, token: str) -> int:
    r = httpx.get(
        f"{lake.catalog_uri}/v1/config",
        params={"warehouse": tenant.warehouse},
        headers={"Authorization": f"Bearer {token}"},
    )
    return r.status_code


def _store_answer(lake: LakeVendingStack, vended: dict[str, str], key: str) -> str:
    """HEAD *key* with a vended credential used directly: ``OK`` or the store's error code."""

    s3 = boto3.client(
        "s3",
        endpoint_url=lake.s3_endpoint,
        region_name="local-01",
        config=Config(s3={"addressing_style": "path"}),
        aws_access_key_id=vended["s3.access-key-id"],
        aws_secret_access_key=vended["s3.secret-access-key"],
        aws_session_token=vended["s3.session-token"],
    )
    try:
        s3.head_object(Bucket=BUCKET, Key=key)

    except ClientError as error:
        return str(error.response["ResponseMetadata"]["HTTPStatusCode"])

    return "OK"


def _requests(con: Any, fragment: str) -> list[str]:
    """Statuses of the requests DuckDB's HTTP log saw whose URL contains *fragment*."""

    rows = con.execute(
        "SELECT message FROM duckdb_logs WHERE type = 'HTTP' AND contains(message, $f)",
        {"f": fragment},
    ).fetchall()
    return [m.group(1) if (m := _STATUS.search(msg)) else "" for (msg,) in rows]


def _plain_session(lake: LakeVendingStack, tenant: LakeTenant) -> Any:
    """§3.4 steps 1-4 without the routed client's drop, cache off so reads reach the store."""

    con = lake.session(tenant.principal, tenant.warehouse)
    con.execute("SET threads=1")
    con.execute("SET enable_external_file_cache=false")
    con.execute("CALL enable_logging('HTTP')")
    return con


# ----------------------- #


@dataclass(frozen=True, slots=True)
class _TokenExpiry:
    t0_token_status: int
    after: Any
    builds: int


@pytest.fixture(scope="module")
def token_expiry(lake: LakeVendingStack) -> _TokenExpiry:
    a = lake.tenants["tenant-a"]

    async def scenario() -> _TokenExpiry:
        client = _routed(lake)
        await client.startup()

        try:
            await _dynamic(client)
            token = lake.token(a.principal)
            await asyncio.sleep(1 + _TOKEN_LEEWAY + _MARGIN)
            status = _catalog_status(lake, a, token)
            after = await _attempt(_dynamic(client))
            return _TokenExpiry(status, after, client.pool_stats().created)

        finally:
            await client.close()

    return asyncio.run(scenario())


class TestCatalogTokenExpiry:
    """Battery 11c — the catalog token expires under a live instance, which refreshes it."""

    def test_a_token_from_the_start_is_refused_by_now(self, token_expiry: _TokenExpiry) -> None:
        assert token_expiry.t0_token_status == 401

    def test_a_statement_after_expiry_reads_without_a_rebuild(
        self, token_expiry: _TokenExpiry
    ) -> None:
        assert token_expiry.after == [{"n": 3}]
        assert token_expiry.builds == 1


# ----------------------- #


@dataclass(frozen=True, slots=True)
class _CredentialExpiry:
    waited: float
    store_before: str
    store_after: str
    analytics_after: Any
    dynamic_after: Any
    builds: int
    span_after: Any
    span_store_after: list[str]
    span_catalog_after: list[str]
    kept_after: Any


@pytest.fixture(scope="module")
def credential_expiry(lake: LakeVendingStack) -> _CredentialExpiry:
    a = lake.tenants["tenant-a"]

    async def scenario() -> _CredentialExpiry:
        client = _routed(lake)
        await client.startup()

        try:
            await _dynamic(client)
            await _analytics(client)  # last at t0: it would leave the secret without D-047

            span = _plain_session(lake, a)
            stream = span.cursor()
            stream.execute("SELECT id, pad FROM lake.gold.events")
            stream.fetchmany(1)
            store_seen = len(_requests(span, "/data/"))
            catalog_seen = len(_requests(span, "/tables/events"))

            kept = _plain_session(lake, a)
            kept.execute(_ORDERS).fetchall()

            loaded = lake.load_table(a.principal, a.warehouse).json()
            (credential,) = loaded["storage-credentials"]
            vended = credential["config"]
            data_file = kept.execute(
                "SELECT file_path FROM iceberg_metadata(lake.gold.orders)"
            ).fetchone()[0]
            key = data_file.split(f"s3://{BUCKET}/", 1)[1]
            store_before = _store_answer(lake, vended, key)

            waited = int(vended["s3.session-token-expires-at-ms"]) / 1000 - time.time() + _MARGIN
            await asyncio.sleep(waited)

            return _CredentialExpiry(
                waited=waited,
                store_before=store_before,
                store_after=_store_answer(lake, vended, key),
                analytics_after=await _attempt(_analytics(client)),
                dynamic_after=await _attempt(_dynamic(client)),
                builds=client.pool_stats().created,
                span_after=_attempt_sync(lambda: len(stream.fetchall())),
                span_store_after=_requests(span, "/data/")[store_seen:],
                span_catalog_after=_requests(span, "/tables/events")[catalog_seen:],
                kept_after=_attempt_sync(lambda: kept.execute(_ORDERS).fetchall()),
            )

        finally:
            await client.close()

    return asyncio.run(scenario())


class TestVendedCredentialExpiry:
    """Battery 11b — the storage credential a statement was vended expires."""

    def test_the_store_refuses_the_credential_once_it_expires(
        self, credential_expiry: _CredentialExpiry
    ) -> None:
        """The negative control: without it, every read below would prove nothing."""

        assert credential_expiry.waited > 0
        assert credential_expiry.store_before == "OK"
        assert credential_expiry.store_after == "400"  # RustFS: InvalidRequest, check key failed

    def test_routed_reads_after_expiry_load_afresh_on_the_same_instance(
        self, credential_expiry: _CredentialExpiry
    ) -> None:
        assert credential_expiry.analytics_after == [{"n": 3}]
        assert credential_expiry.dynamic_after == [{"n": 3}]
        assert credential_expiry.builds == 1

    def test_a_statement_held_open_across_expiry_fails_loud(
        self, credential_expiry: _CredentialExpiry
    ) -> None:
        """duckdb-iceberg#1352: the engine does not re-vend mid-statement; the next read fails."""

        error = credential_expiry.span_after
        assert isinstance(error, duckdb.Error)
        assert "HTTP 400" in str(error)
        # One store request. DuckDB 2 logs a failed GET with no response, so no status (D-067).
        assert credential_expiry.span_store_after == ([""] if _ON_DUCKDB_2 else ["BadRequest_400"])
        assert credential_expiry.span_catalog_after == []

    def test_a_kept_secret_outranks_the_fresh_one_after_expiry(
        self, credential_expiry: _CredentialExpiry
    ) -> None:
        """Why every routed read drops its secrets: the next statement fetches a fresh one and
        still signs with the expired one (EXECUTION-LOG D-047). On DuckDB 2 nothing is kept, so
        the next statement loads afresh and reads (D-056)."""

        if _ON_DUCKDB_2:
            assert credential_expiry.kept_after == [(3,)]
            return

        error = credential_expiry.kept_after
        assert isinstance(error, duckdb.HTTPException)
        assert "HTTP 400" in str(error)
