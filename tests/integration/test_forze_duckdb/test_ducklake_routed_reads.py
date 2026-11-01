"""Routed DuckDB reads over per-tenant DuckLakes (issue #800), against a real Postgres catalog.

Every read goes through the production path: ``RoutedDuckDbClient`` resolves a tenant's
``DuckLakeTenantCatalog`` and its secret, builds an instance with the DuckLake sequence, attests
it, and only then serves it. The stack (``tests/support/ducklake.py``) gives each tenant its own
metadata schema, reader role and data path, as the application provisions them. The module runs
on DuckDB 1.5 and 2; where the two differ, a test says which is which.

``_fault`` rewrites the sequence the client runs, as the Iceberg battery's seam does, so each
attestation leg runs the shipped code with one step taken away.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import os
from collections.abc import AsyncIterator, Callable, Iterator
from datetime import timedelta
from secrets import token_hex
from typing import Any
from uuid import UUID

import duckdb
import pytest

from forze.application.contracts.dynamic_read import DynamicReadPort, DynamicReadSpec
from forze.application.contracts.secrets import SecretRef
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.dynamic_read import STATEMENT_INVALID_CODE, WRITE_REFUSED_CODE
from forze.base.exceptions import CoreException
from forze_duckdb import (
    DuckDbDynamicReadConfig,
    DuckLakeTenantCatalog,
    RoutedDuckDbClient,
)
from forze_duckdb.adapters import DuckDbDynamicReadAdapter
from forze_duckdb.kernel.client import routed_client
from tests.integration.test_forze_duckdb.conftest import S3_ACCESS_KEY, S3_SECRET_KEY
from tests.support.ducklake import (
    BUCKET,
    DATABASE,
    DuckLakeStack,
    DuckLakeTenant,
    S3Store,
    ducklake_stack,
)

pytestmark = pytest.mark.integration

_A = UUID("aaaaaaaa-0000-0000-0000-00000000000a")
_B = UUID("bbbbbbbb-0000-0000-0000-00000000000b")
_C = UUID("cccccccc-0000-0000-0000-00000000000c")
_D = UUID("dddddddd-0000-0000-0000-00000000000d")
_S = UUID("eeeeeeee-0000-0000-0000-00000000000e")
_NAMES = {_A: "a", _B: "b", _C: "c", _D: "d", _S: "s"}

_ATTESTATION_FAILED = "duckdb_session_attestation_failed"
_SETUP_FAILED = "duckdb_session_setup_failed"
_ON_DUCKDB_2 = int(duckdb.__version__.split(".", 1)[0]) >= 2

_tenant: contextvars.ContextVar[UUID | None] = contextvars.ContextVar("tenant", default=None)

# ----------------------- #


@pytest.fixture(scope="module")
def stack(docker_available: None, s3_container: tuple[Any, str]) -> Iterator[DuckLakeStack]:
    container, endpoint = s3_container
    container.get_client().make_bucket(BUCKET)
    store = S3Store(endpoint=endpoint, access_key=S3_ACCESS_KEY, secret_key=S3_SECRET_KEY)

    with ducklake_stack(store=store) as built:
        yield built


class _Secrets:
    """Each tenant's reader role, and the store key for the tenant on ``s3://``."""

    def __init__(self, stack: DuckLakeStack) -> None:
        self.stack = stack
        self.overrides: dict[UUID, dict[str, Any]] = {}

    def payload(self, tenant: UUID) -> dict[str, Any]:
        t = self.stack.tenants[_NAMES[tenant]]
        payload: dict[str, Any] = {"user": t.reader, "password": t.reader_password}

        if t.data_path.startswith("s3://") and self.stack.store is not None:
            store = self.stack.store
            payload["store"] = {
                "access_key_id": store.access_key,
                "secret_access_key": store.secret_key,
                "region": "us-east-1",
                "endpoint": store.endpoint,
                "url_style": "path",
                "use_ssl": False,
            }

        return {**payload, **self.overrides.get(tenant, {})}

    async def resolve_str(self, ref: SecretRef) -> str:
        return json.dumps(self.payload(UUID(ref.path)))

    async def exists(self, ref: SecretRef) -> bool:
        return UUID(ref.path) in _NAMES


@pytest.fixture
def secrets(stack: DuckLakeStack) -> _Secrets:
    return _Secrets(stack)


def _catalog(stack: DuckLakeStack, tenant: UUID) -> DuckLakeTenantCatalog:
    t = stack.tenants[_NAMES[tenant]]

    return DuckLakeTenantCatalog(
        host=stack.host,
        port=stack.port,
        database=DATABASE,
        metadata_schema=t.schema,
        data_path=t.data_path,
    )


@pytest.fixture
async def build_routed(
    stack: DuckLakeStack, secrets: _Secrets
) -> AsyncIterator[Callable[..., RoutedDuckDbClient]]:
    built: list[RoutedDuckDbClient] = []

    def build(**kwargs: Any) -> RoutedDuckDbClient:
        client = RoutedDuckDbClient(
            secrets=secrets,
            secret_ref_for_tenant=lambda tenant: SecretRef(path=str(tenant)),
            tenant_provider=_tenant.get,
            catalog_for_tenant=lambda tenant: _catalog(stack, tenant),
            **kwargs,
        )
        built.append(client)
        return client

    yield build

    for client in built:
        await client.close()


@pytest.fixture
async def routed(build_routed: Callable[..., RoutedDuckDbClient]) -> RoutedDuckDbClient:
    client = build_routed()
    await client.startup()
    return client


def _identity() -> TenantIdentity | None:
    tenant = _tenant.get()
    return None if tenant is None else TenantIdentity(tenant_id=tenant)


def _port(client: RoutedDuckDbClient, namespace: str = "gold") -> DynamicReadPort:
    config = DuckDbDynamicReadConfig(
        provenance="trusted", namespace=namespace, statement_timeout=timedelta(seconds=5)
    )
    return DuckDbDynamicReadAdapter(
        client=client,
        spec=DynamicReadSpec(name="lake_orders", row_cap=100),
        config=config,
        statement_timeout=config.statement_timeout,
        tenant_provider=_identity,
    )


async def _rows(client: RoutedDuckDbClient, tenant: UUID, sql: str) -> list[dict[str, Any]]:
    """Run *sql* on the tenant's instance directly, below the adapter: analytics SQL."""

    async with client.client_scope(tenant) as inner:
        return list((await inner.run_query(sql)).rows)


async def _engine_refusal(client: RoutedDuckDbClient, tenant: UUID, sql: str) -> BaseException:
    with pytest.raises((duckdb.Error, CoreException)) as ei:
        await _rows(client, tenant, sql)

    return ei.value


async def _build_refusal(client: RoutedDuckDbClient, tenant: UUID) -> CoreException:
    with pytest.raises(CoreException) as ei:
        await _rows(client, tenant, "SELECT 1")

    return ei.value


def _fault(monkeypatch: pytest.MonkeyPatch, rewrite: Callable[[list[str]], list[str]]) -> None:
    real = routed_client._ducklake_session_statements
    monkeypatch.setattr(
        routed_client,
        "_ducklake_session_statements",
        lambda *args, **kwargs: rewrite(real(*args, **kwargs)),
    )


def _replace(old: str, new: str) -> Callable[[list[str]], list[str]]:
    return lambda statements: [s.replace(old, new) for s in statements]


# ----------------------- #


class TestTenantReads:
    async def test_one_plan_reads_each_tenants_own_rows(self, routed: RoutedDuckDbClient) -> None:
        port = _port(routed)

        for tenant, rows in ((_A, 5), (_B, 7)):
            _tenant.set(tenant)
            result = await port.run("SELECT tenant, count(*) AS n FROM orders GROUP BY 1")

            assert [dict(r) for r in result] == [{"tenant": _NAMES[tenant], "n": rows}]

    async def test_analytics_sql_reads_gold_and_silver(self, routed: RoutedDuckDbClient) -> None:
        rows = await _rows(
            routed,
            _A,
            "SELECT (SELECT count(*) FROM lake.gold.orders) AS gold, "
            "(SELECT count(*) FROM lake.silver.raw) AS silver",
        )

        assert rows == [{"gold": 5, "silver": 3}]

    async def test_a_held_instance_sees_what_the_writer_commits_later(
        self, routed: RoutedDuckDbClient, stack: DuckLakeStack
    ) -> None:
        b = stack.tenants["b"]
        assert await _rows(routed, _B, "SELECT count(*) AS n FROM lake.gold.orders") == [{"n": 7}]

        with stack.writer(b) as con:
            con.execute("INSERT INTO lake.gold.orders SELECT 100 + i, 'b' FROM range(3) t(i)")
            con.execute("CREATE TABLE lake.gold.late AS SELECT 1 AS x")

        try:
            assert await _rows(routed, _B, "SELECT count(*) AS n FROM lake.gold.orders") == [
                {"n": 10}
            ]
            assert await _rows(routed, _B, "SELECT x FROM lake.gold.late") == [{"x": 1}]

        finally:
            with stack.writer(b) as con:
                con.execute("DELETE FROM lake.gold.orders WHERE id >= 100")
                con.execute("DROP TABLE lake.gold.late")


class TestConfinement:
    async def test_another_tenants_data_path_is_refused_by_the_allowlist(
        self, routed: RoutedDuckDbClient, stack: DuckLakeStack
    ) -> None:
        other = stack.tenants["b"].data_path
        refusal = await _engine_refusal(
            routed, _A, f"SELECT count(*) FROM read_parquet('{other}**/*.parquet')"
        )

        assert "disabled by configuration" in str(refusal)

    async def test_another_tenants_metadata_schema_is_refused_by_the_role(
        self, routed: RoutedDuckDbClient, stack: DuckLakeStack
    ) -> None:
        other = stack.tenants["b"].schema
        refusal = await _engine_refusal(
            routed, _A, f"SELECT count(*) FROM __ducklake_metadata_lake.{other}.ducklake_table"
        )

        assert other in str(refusal)

    async def test_attaching_another_lake_or_database_is_refused(
        self, routed: RoutedDuckDbClient, stack: DuckLakeStack
    ) -> None:
        b = stack.tenants["b"]
        dsn = (
            f"host={stack.host} port={stack.port} dbname={DATABASE} "
            f"user={b.reader} password={b.reader_password}"
        )

        for sql in (
            f"ATTACH '{dsn}' AS other (TYPE postgres)",
            (
                f"ATTACH 'ducklake:postgres:{dsn}' AS other "
                f"(DATA_PATH '{b.data_path}', METADATA_SCHEMA '{b.schema}', READ_ONLY)"
            ),
        ):
            assert "disabled through configuration" in str(await _engine_refusal(routed, _A, sql))

    async def test_the_lake_and_its_metadata_take_no_write(
        self, routed: RoutedDuckDbClient, stack: DuckLakeStack
    ) -> None:
        schema = stack.tenants["a"].schema

        for sql in (
            "CREATE TABLE lake.gold.evil AS SELECT 1 AS x",
            "INSERT INTO lake.gold.orders VALUES (99, 'x')",
            f"DELETE FROM __ducklake_metadata_lake.{schema}.ducklake_tag",
            "CALL ducklake_expire_snapshots('lake', older_than => now())",
        ):
            assert "read-only" in str(await _engine_refusal(routed, _A, sql))

    async def test_a_dynamic_read_refuses_the_metadata_database_and_maintenance(
        self, routed: RoutedDuckDbClient, stack: DuckLakeStack
    ) -> None:
        port = _port(routed)
        _tenant.set(_A)
        schema = stack.tenants["a"].schema

        for sql, code in (
            ("SELECT * FROM postgres_query('__ducklake_metadata_lake', 'SELECT 1')", None),
            (f"SELECT * FROM __ducklake_metadata_lake.{schema}.ducklake_table", None),
            ("SELECT * FROM ducklake_snapshots('lake')", None),
            ("CALL ducklake_cleanup_old_files('lake', cleanup_all => true)", WRITE_REFUSED_CODE),
        ):
            with pytest.raises(CoreException) as ei:
                await port.run(sql)

            assert ei.value.code == (code or STATEMENT_INVALID_CODE), sql


class TestThePasswordStaysOut:
    async def test_no_statement_can_read_the_metadata_password(
        self, routed: RoutedDuckDbClient, stack: DuckLakeStack
    ) -> None:
        password = stack.tenants["a"].reader_password
        rows = await _rows(
            routed,
            _A,
            "SELECT (SELECT list(path) FROM duckdb_databases())::VARCHAR AS paths, "
            "(SELECT list(secret_string) FROM duckdb_secrets())::VARCHAR AS secrets, "
            "current_setting('allowed_paths')::VARCHAR AS allowed",
        )

        assert password not in json.dumps(rows)
        assert "password=redacted" in rows[0]["secrets"]

    async def test_a_failed_attach_reports_no_password(
        self, build_routed: Callable[..., RoutedDuckDbClient], secrets: _Secrets
    ) -> None:
        wrong = token_hex(16)
        secrets.overrides[_A] = {"password": wrong}
        client = build_routed()
        await client.startup()

        error = await _build_refusal(client, _A)

        assert error.code == _SETUP_FAILED
        assert wrong not in json.dumps(error.details)


class TestAttestation:
    async def test_a_reader_role_that_can_write_its_metadata_is_refused(
        self, routed: RoutedDuckDbClient
    ) -> None:
        error = await _build_refusal(routed, _C)

        assert error.code == _ATTESTATION_FAILED
        assert error.details["problems"] == ["metadata role can write 1 tables"]

    async def test_a_reader_role_that_writes_a_view_or_reads_outside_its_schema_is_refused(
        self, routed: RoutedDuckDbClient
    ) -> None:
        error = await _build_refusal(routed, _D)

        assert error.code == _ATTESTATION_FAILED
        # A view written through, and a column updated; a table and a column read outside.
        assert error.details["problems"] == [
            "metadata role can write 2 tables",
            "metadata role can read 1 other lakes' metadata",
            "metadata role can read 2 relations outside its schema",
            "metadata schema holds 2 views reading outside it",
        ]

    async def test_a_password_in_the_attach_string_is_refused(
        self,
        build_routed: Callable[..., RoutedDuckDbClient],
        stack: DuckLakeStack,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        a = stack.tenants["a"]
        dsn = (
            f"host={stack.host} port={stack.port} dbname={DATABASE} "
            f"user={a.reader} password={a.reader_password}"
        )
        _fault(monkeypatch, _replace("'ducklake:postgres:'", f"'ducklake:postgres:{dsn}'"))
        client = build_routed()
        await client.startup()

        error = await _build_refusal(client, _A)

        assert error.code == _ATTESTATION_FAILED
        assert "lake attached with more than the metadata path" in error.details["problems"]
        assert a.reader_password not in json.dumps(error.details)

    @pytest.mark.parametrize(
        ("fault", "problem"),
        [
            (_replace(", READ_ONLY)", ")"), "lake attached writable"),
            (
                _replace("SET GLOBAL lock_configuration=true", "SELECT 1"),
                "setting lock_configuration",
            ),
            (
                _replace("SET GLOBAL enable_external_file_cache=false", "SELECT 1"),
                "setting enable_external_file_cache",
            ),
        ],
    )
    async def test_a_session_missing_a_step_serves_nothing(
        self,
        build_routed: Callable[..., RoutedDuckDbClient],
        monkeypatch: pytest.MonkeyPatch,
        fault: Callable[[list[str]], list[str]],
        problem: str,
    ) -> None:
        _fault(monkeypatch, fault)
        client = build_routed()
        await client.startup()

        error = await _build_refusal(client, _A)

        assert error.code == _ATTESTATION_FAILED
        assert problem in error.details["problems"]

    async def test_a_widened_allowlist_is_refused(
        self,
        build_routed: Callable[..., RoutedDuckDbClient],
        stack: DuckLakeStack,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        a = stack.tenants["a"]
        _fault(
            monkeypatch,
            _replace(
                f"allowed_directories=['{a.data_path}']", f"allowed_directories=['{stack.root}/']"
            ),
        )
        client = build_routed()
        await client.startup()

        error = await _build_refusal(client, _A)

        assert error.code == _ATTESTATION_FAILED
        assert "setting allowed_directories" in error.details["problems"]
        assert (
            f"path {a.data_path.rstrip('/')}-forze-attest/x not refused"
            in error.details["problems"]
        )


class TestStoreDataPath:
    async def test_a_tenant_on_s3_reads_its_prefix_and_nothing_beside_it(
        self, routed: RoutedDuckDbClient
    ) -> None:
        assert await _rows(routed, _S, "SELECT count(*) AS n FROM lake.gold.orders") == [{"n": 4}]

        for path in (f"s3://{BUCKET}/a/x.parquet", f"s3://{BUCKET}/s-x/x.parquet"):
            refusal = await _engine_refusal(routed, _S, f"SELECT * FROM read_parquet('{path}')")
            assert "disabled by configuration" in str(refusal)

        secrets = await _rows(
            routed, _S, "SELECT DISTINCT type FROM duckdb_secrets() ORDER BY type"
        )
        assert secrets == [{"type": "postgres"}, {"type": "s3"}]

    async def test_a_dot_segment_under_s3_goes_to_the_store(
        self, routed: RoutedDuckDbClient
    ) -> None:
        """D-104: the engine sends it to the store as it is; only the key's scope refuses it."""

        sql = f"SELECT * FROM read_parquet('s3://{BUCKET}/s/../a/x.parquet')"
        refusal = await _engine_refusal(routed, _S, sql)

        assert "disabled by configuration" not in str(refusal)
        assert "HTTP" in str(refusal)


class TestEngineFacts:
    """What the engine does that the docs and the deployment must answer for (EXECUTION-LOG
    D-098 to D-103). Each is pinned so an engine bump that changes it fails here first."""

    async def test_an_attach_without_the_unnamed_secret_fails_with_persistent_secrets_off(
        self,
        build_routed: Callable[..., RoutedDuckDbClient],
        stack: DuckLakeStack,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """D-098: with no secret in memory, the attach looks in persistent storage, which is off.
        The unnamed secret is what lets the sequence keep it off."""

        a = stack.tenants["a"]
        dsn = (
            f"host={stack.host} port={stack.port} dbname={DATABASE} "
            f"user={a.reader} password={a.reader_password}"
        )

        def without_the_secret(statements: list[str]) -> list[str]:
            kept = [s for s in statements if not s.startswith("CREATE SECRET (TYPE postgres")]
            return [s.replace("'ducklake:postgres:'", f"'ducklake:postgres:{dsn}'") for s in kept]

        _fault(monkeypatch, without_the_secret)
        client = build_routed()
        await client.startup()

        error = await _build_refusal(client, _A)

        assert error.code == _SETUP_FAILED
        assert "secret storage" in error.details["error"]
        assert a.reader_password not in json.dumps(error.details)

    async def test_the_engine_admits_writes_under_the_data_path(
        self, routed: RoutedDuckDbClient, stack: DuckLakeStack
    ) -> None:
        """D-103: the allowlist is not read-only; a read-only mount or store key is."""

        target = f"{stack.tenants['a'].data_path}forze-write-probe.parquet"

        try:
            await _rows(routed, _A, f"COPY (SELECT 1 AS i) TO '{target}'")
            assert await asyncio.to_thread(os.path.exists, target)

        finally:
            await asyncio.to_thread(lambda: os.path.exists(target) and os.remove(target))

    @pytest.mark.parametrize(
        ("name", "sql"),
        [
            ("postgres_scan", "SELECT * FROM postgres_scan('{dsn}', 'public', 'x')"),
            ("postgres_attach", "CALL postgres_attach('{dsn}')"),
        ],
    )
    async def test_the_dsn_functions_reach_past_external_access_on_duckdb_1_5_only(
        self, routed: RoutedDuckDbClient, name: str, sql: str
    ) -> None:
        """D-101: on 1.5 the extension's DSN functions open connections external access does
        not stop; DuckDB 2's extension has neither."""

        dsn = "host=127.0.0.1 port=1 dbname=x user=x connect_timeout=2"
        refusal = await _engine_refusal(routed, _A, sql.format(dsn=dsn))

        if _ON_DUCKDB_2:
            assert f"{name} does not exist" in str(refusal)

        else:
            assert "Unable to connect to Postgres" in str(refusal)

    async def test_the_unnamed_secret_does_not_ride_along_another_dsn(
        self, routed: RoutedDuckDbClient, stack: DuckLakeStack
    ) -> None:
        """D-101: on 1.5 a DSN without a password reaches the metadata database unauthenticated:
        the unnamed secret is not merged into it."""

        a = stack.tenants["a"]

        if _ON_DUCKDB_2:
            pytest.skip("DuckDB 2's postgres extension has no DSN functions")

        dsn = f"host={stack.host} port={stack.port} dbname={DATABASE} user={a.reader}"
        refusal = await _engine_refusal(
            routed, _A, f"SELECT * FROM postgres_scan('{dsn}', '{a.schema}', 'ducklake_table')"
        )

        assert "no password supplied" in str(refusal)

    async def test_a_statement_can_query_the_metadata_database_as_the_role(
        self, routed: RoutedDuckDbClient, stack: DuckLakeStack
    ) -> None:
        """D-102: the role's grants, which the attestation reads, are the bound."""

        rows = await _rows(
            routed,
            _A,
            "SELECT * FROM postgres_query('__ducklake_metadata_lake', 'SELECT current_user AS u')",
        )

        assert rows == [{"u": stack.tenants["a"].reader}]


def _tenant_of(stack: DuckLakeStack, tenant: UUID) -> DuckLakeTenant:
    return stack.tenants[_NAMES[tenant]]
