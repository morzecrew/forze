"""RFC 0078 P1 acceptance battery: routed DuckDB lake reads against a real vending stack.

Every statement here goes through the production path: ``DuckDbDynamicReadAdapter`` →
``RoutedDuckDbClient`` → a per-tenant instance built by the §3.4 sequence and attested, attached
to the tenant's Lakekeeper warehouse (``tests/support/lakekeeper.py``). The numbered classes are
the items of RFC 0078 §7. Item 2 is the unit suite's (``test_duckdb_dynamic_read_wiring.py``),
item 11 is ``test_lake_credential_expiry.py``'s, and item 17 stays deferred (EXECUTION-LOG
D-059). The module runs on DuckDB 1.5 and 2; where the two differ, a test says which is which.

Two seams make the belt-and-braces items possible without a second code path:
``_stub_classification`` swaps the client's parser check for "run the first statement", and
``_fault`` rewrites the hardening sequence the routed client runs. Both are monkeypatches of
module-level helpers, so each leg runs the shipped code with exactly one layer taken away.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import os
import re
import shutil
import threading
import time
from collections.abc import AsyncIterator, Callable, Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import MagicMock
from uuid import UUID

import certifi
import duckdb
import pytest

from forze.application.contracts.dynamic_read import DynamicReadPort, DynamicReadSpec
from forze.application.contracts.secrets import SecretRef
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.dynamic_read import (
    MULTI_STATEMENT_CODE,
    PERMISSION_DENIED_CODE,
    ROW_CAP_EXCEEDED_CODE,
    STATEMENT_INVALID_CODE,
    STATEMENT_TOO_LARGE_CODE,
    TIMEOUT_CODE,
    WRITE_REFUSED_CODE,
)
from forze.base.exceptions import CoreException
from forze_duckdb import (
    DuckDbConfig,
    DuckDbDynamicReadConfig,
    LakeTenantCatalog,
    RoutedDuckDbClient,
)
from forze_duckdb.adapters import DuckDbDynamicReadAdapter
from forze_duckdb.kernel.client import client as client_module
from forze_duckdb.kernel.client import routed_client
from tests.integration.test_forze_duckdb.test_lake_vending_probes import (
    TestFixtureSelfTest,  # noqa: F401  # battery 1 re-runs first, against this module's stack
)
from tests.support.lakekeeper import (
    BUCKET,
    OAUTH2_SCOPE,
    LakePrincipal,
    LakeTenant,
    LakeVendingStack,
    lake_vending_stack,
)

pytestmark = pytest.mark.integration

_A = UUID("aaaaaaaa-0000-0000-0000-000000000001")
_B = UUID("bbbbbbbb-0000-0000-0000-000000000002")
_C = UUID("cccccccc-0000-0000-0000-000000000003")
"""Tenant C holds the third, ungranted principal and points it at tenant A's warehouse."""

_ROUTE = "lake_widgets"
_SETUP_FAILED = "duckdb_session_setup_failed"
_ORDERS = "SELECT id, tenant FROM gold.orders ORDER BY id"
_SPIN = "SELECT count(*) AS n FROM range(20000000000) t(i) WHERE i % 7 = 3"
"""CPU-bound and long: stops only on interrupt."""
_ALLOWLIST_REFUSAL = "file system operations are disabled by configuration"
_ON_DUCKDB_2 = int(duckdb.__version__.split(".", 1)[0]) >= 2

_tenant: contextvars.ContextVar[UUID | None] = contextvars.ContextVar("tenant", default=None)

# ----------------------- #


@pytest.fixture(scope="module")
def lake(docker_available: None) -> Iterator[LakeVendingStack]:
    with lake_vending_stack() as stack:
        yield stack


@pytest.fixture
def tenant_a(lake: LakeVendingStack) -> LakeTenant:
    return lake.tenants["tenant-a"]


@pytest.fixture
def tenant_b(lake: LakeVendingStack) -> LakeTenant:
    return lake.tenants["tenant-b"]


class _Secrets:
    """The secrets backend: each tenant's principal, taken from the stack."""

    def __init__(self, principals: dict[UUID, LakePrincipal]) -> None:
        self.principals = principals

    async def resolve_str(self, ref: SecretRef) -> str:
        p = self.principals[UUID(ref.path)]
        return json.dumps({"client_id": p.client_id, "client_secret": p.client_secret})

    async def exists(self, ref: SecretRef) -> bool:
        return UUID(ref.path) in self.principals


def _identity() -> TenantIdentity | None:
    tenant = _tenant.get()
    return None if tenant is None else TenantIdentity(tenant_id=tenant)


@pytest.fixture
def secrets(lake: LakeVendingStack) -> _Secrets:
    return _Secrets(
        {
            _A: lake.tenants["tenant-a"].principal,
            _B: lake.tenants["tenant-b"].principal,
            _C: lake.ungranted,
        }
    )


@pytest.fixture
def build_routed(
    lake: LakeVendingStack, secrets: _Secrets
) -> Iterator[Callable[..., RoutedDuckDbClient]]:
    warehouses = {_A: lake.tenants["tenant-a"], _B: lake.tenants["tenant-b"]}
    warehouses[_C] = warehouses[_A]

    def catalog(tenant: UUID) -> LakeTenantCatalog:
        t = warehouses[tenant]
        return LakeTenantCatalog(
            uri=lake.catalog_uri,
            warehouse=t.warehouse,
            oauth2_server_uri=lake.token_uri,
            oauth2_scope=OAUTH2_SCOPE,
            storage_prefix=f"s3://{BUCKET}/{t.key_prefix}/",
        )

    def build(**kwargs: Any) -> RoutedDuckDbClient:
        return RoutedDuckDbClient(
            secrets=secrets,
            secret_ref_for_tenant=lambda tenant: SecretRef(path=str(tenant)),
            tenant_provider=_tenant.get,
            catalog_for_tenant=catalog,
            **kwargs,
        )

    yield build


@pytest.fixture
async def routed(
    build_routed: Callable[..., RoutedDuckDbClient],
) -> AsyncIterator[RoutedDuckDbClient]:
    client = build_routed()
    await client.startup()

    try:
        yield client

    finally:
        await client.close()


def _port(
    client: RoutedDuckDbClient,
    *,
    row_cap: int = 100,
    timeout: timedelta = timedelta(seconds=5),
    namespace: str = "gold",
    **spec: Any,
) -> DynamicReadPort:
    config = DuckDbDynamicReadConfig(
        provenance="trusted", namespace=namespace, statement_timeout=timeout
    )
    return DuckDbDynamicReadAdapter(
        client=client,
        spec=DynamicReadSpec(name=_ROUTE, row_cap=row_cap, **spec),
        config=config,
        statement_timeout=config.statement_timeout,
        tenant_provider=_identity,
    )


async def _run(port: DynamicReadPort, tenant: UUID, sql: str, **options: Any) -> Any:
    _tenant.set(tenant)
    return await port.run(sql, options=options or None)


async def _refused(port: DynamicReadPort, tenant: UUID, sql: str) -> CoreException:
    with pytest.raises(CoreException) as ei:
        await _run(port, tenant, sql)

    return ei.value


async def _inner_rows(client: RoutedDuckDbClient, tenant: UUID, sql: str) -> list[Any]:
    """Read the tenant's instance directly, below the adapter — for asserting on its state."""

    async with client.client_scope(tenant) as inner:
        return (await inner.run_query(sql)).rows


@pytest.fixture
def stub_classification(monkeypatch: pytest.MonkeyPatch) -> None:
    """Take the parser belt away: the client runs whatever the first statement is."""

    monkeypatch.setattr(
        client_module, "_single_select", lambda cursor, sql: cursor.extract_statements(sql)[0]
    )


def _fault(
    monkeypatch: pytest.MonkeyPatch,
    rewrite: Callable[[list[str]], list[str]],
    *,
    attest: bool = True,
) -> None:
    """Run the hardening sequence through *rewrite*; ``attest=False`` also waives step 8."""

    real = routed_client._session_statements
    monkeypatch.setattr(
        routed_client,
        "_session_statements",
        lambda *args, **kwargs: rewrite(real(*args, **kwargs)),
    )

    if not attest:

        async def waived(client: Any, expected: dict[str, object]) -> list[str]:
            return []

        monkeypatch.setattr(routed_client, "_attest", waived)


def _without(*fragments: str) -> Callable[[list[str]], list[str]]:
    return lambda statements: [s for s in statements if not any(f in s for f in fragments)]


# ----------------------- #


class TestDisjointTenants:
    """Battery 3 — one tenant-neutral plan, two tenants, disjoint rows."""

    async def test_the_same_plan_reads_each_tenants_own_rows(
        self, routed: RoutedDuckDbClient
    ) -> None:
        port = _port(routed)

        a_rows = await _run(port, _A, _ORDERS)
        b_rows = await _run(port, _B, _ORDERS)

        assert {r["tenant"] for r in a_rows} == {"tenant-a"}
        assert {r["tenant"] for r in b_rows} == {"tenant-b"}
        assert len(a_rows) == len(b_rows) == 3

    async def test_a_tenant_placeholder_binds_the_bound_tenant(
        self, routed: RoutedDuckDbClient
    ) -> None:
        rows = await _run(_port(routed), _A, "SELECT $tenant AS t, count(*) AS n FROM orders")

        assert rows == [{"t": str(_A), "n": 3}]

    async def test_a_nested_namespace_is_one_schema_to_use(
        self, routed: RoutedDuckDbClient, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        """``gold.sub`` is one namespace two levels deep, which DuckDB lists as one schema."""

        import pyarrow as pa

        catalog = lake.admin_catalog(tenant_a)
        catalog.create_namespace_if_not_exists(("gold", "sub"))
        data = pa.table({"id": pa.array([7], pa.int64())})
        catalog.create_table(("gold", "sub", "deep"), schema=data.schema).append(data)

        try:
            rows = await _run(_port(routed, namespace="gold.sub"), _A, "SELECT id FROM deep")

            assert rows == [{"id": 7}]

        finally:
            catalog.drop_table(("gold", "sub", "deep"))
            catalog.drop_namespace(("gold", "sub"))


class TestCrossTenantThroughTheCatalog:
    """Battery 4 — tenant A reaching for tenant B through names: a refusal, never rows."""

    async def test_another_catalog_or_namespace_does_not_resolve(
        self, routed: RoutedDuckDbClient
    ) -> None:
        port = _port(routed)

        for sql in (
            "SELECT * FROM wh_tenant_b.gold.orders",
            "SELECT * FROM tenant_b.orders",
            "SELECT * FROM zzz.gold.orders",
            "SELECT * FROM lake.zzz.gold.orders",
            # D-072: a table function would get past the name check.
            "SELECT * FROM query('FROM zzz.gold.orders')",
            "SELECT * FROM query_table('zzz.gold.orders')",
            "SELECT * FROM query_table('wh_tenant_b.gold.orders')",
        ):
            assert (await _refused(port, _A, sql)).code == STATEMENT_INVALID_CODE

    async def test_with_classification_stubbed_an_unknown_first_part_never_reads_b(
        self, routed: RoutedDuckDbClient, stub_classification: None
    ) -> None:
        """D-058, what the refusal above is for: DuckDB 2 reads an unknown first part as a
        nested namespace and resolves its last level in A's own warehouse. Never B's rows."""

        port = _port(routed)

        for sql in (
            "SELECT tenant FROM wh_tenant_b.gold.orders",
            "SELECT tenant FROM zzz.gold.orders",
            "SELECT tenant FROM query('FROM zzz.gold.orders')",
            "SELECT tenant FROM query_table('wh_tenant_b.gold.orders')",
        ):
            if _ON_DUCKDB_2:
                assert {row["tenant"] for row in await _run(port, _A, sql)} == {"tenant-a"}

            else:
                assert (await _refused(port, _A, sql)).code == STATEMENT_INVALID_CODE

    async def test_attaching_the_other_warehouse_is_refused(
        self, routed: RoutedDuckDbClient, tenant_b: LakeTenant, lake: LakeVendingStack
    ) -> None:
        attach = (
            f"ATTACH '{tenant_b.warehouse}' AS b (TYPE iceberg, ENDPOINT '{lake.catalog_uri}', "
            "SECRET lake_cat, ACCESS_DELEGATION_MODE 'vended_credentials')"
        )

        error = await _refused(_port(routed), _A, attach)

        assert error.code == WRITE_REFUSED_CODE
        assert error.details == {"route": _ROUTE, "statement_type": "ATTACH"}

    async def test_with_classification_stubbed_the_catalog_hides_the_warehouse(
        self,
        routed: RoutedDuckDbClient,
        tenant_b: LakeTenant,
        lake: LakeVendingStack,
        stub_classification: None,
    ) -> None:
        attach = (
            f"ATTACH '{tenant_b.warehouse}' AS b (TYPE iceberg, ENDPOINT '{lake.catalog_uri}', "
            "SECRET lake_cat, ACCESS_DELEGATION_MODE 'vended_credentials')"
        )

        error = await _refused(_port(routed), _A, attach)

        assert error.code == PERMISSION_DENIED_CODE
        assert "NotFound_404" in error.details["detail"]


class TestCrossTenantByPath:
    """Battery 5 — B's objects named by path from A's instance, the store's root key ambient."""

    @pytest.fixture(autouse=True)
    def _root_key_in_env(self, lake: LakeVendingStack, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", lake.root_access_key)
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", lake.root_secret_key)
        monkeypatch.setenv("AWS_REGION", "local-01")

    async def test_b_named_by_path_is_refused_before_it_runs(
        self, routed: RoutedDuckDbClient, lake: LakeVendingStack, tenant_b: LakeTenant
    ) -> None:
        """D-072: a dynamic read may not hand the engine a path at all."""

        loaded = lake.load_table(tenant_b.principal, tenant_b.warehouse).json()
        meta, data = loaded["metadata-location"], loaded["metadata"]["location"]
        port = _port(routed)

        for sql, detail in (
            (f"SELECT * FROM iceberg_scan('{meta}')", "table function not allowed: iceberg_scan"),
            (
                f"SELECT * FROM read_parquet('{data}/data/*.parquet')",
                "table function not allowed: read_parquet",
            ),
            (
                f"SELECT * FROM '{data}/data/x.parquet'",
                f"path as a table name: {data}/data/x.parquet",
            ),
        ):
            error = await _refused(port, _A, sql)

            assert error.code == STATEMENT_INVALID_CODE
            assert error.details["detail"] == detail

    async def test_b_is_refused_inside_the_engine_and_nothing_goes_out(
        self,
        build_routed: Callable[..., RoutedDuckDbClient],
        monkeypatch: pytest.MonkeyPatch,
        lake: LakeVendingStack,
        tenant_b: LakeTenant,
        stub_classification: None,
    ) -> None:
        loaded = lake.load_table(tenant_b.principal, tenant_b.warehouse).json()
        meta, data = loaded["metadata-location"], loaded["metadata"]["location"]
        # Logging goes on before the lock: DuckDB 2's lock refuses it after (D-060).
        _fault(monkeypatch, lambda ss: [*ss[:-1], "CALL enable_logging('HTTP')", ss[-1]])
        routed = build_routed()
        await routed.startup()

        try:
            port = _port(routed)
            count = "SELECT count(*) AS n FROM duckdb_logs WHERE type = 'HTTP'"
            await _run(port, _A, _ORDERS)  # the positive control: a permitted read is logged
            (before,) = await _inner_rows(routed, _A, count)
            assert before["n"] > 0

            for sql in (
                f"SELECT * FROM iceberg_scan('{meta}')",
                f"SELECT * FROM read_parquet('{data}/data/*.parquet')",
            ):
                error = await _refused(port, _A, sql)

                assert error.code == PERMISSION_DENIED_CODE
                assert _ALLOWLIST_REFUSAL in error.details["detail"]

            # Not one request more, so none was signed with the root key.
            assert await _inner_rows(routed, _A, count) == [before]

        finally:
            await routed.close()


class TestClassification:
    """Battery 6 — refused before execution; and the engine still refuses with the belt off."""

    @pytest.mark.parametrize(
        ("sql", "statement_type"),
        [
            ("COPY (SELECT 1) TO 'x.csv'", "COPY"),
            ("ATTACH 'local.db' AS l", "ATTACH"),
            ("SET threads=1", "SET"),
            ("INSTALL aws", "LOAD"),
            ("CREATE SECRET s (TYPE s3, PROVIDER config)", "CREATE"),
            ("DROP SECRET lake_cat", "DROP"),
        ],
    )
    async def test_a_non_select_is_refused_with_its_type(
        self, routed: RoutedDuckDbClient, sql: str, statement_type: str
    ) -> None:
        error = await _refused(_port(routed), _A, sql)

        assert error.code == WRITE_REFUSED_CODE
        assert error.details == {"route": _ROUTE, "statement_type": statement_type}
        secrets = await _inner_rows(routed, _A, "SELECT name FROM duckdb_secrets()")
        assert secrets == [{"name": "lake_cat"}]  # neither CREATE nor DROP ran

    async def test_a_multi_statement_string_is_refused(self, routed: RoutedDuckDbClient) -> None:
        error = await _refused(_port(routed), _A, f"{_ORDERS}; {_ORDERS}")

        assert error.code == MULTI_STATEMENT_CODE

    async def test_with_classification_stubbed_a_local_attach_still_fails(
        self, routed: RoutedDuckDbClient, tmp_path: Path, stub_classification: None
    ) -> None:
        target = tmp_path / "local.db"

        error = await _refused(_port(routed), _A, f"ATTACH '{target}' AS l")

        assert error.code == PERMISSION_DENIED_CODE
        assert _ALLOWLIST_REFUSAL in error.details["detail"]
        assert not target.exists()


class TestLocalFilesystem:
    """Battery 7 — refused by the session, then by the process with the session's layer gone."""

    async def test_local_reads_and_writes_are_refused_by_the_session(
        self, routed: RoutedDuckDbClient, tmp_path: Path, stub_classification: None
    ) -> None:
        target = tmp_path / "x.csv"
        port = _port(routed)

        for sql in (
            "SELECT * FROM read_csv('/etc/passwd')",
            "SELECT * FROM read_text('/proc/self/environ')",
            f"COPY (SELECT 1) TO '{target}'",
        ):
            error = await _refused(port, _A, sql)

            assert error.code == PERMISSION_DENIED_CODE
            assert _ALLOWLIST_REFUSAL in error.details["detail"]

        assert not target.exists()

    @pytest.mark.skipif(os.geteuid() == 0, reason="root ignores the directory's permissions")
    async def test_without_the_session_layer_the_process_control_refuses(
        self,
        build_routed: Callable[..., RoutedDuckDbClient],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        stub_classification: None,
    ) -> None:
        # The deployment's control stands in as a directory this process cannot write.
        locked = tmp_path / "locked"
        locked.mkdir()
        locked.chmod(0o500)
        _fault(
            monkeypatch,
            _without("allowed_directories", "allowed_paths", "enable_external_access"),
            attest=False,
        )
        client = build_routed()
        await client.startup()

        try:
            with pytest.raises(CoreException) as ei:
                await _run(_port(client), _A, f"COPY (SELECT 1) TO '{locked}/x.csv'")

            assert "Permission denied" in ei.value.details["error"]
            assert list(locked.iterdir()) == []

        finally:
            await client.close()
            locked.chmod(0o700)


class TestHttpEgress:
    """Battery 8 — fetch-by-URL is refused by classification (D-072), and inside the engine by
    the allowlist with classification stubbed (D-062)."""

    async def test_a_url_is_refused_before_it_runs(
        self, routed: RoutedDuckDbClient, lake: LakeVendingStack
    ) -> None:
        port = _port(routed)

        for sql in (
            "SELECT * FROM read_csv('https://example.com/x.csv')",
            f"SELECT * FROM read_text('{lake.catalog_uri}/v1/config')",
            f"SELECT * FROM '{lake.catalog_uri}/v1/x.csv'",
        ):
            assert (await _refused(port, _A, sql)).code == STATEMENT_INVALID_CODE

    async def test_an_https_read_is_refused(
        self, routed: RoutedDuckDbClient, stub_classification: None
    ) -> None:
        error = await _refused(
            _port(routed), _A, "SELECT * FROM read_csv('https://example.com/x.csv')"
        )

        assert error.code == PERMISSION_DENIED_CODE
        assert _ALLOWLIST_REFUSAL in error.details["detail"]

    async def test_only_the_catalog_tree_and_the_token_endpoint_are_reachable(
        self, routed: RoutedDuckDbClient, lake: LakeVendingStack, stub_classification: None
    ) -> None:
        """D-064: what the allowlist lets the engine open over HTTP, and what it does not."""

        port = _port(routed)
        catalog_origin = lake.catalog_uri.rsplit("/", 1)[0]
        idp_sibling = lake.token_uri.rsplit("/", 1)[0] + "/certs"

        for url in (f"{catalog_origin}/health", idp_sibling):
            error = await _refused(port, _A, f"SELECT * FROM read_text('{url}')")

            assert error.code == PERMISSION_DENIED_CODE
            assert _ALLOWLIST_REFUSAL in error.details["detail"]

        # Inside the tree the request goes out, without the principal's token.
        config = f"SELECT * FROM read_text('{lake.catalog_uri}/v1/config')"
        inside = await _refused(port, _A, config)

        assert inside.code == PERMISSION_DENIED_CODE
        assert "401" in inside.details["detail"]


class TestLock:
    """Battery 9 — the locked session refuses reconfiguration, on the parent and on cursors."""

    # DuckDB 2 quotes the option name twice (D-060), hence the ``"+``.
    _ATTEMPTS: ClassVar[dict[str, str]] = {
        "SET allowed_directories=['/']": (
            'Cannot change configuration option "+allowed_directories"+ - the configuration has '
            "been locked"
        ),
        "SET lock_configuration=false": (
            'Cannot change configuration option "+lock_configuration"+ - the configuration has '
            "been locked"
        ),
        "LOAD aws": "Loading external extensions is disabled through configuration",
        "SELECT * FROM duckdb_secrets(redact=false)": "Displaying unredacted secrets is disabled",
    }

    async def test_each_attempt_is_refused_on_a_cursor(
        self, routed: RoutedDuckDbClient, stub_classification: None
    ) -> None:
        port = _port(routed)

        for sql, refusal in self._ATTEMPTS.items():
            error = await _refused(port, _A, sql)

            assert error.code == PERMISSION_DENIED_CODE
            assert re.search(refusal, error.details["detail"])

    async def test_each_attempt_is_refused_on_the_parent_connection(
        self, routed: RoutedDuckDbClient
    ) -> None:
        await _run(_port(routed), _A, _ORDERS)

        async with routed.client_scope(_A) as inner:
            parent = inner._DuckDbClient__conn  # type: ignore[attr-defined]

            for sql, refusal in self._ATTEMPTS.items():
                with pytest.raises(duckdb.Error, match=refusal):
                    parent.execute(sql).fetchall()


class TestReadOnly:
    """Battery 10 — writes are refused; with the belt and the READ_ONLY attach gone, the
    catalog and the store refuse, which is what makes them the read-only boundary."""

    _WRITES = (
        "INSERT INTO gold.orders VALUES (99, 'x')",
        "UPDATE gold.orders SET tenant = 'x'",
        "DELETE FROM gold.orders",
        "CREATE TABLE gold.t_new AS SELECT 1 AS i",
    )

    async def test_writes_are_refused_by_classification(self, routed: RoutedDuckDbClient) -> None:
        port = _port(routed)

        for sql in self._WRITES:
            assert (await _refused(port, _A, sql)).code == WRITE_REFUSED_CODE

    async def test_with_classification_stubbed_the_read_only_attach_refuses(
        self, routed: RoutedDuckDbClient, stub_classification: None
    ) -> None:
        port = _port(routed)

        for sql in self._WRITES[:3]:
            assert (await _refused(port, _A, sql)).code == WRITE_REFUSED_CODE

        assert len(await _run(port, _A, _ORDERS)) == 3

    async def test_without_read_only_the_store_and_the_catalog_refuse(
        self,
        build_routed: Callable[..., RoutedDuckDbClient],
        monkeypatch: pytest.MonkeyPatch,
        stub_classification: None,
    ) -> None:
        def writable(statements: list[str]) -> list[str]:
            return [s.replace(", READ_ONLY)", ")") for s in statements]

        _fault(monkeypatch, writable, attest=False)
        client = build_routed()
        await client.startup()
        refusals = {
            **dict.fromkeys(self._WRITES[:3], "HTTP (code )?403"),  # the store, on the data file
            "CREATE TABLE gold.t_new (i INT)": "Forbidden_403",  # the catalog
            # CTAS: 1.5 stages locally first; 2 asks the catalog first
            self._WRITES[3]: "NamespaceActionForbidden" if _ON_DUCKDB_2 else _ALLOWLIST_REFUSAL,
        }

        try:
            port = _port(client)

            for sql, by in refusals.items():
                error = await _refused(port, _A, sql)

                assert error.code == PERMISSION_DENIED_CODE
                assert re.search(by, error.details["detail"])

            assert len(await _run(port, _A, _ORDERS)) == 3

        finally:
            await client.close()


class TestShell:
    """Battery 12 — the governance shell on a real lake read."""

    async def test_one_row_over_the_cap_is_refused_not_truncated(
        self, routed: RoutedDuckDbClient
    ) -> None:
        assert len(await _run(_port(routed, row_cap=3), _A, _ORDERS)) == 3

        error = await _refused(_port(routed, row_cap=2), _A, _ORDERS)

        assert error.code == ROW_CAP_EXCEEDED_CODE

    async def test_the_byte_cap_refuses_before_the_engine(
        self, routed: RoutedDuckDbClient
    ) -> None:
        error = await _refused(_port(routed, max_statement_bytes=10), _A, _ORDERS)

        assert error.code == STATEMENT_TOO_LARGE_CODE

    async def test_call_options_clamp_down(self, routed: RoutedDuckDbClient) -> None:
        port = _port(routed, row_cap=10)

        with pytest.raises(CoreException) as ei:
            await _run(port, _A, _ORDERS, row_cap=2)

        assert ei.value.code == ROW_CAP_EXCEEDED_CODE
        assert len(await _run(port, _A, _ORDERS, row_cap=1_000)) == 3

    async def test_a_long_statement_times_out_within_its_bound(
        self, routed: RoutedDuckDbClient
    ) -> None:
        timeout = timedelta(milliseconds=500)
        port = _port(routed, timeout=timeout)
        await _run(port, _A, _ORDERS)  # build the instance outside the clock
        started = time.monotonic()

        error = await _refused(port, _A, _SPIN)

        assert error.code == TIMEOUT_CODE
        bound = timeout + routed.http_timeout
        assert time.monotonic() - started < bound.total_seconds()

    async def test_the_instance_keeps_no_vended_secret_after_a_statement(
        self, routed: RoutedDuckDbClient
    ) -> None:
        port = _port(routed)

        for _ in range(3):
            await _run(port, _A, _ORDERS)

        secrets = await _inner_rows(routed, _A, "SELECT name FROM duckdb_secrets()")
        assert secrets == [{"name": "lake_cat"}]  # D-025: only the catalog principal

    async def test_an_analytics_read_keeps_no_vended_secret_either(
        self, routed: RoutedDuckDbClient
    ) -> None:
        _tenant.set(_A)
        await routed.run_query("SELECT count(*) AS n FROM lake.gold.orders")

        secrets = await _inner_rows(routed, _A, "SELECT name FROM duckdb_secrets()")
        assert secrets == [{"name": "lake_cat"}]  # D-047: kept, it fails reads once expired


class TestPool:
    """Battery 13 — eviction, rotation, the D-026 rebuild rule, the process-wide bound."""

    async def test_lru_eviction_lets_a_running_statement_finish(
        self, build_routed: Callable[..., RoutedDuckDbClient]
    ) -> None:
        client = build_routed(max_cached_tenants=1)
        await client.startup()

        try:
            port = _port(client, timeout=timedelta(seconds=30))
            await _run(port, _A, _ORDERS)
            slow = "SELECT count(*) AS n FROM range(400000000) t(i) WHERE i % 7 = 3"
            running = asyncio.create_task(_run(port, _A, slow))
            await asyncio.sleep(0.1)

            assert len(await _run(port, _B, _ORDERS)) == 3  # evicts A mid-statement
            assert (await running)[0]["n"] > 0
            assert client.cached_tenant_ids() == (_B,)

        finally:
            await client.close()

    async def test_evict_tenant_rebuilds_from_the_rotated_secret(
        self, routed: RoutedDuckDbClient, secrets: _Secrets, tenant_a: LakeTenant
    ) -> None:
        port = _port(routed)
        assert len(await _run(port, _A, _ORDERS)) == 3
        good = secrets.principals[_A]
        secrets.principals[_A] = LakePrincipal(good.client_id, good.client_secret[::-1], "")

        try:
            assert len(await _run(port, _A, _ORDERS)) == 3  # no signal yet: the cached instance

            await routed.evict_tenant(_A)
            error = await _refused(port, _A, _ORDERS)

            assert error.code == _SETUP_FAILED  # rebuilt, with the rotated secret
            assert "Unauthorized_401" in error.details["error"]

        finally:
            secrets.principals[_A] = good

    async def test_a_restored_grant_needs_an_evict_to_be_seen(
        self, routed: RoutedDuckDbClient, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        port = _port(routed)
        assert len(await _run(port, _A, _ORDERS)) == 3
        lake.set_grant(tenant_a, tenant_a.principal, granted=False)

        try:
            assert (await _refused(port, _A, _ORDERS)).code == STATEMENT_INVALID_CODE

        finally:
            lake.set_grant(tenant_a, tenant_a.principal, granted=True)

        # D-026: the namespace stays missing for this instance's life...
        assert (await _refused(port, _A, _ORDERS)).code == STATEMENT_INVALID_CODE

        await routed.evict_tenant(_A)
        assert len(await _run(port, _A, _ORDERS)) == 3  # ...and a rebuilt one sees the grant

    async def test_a_failed_drop_retires_the_instance_and_the_next_call_rebuilds(
        self, routed: RoutedDuckDbClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """D-049 end to end: the read keeps its rows, and no later call runs on the instance."""

        port = _port(routed)
        await _run(port, _A, _ORDERS)
        first = routed._peek_client(_A)
        real_drop = client_module._drop_vended_secrets

        def failing_drop(conn: Any, lock: Any) -> None:
            raise RuntimeError("drop failed")

        monkeypatch.setattr(client_module, "_drop_vended_secrets", failing_drop)

        assert len(await _run(port, _A, _ORDERS)) == 3

        assert first is not None and first.retired
        assert routed._peek_client(_A) is None  # evicted as its scope ended
        monkeypatch.setattr(client_module, "_drop_vended_secrets", real_drop)

        assert len(await _run(port, _A, _ORDERS)) == 3
        second = routed._peek_client(_A)
        assert second is not None and second is not first and not second.retired
        assert routed.pool_stats().created == 2

    async def test_a_late_worker_retire_is_evicted_by_the_next_call(
        self, routed: RoutedDuckDbClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """D-049's entry check: a timed-out read's worker retires the instance after its scope."""

        await _run(_port(routed), _A, _ORDERS)
        first = routed._peek_client(_A)
        real_drop = client_module._drop_vended_secrets
        release, retired = threading.Event(), threading.Event()

        def late_failing_drop(conn: Any, lock: Any) -> None:
            release.wait(10)  # held until the timed-out caller has left its scope
            raise RuntimeError("drop failed")

        log = MagicMock()
        log.warning.side_effect = lambda *args, **kwargs: retired.set()  # logged once retired
        monkeypatch.setattr(client_module, "logger", log)
        monkeypatch.setattr(client_module, "_drop_vended_secrets", late_failing_drop)
        timed = _port(routed, timeout=timedelta(seconds=0.5))

        assert (await _refused(timed, _A, _SPIN)).code == TIMEOUT_CODE
        assert first is not None and not first.retired
        assert routed._peek_client(_A) is first  # its scope ended with nothing to evict

        release.set()

        assert await asyncio.to_thread(retired.wait, 10)
        assert first.retired
        monkeypatch.setattr(client_module, "_drop_vended_secrets", real_drop)

        assert len(await _run(_port(routed), _A, _ORDERS)) == 3
        assert routed._peek_client(_A) is not first
        assert routed.pool_stats().created == 2

    async def test_the_process_wide_semaphore_bounds_statements_across_tenants(
        self, build_routed: Callable[..., RoutedDuckDbClient]
    ) -> None:
        client = build_routed(max_concurrent_queries_total=1)
        await client.startup()

        try:
            port = _port(client, timeout=timedelta(seconds=30))
            await _run(port, _A, _ORDERS)
            await _run(port, _B, _ORDERS)
            slow = "SELECT count(*) AS n FROM range(400000000) t(i) WHERE i % 7 = 3"
            running = asyncio.create_task(_run(port, _A, slow))
            await asyncio.sleep(0.1)
            waiting = asyncio.create_task(_run(port, _B, _ORDERS))
            await asyncio.sleep(0.1)

            assert not waiting.done()  # B queues behind A's slot, though B's instance is idle

            await running
            assert len(await waiting) == 3

        finally:
            await client.close()


class TestAttestation:
    """Battery 14 — a session missing any one step refuses to serve.

    Every step of the sequence is removed or undone once: each `SET`, the `READ_ONLY` attach,
    the `GLOBAL` scope, each connect option, the extension directory. Removing `ATTACH` or
    adding a secret or a database fails the attestation. Removing a `LOAD` or the catalog
    secret makes a later step fail outright, so no session exists to attest.
    """

    @pytest.mark.parametrize(
        ("rewrite", "problems"),
        [
            (
                _without("enable_global_s3_configuration"),
                ["setting enable_global_s3_configuration"],
            ),
            (_without("s3_access_key_id"), ["setting s3_access_key_id"]),
            (_without("s3_secret_access_key"), ["setting s3_secret_access_key"]),
            (_without("s3_session_token"), ["setting s3_session_token"]),
            (_without("enable_external_access"), ["setting enable_external_access",
                                                  "path /forze-attest not refused",
                                                  "path http://forze-attest.invalid/ not refused"]),
            (_without("http_timeout"), ["setting http_timeout"]),
            # Off by default, so removing the step changes nothing; turning it on does.
            (lambda ss: [s.replace("metadata_cache=false", "metadata_cache=true") for s in ss],
             ["setting enable_http_metadata_cache"]),
            (_without("enable_external_file_cache"), ["setting enable_external_file_cache"]),
            (_without("lock_configuration"), ["setting lock_configuration"]),
            (lambda ss: [s.replace(", READ_ONLY)", ")") for s in ss], ["lake attached writable"]),
            (lambda ss: [s.replace("SET GLOBAL s3_", "SET s3_") for s in ss],
             ["setting s3_access_key_id", "setting s3_secret_access_key",
              "setting s3_session_token"]),
            (lambda ss: [s.replace("SET GLOBAL enable_global", "SET enable_global") for s in ss],
             ["setting enable_global_s3_configuration"]),
            (lambda ss: [*ss[:-1], "CREATE SECRET x (TYPE s3, PROVIDER config)", ss[-1]],
             ["secret type s3"]),
            (lambda ss: [*ss[:-1], "ATTACH ':memory:' AS extra", ss[-1]],
             ["databases ['extra', 'lake', 'memory', 'system', 'temp']"]),
            (_without("ATTACH"), ["databases ['memory', 'system', 'temp']"]),
        ],
        ids=[
            "global_s3",
            "key_id",
            "secret_key",
            "session_token",
            "external_access",
            "http_timeout",
            "metadata_cache",
            "file_cache",
            "lock",
            "writable_attach",
            "session_scoped_s3_keys",
            "session_scoped_global_s3",
            "extra_secret",
            "extra_database",
            "no_attach",
        ],
    )
    async def test_a_session_missing_a_step_is_refused(
        self,
        build_routed: Callable[..., RoutedDuckDbClient],
        monkeypatch: pytest.MonkeyPatch,
        rewrite: Callable[[list[str]], list[str]],
        problems: list[str],
    ) -> None:
        _fault(monkeypatch, rewrite)
        client = build_routed()
        await client.startup()

        try:
            error = await _refused(_port(client), _A, _ORDERS)

            assert error.code == "duckdb_session_attestation_failed"
            assert error.details["problems"] == problems

        finally:
            await client.close()

    @pytest.mark.parametrize("step", ["allowed_directories", "allowed_paths"])
    async def test_a_session_missing_an_allowlist_step_is_refused(
        self,
        build_routed: Callable[..., RoutedDuckDbClient],
        monkeypatch: pytest.MonkeyPatch,
        step: str,
    ) -> None:
        """Without its allowlist entry, DuckDB 2 refuses the catalog's or the IdP's own request,
        so the session never builds (D-055); DuckDB 1.5 builds it and attestation refuses it."""

        _fault(monkeypatch, _without(step))
        client = build_routed()
        await client.startup()

        try:
            error = await _refused(_port(client), _A, _ORDERS)

            if _ON_DUCKDB_2:
                assert error.code == _SETUP_FAILED
                assert _ALLOWLIST_REFUSAL in error.details["error"]

            else:
                assert error.code == "duckdb_session_attestation_failed"
                assert error.details["problems"] == [f"setting {step}"]

        finally:
            await client.close()

    @pytest.mark.parametrize("dropped", [False, True], ids=["shipped", "dropped"])
    async def test_the_ca_bundle_is_allow_listed_and_attested(
        self,
        build_routed: Callable[..., RoutedDuckDbClient],
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        dropped: bool,
    ) -> None:
        """D-063 on this HTTP-only stack: the bundle is set, allow-listed and read back. The P0
        module's ``test_on_2_https_needs_the_ca_bundle_on_the_allowlist`` covers HTTPS. It must
        be a real bundle: once set, DuckDB 2 hands it to curl for every request, plain HTTP too."""

        bundle = tmp_path / "ca.pem"
        shutil.copyfile(certifi.where(), bundle)

        if dropped:
            _fault(monkeypatch, _without("SET GLOBAL ca_cert_file"))

        client = build_routed(ca_cert_file=str(bundle))
        await client.startup()

        try:
            if not dropped:
                assert len(await _run(_port(client), _A, _ORDERS)) == 3
                return

            error = await _refused(_port(client), _A, _ORDERS)

            assert error.code == "duckdb_session_attestation_failed"
            assert error.details["problems"] == ["setting ca_cert_file"]

        finally:
            await client.close()

    @pytest.mark.parametrize(
        ("break_it", "fragment"),
        [
            (
                lambda m: m.setattr(routed_client, "_EXTENSIONS", ("httpfs",)),
                "Secret type 'iceberg' does not exist",
            ),
            (
                lambda m: m.setattr(routed_client, "_EXTENSIONS", ("iceberg",)),
                'Setting with name "enable_global_s3_configuration" is not in the catalog',
            ),
            (
                lambda m: _fault(m, _without("CREATE SECRET")),
                "No ICEBERG secret by the name of 'lake_cat' could be found",
            ),
        ],
        ids=["no_load_iceberg", "no_load_httpfs", "no_catalog_secret"],
    )
    async def test_a_session_missing_a_prerequisite_never_builds(
        self,
        build_routed: Callable[..., RoutedDuckDbClient],
        monkeypatch: pytest.MonkeyPatch,
        break_it: Callable[[pytest.MonkeyPatch], None],
        fragment: str,
    ) -> None:
        """Without an extension or the catalog secret a later step fails, so there is no
        session to attest. It is the deployment's fault, never a statement code (D-038)."""

        break_it(monkeypatch)
        client = build_routed()
        await client.startup()

        try:
            error = await _refused(_port(client), _A, _ORDERS)

            assert error.code == _SETUP_FAILED
            assert fragment in error.details["error"]

        finally:
            await client.close()

    @pytest.fixture(scope="class")
    def extension_directory(
        self, lake: LakeVendingStack, tmp_path_factory: pytest.TempPathFactory
    ) -> Path:
        """A copy of the installed extensions, as an image would bake them in."""

        installed = Path.home() / ".duckdb" / "extensions"

        if not installed.is_dir():
            pytest.skip("no installed DuckDB extensions to copy")

        target = tmp_path_factory.mktemp("duckdb") / "extensions"
        shutil.copytree(installed, target)
        return target

    @pytest.mark.parametrize("dropped", [False, True], ids=["shipped", "dropped"])
    async def test_the_extension_directory_is_attested(
        self,
        build_routed: Callable[..., RoutedDuckDbClient],
        monkeypatch: pytest.MonkeyPatch,
        extension_directory: Path,
        dropped: bool,
    ) -> None:
        if dropped:
            real = routed_client._connect_config
            monkeypatch.setattr(
                routed_client,
                "_connect_config",
                lambda directory: {
                    k: v for k, v in real(directory).items() if k != "extension_directory"
                },
            )

        client = build_routed(extension_directory=str(extension_directory))
        await client.startup()

        try:
            if not dropped:
                assert len(await _run(_port(client), _A, _ORDERS)) == 3
                return

            error = await _refused(_port(client), _A, _ORDERS)

            assert error.code == "duckdb_session_attestation_failed"
            assert error.details["problems"] == ["setting extension_directory"]

        finally:
            await client.close()

    @pytest.mark.parametrize(
        "option",
        [
            "autoinstall_known_extensions",
            "autoload_known_extensions",
            "allow_community_extensions",
            "temp_directory",
        ],
    )
    async def test_a_session_missing_a_connect_option_is_refused(
        self,
        build_routed: Callable[..., RoutedDuckDbClient],
        monkeypatch: pytest.MonkeyPatch,
        option: str,
    ) -> None:
        real = routed_client._connect_config
        monkeypatch.setattr(
            routed_client,
            "_connect_config",
            lambda directory: {k: v for k, v in real(directory).items() if k != option},
        )
        client = build_routed()
        await client.startup()

        try:
            error = await _refused(_port(client), _A, _ORDERS)

            assert error.code == "duckdb_session_attestation_failed"
            assert f"setting {option}" in error.details["problems"]

        finally:
            await client.close()

    async def test_without_the_persistent_secrets_option_the_session_never_builds(
        self, build_routed: Callable[..., RoutedDuckDbClient], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Left on, ``CREATE SECRET`` reads the persistent-secret directory, which the
        allowlist already refuses: the session fails before it can attest anything."""

        real = routed_client._connect_config
        monkeypatch.setattr(
            routed_client,
            "_connect_config",
            lambda directory: {**real(directory), "allow_persistent_secrets": True},
        )
        client = build_routed()
        await client.startup()

        try:
            error = await _refused(_port(client), _A, _ORDERS)

            assert error.code == _SETUP_FAILED
            assert "stored_secrets" in error.details["error"]

        finally:
            await client.close()

    async def test_the_shipped_sequence_attests(
        self, build_routed: Callable[..., RoutedDuckDbClient]
    ) -> None:
        client = build_routed(engine=DuckDbConfig(threads=1, memory_limit="512MB"))
        await client.startup()

        try:
            assert len(await _run(_port(client), _A, _ORDERS)) == 3

        finally:
            await client.close()


class TestTaxonomy:
    """Battery 16 — the refusals this battery provokes that the classes above do not."""

    async def test_a_revoked_grant_is_refused_at_the_next_statement(
        self, routed: RoutedDuckDbClient, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        port = _port(routed)
        assert len(await _run(port, _A, _ORDERS)) == 3
        lake.set_grant(tenant_a, tenant_a.principal, granted=False)

        try:
            error = await _refused(port, _A, _ORDERS)

        finally:
            lake.set_grant(tenant_a, tenant_a.principal, granted=True)

        assert error.code == STATEMENT_INVALID_CODE  # Lakekeeper's 404, as a missing schema
        assert 'schema "gold" does not exist' in error.details["detail"]

    async def test_a_principal_without_a_grant_cannot_build_an_instance(
        self, routed: RoutedDuckDbClient
    ) -> None:
        error = await _refused(_port(routed), _C, _ORDERS)

        assert error.code == _SETUP_FAILED  # the tenant's wiring, not its statement
        assert "NotFound_404" in error.details["error"]

    async def test_a_describe_only_principal_gets_no_rows(
        self,
        routed: RoutedDuckDbClient,
        lake: LakeVendingStack,
        tenant_a: LakeTenant,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
        lake.set_grant(tenant_a, lake.ungranted, granted=True, privilege="describe")

        try:
            error = await _refused(_port(routed), _C, _ORDERS)

        finally:
            lake.set_grant(tenant_a, lake.ungranted, granted=False, privilege="describe")

        assert error.code == PERMISSION_DENIED_CODE
        assert "No region was provided via the vended credentials" in error.details["detail"]

    @pytest.mark.parametrize(
        ("sql", "code"),
        [
            ("SELEC 1", STATEMENT_INVALID_CODE),
            ("SELECT nope FROM gold.orders", STATEMENT_INVALID_CODE),
            ("SELECT * FROM gold.nope", STATEMENT_INVALID_CODE),
            ("SELECT tenant::int FROM gold.orders", STATEMENT_INVALID_CODE),
            ("SELECT $missing", STATEMENT_INVALID_CODE),
        ],
    )
    async def test_a_wrong_statement_is_the_callers(
        self, routed: RoutedDuckDbClient, sql: str, code: str
    ) -> None:
        error = await _refused(_port(routed), _A, sql)

        assert error.code == code
        assert "tenant-a" not in repr(error.details)  # a conversion never echoes the value
