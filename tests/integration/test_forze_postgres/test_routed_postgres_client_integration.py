"""Integration tests for :class:`~forze_postgres.kernel.platform.RoutedPostgresClient`."""

from __future__ import annotations

from collections.abc import Callable
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest

pytest.importorskip("psycopg")

from forze.application.contracts.secrets import SecretRef
from forze.base.errors import InfrastructureError, SecretNotFoundError

from forze_postgres.kernel.platform import PostgresClient, PostgresConfig, RoutedPostgresClient


def _ref(tid: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tid}/dsn")


def _normalize_postgres_url(url: str) -> str:
    if url.startswith("postgresql+psycopg://"):
        return url.replace("postgresql+psycopg://", "postgresql://")
    return url


class _MemSecretsByPath:
    """Maps secret paths to DSN strings."""

    def __init__(
        self,
        paths: dict[str, str],
        *,
        missing_path: str | None = None,
        broken_path: str | None = None,
    ) -> None:
        self._paths = paths
        self._missing_path = missing_path
        self._broken_path = broken_path

    async def resolve_str(self, ref: SecretRef) -> str:
        if self._broken_path is not None and ref.path == self._broken_path:
            raise RuntimeError("vault unavailable")
        if self._missing_path is not None and ref.path == self._missing_path:
            raise SecretNotFoundError(
                f"No secret for {ref.path!r}",
                details={"ref": ref.path},
            )
        try:
            return self._paths[ref.path]
        except KeyError as e:
            raise SecretNotFoundError(
                f"No secret for {ref.path!r}",
                details={"ref": ref.path},
            ) from e

    async def exists(self, ref: SecretRef) -> bool:
        return ref.path in self._paths


class _MemSecretsCallableTenant(_MemSecretsByPath):
    """Looks up tenants/*/dsn paths (callable-style refs)."""

    def __init__(
        self,
        dsns_by_tenant: dict[UUID, str],
        *,
        missing_tenant: UUID | None = None,
        broken_tenant: UUID | None = None,
    ) -> None:
        paths = {
            f"tenants/{tid}/dsn": dsn for tid, dsn in dsns_by_tenant.items()
        }
        missing_path = (
            f"tenants/{missing_tenant}/dsn" if missing_tenant is not None else None
        )
        broken_path = (
            f"tenants/{broken_tenant}/dsn" if broken_tenant is not None else None
        )
        super().__init__(paths, missing_path=missing_path, broken_path=broken_path)


def _tenant_holder() -> tuple[Callable[[], UUID | None], Callable[[UUID | None], None]]:
    slot: list[UUID | None] = [None]

    def getter() -> UUID | None:
        return slot[0]

    def setter(value: UUID | None) -> None:
        slot[0] = value

    return getter, setter


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_postgres_sql_execute_fetch_mapping_refs(postgres_container) -> None:
    url = _normalize_postgres_url(postgres_container.get_connection_url())
    t1 = uuid4()
    ref_custom = SecretRef(path=f"routed/pg/it/{uuid4().hex}")
    secrets = _MemSecretsByPath({ref_custom.path: url})
    tenant_get, tenant_set = _tenant_holder()

    cfg = PostgresConfig(min_size=1, max_size=5, max_concurrent_queries=6)
    routed = RoutedPostgresClient(
        secrets=secrets,
        secret_ref_for_tenant={t1: ref_custom},
        tenant_provider=tenant_get,
        pool_config=cfg,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        assert routed.query_concurrency_limit() == 6

        tbl = f"routed_pg_{uuid4().hex[:10]}"
        async with routed.transaction():
            await routed.execute(
                f"CREATE TEMP TABLE {tbl} (id int PRIMARY KEY, msg text)",
            )
            ins_rc = await routed.execute(
                f"INSERT INTO {tbl} VALUES (1, 'a')",
                return_rowcount=True,
            )
            assert ins_rc == 1

            await routed.execute(f"INSERT INTO {tbl} VALUES (2, 'b')")
            await routed.execute_many(
                f"INSERT INTO {tbl} VALUES (%s, %s)",
                [(3, "c"), (4, "d")],
            )

            rows_d = await routed.fetch_all(
                f"SELECT id, msg FROM {tbl} ORDER BY id",
                row_factory="dict",
            )
            assert len(rows_d) == 4
            assert rows_d[0]["msg"] == "a"

            rows_t = await routed.fetch_all(
                f"SELECT id FROM {tbl} ORDER BY id",
                row_factory="tuple",
            )
            assert rows_t[0] == (1,)

            one_d = await routed.fetch_one(
                f"SELECT id, msg FROM {tbl} WHERE id = %s",
                (2,),
                row_factory="dict",
            )
            assert one_d is not None and one_d["msg"] == "b"

            one_t = await routed.fetch_one(
                f"SELECT id, msg FROM {tbl} WHERE id = %s",
                (3,),
                row_factory="tuple",
            )
            assert one_t == (3, "c")

            val = await routed.fetch_value(
                f"SELECT msg FROM {tbl} WHERE id = %s",
                (4,),
            )
            assert val == "d"

            await routed.execute(f"DROP TABLE {tbl}")
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_postgres_query_concurrency_branches(postgres_container) -> None:
    url = _normalize_postgres_url(postgres_container.get_connection_url())
    t1 = uuid4()
    secrets = _MemSecretsCallableTenant({t1: url})
    tenant_get, tenant_set = _tenant_holder()

    cfg = PostgresConfig(min_size=1, max_size=7, pool_headroom=2)
    routed = RoutedPostgresClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        pool_config=cfg,
        max_cached_tenants=4,
    )
    await routed.startup()
    try:
        expected_fallback = max(1, cfg.max_size - cfg.pool_headroom)

        tenant_set(None)
        assert routed.query_concurrency_limit() == expected_fallback

        tenant_set(t1)
        assert routed.query_concurrency_limit() == expected_fallback

        await routed.fetch_one("SELECT 1")

        assert routed.query_concurrency_limit() == expected_fallback
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_postgres_transaction_bound_connection_require_tx(postgres_container) -> None:
    url = _normalize_postgres_url(postgres_container.get_connection_url())
    t1 = uuid4()
    secrets = _MemSecretsCallableTenant({t1: url})
    tenant_get, tenant_set = _tenant_holder()

    routed = RoutedPostgresClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        pool_config=PostgresConfig(min_size=1, max_size=5),
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        tenant_set(None)
        assert routed.is_in_transaction() is False

        tenant_set(t1)
        assert routed.is_in_transaction() is False

        await routed.fetch_one("SELECT 1")
        assert routed.is_in_transaction() is False

        with pytest.raises(InfrastructureError, match="Transactional context"):
            routed.require_transaction()

        async with routed.transaction():
            assert routed.is_in_transaction() is True
            routed.require_transaction()
            assert await routed.fetch_value("SELECT 2") == 2

        assert routed.is_in_transaction() is False

        async with routed.bound_connection():
            assert await routed.fetch_value("SELECT 3") == 3
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_postgres_lru_eviction(postgres_container) -> None:
    url = _normalize_postgres_url(postgres_container.get_connection_url())
    t1, t2, t3 = uuid4(), uuid4(), uuid4()
    secrets = _MemSecretsCallableTenant({t1: url, t2: url, t3: url})
    tenant_get, tenant_set = _tenant_holder()

    routed = RoutedPostgresClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        pool_config=PostgresConfig(min_size=1, max_size=5),
        max_cached_tenants=2,
    )
    await routed.startup()
    closes: list[int] = []
    real_close = PostgresClient.close

    async def counting_close(self: PostgresClient) -> None:
        closes.append(1)
        await real_close(self)

    try:
        with patch.object(PostgresClient, "close", counting_close):
            tenant_set(t1)
            assert await routed.fetch_value("SELECT 1") == 1
            tenant_set(t2)
            assert await routed.fetch_value("SELECT 2") == 2
            tenant_set(t1)
            assert await routed.fetch_value("SELECT 11") == 11
            tenant_set(t3)
            assert await routed.fetch_value("SELECT 3") == 3
            assert sum(closes) == 1

        tenant_set(t2)
        assert await routed.fetch_value("SELECT 22") == 22
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_postgres_require_transaction_without_context(postgres_container) -> None:
    url = _normalize_postgres_url(postgres_container.get_connection_url())
    t1 = uuid4()
    secrets = _MemSecretsCallableTenant({t1: url})
    tenant_get, tenant_set = _tenant_holder()

    routed = RoutedPostgresClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        pool_config=PostgresConfig(min_size=1, max_size=5),
        max_cached_tenants=4,
    )
    await routed.startup()
    try:
        tenant_set(None)
        with pytest.raises(InfrastructureError, match="Transactional context"):
            routed.require_transaction()

        tenant_set(t1)
        await routed.fetch_one("SELECT 1")
        await routed.evict_tenant(t1)

        with pytest.raises(InfrastructureError, match="Transactional context"):
            routed.require_transaction()
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_postgres_evict_unknown_and_secret_errors(postgres_container) -> None:
    url = _normalize_postgres_url(postgres_container.get_connection_url())
    t_ok, t_miss, t_break = uuid4(), uuid4(), uuid4()

    tenant_get, tenant_set = _tenant_holder()

    routed_miss = RoutedPostgresClient(
        secrets=_MemSecretsCallableTenant({t_ok: url}, missing_tenant=t_miss),
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    await routed_miss.startup()
    try:
        tenant_set(t_miss)
        with pytest.raises(SecretNotFoundError):
            await routed_miss.fetch_one("SELECT 1")
    finally:
        await routed_miss.close()

    routed_break = RoutedPostgresClient(
        secrets=_MemSecretsCallableTenant({t_ok: url}, broken_tenant=t_break),
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    await routed_break.startup()
    try:
        tenant_set(t_break)
        with pytest.raises(InfrastructureError, match="Failed to resolve database secret"):
            await routed_break.fetch_one("SELECT 1")
    finally:
        await routed_break.close()

    routed_ok = RoutedPostgresClient(
        secrets=_MemSecretsCallableTenant({t_ok: url}),
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t_ok)
    await routed_ok.startup()
    try:
        await routed_ok.fetch_one("SELECT 1")
        await routed_ok.evict_tenant(t_ok)
        await routed_ok.evict_tenant(uuid4())
        assert await routed_ok.fetch_value("SELECT 5") == 5
    finally:
        await routed_ok.close()
