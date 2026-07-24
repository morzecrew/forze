"""Unit tests for :class:`~forze_postgres.kernel.client.RoutedPostgresClient`."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from forze.application.contracts.secrets import SecretRef
from forze.base.exceptions import CoreException
from forze_postgres.kernel.client import RoutedPostgresClient

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


class _MemSecrets:
    def __init__(self, paths: dict[UUID, str]) -> None:
        self.paths = paths

    async def resolve_str(self, ref: SecretRef) -> str:
        for tid, dsn in self.paths.items():
            if ref.path == f"tenants/{tid}/dsn":
                return dsn
        raise RuntimeError("missing")

    async def exists(self, ref: SecretRef) -> bool:
        return any(ref.path == f"tenants/{tid}/dsn" for tid in self.paths)


def _ref(tid: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tid}/dsn")


@pytest.mark.asyncio
async def test_routed_requires_startup() -> None:
    secrets = _MemSecrets({_T1: "postgresql://localhost/db1"})
    tenant: UUID | None = None

    routed = RoutedPostgresClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: tenant,
        max_cached_tenants=2,
    )

    tenant = _T1
    with pytest.raises(CoreException, match="not started"):
        await routed.health()


@pytest.mark.asyncio
async def test_routed_eviction_closes_old_pool() -> None:
    secrets = _MemSecrets(
        {
            _T1: "postgresql://localhost/db1",
            _T2: "postgresql://localhost/db2",
        }
    )
    cur: UUID | None = None

    routed = RoutedPostgresClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: cur,
        max_cached_tenants=1,
    )
    await routed.startup()

    instances: list[MagicMock] = []

    def _make_client() -> MagicMock:
        inst = MagicMock()
        inst.initialize = AsyncMock()
        inst.close = AsyncMock()
        inst.health = AsyncMock(return_value=("ok", True))
        instances.append(inst)
        return inst

    with patch(
        "forze_postgres.kernel.client.routed_client.PostgresClient",
        side_effect=_make_client,
    ):
        cur = _T1
        await routed.health()
        cur = _T2
        await routed.health()
        assert instances[0].close.await_count == 1

    await routed.close()
    assert instances[1].close.await_count == 1


@pytest.mark.asyncio
async def test_routed_lru_defers_close_while_tenant_pool_still_in_use() -> None:
    """LRU eviction must not close a pool until the last in-flight operation completes."""
    secrets = _MemSecrets(
        {
            _T1: "postgresql://localhost/db1",
            _T2: "postgresql://localhost/db2",
        }
    )
    cur: UUID | None = None

    routed = RoutedPostgresClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: cur,
        max_cached_tenants=1,
    )
    await routed.startup()

    gate = asyncio.Event()
    instances: list[MagicMock] = []

    def _make_client() -> MagicMock:
        inst = MagicMock()
        inst.initialize = AsyncMock()
        inst.close = AsyncMock()

        if not instances:

            async def slow_health() -> tuple[str, bool]:
                await gate.wait()
                return "ok", True

            inst.health = AsyncMock(side_effect=slow_health)
        else:
            inst.health = AsyncMock(return_value=("ok", True))

        instances.append(inst)
        return inst

    with patch(
        "forze_postgres.kernel.client.routed_client.PostgresClient",
        side_effect=_make_client,
    ):
        cur = _T1
        t1 = asyncio.create_task(routed.health())

        await asyncio.sleep(0.05)
        assert not instances[0].close.called

        cur = _T2
        t2 = asyncio.create_task(routed.health())
        await asyncio.sleep(0.05)

        assert instances[0].close.await_count == 0

        gate.set()
        await asyncio.gather(t1, t2)

        assert instances[0].close.await_count == 1

    await routed.close()
    assert instances[1].close.await_count == 1


def test_routed_postgres_rejects_zero_max_cached_tenants() -> None:
    secrets = _MemSecrets({_T1: "postgresql://localhost/db1"})
    with pytest.raises(CoreException, match="max_entries"):
        RoutedPostgresClient(
            secrets=secrets,
            secret_ref_for_tenant=_ref,
            tenant_provider=lambda: _T1,
            max_cached_tenants=0,
        )


@pytest.mark.asyncio
async def test_routed_dedup_shares_pool_for_same_dsn() -> None:
    shared_dsn = "postgresql://localhost/shared"
    secrets = _MemSecrets({_T1: shared_dsn, _T2: shared_dsn})
    cur: UUID | None = None

    routed = RoutedPostgresClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: cur,
        max_cached_tenants=4,
    )
    await routed.startup()

    instances: list[MagicMock] = []

    def _make_client() -> MagicMock:
        inst = MagicMock()
        inst.initialize = AsyncMock()
        inst.close = AsyncMock()
        inst.health = AsyncMock(return_value=("ok", True))
        instances.append(inst)
        return inst

    with patch(
        "forze_postgres.kernel.client.routed_client.PostgresClient",
        side_effect=_make_client,
    ):
        cur = _T1
        await routed.health()
        cur = _T2
        await routed.health()

    assert len(instances) == 1

    await routed.close()


@pytest.mark.asyncio
async def test_routed_requires_tenant() -> None:
    secrets = _MemSecrets({_T1: "postgresql://localhost/db1"})
    routed = RoutedPostgresClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: None,
        max_cached_tenants=4,
    )
    await routed.startup()
    with pytest.raises(CoreException, match="Tenant ID"):
        await routed.health()


@pytest.mark.asyncio
async def test_routed_scope_methods_delegate_to_the_tenant_client() -> None:
    """Every scoped facade method resolves the tenant client and delegates."""
    from contextlib import asynccontextmanager

    secrets = _MemSecrets({_T1: "postgresql://localhost/db1"})
    routed = RoutedPostgresClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: _T1,
        max_cached_tenants=2,
    )
    await routed.startup()

    inner = MagicMock()
    inner.initialize = AsyncMock()
    inner.close = AsyncMock()
    inner.execute = AsyncMock(return_value=3)
    inner.execute_many = AsyncMock()
    inner.fetch_all = AsyncMock(return_value=[{"a": 1}])
    inner.fetch_one = AsyncMock(return_value={"a": 1})
    inner.fetch_value = AsyncMock(return_value=42)

    @asynccontextmanager
    async def _yield(value):  # type: ignore[no-untyped-def]
        yield value

    inner.bound_connection = lambda: _yield("conn")
    inner.transaction = lambda *, options=None: _yield("tx-conn")
    inner.detached = lambda: _yield(None)

    async def _batched(*args, **kwargs):  # type: ignore[no-untyped-def]
        yield [{"a": 1}]
        yield [{"a": 2}]

    inner.fetch_all_batched = _batched

    with patch(
        "forze_postgres.kernel.client.routed_client.PostgresClient",
        return_value=inner,
    ):
        assert await routed.execute("UPDATE t SET x = 1", return_rowcount=True) == 3
        assert await routed.execute("UPDATE t SET x = 1") is None
        await routed.execute_many("UPDATE t SET x = 1", [{}])
        assert await routed.fetch_all("SELECT 1") == [{"a": 1}]
        assert await routed.fetch_one("SELECT 1") == {"a": 1}
        assert await routed.fetch_value("SELECT 1") == 42

        chunks = [c async for c in routed.fetch_all_batched("SELECT 1", batch_size=10)]
        assert chunks == [[{"a": 1}], [{"a": 2}]]

        async with routed.bound_connection() as conn:
            assert conn == "conn"

        async with routed.transaction() as tx_conn:
            assert tx_conn == "tx-conn"

        async with routed.detached():
            pass

    await routed.close()
