"""Unit tests for :class:`~forze.application.contracts.tenancy.registry.TenantClientRegistry`."""

from datetime import timedelta
from uuid import UUID

import pytest

from forze.application.contracts.tenancy import TenantClientRegistry
from forze.base.exceptions import CoreException

# ----------------------- #

_TID = UUID("11111111-1111-1111-1111-111111111111")


@pytest.mark.asyncio
async def test_peek_returns_none_before_create() -> None:
    registry: TenantClientRegistry[str, str] = TenantClientRegistry(
        max_entries=2,
        create=lambda tid: _async_return(f"client-{tid}"),
        dispose=lambda _c: _async_return(None),
        guarded=False,
    )
    await registry.startup()

    assert registry.peek(_TID) is None


@pytest.mark.asyncio
async def test_peek_returns_client_after_get() -> None:
    registry: TenantClientRegistry[str, str] = TenantClientRegistry(
        max_entries=2,
        create=lambda tid: _async_return(f"client-{tid}"),
        dispose=lambda _c: _async_return(None),
        guarded=False,
    )
    await registry.startup()
    registry.set_fingerprint(_TID, "fp")

    created = await registry.get(_TID)

    assert registry.peek(_TID) == created


def test_rejects_zero_max_entries() -> None:
    with pytest.raises(CoreException, match="max_entries"):
        TenantClientRegistry(
            max_entries=0,
            create=lambda tid: _async_return("x"),
            dispose=lambda _c: _async_return(None),
        )


def test_fingerprint_cache_is_bounded_to_max_entries() -> None:
    registry: TenantClientRegistry[str, str] = TenantClientRegistry(
        max_entries=2,
        create=lambda tid: _async_return("x"),
        dispose=lambda _c: _async_return(None),
    )

    tids = [UUID(int=i) for i in range(5)]
    for tid in tids:
        registry.set_fingerprint(tid, f"fp-{tid}")

    # Only the most-recent ``max_entries`` fingerprints survive; the rest are
    # evicted (and would simply be recomputed on the tenant's next access).
    assert registry.get_fingerprint(tids[0]) is None
    assert registry.get_fingerprint(tids[1]) is None
    assert registry.get_fingerprint(tids[2]) is None
    assert registry.get_fingerprint(tids[3]) == f"fp-{tids[3]}"
    assert registry.get_fingerprint(tids[4]) == f"fp-{tids[4]}"


def test_fingerprint_cache_get_is_lru_touch() -> None:
    registry: TenantClientRegistry[str, str] = TenantClientRegistry(
        max_entries=2,
        create=lambda tid: _async_return("x"),
        dispose=lambda _c: _async_return(None),
    )
    a, b, c = UUID(int=1), UUID(int=2), UUID(int=3)

    registry.set_fingerprint(a, "fp-a")
    registry.set_fingerprint(b, "fp-b")

    # Touch ``a`` so it becomes most-recent; ``b`` is now the eviction candidate.
    assert registry.get_fingerprint(a) == "fp-a"
    registry.set_fingerprint(c, "fp-c")

    assert registry.get_fingerprint(b) is None
    assert registry.get_fingerprint(a) == "fp-a"
    assert registry.get_fingerprint(c) == "fp-c"


@pytest.mark.asyncio
async def test_use_works_on_simple_registry() -> None:
    # Regression: ``use()`` used to raise "Use is not supported for simple registry",
    # so a routed client built with the default ``guarded=False`` blew up at runtime
    # on its first scoped access. It now hands out the client unleased instead.
    created: list[UUID] = []

    async def _create(tid: UUID) -> str:
        created.append(tid)
        return f"client-{tid}"

    registry: TenantClientRegistry[str, str] = TenantClientRegistry(
        max_entries=2,
        create=_create,
        dispose=lambda _c: _async_return(None),
        guarded=False,
    )
    await registry.startup()
    registry.set_fingerprint(_TID, "fp")

    async with registry.use(_TID) as client:
        assert client == f"client-{_TID}"

    async with registry.use(_TID) as client:
        assert client == f"client-{_TID}"

    assert created == [_TID]  # one build, reused across scopes


@pytest.mark.asyncio
async def test_use_works_on_guarded_registry() -> None:
    registry: TenantClientRegistry[str, str] = TenantClientRegistry(
        max_entries=2,
        create=lambda tid: _async_return(f"client-{tid}"),
        dispose=lambda _c: _async_return(None),
        guarded=True,
    )
    await registry.startup()
    registry.set_fingerprint(_TID, "fp")

    async with registry.use(_TID) as client:
        assert client == f"client-{_TID}"


@pytest.mark.asyncio
async def test_get_refused_on_guarded_registry_points_to_use() -> None:
    # ``get()`` hands out an unleased reference, which would defeat the guarded
    # registry's drain-after-release contract — the error must say what to call.
    registry: TenantClientRegistry[str, str] = TenantClientRegistry(
        max_entries=2,
        create=lambda tid: _async_return(f"client-{tid}"),
        dispose=lambda _c: _async_return(None),
        guarded=True,
    )
    await registry.startup()
    registry.set_fingerprint(_TID, "fp")

    with pytest.raises(CoreException, match="use\\(\\)"):
        await registry.get(_TID)


def test_is_fingerprint_expired() -> None:
    registry: TenantClientRegistry[str, str] = TenantClientRegistry(
        max_entries=4,
        create=lambda tid: _async_return("x"),
        dispose=lambda _c: _async_return(None),
    )
    tid = UUID(int=1)

    # No fingerprint cached yet -> treated as expired.
    assert registry.is_fingerprint_expired(tid, timedelta(seconds=60)) is True

    registry.set_fingerprint(tid, "fp")

    # Just stamped -> within a generous TTL, but past a negative TTL.
    assert registry.is_fingerprint_expired(tid, timedelta(seconds=60)) is False
    assert registry.is_fingerprint_expired(tid, timedelta(seconds=-1)) is True


async def _async_return[T](value: T) -> T:
    return value
