"""Unit tests for :class:`~forze.application.contracts.tenancy.registry.TenantClientRegistry`."""

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


async def _async_return[T](value: T) -> T:
    return value
