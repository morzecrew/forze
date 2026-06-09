"""Tests for per-tenant :class:`~forze_mock.tenancy.MockRoutedStateRegistry`."""

from uuid import UUID

import pytest

from forze_mock.state import MockState
from forze_mock.tenancy import MockRoutedStateRegistry

# ----------------------- #

_T1 = UUID("00000000-0000-4000-8000-000000000001")
_T2 = UUID("00000000-0000-4000-8000-000000000002")


@pytest.mark.asyncio
async def test_routed_state_isolates_tenant_buckets() -> None:
    registry = MockRoutedStateRegistry(max_entries=4)

    await registry.startup()
    try:
        async with registry.use(_T1) as s1:
            s1.documents["d"] = {}
            s1.documents["d"][_T1] = {"id": str(_T1), "n": "a"}

        async with registry.use(_T2) as s2:
            assert "d" not in s2.documents or _T1 not in s2.documents.get("d", {})

        async with registry.use(_T1) as s1b:
            assert _T1 in s1b.documents["d"]
    finally:
        await registry.close()


@pytest.mark.asyncio
async def test_routed_state_factory_custom() -> None:
    seen: list[MockState] = []

    def factory() -> MockState:
        s = MockState()
        seen.append(s)
        return s

    registry = MockRoutedStateRegistry(max_entries=2, state_factory=factory)
    await registry.startup()
    try:
        async with registry.use(_T1) as _:
            pass
        assert len(seen) == 1
    finally:
        await registry.close()
