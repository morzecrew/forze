"""Routed-client delegation for the new stream observability calls.

# covers: RoutedRedisClient.xlen / .xinfo_groups
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest

pytest.importorskip("redis")

from forze_redis.kernel.client.routed_client import RoutedRedisClient

# ----------------------- #


class _Inner:
    async def xlen(self, stream: str) -> int:
        return 7

    async def xinfo_groups(self, stream: str) -> list[dict[str, object]]:
        return [{"name": "g", "pending": 0, "lag": 0}]


async def test_xlen_and_xinfo_groups_delegate_to_the_tenant_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inner = _Inner()

    async def _get_client(self: Any) -> Any:
        return inner

    monkeypatch.setattr(RoutedRedisClient, "_get_client", _get_client)

    routed = RoutedRedisClient(
        secrets=None,  # type: ignore[arg-type]  # unused — _get_client is stubbed
        secret_ref_for_tenant={},
        tenant_provider=lambda: UUID(int=1),
    )

    assert await routed.xlen("s") == 7
    assert await routed.xinfo_groups("s") == [{"name": "g", "pending": 0, "lag": 0}]
