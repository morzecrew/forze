"""The in-memory cache honours the shared :class:`CachePort` value contract."""

import pytest

from forze_mock.adapters import MockCacheAdapter
from forze_mock.state import MockState
from tests.support.cache_conformance import assert_value_contract


@pytest.mark.asyncio
async def test_value_contract() -> None:
    await assert_value_contract(MockCacheAdapter(state=MockState(), namespace="ns"))
