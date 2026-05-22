"""Tests for forze.application.contracts.mapping."""

from __future__ import annotations

from forze.application.contracts.mapping import Mapper
from forze.application.execution import Deps, ExecutionContext


class _StubMapper:
    async def __call__(self, source: int) -> str:
        return str(source * 2)


class TestMapperProtocol:
    def test_stub_satisfies_mapper_protocol(self) -> None:
        mapper: Mapper[int, str] = _StubMapper()
        assert callable(mapper)

    async def test_call_maps_source(self) -> None:
        m = _StubMapper()
        assert await m(3) == "6"
        _ = ExecutionContext(deps=Deps.plain({}))
        assert await m(4) == "8"
