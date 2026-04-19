"""Tests for forze.application.contracts.mapping.ports."""

from __future__ import annotations

from forze.application.contracts.mapping import MapperPort
from forze.application.execution import Deps, ExecutionContext


class _StubMapper:
    async def __call__(self, source: int, /, *, ctx: ExecutionContext | None = None) -> str:
        _ = ctx
        return str(source * 2)


class TestMapperPort:
    def test_runtime_checkable(self) -> None:
        assert isinstance(_StubMapper(), MapperPort)

    async def test_call_with_and_without_ctx(self) -> None:
        m = _StubMapper()
        assert await m(3) == "6"
        ctx = ExecutionContext(deps=Deps.plain({}))
        assert await m(4, ctx=ctx) == "8"
