"""Additional unit tests for :class:`~forze.base.exceptions.interceptor.ExceptionInterceptor`."""

from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager

import pytest

from forze.base.exceptions import (
    ChainExceptionMapper,
    CoreException,
    ExceptionInterceptor,
    exc,
)

# ----------------------- #


def _mapper() -> ChainExceptionMapper:
    return ChainExceptionMapper.chain(
        lambda e, *, site, details: exc.internal(f"{site}:{e}", code="wrapped")
    )


class TestExceptionInterceptorGenerators:
    def test_generator_throw_at_yield_is_mapped(self) -> None:
        interceptor = ExceptionInterceptor(mapper=_mapper())

        @interceptor.generator(site="gen")
        def stream() -> __import__("typing").Generator[int, None, None]:
            yield 1

        gen = stream()
        assert next(gen) == 1

        with pytest.raises(CoreException, match="gen:"):
            gen.throw(ValueError("consumer"))

    @pytest.mark.asyncio
    async def test_asyncgenerator_throw_at_yield_is_mapped(self) -> None:
        interceptor = ExceptionInterceptor(mapper=_mapper())

        @interceptor.asyncgenerator(site="agen")
        async def stream() -> __import__("collections.abc").AsyncGenerator[int, None]:
            yield 1

        agen = stream()
        assert await agen.__anext__() == 1

        with pytest.raises(CoreException, match="agen:"):
            await agen.athrow(ValueError("consumer"))

    @pytest.mark.asyncio
    async def test_asynccontextmanager_intercepts_enter_failure(self) -> None:
        interceptor = ExceptionInterceptor(mapper=_mapper())

        @interceptor.asynccontextmanager(site="acm")
        @asynccontextmanager
        async def broken() -> __import__("collections.abc").AsyncGenerator[None, None]:
            raise ValueError("enter")
            yield

        with pytest.raises(CoreException, match="acm:enter"):
            async with broken():
                pass
