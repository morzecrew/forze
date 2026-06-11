"""Additional unit tests for :class:`~forze.base.exceptions.interceptor.ExceptionInterceptor`."""

from __future__ import annotations

import asyncio
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


class TestExceptionInterceptorCancellation:
    @pytest.mark.asyncio
    async def test_coroutine_cancellederror_passes_through(self) -> None:
        interceptor = ExceptionInterceptor(mapper=_mapper())

        @interceptor.coroutine(site="coro")
        async def cancelled() -> None:
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await cancelled()

    @pytest.mark.asyncio
    async def test_timeout_cancellation_passes_through_coroutine(self) -> None:
        interceptor = ExceptionInterceptor(mapper=_mapper())

        @interceptor.coroutine(site="slow")
        async def slow() -> None:
            await asyncio.sleep(10)

        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.01):
                await slow()

    @pytest.mark.asyncio
    async def test_asyncgenerator_cancellederror_at_yield_passes_through(self) -> None:
        interceptor = ExceptionInterceptor(mapper=_mapper())

        @interceptor.asyncgenerator(site="agen")
        async def stream() -> __import__("collections.abc").AsyncGenerator[int, None]:
            yield 1

        agen = stream()
        assert await agen.__anext__() == 1

        with pytest.raises(asyncio.CancelledError):
            await agen.athrow(asyncio.CancelledError())

    def test_contextmanager_cancellederror_passes_through(self) -> None:
        interceptor = ExceptionInterceptor(mapper=_mapper())

        @interceptor.contextmanager(site="cm")
        @contextmanager
        def cancelled() -> __import__("typing").Generator[None, None, None]:
            raise asyncio.CancelledError
            yield

        with pytest.raises(asyncio.CancelledError):
            with cancelled():
                pass

    def test_contextmanager_body_cancellederror_passes_through(self) -> None:
        interceptor = ExceptionInterceptor(mapper=_mapper())

        @interceptor.contextmanager(site="cm")
        @contextmanager
        def scope() -> __import__("typing").Generator[None, None, None]:
            yield

        with pytest.raises(asyncio.CancelledError):
            with scope():
                raise asyncio.CancelledError

    @pytest.mark.asyncio
    async def test_asynccontextmanager_cancellederror_passes_through(self) -> None:
        interceptor = ExceptionInterceptor(mapper=_mapper())

        @interceptor.asynccontextmanager(site="acm")
        @asynccontextmanager
        async def cancelled() -> __import__("collections.abc").AsyncGenerator[
            None, None
        ]:
            raise asyncio.CancelledError
            yield

        with pytest.raises(asyncio.CancelledError):
            async with cancelled():
                pass

    @pytest.mark.asyncio
    async def test_timeout_cancellation_passes_through_asynccontextmanager(
        self,
    ) -> None:
        interceptor = ExceptionInterceptor(mapper=_mapper())

        @interceptor.asynccontextmanager(site="acm")
        @asynccontextmanager
        async def scope() -> __import__("collections.abc").AsyncGenerator[None, None]:
            yield

        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.01):
                async with scope():
                    await asyncio.sleep(10)
