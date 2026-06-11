"""Additional unit tests for :class:`~forze.base.exceptions.interceptor.ExceptionInterceptor`."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
from typing import Any

import pytest
from pydantic import BaseModel, SecretStr

from forze.base.exceptions import (
    ChainExceptionMapper,
    CoreException,
    ExceptionInterceptor,
    exc,
)

# ----------------------- #


@pytest.fixture
def dump_spy(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Spy on the bound-args sanitizer used for error-context details."""

    import forze.base.scrubbing as scrubbing

    calls: list[Any] = []
    real = scrubbing.dump_bound_args_for_errors

    def spy(bound: Any) -> Any:
        calls.append(bound)
        return real(bound)

    monkeypatch.setattr(scrubbing, "dump_bound_args_for_errors", spy)
    return calls


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


class TestLazySuccessPath:
    """Successful wrapped calls must never materialize error-context details."""

    @pytest.mark.asyncio
    async def test_coroutine_success_no_sanitize(self, dump_spy: list[Any]) -> None:
        interceptor = ExceptionInterceptor(mapper=_mapper())

        @interceptor.coroutine(site="coro")
        async def fetch(query: str, params: dict[str, Any]) -> str:
            return query

        assert await fetch("SELECT 1", {"limit": 10}) == "SELECT 1"
        assert dump_spy == []

    def test_function_success_no_sanitize(self, dump_spy: list[Any]) -> None:
        interceptor = ExceptionInterceptor(mapper=_mapper())

        @interceptor.function(site="fn")
        def fetch(query: str, params: dict[str, Any]) -> str:
            return query

        assert fetch("SELECT 1", {"limit": 10}) == "SELECT 1"
        assert dump_spy == []

    def test_generator_success_no_sanitize(self, dump_spy: list[Any]) -> None:
        interceptor = ExceptionInterceptor(mapper=_mapper())

        @interceptor.generator(site="gen")
        def stream(n: int) -> __import__("typing").Generator[int, None, None]:
            yield from range(n)

        assert list(stream(3)) == [0, 1, 2]
        assert dump_spy == []

    @pytest.mark.asyncio
    async def test_asyncgenerator_success_no_sanitize(
        self, dump_spy: list[Any]
    ) -> None:
        interceptor = ExceptionInterceptor(mapper=_mapper())

        @interceptor.asyncgenerator(site="agen")
        async def stream(
            n: int,
        ) -> __import__("collections.abc").AsyncGenerator[int, None]:
            for i in range(n):
                yield i

        assert [x async for x in stream(3)] == [0, 1, 2]
        assert dump_spy == []

    def test_contextmanager_success_no_sanitize(self, dump_spy: list[Any]) -> None:
        interceptor = ExceptionInterceptor(mapper=_mapper())

        @interceptor.contextmanager(site="cm")
        @contextmanager
        def scope(name: str) -> __import__("typing").Generator[str, None, None]:
            yield name

        with scope("tx") as value:
            assert value == "tx"

        assert dump_spy == []

    @pytest.mark.asyncio
    async def test_asynccontextmanager_success_no_sanitize(
        self, dump_spy: list[Any]
    ) -> None:
        interceptor = ExceptionInterceptor(mapper=_mapper())

        @interceptor.asynccontextmanager(site="acm")
        @asynccontextmanager
        async def scope(
            name: str,
        ) -> __import__("collections.abc").AsyncGenerator[str, None]:
            yield name

        async with scope("tx") as value:
            assert value == "tx"

        assert dump_spy == []

    @pytest.mark.asyncio
    async def test_coroutine_cancellederror_no_materialization(
        self, dump_spy: list[Any]
    ) -> None:
        interceptor = ExceptionInterceptor(mapper=_mapper())

        @interceptor.coroutine(site="coro")
        async def cancelled(query: str) -> None:
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await cancelled("SELECT 1")

        assert dump_spy == []

    @pytest.mark.asyncio
    async def test_coroutine_coreexception_no_materialization(
        self, dump_spy: list[Any]
    ) -> None:
        interceptor = ExceptionInterceptor(mapper=_mapper())

        @interceptor.coroutine(site="coro")
        async def failing(query: str) -> None:
            raise exc.conflict("already exists", code="dup")

        with pytest.raises(CoreException, match="already exists"):
            await failing("SELECT 1")

        assert dump_spy == []


class _Payload(BaseModel):
    name: str = "order"
    password: SecretStr = SecretStr("hunter2")
    api_key: str = "sk-123"


_GOLDEN_PAYLOAD = {
    "name": "order",
    "password": "**********",
    "api_key": "**********",
}


class TestLazyErrorPathDetails:
    """The error path still produces the exact pre-lazy details shape."""

    def _capture_mapper(self, captured: dict[str, Any]) -> ChainExceptionMapper:
        def mapper(e, *, site, details=None):  # type: ignore[no-untyped-def]
            captured["site"] = site
            captured["details"] = details
            return exc.internal("boom", code="wrapped")

        return ChainExceptionMapper.chain(mapper)

    def test_function_error_details_golden(self) -> None:
        captured: dict[str, Any] = {}
        interceptor = ExceptionInterceptor(mapper=self._capture_mapper(captured))

        @interceptor.function(site="db.insert")
        def insert(
            query: str,
            payload: _Payload,
            *,
            limit: int = 10,
            token: str = "secret-token",
        ) -> None:
            raise ValueError("nope")

        with pytest.raises(CoreException) as exc_info:
            insert("INSERT", _Payload())

        assert captured["site"] == "db.insert"
        assert captured["details"] == {
            "query": "INSERT",
            "payload": _GOLDEN_PAYLOAD,
            "limit": 10,
            "token": "secret-token",
        }
        assert exc_info.value.details == {"site": "db.insert"}

    @pytest.mark.asyncio
    async def test_coroutine_error_details_golden(self) -> None:
        captured: dict[str, Any] = {}
        interceptor = ExceptionInterceptor(mapper=self._capture_mapper(captured))

        @interceptor.coroutine(site="db.insert")
        async def insert(query: str, payload: _Payload) -> None:
            raise ValueError("nope")

        with pytest.raises(CoreException):
            await insert("INSERT", _Payload())

        assert captured["site"] == "db.insert"
        assert captured["details"] == {
            "query": "INSERT",
            "payload": _GOLDEN_PAYLOAD,
        }

    def test_function_self_and_cls_excluded(self) -> None:
        captured: dict[str, Any] = {}
        interceptor = ExceptionInterceptor(mapper=self._capture_mapper(captured))

        class Client:
            @interceptor.function(site="client.run")
            def run(self, query: str) -> None:
                raise ValueError("nope")

        with pytest.raises(CoreException):
            Client().run("SELECT 1")

        assert captured["details"] == {"query": "SELECT 1"}

    def test_generator_throw_at_yield_has_details(self) -> None:
        """Lazy capture survives across yields: details still materialize late."""

        captured: dict[str, Any] = {}
        interceptor = ExceptionInterceptor(mapper=self._capture_mapper(captured))

        @interceptor.generator(site="gen")
        def stream(query: str) -> __import__("typing").Generator[int, None, None]:
            yield 1
            yield 2

        gen = stream("SELECT 1")
        assert next(gen) == 1

        with pytest.raises(CoreException):
            gen.throw(ValueError("consumer"))

        assert captured["details"] == {"query": "SELECT 1"}

    @pytest.mark.asyncio
    async def test_asyncgenerator_throw_at_yield_has_details(self) -> None:
        """Lazy capture survives across yields: details still materialize late."""

        captured: dict[str, Any] = {}
        interceptor = ExceptionInterceptor(mapper=self._capture_mapper(captured))

        @interceptor.asyncgenerator(site="agen")
        async def stream(
            query: str,
        ) -> __import__("collections.abc").AsyncGenerator[int, None]:
            yield 1
            yield 2

        agen = stream("SELECT 1")
        assert await agen.__anext__() == 1

        with pytest.raises(CoreException):
            await agen.athrow(ValueError("consumer"))

        assert captured["details"] == {"query": "SELECT 1"}

    def test_contextmanager_enter_failure_has_details(self) -> None:
        captured: dict[str, Any] = {}
        interceptor = ExceptionInterceptor(mapper=self._capture_mapper(captured))

        @interceptor.contextmanager(site="cm")
        @contextmanager
        def scope(name: str) -> __import__("typing").Generator[None, None, None]:
            raise ValueError("enter")
            yield

        with pytest.raises(CoreException):
            with scope("tx"):
                pass

        assert captured["site"] == "cm"
        assert captured["details"] == {"name": "tx"}

    @pytest.mark.asyncio
    async def test_asynccontextmanager_exit_failure_has_details(self) -> None:
        captured: dict[str, Any] = {}
        interceptor = ExceptionInterceptor(mapper=self._capture_mapper(captured))

        @interceptor.asynccontextmanager(site="acm")
        @asynccontextmanager
        async def scope(
            name: str,
        ) -> __import__("collections.abc").AsyncGenerator[None, None]:
            yield
            raise ValueError("exit")

        with pytest.raises(CoreException):
            async with scope("tx"):
                pass

        assert captured["site"] == "acm"
        assert captured["details"] == {"name": "tx"}

    def test_materialization_failure_falls_back_to_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import forze.base.scrubbing as scrubbing

        def broken(bound: Any) -> Any:
            raise RuntimeError("sanitizer broke")

        monkeypatch.setattr(scrubbing, "dump_bound_args_for_errors", broken)

        captured: dict[str, Any] = {}
        interceptor = ExceptionInterceptor(mapper=self._capture_mapper(captured))

        @interceptor.function(site="fn")
        def failing(query: str) -> None:
            raise ValueError("nope")

        with pytest.raises(CoreException):
            failing("SELECT 1")

        assert captured["details"] is None
