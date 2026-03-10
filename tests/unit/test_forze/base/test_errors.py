import warnings
from contextlib import asynccontextmanager, contextmanager
from typing import Any

import pytest
from pydantic import BaseModel
from pydantic import ValidationError as PydanticValidationError

from forze.base.errors import (
    ConcurrencyError,
    ConflictError,
    CoreError,
    InfrastructureError,
    NotFoundError,
    ValidationError,
    _default_error_hanlder,
    _is_async_iterator,
    _is_awaitable,
    _is_contextmanager,
    _is_iterator,
    error_handler,
    handled,
)

# ----------------------- #
# CoreError and subclasses


class TestCoreErrorHierarchy:
    def test_core_error_str_includes_code_and_message(self) -> None:
        err = CoreError(message="Something went wrong", code="oops")
        assert str(err) == "oops: Something went wrong"

    def test_core_error_defaults(self) -> None:
        err = CoreError()
        assert err.code == "internal_error"
        assert err.message == "An internal error occurred"
        assert err.details is None

    def test_core_error_with_details(self) -> None:
        err = CoreError(message="err", details={"key": "val"})
        assert err.details == {"key": "val"}

    def test_not_found_error_defaults(self) -> None:
        err = NotFoundError()
        assert err.code == "not_found"
        assert err.message == "Resource not found"

    def test_conflict_error_defaults(self) -> None:
        err = ConflictError()
        assert err.code == "conflict"

    def test_validation_error_defaults(self) -> None:
        err = ValidationError()
        assert err.code == "validation_error"

    def test_infrastructure_error_defaults(self) -> None:
        err = InfrastructureError()
        assert err.code == "infrastructure_error"

    def test_concurrency_error_defaults(self) -> None:
        err = ConcurrencyError()
        assert err.code == "concurrency_error"

    @pytest.mark.parametrize(
        "exc_cls",
        [
            NotFoundError,
            ConflictError,
            ValidationError,
            InfrastructureError,
            ConcurrencyError,
        ],
    )
    def test_all_subclasses_are_core_error(self, exc_cls: type[CoreError]) -> None:
        assert issubclass(exc_cls, CoreError)
        assert isinstance(exc_cls(), CoreError)

    def test_subclass_custom_message(self) -> None:
        err = NotFoundError(message="custom msg")
        assert err.message == "custom msg"
        assert err.code == "not_found"

    def test_core_error_is_exception(self) -> None:
        with pytest.raises(CoreError):
            raise CoreError("boom")


# ----------------------- #
# _default_error_handler


class TestDefaultErrorHandler:
    def test_maps_pydantic_validation_error(self) -> None:
        class M(BaseModel):
            x: int

        try:
            M.model_validate({"x": "not_int"})
        except PydanticValidationError as e:
            result = _default_error_hanlder(e, "op")
            assert isinstance(result, ValidationError)

    def test_returns_none_for_unknown_exception(self) -> None:
        result = _default_error_hanlder(RuntimeError("boom"), "op")
        assert result is None


# ----------------------- #
# error_handler decorator


class TestErrorHandler:
    def test_wraps_unknown_exception(self) -> None:
        @error_handler
        def custom(e: Exception, op: str, **kwargs: Any) -> CoreError:
            return CoreError(message=f"{op}: {e}", code="wrapped")

        err = custom(RuntimeError("boom"), "my_op")
        assert err.code == "wrapped"
        assert "boom" in err.message

    def test_pydantic_error_takes_priority(self) -> None:
        @error_handler
        def custom(e: Exception, op: str, **kwargs: Any) -> CoreError:
            return CoreError(message="fallback", code="fallback")

        class M(BaseModel):
            x: int

        try:
            M.model_validate({"x": "nope"})
        except PydanticValidationError as e:
            err = custom(e, "validate")
            assert isinstance(err, ValidationError)


# ----------------------- #
# Type guards


class TestTypeGuards:
    def test_is_awaitable_positive(self) -> None:
        async def coro() -> None:
            pass

        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                category=RuntimeWarning,
                message="coroutine .* was never awaited",
            )
            assert _is_awaitable(coro())

    def test_is_awaitable_negative(self) -> None:
        assert not _is_awaitable(42)
        assert not _is_awaitable("string")

    def test_is_iterator_positive(self) -> None:
        assert _is_iterator(iter([1, 2, 3]))

    def test_is_iterator_excludes_str(self) -> None:
        assert not _is_iterator("hello")

    def test_is_iterator_excludes_bytes(self) -> None:
        assert not _is_iterator(b"data")

    def test_is_iterator_excludes_bytearray(self) -> None:
        assert not _is_iterator(bytearray(b"data"))

    def test_is_iterator_excludes_memoryview(self) -> None:
        assert not _is_iterator(memoryview(b"data"))

    def test_is_async_iterator_positive(self) -> None:
        class AsyncIt:
            def __aiter__(self) -> "AsyncIt":
                return self

            async def __anext__(self) -> int:
                raise StopAsyncIteration

        assert _is_async_iterator(AsyncIt())

    def test_is_async_iterator_negative(self) -> None:
        assert not _is_async_iterator([1, 2])

    def test_is_contextmanager_negative_for_regular_fn(self) -> None:
        def regular() -> int:
            return 1

        assert not _is_contextmanager(regular)


# ----------------------- #
# handled decorator: sync


class TestHandledSync:
    def test_wraps_sync_function(self) -> None:
        def handler(e: Exception, op: str, **kw: Any) -> CoreError:
            return CoreError(message=str(e), code="handled")

        @handled(handler, op="sync_op")
        def fn(x: int) -> int:
            if x < 0:
                raise ValueError("neg")
            return x * 2

        assert fn(2) == 4
        with pytest.raises(CoreError, match="neg"):
            fn(-1)

    def test_auto_infers_op_name(self) -> None:
        ops: list[str] = []

        def handler(e: Exception, op: str, **kw: Any) -> CoreError:
            ops.append(op)
            return CoreError(message=str(e), code="h")

        @handled(handler)
        def my_fn() -> None:
            raise RuntimeError("err")

        with pytest.raises(CoreError):
            my_fn()
        assert ops == ["my_fn"]

    def test_returns_string_not_wrapped_as_iterator(self) -> None:
        def handler(e: Exception, op: str, **kw: Any) -> CoreError:
            return CoreError(message=str(e), code="h")

        @handled(handler)
        def fn() -> str:
            return "abc"

        assert fn() == "abc"

    def test_returns_bytes_not_wrapped_as_iterator(self) -> None:
        def handler(e: Exception, op: str, **kw: Any) -> CoreError:
            return CoreError(message=str(e), code="h")

        @handled(handler)
        def fn() -> bytes:
            return b"data"

        assert fn() == b"data"


# ----------------------- #
# handled decorator: async


class TestHandledAsync:
    @pytest.mark.asyncio
    async def test_wraps_async_function(self) -> None:
        def handler(e: Exception, op: str, **kw: Any) -> CoreError:
            return CoreError(message=str(e), code="handled")

        @handled(handler, op="async_op")
        async def fn(x: int) -> int:
            if x < 0:
                raise ValueError("neg")
            return x * 2

        assert await fn(2) == 4
        with pytest.raises(CoreError, match="neg"):
            await fn(-1)


# ----------------------- #
# handled decorator: sync generator


class TestHandledSyncGenerator:
    def test_wraps_sync_generator_iteration_error(self) -> None:
        def handler(e: Exception, op: str, **kw: Any) -> CoreError:
            return CoreError(message=str(e), code="h")

        @handled(handler)
        def gen(n: int):
            for i in range(n):
                if i == 2:
                    raise ValueError("stop")
                yield i

        out = []
        with pytest.raises(CoreError, match="stop"):
            for v in gen(5):
                out.append(v)
        assert out == [0, 1]

    def test_wraps_sync_generator_init_error(self) -> None:
        def handler(e: Exception, op: str, **kw: Any) -> CoreError:
            return CoreError(message=str(e), code="h")

        @handled(handler)
        def gen():
            raise ValueError("init_fail")
            yield 1  # noqa: unreachable

        with pytest.raises(CoreError, match="init_fail"):
            list(gen())


# ----------------------- #
# handled decorator: async generator


class TestHandledAsyncGenerator:
    @pytest.mark.asyncio
    async def test_wraps_async_generator_iteration_error(self) -> None:
        def handler(e: Exception, op: str, **kw: Any) -> CoreError:
            return CoreError(message=str(e), code="h")

        @handled(handler)
        async def gen(n: int):
            for i in range(n):
                if i == 2:
                    raise ValueError("stop")
                yield i

        out = []
        with pytest.raises(CoreError, match="stop"):
            async for v in gen(5):
                out.append(v)
        assert out == [0, 1]

    @pytest.mark.asyncio
    async def test_wraps_async_generator_init_error(self) -> None:
        def handler(e: Exception, op: str, **kw: Any) -> CoreError:
            return CoreError(message=str(e), code="h")

        @handled(handler)
        async def gen():
            raise ValueError("init_fail")
            yield 1  # noqa: unreachable

        with pytest.raises(CoreError, match="init_fail"):
            async for _ in gen():
                pass


# ----------------------- #
# handled decorator: awaitable return, context managers, iterators


class TestHandledReturnTypes:
    @pytest.mark.asyncio
    async def test_fn_returning_awaitable(self) -> None:
        def handler(e: Exception, op: str, **kw: Any) -> CoreError:
            return CoreError(message=str(e), code="h")

        @handled(handler)
        def fn(x: int):
            async def _inner():
                if x < 0:
                    raise ValueError("neg")
                return x * 2

            return _inner()

        assert await fn(2) == 4
        with pytest.raises(CoreError, match="neg"):
            await fn(-1)

    def test_fn_returning_sync_cm_passthrough(self) -> None:
        def handler(e: Exception, op: str, **kw: Any) -> CoreError:
            return CoreError(message=str(e), code="h")

        @handled(handler)
        def fn():
            @contextmanager
            def _cm():
                yield 42

            return _cm()

        with fn() as v:
            assert v == 42

    @pytest.mark.asyncio
    async def test_fn_returning_async_cm_passthrough(self) -> None:
        def handler(e: Exception, op: str, **kw: Any) -> CoreError:
            return CoreError(message=str(e), code="h")

        @handled(handler)
        def fn():
            @asynccontextmanager
            async def _acm():
                yield 42

            return _acm()

        async with fn() as v:
            assert v == 42

    def test_fn_returning_sync_iterator_wraps_errors(self) -> None:
        def handler(e: Exception, op: str, **kw: Any) -> CoreError:
            return CoreError(message=str(e), code="h")

        @handled(handler)
        def fn():
            for i in range(3):
                if i == 2:
                    raise ValueError("fail")
                yield i

        out = []
        with pytest.raises(CoreError, match="fail"):
            for v in fn():
                out.append(v)
        assert out == [0, 1]

    @pytest.mark.asyncio
    async def test_fn_returning_async_iterator_wraps_errors(self) -> None:
        def handler(e: Exception, op: str, **kw: Any) -> CoreError:
            return CoreError(message=str(e), code="h")

        @handled(handler)
        def fn():
            async def _gen():
                for i in range(3):
                    if i == 2:
                        raise ValueError("fail")
                    yield i

            return _gen()

        out = []
        with pytest.raises(CoreError, match="fail"):
            async for v in fn():
                out.append(v)
        assert out == [0, 1]
