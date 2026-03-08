from contextlib import asynccontextmanager, contextmanager

import pytest
from pydantic import BaseModel, ValidationError as PydanticValidationError

from forze.base.errors import (
    ConflictError,
    CoreError,
    NotFoundError,
    ValidationError,
    error_handler,
    handled,
)


def test_core_error_str_includes_code_and_message() -> None:
    err = CoreError(message="Something went wrong", code="oops")
    assert str(err) == "oops: Something went wrong"


@pytest.mark.parametrize(
    "exc, expected_type",
    [
        (ValidationError("v"), ValidationError),
        (NotFoundError("n"), NotFoundError),
        (ConflictError("c"), ConflictError),
    ],
)
def test_subclass_errors_are_core_error(
    exc: CoreError, expected_type: type[CoreError]
) -> None:
    assert isinstance(exc, CoreError)
    assert isinstance(exc, expected_type)


def test_error_handler_wraps_unknown_exception_with_core_error() -> None:
    """Custom error handler should only see exceptions that default handler did not map."""

    @error_handler
    def custom_handler(
        e: Exception, op: str, **kwargs: object
    ) -> CoreError:  # pragma: no cover - behaviour tested via wrapper
        return CoreError(message=f"{op}: {e}", code="wrapped")

    # simulate unknown exception
    err = custom_handler(RuntimeError("boom"), "op")
    assert isinstance(err, CoreError)
    assert err.code == "wrapped"
    assert "boom" in err.message


def test_handled_decorator_wraps_synchronous_function() -> None:
    calls: list[str] = []

    def handler(e: Exception, op: str, **kwargs: object) -> CoreError:
        calls.append(op)
        return CoreError(message=str(e), code="handled")

    @handled(handler, op="sync_op")
    def fn(x: int) -> int:
        if x < 0:
            raise ValueError("neg")
        return x * 2

    assert fn(2) == 4

    with pytest.raises(CoreError) as ei:
        fn(-1)

    assert ei.value.code == "handled"
    assert "neg" in ei.value.message
    assert calls == ["sync_op"]


def test_error_handler_maps_pydantic_validation_error_to_validation_error() -> None:
    """Default handler maps PydanticValidationError to ValidationError."""

    @error_handler
    def custom_handler(e: Exception, op: str, **kwargs: object) -> CoreError:
        return CoreError(message=str(e), code="fallback")

    class M(BaseModel):
        x: int

    try:
        M.model_validate({"x": "not_an_int"})
    except PydanticValidationError as e:
        err = custom_handler(e, "validate")
        assert isinstance(err, ValidationError)
        assert err.message == "M"


# ----------------------- #
# handled decorator: async coroutine


@pytest.mark.asyncio
async def test_handled_wraps_async_coroutine() -> None:
    calls: list[str] = []

    def handler(e: Exception, op: str, **kwargs: object) -> CoreError:
        calls.append(op)
        return CoreError(message=str(e), code="handled")

    @handled(handler, op="async_op")
    async def fn(x: int) -> int:
        if x < 0:
            raise ValueError("neg")
        return x * 2

    assert await fn(2) == 4

    with pytest.raises(CoreError) as ei:
        await fn(-1)

    assert ei.value.code == "handled"
    assert "neg" in ei.value.message
    assert calls == ["async_op"]


# ----------------------- #
# handled decorator: async generator


@pytest.mark.asyncio
async def test_handled_wraps_async_generator() -> None:
    calls: list[str] = []

    def handler(e: Exception, op: str, **kwargs: object) -> CoreError:
        calls.append(op)
        return CoreError(message=str(e), code="handled")

    @handled(handler)
    async def gen(x: int):
        for i in range(x):
            if i == 2:
                raise ValueError("stop")
            yield i

    it = gen(5)
    out = []
    with pytest.raises(CoreError) as ei:
        async for v in it:
            out.append(v)

    assert out == [0, 1]
    assert ei.value.code == "handled"
    assert "stop" in ei.value.message
    assert calls == ["gen"]


@pytest.mark.asyncio
async def test_handled_async_generator_init_error() -> None:
    """Error when creating the async generator (before first yield) is wrapped."""

    def handler(e: Exception, op: str, **kwargs: object) -> CoreError:
        return CoreError(message=str(e), code="handled")

    @handled(handler)
    async def gen_bad():
        raise ValueError("init_fail")
        yield 1  # noqa: unreachable

    with pytest.raises(CoreError) as ei:
        async for _ in gen_bad():
            pass

    assert ei.value.code == "handled"
    assert "init_fail" in ei.value.message


# ----------------------- #
# handled decorator: sync generator


def test_handled_wraps_sync_generator() -> None:
    calls: list[str] = []

    def handler(e: Exception, op: str, **kwargs: object) -> CoreError:
        calls.append(op)
        return CoreError(message=str(e), code="handled")

    @handled(handler)
    def gen(x: int):
        for i in range(x):
            if i == 2:
                raise ValueError("stop")
            yield i

    it = gen(5)
    out = []
    with pytest.raises(CoreError) as ei:
        for v in it:
            out.append(v)

    assert out == [0, 1]
    assert ei.value.code == "handled"
    assert "stop" in ei.value.message
    assert calls == ["gen"]


def test_handled_sync_generator_init_error() -> None:
    """Error when creating the generator (before first yield) is wrapped."""

    def handler(e: Exception, op: str, **kwargs: object) -> CoreError:
        return CoreError(message=str(e), code="handled")

    @handled(handler)
    def gen_bad():
        raise ValueError("init_fail")
        yield 1  # noqa: unreachable

    with pytest.raises(CoreError) as ei:
        list(gen_bad())

    assert ei.value.code == "handled"
    assert "init_fail" in ei.value.message


# ----------------------- #
# handled decorator: returns awaitable


@pytest.mark.asyncio
async def test_handled_wraps_fn_returning_awaitable() -> None:
    calls: list[str] = []

    def handler(e: Exception, op: str, **kwargs: object) -> CoreError:
        calls.append(op)
        return CoreError(message=str(e), code="handled")

    @handled(handler)
    def fn(x: int):
        async def _inner():
            if x < 0:
                raise ValueError("neg")
            return x * 2

        return _inner()

    assert await fn(2) == 4

    with pytest.raises(CoreError) as ei:
        await fn(-1)

    assert ei.value.code == "handled"
    assert "neg" in ei.value.message
    assert calls == ["fn"]


@pytest.mark.asyncio
async def test_handled_awaitable_error_in_awaited() -> None:
    """Error raised during await of returned awaitable is wrapped."""
    calls: list[str] = []

    def handler(e: Exception, op: str, **kwargs: object) -> CoreError:
        calls.append(op)
        return CoreError(message=str(e), code="handled")

    @handled(handler)
    def fn():
        async def _inner():
            raise RuntimeError("await_fail")

        return _inner()

    with pytest.raises(CoreError) as ei:
        await fn()

    assert ei.value.code == "handled"
    assert "await_fail" in ei.value.message
    assert calls == ["fn"]


# ----------------------- #
# handled decorator: returns context manager
# Note: _is_contextmanager requires a function with return annotation
# AbstractContextManager; @contextmanager returns a generator-based CM, so
# the wrapper is not applied to typical CMs. We test the success path only.


def test_handled_fn_returning_context_manager_passthrough() -> None:
    """Sync fn returning a CM passes it through (no wrapper when type check fails)."""

    def handler(e: Exception, op: str, **kwargs: object) -> CoreError:
        return CoreError(message=str(e), code="handled")

    @handled(handler)
    def fn_returns_cm():
        @contextmanager
        def _cm():
            yield 42

        return _cm()

    with fn_returns_cm() as v:
        assert v == 42


# ----------------------- #
# handled decorator: returns async context manager


@pytest.mark.asyncio
async def test_handled_fn_returning_async_context_manager_passthrough() -> None:
    """Sync fn returning an ACM passes it through (no wrapper when type check fails)."""

    def handler(e: Exception, op: str, **kwargs: object) -> CoreError:
        return CoreError(message=str(e), code="handled")

    @handled(handler)
    def fn_returns_acm():
        @asynccontextmanager
        async def _acm():
            yield 42

        return _acm()

    async with fn_returns_acm() as v:
        assert v == 42


# ----------------------- #
# handled decorator: returns iterator


def test_handled_wraps_fn_returning_iterator() -> None:
    calls: list[str] = []

    def handler(e: Exception, op: str, **kwargs: object) -> CoreError:
        calls.append(op)
        return CoreError(message=str(e), code="handled")

    @handled(handler)
    def fn_returns_iter():
        for i in range(3):
            if i == 2:
                raise ValueError("iter_fail")
            yield i

    it = fn_returns_iter()
    out = []
    with pytest.raises(CoreError) as ei:
        for v in it:
            out.append(v)

    assert out == [0, 1]
    assert ei.value.code == "handled"
    assert "iter_fail" in ei.value.message


# ----------------------- #
# handled decorator: returns async iterator


@pytest.mark.asyncio
async def test_handled_wraps_fn_returning_async_iterator() -> None:
    calls: list[str] = []

    def handler(e: Exception, op: str, **kwargs: object) -> CoreError:
        calls.append(op)
        return CoreError(message=str(e), code="handled")

    @handled(handler)
    def fn_returns_async_iter():
        async def _gen():
            for i in range(3):
                if i == 2:
                    raise ValueError("aiter_fail")
                yield i

        return _gen()

    it = fn_returns_async_iter()
    out = []
    with pytest.raises(CoreError) as ei:
        async for v in it:
            out.append(v)

    assert out == [0, 1]
    assert ei.value.code == "handled"
    assert "aiter_fail" in ei.value.message


# ----------------------- #
# _is_iterator: str/bytes excluded


def test_handled_iterator_excludes_strings() -> None:
    """Sync fn returning str is not wrapped as iterator (str is iterable but excluded)."""

    def handler(e: Exception, op: str, **kwargs: object) -> CoreError:
        return CoreError(message=str(e), code="handled")

    @handled(handler)
    def fn_returns_str():
        return "abc"

    result = fn_returns_str()
    assert result == "abc"
