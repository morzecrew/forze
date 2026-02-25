import pytest

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
def test_subclass_errors_are_core_error(exc: CoreError, expected_type: type[CoreError]) -> None:
    assert isinstance(exc, CoreError)
    assert isinstance(exc, expected_type)


def test_error_handler_wraps_unknown_exception_with_core_error() -> None:
    """Custom error handler should only see exceptions that default handler did not map."""

    @error_handler
    def custom_handler(e: Exception, op: str, **kwargs: object) -> CoreError:  # pragma: no cover - behaviour tested via wrapper
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

