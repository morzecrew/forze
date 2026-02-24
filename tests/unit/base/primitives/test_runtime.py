import pytest

from forze.base.primitives import RuntimeVar
from forze.base.errors import CoreError


def test_runtime_var_set_once_and_get() -> None:
    rv: RuntimeVar[int] = RuntimeVar("int_rt")
    rv.set_once(42)
    assert rv.get() == 42


def test_runtime_var_rejects_none_and_double_set() -> None:
    rv: RuntimeVar[int] = RuntimeVar("int_rt")

    with pytest.raises(CoreError):
        rv.set_once(None)  # type: ignore[arg-type]

    rv.set_once(1)

    with pytest.raises(CoreError):
        rv.set_once(2)


def test_runtime_var_reset_allows_re_set() -> None:
    rv: RuntimeVar[int] = RuntimeVar("int_rt")
    rv.set_once(5)
    rv.reset()
    rv.set_once(10)
    assert rv.get() == 10


def test_runtime_var_get_raises_if_not_set() -> None:
    rv: RuntimeVar[int] = RuntimeVar("int_rt")

    with pytest.raises(CoreError):
        rv.get()

