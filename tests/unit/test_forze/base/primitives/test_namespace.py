"""Unit tests for :class:`forze.base.primitives.StrKeyNamespace` and its key memo."""

from enum import Enum, StrEnum

import pytest

from forze.base.exceptions import CoreException
from forze.base.primitives import StrKeyNamespace
from forze.base.primitives.namespace import _joined_key

# ----------------------- #


class _Op(StrEnum):
    GET = "get"


class _MixedOp(str, Enum):  # noqa: UP042 - the mixed-in form is the point
    """Equal to its value, but ``str()`` renders ``_MixedOp.GET``."""

    GET = "get"


@pytest.fixture(autouse=True)
def _cold_memo():
    _joined_key.cache_clear()
    yield
    _joined_key.cache_clear()


# ....................... #


def test_key_joins_prefix_and_parts() -> None:
    assert StrKeyNamespace(prefix="orders").key("get") == "orders.get"
    assert StrKeyNamespace(prefix="orders").key("a", "b") == "orders.a.b"
    assert StrKeyNamespace(prefix="orders", sep=":").key(_Op.GET) == "orders:get"


def test_equal_inputs_hit_the_memo() -> None:
    first = StrKeyNamespace(prefix="orders").key(_Op.GET)
    # Another instance with the same inputs shares the entry.
    second = StrKeyNamespace(prefix="orders").key(_Op.GET)

    info = _joined_key.cache_info()
    assert first == second == "orders.get"
    assert (info.hits, info.misses) == (1, 1)


@pytest.mark.parametrize(
    ("ns", "parts", "expected"),
    [
        (StrKeyNamespace(prefix="users"), ("get",), "users.get"),
        (StrKeyNamespace(prefix="orders", sep=":"), ("get",), "orders:get"),
        (StrKeyNamespace(prefix="orders"), ("list",), "orders.list"),
        (StrKeyNamespace(prefix="orders"), ("get", "x"), "orders.get.x"),
        (StrKeyNamespace(prefix="orders"), (_Op.GET,), "orders.get"),
    ],
)
def test_any_differing_input_misses(
    ns: StrKeyNamespace, parts: tuple[str, ...], expected: str
) -> None:
    StrKeyNamespace(prefix="orders").key("get")

    assert ns.key(*parts) == expected
    assert _joined_key.cache_info().misses == 2


def test_a_part_equal_by_value_but_rendered_differently_never_shares_an_entry() -> None:
    # ``_MixedOp.GET == "get"``, yet ``str()`` gives ``_MixedOp.GET``: keyed by value
    # alone, the memo would hand back ``orders:get``.
    ns = StrKeyNamespace(prefix="orders", sep=":")

    assert ns.key("get") == "orders:get"
    assert ns.key(_MixedOp.GET) == "orders:_MixedOp.GET"
    # With the default separator it is an invalid part, raised even after a valid twin.
    dotted = StrKeyNamespace(prefix="orders")
    dotted.key("get")

    with pytest.raises(CoreException, match="must not contain separator"):
        dotted.key(_MixedOp.GET)


def test_invalid_parts_raise_on_every_call() -> None:
    ns = StrKeyNamespace(prefix="orders")

    for _ in range(2):
        with pytest.raises(CoreException, match="must not contain separator"):
            ns.key("a.b")

        with pytest.raises(CoreException, match="non-empty"):
            ns.key("")

    with pytest.raises(CoreException, match="No parts"):
        ns.key()


def test_prefix_is_validated_at_construction() -> None:
    with pytest.raises(CoreException, match="must not contain separator"):
        StrKeyNamespace(prefix="a.b")

    with pytest.raises(CoreException, match="non-empty"):
        StrKeyNamespace(prefix="")


def test_memo_is_bounded() -> None:
    assert _joined_key.cache_info().maxsize == 1024
