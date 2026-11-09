"""The in-memory matcher's compile-time work answers exactly as the per-row path did.

:func:`compile_expr` turns a membership operand into a hash lookup and ``$like`` / ``$ilike``
regexes are compiled once per pattern. Both are pinned here against the per-row reference —
the linear :func:`_eq` scan and ``re.search`` over a freshly built pattern — over values of
mixed types, including the ones ``_eq`` treats specially and the ones that make it raise.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, timedelta, timezone, tzinfo
from decimal import Decimal
from enum import Enum, IntEnum, StrEnum
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

import hypothesis.strategies as st
from hypothesis import given, settings

from forze.application.contracts.querying import (
    ELEM_SCALAR_FIELD,
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryNot,
    QueryOr,
)
from forze.application.contracts.querying.internal import like_pattern_to_regex
from forze.application.contracts.querying.internal.matching import (
    _eq,  # pyright: ignore[reportPrivateUsage]
    _like_regex,  # pyright: ignore[reportPrivateUsage]
    _match_expr,  # pyright: ignore[reportPrivateUsage]
    _match_text,  # pyright: ignore[reportPrivateUsage]
    _memb_contains,  # pyright: ignore[reportPrivateUsage]
    _memb_keys,  # pyright: ignore[reportPrivateUsage]
    _prepare,  # pyright: ignore[reportPrivateUsage]
    _PreparedField,  # pyright: ignore[reportPrivateUsage]
    compile_expr,
)

# ----------------------- #


class _Color(StrEnum):
    RED = "red"


class _Level(IntEnum):
    ONE = 1


class _Ref(Enum):
    ID = UUID(int=7)
    NONE = None
    PAIR = (1, 2)


class _Anything:
    """An app type whose ``==`` matches every value: no hash lookup may stand in for it."""

    def __eq__(self, other: object) -> bool:
        return True

    __hash__ = object.__hash__


class _Floating(tzinfo):
    """A zone that names no offset: its datetimes are naive to ``==``."""

    def utcoffset(self, dt: datetime | None) -> timedelta | None:
        return None


class _Broken(tzinfo):
    """A zone whose offset cannot be read: comparing its datetimes raises."""

    def utcoffset(self, dt: datetime | None) -> timedelta | None:
        raise RuntimeError("no offset")


_U = UUID("12345678-9abc-4def-8abc-def012345678")
_BERLIN = ZoneInfo("Europe/Berlin")
_NOON_UTC = datetime(2026, 1, 1, 12, tzinfo=UTC)

# Every family ``_eq`` has a rule for, and values that collide across them.
_POOL: list[Any] = [
    # numbers, equal across types
    0,
    1,
    2,
    True,
    False,
    0.0,
    -0.0,
    1.0,
    1.5,
    float("inf"),
    float("nan"),
    Decimal("1"),
    Decimal("1.0"),
    Decimal("1.5"),
    Decimal("Infinity"),
    Decimal("NaN"),
    Decimal("sNaN"),  # raises when compared
    # text, and text that names other values
    "",
    "1",
    "True",
    "None",
    "red",
    str(_U),
    str(_U).upper(),
    str(UUID(int=7)),
    "2026-01-01",
    None,
    b"red",
    # UUIDs, matched by their text
    _U,
    UUID(int=7),
    # one instant in three zones, a naive wall clock, a repeated hour, a date
    _NOON_UTC,
    _NOON_UTC.astimezone(timezone(timedelta(hours=5))),
    _NOON_UTC.astimezone(_BERLIN),
    datetime(2026, 1, 1, 12),
    datetime(2026, 1, 1, 12, fold=1),
    datetime(2026, 1, 1, 12, tzinfo=_Floating()),
    datetime(2026, 1, 1, 12, tzinfo=_Broken()),
    datetime(2026, 10, 25, 2, 30, tzinfo=_BERLIN),
    datetime(2026, 10, 25, 2, 30, fold=1, tzinfo=_BERLIN),
    datetime(2026, 10, 25, 0, 30, tzinfo=UTC),
    datetime(2026, 10, 25, 1, 30, tzinfo=UTC),
    date(2026, 1, 1),
    # members, compared by their values
    _Color.RED,
    _Level.ONE,
    _Ref.ID,
    _Ref.NONE,
    _Ref.PAIR,
    # unhashable, containers, app types, and a mapping enum values cannot flatten
    [1, 2],
    ("red",),
    {"a": 1},
    {_Color.RED: 1, "red": 2},
    _Anything(),
]


def _linear(field_value: Any, values: Sequence[Any]) -> bool:
    """The membership test as it was: every item against every candidate, in order."""

    items = (
        field_value
        if isinstance(field_value, Sequence)
        and not isinstance(field_value, (str, bytes, bytearray))
        else (field_value,)
    )

    return any(_eq(item, candidate) for item in items for candidate in values)


def _outcome(fn: Callable[..., Any], *args: Any) -> tuple[str, Any]:
    try:
        return ("ok", fn(*args))

    except Exception as error:
        return ("raised", type(error))


# ....................... #


def test_keyed_lookup_matches_eq_for_every_pair() -> None:
    for candidate in _POOL:
        keys = _memb_keys([candidate])

        for value in _POOL:
            assert _outcome(_memb_contains, value, [candidate], keys) == _outcome(
                _linear, value, [candidate]
            ), (value, candidate)


# By index: sampling the values themselves would hash them, and some raise when hashed.
_values = st.integers(0, len(_POOL) - 1).map(_POOL.__getitem__)
_rows = st.one_of(_values, st.lists(_values, max_size=4))


@settings(max_examples=400, deadline=None)
@given(row=_rows, candidates=st.lists(_values, max_size=8))
def test_keyed_lookup_matches_linear_scan(row: Any, candidates: list[Any]) -> None:
    keys = _memb_keys(candidates)

    assert _outcome(_memb_contains, row, candidates, keys) == _outcome(_linear, row, candidates)


def test_unkeyed_candidate_turns_the_lookup_off() -> None:
    assert _memb_keys(["a", _U, 1, None, _NOON_UTC]) is not None
    assert _memb_keys(["a", _Anything()]) is None
    assert _memb_keys(["a", float("nan")]) is None
    assert _memb_keys(["a", [1]]) is None
    assert _memb_keys("abc") is None  # not a parsed list operand
    # A member is keyed by its value, as ``_eq`` compares it.
    assert _memb_keys([_Color.RED, _Level.ONE, _Ref.ID]) == frozenset({"red", 1, str(UUID(int=7))})


def test_keys_follow_eq_across_types() -> None:
    keys = _memb_keys([1, str(_U), _NOON_UTC])
    assert keys is not None

    assert _memb_contains(True, [1, str(_U), _NOON_UTC], keys)
    assert _memb_contains(Decimal("1.0"), [1, str(_U), _NOON_UTC], keys)
    assert _memb_contains(_U, [1, str(_U), _NOON_UTC], keys)
    assert _memb_contains(_NOON_UTC.astimezone(_BERLIN), [1, str(_U), _NOON_UTC], keys)
    assert not _memb_contains(datetime(2026, 1, 1, 12), [1, str(_U), _NOON_UTC], keys)
    assert not _memb_contains(str(_U).upper(), [1, str(_U), _NOON_UTC], keys)


# ....................... #

_OPS = ["$in", "$nin", "$superset", "$subset", "$disjoint", "$overlaps"]
_ABSENT = object()


def _leaf(op: str, candidates: list[Any]) -> QueryField:
    return QueryField("f", op, candidates)  # type: ignore[arg-type]


def test_compiled_leaf_matches_per_row_path_for_every_pair() -> None:
    for op in _OPS:
        for candidate in _POOL:
            for candidates in ([candidate], [candidate, 2]):
                leaf = _leaf(op, candidates)
                compiled = compile_expr(leaf)

                for value in _POOL:
                    row = {"f": value}

                    assert _outcome(compiled, row) == _outcome(_match_expr, row, leaf), (
                        op,
                        value,
                        candidates,
                    )


@settings(max_examples=300, deadline=None)
@given(
    op=st.sampled_from(_OPS),
    candidates=st.lists(_values, max_size=6),
    value=st.one_of(st.just(_ABSENT), _rows),
    shape=st.sampled_from(["leaf", "not", "or", "and", "elem"]),
)
def test_compiled_expression_matches_per_row_path(
    op: str,
    candidates: list[Any],
    value: Any,
    shape: str,
) -> None:
    leaf = _leaf(op, candidates)
    other = QueryField("g", "$eq", 1)
    expr: QueryExpr = {
        "leaf": leaf,
        "not": QueryNot(leaf),
        "or": QueryOr((other, leaf)),
        "and": QueryAnd((QueryNot(other), leaf)),
        "elem": QueryElem("items", "$any", QueryField(ELEM_SCALAR_FIELD, op, candidates)),  # type: ignore[arg-type]
    }[shape]
    row: dict[str, Any] = {"g": 0}

    if value is not _ABSENT:
        row["f"] = value
        row["items"] = value if isinstance(value, list) else [value]

    compiled = compile_expr(expr)

    assert _outcome(compiled, row) == _outcome(_match_expr, row, expr)


class _Int(int):
    """An ``int`` subclass: a row holding one is compared with each candidate, not looked up."""


def test_a_compiled_operand_is_a_copy_of_the_callers_list() -> None:
    candidates: list[Any] = [1]
    compiled = compile_expr(_leaf("$in", candidates))
    candidates.append(2)

    # The lookup a builtin row value takes and the scan a subclass takes read one operand.
    assert _outcome(compiled, {"f": 2}) == _outcome(compiled, {"f": _Int(2)}) == ("ok", False)


def test_prepare_replaces_only_membership_leaves() -> None:
    eq = QueryField("a", "$eq", 1)
    compare = QueryCompare("a", "$eq", "b")
    expr = QueryAnd((eq, compare, QueryOr((_leaf("$in", [1]), QueryNot(_leaf("$overlaps", [2]))))))

    prepared = _prepare(expr)

    assert isinstance(prepared, QueryAnd)
    assert prepared.items[0] is eq
    assert prepared.items[1] is compare
    inner = prepared.items[2]
    assert isinstance(inner, QueryOr)
    assert isinstance(inner.items[0], _PreparedField)
    assert inner.items[0].keys == frozenset({1})
    assert isinstance(inner.items[1], QueryNot)
    assert isinstance(inner.items[1].item, _PreparedField)
    assert inner.items[1].item.keys == frozenset({2})
    # A leaf the matcher would not key, and one that cannot be a set, still prepare: per row.
    assert _prepare(_leaf("$in", [_Anything()])).keys is None  # type: ignore[attr-defined]
    assert _prepare(_leaf("$subset", [[1]])).keys is None  # type: ignore[attr-defined]


# ....................... #

_LIKE_ALPHABET = "aAbB%_\\.*+?()[]{}^$|"


@settings(max_examples=300, deadline=None)
@given(
    op=st.sampled_from(["$like", "$ilike"]),
    pattern=st.text(alphabet=_LIKE_ALPHABET, min_size=1, max_size=8),
    data=st.data(),
)
def test_like_matches_per_row_build(op: str, pattern: str, data: st.DataObject) -> None:
    # Texts near the pattern too: a random text seldom matches an anchored one.
    near = [pattern, pattern.swapcase(), pattern.replace("%", "ab").replace("_", "B")]
    text = data.draw(st.one_of(st.sampled_from(near), st.text(alphabet=_LIKE_ALPHABET, max_size=8)))
    regex = like_pattern_to_regex(pattern, case_insensitive=op == "$ilike")

    assert _match_text(text, op, pattern) is (re.search(regex, text) is not None)


def test_like_regex_cache_is_keyed_by_pattern_and_case() -> None:
    _like_regex.cache_clear()

    first = _like_regex("%ab_", False)
    assert _like_regex("%ab_", False) is first
    assert _like_regex.cache_info().hits == 1

    folded = _like_regex("%ab_", True)
    other = _like_regex("%ab", False)
    assert folded is not first and other is not first
    assert _like_regex.cache_info().misses == 3

    assert first.search("xxABc") is None
    assert folded.search("xxABc") is not None
