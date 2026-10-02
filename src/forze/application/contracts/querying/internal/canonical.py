"""One ordering for values that arrive unordered, the same in every process."""

from collections.abc import Mapping
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

# ----------------------- #


def canonical_sort_key(value: Any) -> tuple[Any, ...]:
    """A key that orders values the same way under every ``PYTHONHASHSEED``.

    A set iterates in an order seeded per process. Sorted by this key, its elements come out
    alike everywhere: the key tags each value with its kind first, so values of different
    kinds never compare or tie (a date and its ISO string, ``1`` and ``"1"``), and a nested
    set is keyed by its own sorted elements rather than by a form that iterates it.
    """

    match value:
        case None:
            return (0,)

        case bool():
            return (1, value)

        case int() | float() | Decimal():
            return (2, value)

        case str():
            return (3, value)

        case bytes():
            return (4, value)

        case datetime():
            return (5, value.isoformat())

        case date():
            return (6, value.isoformat())

        case time():
            return (7, value.isoformat())

        case UUID():
            return (8, str(value))

        case Enum():
            return (9, type(value).__qualname__, canonical_sort_key(value.value))

        case list() | tuple():
            return (10, tuple(canonical_sort_key(item) for item in value))  # pyright: ignore[reportUnknownVariableType]

        case set() | frozenset():
            return (11, tuple(sorted(canonical_sort_key(item) for item in value)))  # pyright: ignore[reportUnknownVariableType]

        case Mapping():
            items = sorted(
                (canonical_sort_key(k), canonical_sort_key(v))
                for k, v in value.items()  # pyright: ignore[reportUnknownVariableType]
            )
            return (12, tuple(items))

        case _:
            return (13, type(value).__qualname__, repr(value))
