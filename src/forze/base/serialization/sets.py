"""The order a set is written in."""

from collections.abc import Set
from datetime import date, time, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

from forze.base.primitives import stable_json_bytes

# ----------------------- #

_TOTALLY_ORDERED = (str, bytes, int, float, Decimal, date, time, timedelta, UUID)
"""Types whose ``<`` is a total order. A set's ``<`` means "subset", so disjoint sets compare
false both ways and ``sorted`` would keep their iteration order."""


def _totally_ordered(item: object) -> bool:
    if isinstance(item, tuple):
        return all(_totally_ordered(part) for part in item)

    return isinstance(item, _TOTALLY_ORDERED)


def sorted_set_items(value: Set[Any]) -> list[Any]:
    """*value*'s items in one order in every process: sorted when they are of a type ``<``
    orders totally (strings, numbers, dates, UUIDs, tuples of those), otherwise, or when
    they do not compare (mixed types), by their canonical JSON bytes.

    Iteration order is not an order: string hashing is seeded per process, so a set of
    strings iterates differently in the next interpreter. How a forze model writes a set in
    JSON, and how its hash reads one.
    """

    if all(_totally_ordered(item) for item in value):
        try:
            return sorted(value)

        except TypeError:
            pass

    return sorted(value, key=stable_json_bytes)
