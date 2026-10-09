"""The order a set is written in."""

from collections.abc import Set
from typing import Any

from forze.base.primitives import stable_json_bytes

# ----------------------- #


def sorted_set_items(value: Set[Any]) -> list[Any]:
    """*value*'s items in one order in every process: sorted, or, for items that do not
    compare (a set of mixed types), by their canonical JSON bytes.

    Iteration order is not an order: string hashing is seeded per process, so a set of
    strings iterates differently in the next interpreter. How a forze model writes a set in
    JSON, and how its hash reads one.
    """

    try:
        return sorted(value)

    except TypeError:
        return sorted(value, key=stable_json_bytes)
