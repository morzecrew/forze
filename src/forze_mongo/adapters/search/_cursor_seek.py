"""Keyset seek conditions for Mongo search cursor pagination."""

from __future__ import annotations

from typing import Any

from forze.base.primitives import JsonDict
from forze.domain.constants import ID_FIELD

# ----------------------- #


def _storage_field(field: str) -> str:
    return "_id" if field == ID_FIELD else field


def _cmp_op(direction: str, *, after: bool) -> str:
    """Comparison operator for keyset seek on one column."""

    asc = direction == "asc"

    if after:
        return "$gt" if asc else "$lt"

    return "$lt" if asc else "$gt"


def build_keyset_seek_match(
    key_spec: list[tuple[str, str]],
    values: list[Any],
    *,
    after: bool,
) -> JsonDict:
    """Build a Mongo ``$match`` expression for keyset pagination.

    Uses a disjunction of prefix-equal branches (standard composite keyset).
    """

    branches: list[JsonDict] = []

    for i, (field, direction) in enumerate(key_spec):
        branch: JsonDict = {}
        op = _cmp_op(direction, after=after)
        sf = _storage_field(field)

        for j in range(i):
            prev_field, _ = key_spec[j]
            branch[_storage_field(prev_field)] = values[j]

        branch[sf] = {op: values[i]}
        branches.append(branch)

    return {"$or": branches}
