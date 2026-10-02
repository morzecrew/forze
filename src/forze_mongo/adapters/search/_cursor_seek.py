"""Keyset seek conditions for Mongo search cursor pagination."""

from __future__ import annotations

from typing import Any

from forze.base.primitives import JsonDict
from forze.domain.constants import ID_FIELD

# ----------------------- #


def _storage_field(field: str) -> str:
    return "_id" if field == ID_FIELD else field


def _past(field: str, direction: str, value: Any, *, after: bool) -> JsonDict | None:
    """Rows strictly past *value* on one key, a null being the smallest value.

    Mongo compares only within a type, so ``$gt``/``$lt`` never match a null: a walk past the
    last null, or toward them, needs them named. ``None`` when no row lies that side.
    """

    sf = _storage_field(field)
    larger = after == (direction == "asc")

    if value is None:
        return {sf: {"$ne": None}} if larger else None

    if larger:
        return {sf: {"$gt": value}}

    return {"$or": [{sf: {"$lt": value}}, {sf: None}]}


def build_keyset_sort_spec(
    key_spec: list[tuple[str, str]],
    *,
    flip: bool,
) -> JsonDict:
    """Mongo ``$sort`` spec over the keyset key order; *flip* inverts every key.

    A flipped spec fetches a ``before`` page in descending-from-cursor order, so the
    ``limit + 1`` window is anchored at the cursor (the rows nearest it) instead of at
    the start of the result set.
    """

    out: JsonDict = {}

    for field, direction in key_spec:
        forward = 1 if direction == "asc" else -1
        out[_storage_field(field)] = -forward if flip else forward

    return out


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
        past = _past(field, direction, values[i], after=after)

        if past is None:
            continue

        branch: JsonDict = {_storage_field(f): values[j] for j, (f, _) in enumerate(key_spec[:i])}
        branches.append({"$and": [branch, past]} if branch else past)

    return {"$or": branches} if branches else {"_id": {"$exists": False}}
