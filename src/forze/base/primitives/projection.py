"""Dotted-path projection primitives, shared across every layer.

Field projection (selecting a subset of fields to return) accepts **dotted paths**:
``contract.reg_number`` projects only that nested leaf and reshapes the output to
``{"contract": {"reg_number": ...}}``. Filter and sort already resolve dotted paths the
same way; projection is the third axis to do so.

These helpers are pure (plain ``dict``/``str`` work, no model or codec knowledge) so they
live in ``forze.base`` and can be reused by every consumer that must agree on the projected
shape: the base serialization row-materializer, the in-memory mock adapter (the cross-backend
parity oracle), the Postgres post-fetch reshape, and the Mongo whole-document trim path. A
single implementation is what keeps those shapes identical — a second copy could silently
diverge a backend from the oracle.
"""

from typing import Any, Sequence

from .types import JsonDict

# ----------------------- #

MISSING = object()
"""Sentinel for an absent path — distinct from a present ``None``."""


# ....................... #


def path_get(obj: Any, path: str) -> Any:
    """The value at a dotted *path* in *obj*, or :data:`MISSING` when any segment is absent.

    Traverses ``dict`` nodes only: a non-mapping intermediate (or a missing key) reads as
    :data:`MISSING`, so a present ``None`` and an absent field stay distinguishable.
    """

    cur = obj

    for part in path.split("."):
        if isinstance(cur, dict):
            if part not in cur:
                return MISSING

            cur = cur[part]  # pyright: ignore[reportUnknownVariableType]
            continue

        return MISSING

    return cur  # pyright: ignore[reportUnknownVariableType]


# ....................... #


def projection_roots(fields: Sequence[str]) -> tuple[str, ...]:
    """The deduped root columns a projection must fetch, order preserved.

    A dotted projection path contributes its **root** segment (``contract.reg_number`` →
    ``contract``): a backend selects the whole root column (the JSONB document / Mongo field)
    and :func:`build_projection` reshapes the requested nested leaves out of it afterwards.
    This mirrors how cursor keyset SELECTs reduce nested sort keys to their root column.
    """

    return tuple(dict.fromkeys(field.split(".", 1)[0] for field in fields))


# ....................... #


def _is_prefix_path(ancestor: list[str], path: list[str]) -> bool:
    """Whether *ancestor* is a strict label-prefix of *path* (``a.b`` of ``a.b.c``)."""

    return len(ancestor) < len(path) and path[: len(ancestor)] == ancestor


# ....................... #


def _retained_projection_paths(fields: Sequence[str]) -> list[str]:
    """Dedup *fields* and drop any path subsumed by a requested ancestor.

    Requesting both a root and one of its leaves (``contract`` and ``contract.reg_number``)
    keeps only ``contract`` — the whole object wins over the narrower leaf. Order is preserved.
    """

    unique: list[str] = []
    for field in fields:
        if field not in unique:
            unique.append(field)

    split = {field: field.split(".") for field in unique}

    return [
        field
        for field in unique
        if not any(
            other != field and _is_prefix_path(split[other], split[field])
            for other in unique
        )
    ]


# ....................... #


def _set_path(out: JsonDict, labels: Sequence[str], value: Any) -> None:
    cur = out

    for label in labels[:-1]:
        nxt = cur.get(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            label
        )

        if not isinstance(nxt, dict):
            nxt = {}
            cur[label] = nxt
        cur = nxt  # pyright: ignore[reportUnknownVariableType]

    cur[labels[-1]] = value


# ....................... #


def build_projection(doc: JsonDict, fields: Sequence[str] | None) -> JsonDict:
    """Reshape *doc* to only the requested (possibly dotted) projection *fields*.

    ``None`` *fields* returns a shallow copy of the whole document. Otherwise each path is
    resolved against *doc* and written back into a **nested** output, so ``contract.reg_number``
    yields ``{"contract": {"reg_number": ...}}`` and sibling leaves (``contract.reg_number`` +
    ``contract.signed_at``) merge under one ``contract`` object. A path whose value is absent is
    skipped (the key is omitted, not set to ``None``); a requested root subsumes its leaves.
    """

    if fields is None:
        return dict(doc)

    out: JsonDict = {}

    for path in _retained_projection_paths(fields):
        value = path_get(doc, path)

        if value is MISSING:
            continue

        _set_path(out, path.split("."), value)

    return out
