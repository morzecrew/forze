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


class _PathNode:
    """One node of the projection trie: a requested segment and its descendants.

    ``terminal`` marks a path that ended here — the whole value is taken, subsuming any
    deeper children (so ``contract`` wins over ``contract.reg_number``). ``children`` holds
    the next segments, insertion-ordered so the output mirrors the requested field order.
    """

    __slots__ = ("terminal", "children")

    def __init__(self) -> None:
        self.terminal: bool = False
        self.children: dict[str, _PathNode] = {}


def _build_path_trie(fields: Sequence[str]) -> dict[str, _PathNode]:
    """Group dotted *fields* into a segment trie (order preserved, duplicates merged)."""

    root: dict[str, _PathNode] = {}

    for field in fields:
        children = root
        segments = field.split(".")
        last = len(segments) - 1

        for i, segment in enumerate(segments):
            node = children.get(segment)
            if node is None:
                node = _PathNode()
                children[segment] = node
            if i == last:
                node.terminal = True
            children = node.children

    return root


# ....................... #


_OMIT = object()
"""A projected value that resolved to nothing (a sub-field requested on a scalar)."""


def _project_value(value: Any, children: dict[str, _PathNode]) -> Any:
    """Reshape *value* to the sub-fields in *children*, mirroring its dict/list structure.

    A ``dict`` is pruned to the requested keys; a ``list`` maps the same selection over every
    element (preserving length, so positional alignment across multiple leaves holds); a scalar
    with sub-fields requested resolves to :data:`_OMIT`. An element that contributes no requested
    field becomes ``{}`` rather than dropping, matching the structure-preserving array shape.
    """

    if isinstance(value, list):
        out_list: list[Any] = []
        for element in value:  # pyright: ignore[reportUnknownVariableType]
            projected = _project_value(element, children)
            out_list.append({} if projected is _OMIT else projected)
        return out_list

    if isinstance(value, dict):
        out: JsonDict = {}
        for segment, node in children.items():
            if segment not in value:
                continue
            child_value = value[segment]  # pyright: ignore[reportUnknownVariableType]
            if node.terminal or not node.children:
                out[segment] = child_value
                continue
            sub = _project_value(child_value, node.children)
            if sub is _OMIT:
                continue
            # An empty *dict* from a non-list branch means nothing resolved — omit it (a
            # requested array leaf still yields its length-preserving list, kept above).
            if isinstance(sub, dict) and not sub:
                continue
            out[segment] = sub
        return out

    return _OMIT


def build_projection(doc: JsonDict, fields: Sequence[str] | None) -> JsonDict:
    """Reshape *doc* to only the requested (possibly dotted) projection *fields*.

    ``None`` *fields* returns a shallow copy of the whole document. Otherwise each path is
    resolved against *doc* into a **nested** output: ``contract.reg_number`` yields
    ``{"contract": {"reg_number": ...}}`` and sibling leaves merge under one ``contract``
    object. A dotted path that crosses a **list** maps the selection over each element,
    preserving structure and length — ``items.sku`` + ``items.qty`` over a list of items
    yields ``{"items": [{"sku": ..., "qty": ...}, ...]}``, and nested lists recurse.

    A whole top-level field that the row lacks is returned as ``None`` (the flat-projection
    contract the backends keep — it mirrors a Postgres ``NULL`` column, so every requested
    top-level field is present). A *nested* leaf that is absent is omitted (the key is not
    set), and a requested root subsumes its leaves.
    """

    if fields is None:
        return dict(doc)

    out: JsonDict = {}

    for segment, node in _build_path_trie(fields).items():
        if segment not in doc:
            # A requested whole top-level field stays present as None (flat contract); a
            # dotted path whose root is absent is omitted (the nested leaf "isn't defined").
            if node.terminal:
                out[segment] = None
            continue
        value = doc[segment]
        if node.terminal or not node.children:
            out[segment] = value
            continue
        projected = _project_value(value, node.children)
        if projected is _OMIT:
            continue
        if isinstance(projected, dict) and not projected:
            continue
        out[segment] = projected

    return out
