"""Operator/type compatibility: reject nonsensical operator–field pairings up front.

Capability validation (:mod:`.capabilities`) answers *can this backend compile the
operator at all*; this module answers a different, backend-independent question: *does
the operator make sense for the field it targets*. ``$like`` on an integer, ``$gt`` on a
boolean, ``$superset`` on a scalar, an element quantifier on a non-array — each is a
caller mistake that, left alone, becomes a runtime type error deep in a backend (a 500)
or, worse, silently matches nothing.

:func:`validate_query_field_types` resolves each referenced field's Python type from the
read model and checks it against the operator, raising a clean
:func:`~forze.base.exceptions.exc.precondition` (code
:data:`OPERATOR_TYPE_MISMATCH_CODE`) when they are incompatible. It is **best-effort by
design**: a field whose type cannot be resolved (dynamic ``Any``, an ambiguous union, a
path the model does not describe) is skipped rather than guessed at, so the check never
produces a false rejection. Field *existence* and per-aggregate allow-sets are enforced
elsewhere (:mod:`.field_policy`); this only concerns the operator–type fit of fields that
do resolve.
"""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from types import UnionType
from typing import Any, Final, Mapping, Union, get_args, get_origin
from uuid import UUID

from pydantic import BaseModel

from forze.base.exceptions import exc

from .internal.nodes import (
    ELEM_SCALAR_FIELD,
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryNot,
    QueryOr,
)

# ----------------------- #

OPERATOR_TYPE_MISMATCH_CODE: Final[str] = "query_operator_type_mismatch"
"""Error code raised when an operator is applied to an incompatible field type."""


# Type classes — coarse buckets a field's Python type maps to, each with a fixed set of
# operators that make sense for it. ``UNKNOWN`` means "could not resolve" → skip checks.

_STRING: Final = "string"
_NUMBER: Final = "number"
_BOOL: Final = "bool"
_TEMPORAL: Final = "temporal"
_SCALAR_OTHER: Final = "scalar"  # UUID, Enum, and other opaque scalars
_COLLECTION: Final = "collection"
_MAPPING: Final = "mapping"
_OBJECT: Final = "object"  # a nested model used as a leaf
_UNKNOWN: Final = "unknown"


# Operator groups (mirrors :mod:`.types`; spelled out as runtime sets).
_EQ_OPS: Final[frozenset[str]] = frozenset({"$eq", "$neq"})
_ORD_OPS: Final[frozenset[str]] = frozenset({"$gt", "$gte", "$lt", "$lte"})
_TEXT_OPS: Final[frozenset[str]] = frozenset({"$like", "$ilike", "$regex"})
_MEMB_OPS: Final[frozenset[str]] = frozenset({"$in", "$nin"})
_SET_OPS: Final[frozenset[str]] = frozenset(
    {"$superset", "$subset", "$disjoint", "$overlaps"},
)


# Per-class allowed operators. ``$eq`` / ``$neq`` / ``$null`` are universal and omitted
# here (checked separately) so every class implicitly permits them.

_ALLOWED: Final[dict[str, frozenset[str]]] = {
    # No ``$ord`` on strings: the parser only accepts numeric/temporal ordering
    # operands (``Numeric``), so ``name $gt 5`` would compile to ``text > number``
    # and fail in the backend — reject it here instead.
    _STRING: _TEXT_OPS | _MEMB_OPS,
    _NUMBER: _ORD_OPS | _MEMB_OPS,
    _TEMPORAL: _ORD_OPS | _MEMB_OPS,
    _BOOL: _MEMB_OPS,
    _SCALAR_OTHER: _ORD_OPS | _MEMB_OPS,
    _COLLECTION: _SET_OPS | frozenset({"$empty"}),
    _MAPPING: frozenset(),
    _OBJECT: frozenset(),
}

_CLASS_LABEL: Final[dict[str, str]] = {
    _STRING: "a string",
    _NUMBER: "a number",
    _BOOL: "a boolean",
    _TEMPORAL: "a date/time",
    _SCALAR_OTHER: "a scalar",
    _COLLECTION: "an array",
    _MAPPING: "a mapping",
    _OBJECT: "a nested object",
}


# ....................... #


def _strip_optional(ann: Any) -> Any:
    """Return *ann* with ``None`` removed from a union, or ``Any`` if still ambiguous.

    ``Optional[int]`` → ``int``; ``int | str`` → ``Any`` (a genuine union we won't
    second-guess); a plain annotation passes through.
    """

    origin = get_origin(ann)

    if origin is Union or origin is UnionType:
        members = [a for a in get_args(ann) if a is not type(None)]

        if len(members) == 1:
            return members[0]

        return Any

    return ann


def _as_model(ann: Any) -> type[BaseModel] | None:
    """The :class:`BaseModel` subclass *ann* denotes (after optional-stripping), else None."""

    ann = _strip_optional(ann)

    if isinstance(ann, type) and issubclass(ann, BaseModel):
        return ann

    return None


def _element_annotation(ann: Any) -> Any:
    """Element annotation of a homogeneous collection type, or ``None`` if not a collection."""

    ann = _strip_optional(ann)
    origin = get_origin(ann)

    if origin in (list, set, frozenset, tuple):
        args = get_args(ann)

        if not args:
            return Any

        if origin is tuple and not (len(args) == 2 and args[1] is Ellipsis):
            # A fixed-length heterogeneous tuple — no single element type.
            return Any

        return args[0]

    return None


def _classify(ann: Any) -> str:
    """Map a (resolved) Python annotation to a coarse type class for operator checks."""

    ann = _strip_optional(ann)

    if ann is Any or ann is None:
        return _UNKNOWN

    origin = get_origin(ann)

    if origin in (list, set, frozenset, tuple):
        return _COLLECTION

    if origin in (dict, Mapping) or ann is dict:
        return _MAPPING

    if origin is not None:
        # Some other parametrized generic we don't model — don't guess.
        return _UNKNOWN

    if not isinstance(ann, type):
        return _UNKNOWN

    # bool is a subclass of int — test it first.
    if issubclass(ann, bool):
        return _BOOL

    if issubclass(ann, (int, float, Decimal)):
        return _NUMBER

    if issubclass(ann, str):
        return _STRING

    if issubclass(ann, (datetime, date, time)):
        return _TEMPORAL

    if issubclass(ann, (UUID, Enum)):
        return _SCALAR_OTHER

    if issubclass(ann, BaseModel):
        return _OBJECT

    return _UNKNOWN


# ....................... #


def _resolve_annotation(
    model_type: type[BaseModel],
    segments: list[str],
    hints: Mapping[str, type[Any]],
) -> Any:
    """Resolve the leaf annotation for a dotted field path, or ``Any`` if not walkable.

    Descends nested models segment by segment. Returns ``Any`` (→ skipped) for any path
    the model does not fully describe, so an unresolved path never triggers a rejection.
    """

    path = ".".join(segments)

    if path in hints:
        return hints[path]

    cur: type[BaseModel] | None = model_type
    ann: Any = Any

    for seg in segments:
        if cur is None:
            return Any

        field = cur.model_fields.get(seg)

        if field is None:
            return Any

        ann = field.annotation
        cur = _as_model(ann)

    return ann


# ....................... #


def validate_query_field_types(
    expr: QueryExpr,
    model_type: type[BaseModel] | None,
    *,
    field_type_hints: Mapping[str, type[Any]] | None = None,
) -> None:
    """Raise if any operator is incompatible with the type of the field it targets.

    Walks the parsed filter AST; for each field predicate it resolves the field's Python
    type from *model_type* (falling back to *field_type_hints* for paths the model leaves
    ambiguous) and checks the operator against it, raising
    :func:`~forze.base.exceptions.exc.precondition` (code
    :data:`OPERATOR_TYPE_MISMATCH_CODE`) on a mismatch. Unresolvable types are skipped.

    A no-op when *model_type* is ``None``.
    """

    if model_type is None:
        return

    hints = field_type_hints or {}

    def _fail(detail: str) -> None:
        raise exc.precondition(detail, code=OPERATOR_TYPE_MISMATCH_CODE)

    def _check_op(op: str, ann: Any, *, path: str) -> None:
        if op in _EQ_OPS or op == "$null":
            return  # universal

        cls = _classify(ann)

        if cls is _UNKNOWN:
            return  # type not resolvable — don't guess

        if op in _ALLOWED[cls]:
            return

        _fail(
            f"Operator {op!r} is not valid for field {path!r}, which is "
            f"{_CLASS_LABEL[cls]}.",
        )

    def _walk_elem_inner(
        inner: QueryExpr,
        *,
        elem_ann: Any,
        elem_model: type[BaseModel] | None,
        array_path: str,
    ) -> None:
        match inner:
            case QueryAnd(items) | QueryOr(items):
                for item in items:
                    _walk_elem_inner(
                        item,
                        elem_ann=elem_ann,
                        elem_model=elem_model,
                        array_path=array_path,
                    )

            case QueryNot(item):
                _walk_elem_inner(
                    item,
                    elem_ann=elem_ann,
                    elem_model=elem_model,
                    array_path=array_path,
                )

            case QueryField(name, op, _):
                if name == ELEM_SCALAR_FIELD:
                    # Predicate on the scalar element itself.
                    _check_op(op, elem_ann, path=f"{array_path}[]")

                elif elem_model is not None:
                    ann = _resolve_annotation(elem_model, name.split("."), hints)
                    _check_op(op, ann, path=f"{array_path}[].{name}")

            case QueryElem() as nested:
                _walk_quantifier(nested, base_model=elem_model, prefix=f"{array_path}[].")

            case _:
                pass

    def _walk_quantifier(
        node: QueryElem,
        *,
        base_model: type[BaseModel] | None,
        prefix: str,
    ) -> None:
        array_path = f"{prefix}{node.path}"

        if base_model is None:
            # Can't resolve the array field's type — still descend, but element types
            # are unknown so inner scalar/object checks will skip.
            _walk_elem_inner(
                node.inner,
                elem_ann=Any,
                elem_model=None,
                array_path=array_path,
            )
            return

        field_ann = _resolve_annotation(base_model, node.path.split("."), hints)
        cls = _classify(field_ann)

        if cls not in (_COLLECTION, _UNKNOWN):
            _fail(
                f"Element quantifier {node.quantifier!r} requires an array field, but "
                f"{array_path!r} is {_CLASS_LABEL[cls]}.",
            )

        elem_ann = _element_annotation(field_ann)
        elem_ann = elem_ann if elem_ann is not None else Any

        _walk_elem_inner(
            node.inner,
            elem_ann=elem_ann,
            elem_model=_as_model(elem_ann),
            array_path=array_path,
        )

    def _walk(node: QueryExpr) -> None:
        match node:
            case QueryAnd(items) | QueryOr(items):
                for item in items:
                    _walk(item)

            case QueryNot(item):
                _walk(item)

            case QueryField(name, op, _):
                ann = _resolve_annotation(model_type, name.split("."), hints)
                _check_op(op, ann, path=name)

            case QueryCompare(left, op, right):
                left_ann = _resolve_annotation(model_type, left.split("."), hints)
                right_ann = _resolve_annotation(model_type, right.split("."), hints)
                _check_op(op, left_ann, path=left)
                _check_op(op, right_ann, path=right)

            case QueryElem() as node:
                _walk_quantifier(node, base_model=model_type, prefix="")

            case _:
                pass

    _walk(expr)
