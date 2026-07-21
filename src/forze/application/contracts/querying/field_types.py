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

from collections.abc import Mapping
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from types import UnionType
from typing import Any, Final, Union, get_args, get_origin
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.base.exceptions import exc

from .capabilities import ALL_VALUE_OPS
from .internal.cast import QueryValueCaster
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
from .types import TreePath

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
_HIERARCHY: Final = "hierarchy"  # a TreePath materialized-path field
_UNKNOWN: Final = "unknown"


# Operator groups (mirrors :mod:`.types`; spelled out as runtime sets).
_EQ_OPS: Final[frozenset[str]] = frozenset({"$eq", "$neq"})
_ORD_OPS: Final[frozenset[str]] = frozenset({"$gt", "$gte", "$lt", "$lte"})
_TEXT_OPS: Final[frozenset[str]] = frozenset({"$like", "$ilike", "$regex"})
_MEMB_OPS: Final[frozenset[str]] = frozenset({"$in", "$nin"})
_SET_OPS: Final[frozenset[str]] = frozenset(
    {"$superset", "$subset", "$disjoint", "$overlaps"},
)
_HIERARCHY_OPS: Final[frozenset[str]] = frozenset({"$descendant_of", "$ancestor_of"})

# Operators valid on a field of any type — equality and the null check.
_UNIVERSAL_OPS: Final[frozenset[str]] = _EQ_OPS | frozenset({"$null"})

# Element quantifiers — available on (and only on) array/collection fields.
_QUANTIFIERS: Final[frozenset[str]] = frozenset({"$any", "$all", "$none"})


# Per-class allowed operators. ``$eq`` / ``$neq`` / ``$null`` are universal and omitted
# here (checked separately) so every class implicitly permits them.

_ALLOWED: Final[dict[str, frozenset[str]]] = {
    # No ``$ord`` on strings: this gate is THE guard — the parser admits ``str``
    # ordering operands (the JSON carrier for exact Decimal / datetime bounds), so
    # without this rejection ``name $gt "5"`` would compile to a text comparison.
    _STRING: _TEXT_OPS | _MEMB_OPS,
    _NUMBER: _ORD_OPS | _MEMB_OPS,
    _TEMPORAL: _ORD_OPS | _MEMB_OPS,
    _BOOL: _MEMB_OPS,
    _SCALAR_OTHER: _ORD_OPS | _MEMB_OPS,
    # ``$in`` / ``$nin`` on an array compile to overlap / disjoint (element-wise) on
    # every backend — ``unnest … = ANY`` (Postgres), ``$in`` on an array field (Mongo) —
    # so they are valid alongside the set operators.
    _COLLECTION: _SET_OPS | _MEMB_OPS | frozenset({"$empty"}),
    _MAPPING: frozenset(),
    _OBJECT: frozenset(),
    # A materialized path is still a string — keep text/membership available alongside
    # the hierarchy operators, which are valid *only* here (rejected on a plain string).
    _HIERARCHY: _TEXT_OPS | _MEMB_OPS | _HIERARCHY_OPS,
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
    _HIERARCHY: "a hierarchy path",
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

    # TreePath is a str subtype — match it before the generic string check so its
    # hierarchy operators resolve and a plain string keeps rejecting them.
    if issubclass(ann, TreePath):
        return _HIERARCHY

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
        if op in _UNIVERSAL_OPS:
            return  # universal

        cls = _classify(ann)

        if cls is _UNKNOWN:
            return  # type not resolvable — don't guess

        if op in _ALLOWED[cls]:
            return

        _fail(
            f"Operator {op!r} is not valid for field {path!r}, which is {_CLASS_LABEL[cls]}.",
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
                if nested.path == ELEM_SCALAR_FIELD:
                    # Scalar array-of-arrays: the element itself is the sub-array.
                    cls = _classify(elem_ann)

                    if cls not in (_COLLECTION, _UNKNOWN):
                        _fail(
                            f"Element quantifier {nested.quantifier!r} requires an "
                            f"array, but {array_path}[] is {_CLASS_LABEL[cls]}.",
                        )

                    deeper = _element_annotation(elem_ann)
                    deeper = deeper if deeper is not None else Any
                    _walk_elem_inner(
                        nested.inner,
                        elem_ann=deeper,
                        elem_model=_as_model(deeper),
                        array_path=f"{array_path}[]",
                    )

                else:
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


# ....................... #


def coerce_query_ord_operands(
    expr: QueryExpr,
    model_type: type[BaseModel] | None,
    *,
    field_type_hints: Mapping[str, type[Any]] | None = None,
) -> QueryExpr:
    """Cast JSON string ordering operands to the targeted field's scalar family.

    ``str`` is the JSON carrier for range bounds a JSON number cannot express exactly —
    an exact ``Decimal`` on a money column, an ISO datetime. Casting once here, keyed by
    the read model's annotation and through the same :class:`QueryValueCaster` every
    backend renders with, keeps the backends in lockstep: met raw at each backend's own
    seam, the string would compare as a string wherever a renderer has no field-type
    knowledge (the in-memory matcher, a Meilisearch literal) while Postgres casts and
    matches — the cross-backend divergence the parity harness exists to prevent.

    Non-ordering operators, non-string operands, and unresolvable fields pass through
    untouched (the backend caster stays the authority there); an unparseable string
    raises the caster's ``precondition``, the same refusal a backend cast produces.
    Returns a rebuilt tree — nodes are immutable — sharing every untouched branch.
    """

    if model_type is None:
        return expr

    hints = field_type_hints or {}

    def _coerced_value(op: str, ann: Any, value: Any) -> Any:
        if op not in _ORD_OPS or not isinstance(value, str):
            return value

        cls = _classify(ann)

        if cls == _NUMBER:
            # Decimal, not float: exactness is the whole reason the string form exists,
            # and every backend already renders a Decimal operand correctly.
            return QueryValueCaster.as_decimal(value)

        if cls == _TEMPORAL:
            base = _strip_optional(ann)

            if isinstance(base, type) and issubclass(base, datetime):
                return QueryValueCaster.as_datetime(value, force_tz=True)

            if isinstance(base, type) and issubclass(base, date):
                return QueryValueCaster.as_date(value)

            return QueryValueCaster.as_datetime(value, force_tz=True)

        if cls == _SCALAR_OTHER and _strip_optional(ann) is UUID:
            return QueryValueCaster.as_uuid(value)

        return value

    def _field(node: QueryField, ann: Any) -> QueryField:
        coerced = _coerced_value(node.op, ann, node.value)

        return node if coerced is node.value else attrs.evolve(node, value=coerced)

    def _walk_elem_inner(
        inner: QueryExpr,
        *,
        elem_ann: Any,
        elem_model: type[BaseModel] | None,
    ) -> QueryExpr:
        match inner:
            case QueryAnd(items):
                return QueryAnd(
                    tuple(
                        _walk_elem_inner(item, elem_ann=elem_ann, elem_model=elem_model)
                        for item in items
                    )
                )

            case QueryOr(items):
                return QueryOr(
                    tuple(
                        _walk_elem_inner(item, elem_ann=elem_ann, elem_model=elem_model)
                        for item in items
                    )
                )

            case QueryNot(item):
                return QueryNot(_walk_elem_inner(item, elem_ann=elem_ann, elem_model=elem_model))

            case QueryField(name, _op, _):
                if name == ELEM_SCALAR_FIELD:
                    return _field(inner, elem_ann)

                if elem_model is not None:
                    return _field(inner, _resolve_annotation(elem_model, name.split("."), hints))

                return inner

            case QueryElem() as nested:
                if nested.path == ELEM_SCALAR_FIELD:
                    deeper = _element_annotation(elem_ann)
                    deeper = deeper if deeper is not None else Any

                    return attrs.evolve(
                        nested,
                        inner=_walk_elem_inner(
                            nested.inner, elem_ann=deeper, elem_model=_as_model(deeper)
                        ),
                    )

                return _quantifier(nested, base_model=elem_model)

            case _:
                return inner

    def _quantifier(node: QueryElem, *, base_model: type[BaseModel] | None) -> QueryElem:
        if base_model is None:
            elem_ann: Any = Any

        else:
            field_ann = _resolve_annotation(base_model, node.path.split("."), hints)
            elem_ann = _element_annotation(field_ann)
            elem_ann = elem_ann if elem_ann is not None else Any

        return attrs.evolve(
            node,
            inner=_walk_elem_inner(node.inner, elem_ann=elem_ann, elem_model=_as_model(elem_ann)),
        )

    def _walk(node: QueryExpr) -> QueryExpr:
        match node:
            case QueryAnd(items):
                return QueryAnd(tuple(_walk(item) for item in items))

            case QueryOr(items):
                return QueryOr(tuple(_walk(item) for item in items))

            case QueryNot(item):
                return QueryNot(_walk(item))

            case QueryField(name, _op, _):
                return _field(node, _resolve_annotation(model_type, name.split("."), hints))

            case QueryElem() as elem:
                return _quantifier(elem, base_model=model_type)

            case _:
                return node

    return _walk(expr)


# ....................... #


def classify_field_type(annotation: Any) -> str:
    """Coarse type-class label for a field annotation (the inverse-facing classifier).

    One of ``"string"``, ``"number"``, ``"bool"``, ``"temporal"``, ``"scalar"``,
    ``"collection"``, ``"mapping"``, ``"object"``, or ``"unknown"`` (the last when the
    annotation can't be resolved to a concrete class). The same classification
    :func:`validate_query_field_types` uses, exposed for discovery surfaces.
    """

    return _classify(annotation)


# ....................... #


def field_value_operators(annotation: Any) -> frozenset[str]:
    """The filter value-operators valid on a field of type *annotation*.

    The inverse of the validator: instead of rejecting a bad pairing it enumerates the
    allowed one, for discovery (OpenAPI / MCP). Universal ops (``$eq``/``$neq``/``$null``)
    plus the per-class set; an unresolvable type reports the full value-op surface
    (:data:`~forze.application.contracts.querying.ALL_VALUE_OPS`), since the validator
    likewise places no constraint on it. Element quantifiers are reported separately
    (see :func:`is_quantifiable_field`), as they take a nested predicate, not a value.
    """

    cls = _classify(annotation)

    if cls is _UNKNOWN:
        return ALL_VALUE_OPS

    return _UNIVERSAL_OPS | _ALLOWED[cls]


# ....................... #


def is_quantifiable_field(annotation: Any) -> bool:
    """Whether *annotation* is an array/collection — i.e. element quantifiers apply."""

    return _classify(annotation) is _COLLECTION
