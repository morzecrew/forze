"""Scalar and operator types for filter expressions."""

from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

# ----------------------- #

Numeric = int | float | Decimal | datetime | date | UUID
"""Numeric types for ordering operators (includes :class:`~uuid.UUID` for keyset paging)."""

Scalar = Numeric | bool | str
"""Scalar value types for filter expressions."""

Array = Sequence[Scalar]
"""Array of scalars for membership and set operators."""

# ....................... #

UnaryOp = Literal["$null", "$empty"]
"""Unary operators for null/empty checks."""

OrdOp = Literal["$gt", "$gte", "$lt", "$lte"]
"""Ordering operators."""

EqOp = Literal["$eq", "$neq"]
"""Equality operators."""

CompareOp = Literal["$eq", "$neq", "$gt", "$gte", "$lt", "$lte"]
"""Field-to-field compare operators (equality and ordering only)."""

TextOp = Literal["$like", "$ilike", "$regex"]
"""Text pattern matching operators."""

TextPatternValue = str | Sequence[str]
"""Text operator operand: one pattern or several (OR semantics at parse time)."""

QueryElementQuantifier = Literal["$any", "$all", "$none"]
"""Array element quantifier operators under ``$values``."""

MembOp = Literal["$in", "$nin"]
"""Membership operators."""

ElementOp = EqOp | OrdOp | TextOp | MembOp
"""Operators allowed inside array element quantifiers (``$any``, ``$all``, ``$none``):
comparison, ordering, text patterns, and membership (``$in`` / ``$nin``)."""

SetRelOp = Literal["$superset", "$subset", "$disjoint", "$overlaps"]
"""Set relation operators."""

HierarchyOp = Literal["$descendant_of", "$ancestor_of"]
"""Hierarchy (materialized-path) operators on a :class:`TreePath` field.

``$descendant_of`` keeps rows whose path is *at or below* the given node; ``$ancestor_of``
keeps rows whose path is *at or above* it (the row *has* the given node as a descendant).
Both are **inclusive** (a node is its own ancestor/descendant). Backend-specific
(Postgres ``ltree`` / materialized-path prefix); capability-gated."""

HierarchyValue = str | Sequence[str]
"""Hierarchy operand: one path, or several (``OR`` / "any" semantics)."""

Op = EqOp | OrdOp | MembOp | UnaryOp | SetRelOp | TextOp | HierarchyOp
"""All supported filter operators."""

# ....................... #


class TreePath(str):
    """A materialized hierarchy path (dot-separated labels, e.g. ``"top.science.math"``).

    A marker ``str`` subtype: type a read-model field as ``TreePath`` to make the
    hierarchy operators (:data:`HierarchyOp`) valid on it. Stored as ``ltree`` or plain
    text in Postgres; the operators render per backend and are rejected by backends that
    declare no hierarchy support."""

    __slots__ = ()

    # ....................... #

    @classmethod
    def __get_pydantic_core_schema__(cls, source: Any, handler: Any) -> Any:
        """Validate as a plain string, then wrap in :class:`TreePath`.

        Lets read-models annotate a field as ``TreePath`` directly; Pydantic treats it as
        a ``str`` for validation/serialization while the query layer still sees the marker
        subtype to enable the hierarchy operators.
        """

        from pydantic_core import core_schema

        return core_schema.no_info_after_validator_function(
            cls,
            core_schema.str_schema(),
        )


# ....................... #
# Shit below is only for annotations and short imports


class QueryOp:
    """Namespace grouping all filter-operator type aliases."""

    Unary = UnaryOp
    Ord = OrdOp
    Eq = EqOp
    Compare = CompareOp
    Element = ElementOp
    Memb = MembOp
    SetRel = SetRelOp
    Text = TextOp
    Hierarchy = HierarchyOp
    All = Op


# ....................... #


class QueryValue:
    """Namespace grouping all filter-value type aliases."""

    Scalar = Scalar
    Array = Array
    Numeric = Numeric
