"""Scalar and operator types for filter expressions."""

from datetime import date, datetime
from typing import Literal, Sequence
from uuid import UUID

# ----------------------- #

Numeric = int | float | datetime | date | UUID
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

Op = EqOp | OrdOp | MembOp | UnaryOp | SetRelOp | TextOp
"""All supported filter operators."""

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
    All = Op


# ....................... #


class QueryValue:
    """Namespace grouping all filter-value type aliases."""

    Scalar = Scalar
    Array = Array
    Numeric = Numeric
