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

MembOp = Literal["$in", "$nin"]
"""Membership operators."""

SetRelOp = Literal["$superset", "$subset", "$disjoint", "$overlaps"]
"""Set relation operators."""

Op = EqOp | OrdOp | MembOp | UnaryOp | SetRelOp
"""All supported filter operators."""

# ....................... #
# Shit below is only for annotations and short imports


class QueryOp:
    """Namespace grouping all filter-operator type aliases."""

    Unary = UnaryOp
    Ord = OrdOp
    Eq = EqOp
    Memb = MembOp
    SetRel = SetRelOp
    All = Op


# ....................... #


class QueryValue:
    """Namespace grouping all filter-value type aliases."""

    Scalar = Scalar
    Array = Array
    Numeric = Numeric
