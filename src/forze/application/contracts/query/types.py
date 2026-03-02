"""Scalar and operator types for filter expressions."""

from datetime import date, datetime
from typing import Literal
from uuid import UUID

# ----------------------- #

Numeric = int | float | datetime | date
"""Numeric types for ordering operators."""

Scalar = Numeric | bool | str | UUID
"""Scalar value types for filter expressions."""

Array = list[Scalar]
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
