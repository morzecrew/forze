from datetime import date, datetime
from typing import Literal
from uuid import UUID

# ----------------------- #

Numeric = int | float | datetime | date
Scalar = Numeric | bool | str | UUID
Array = list[Scalar]

# ....................... #

UnaryOp = Literal["$null", "$empty"]
OrdOp = Literal["$gt", "$gte", "$lt", "$lte"]
EqOp = Literal["$eq", "$neq"]
MembOp = Literal["$in", "$nin"]
SetRelOp = Literal["$superset", "$subset", "$disjoint", "$overlaps"]

Op = EqOp | OrdOp | MembOp | UnaryOp | SetRelOp
