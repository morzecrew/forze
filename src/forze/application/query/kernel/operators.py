from typing import Literal

# ----------------------- #

EqOp = Literal["$eq", "$neq"]
OrdOp = Literal["$gt", "$gte", "$lt", "$lte"]
MembOp = Literal["$in", "$nin"]
UnaryOp = Literal["$null", "$empty"]
SetRelOp = Literal["$superset", "$subset", "$disjoint", "$overlaps"]

Op = EqOp | OrdOp | MembOp | UnaryOp | SetRelOp
