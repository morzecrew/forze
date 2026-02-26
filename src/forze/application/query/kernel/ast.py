import attrs

from ..api import Scalar, SeqScalar
from .operators import Op

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class Expr:
    """Base class for all AST nodes."""


@attrs.define(slots=True, frozen=True)
class And(Expr):
    items: tuple[Expr, ...]


@attrs.define(slots=True, frozen=True)
class Or(Expr):
    items: tuple[Expr, ...]


# ....................... #


@attrs.define(slots=True, frozen=True)
class Field(Expr):
    name: str
    op: Op
    value: Scalar | SeqScalar
