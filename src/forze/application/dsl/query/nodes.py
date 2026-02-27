import attrs

from forze.application.contracts.query import Array, Op, Scalar

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class Expr:
    """Base class for all AST nodes."""


@attrs.define(slots=True, frozen=True, match_args=True)
class And(Expr):
    items: tuple[Expr, ...]


@attrs.define(slots=True, frozen=True, match_args=True)
class Or(Expr):
    items: tuple[Expr, ...]


# ....................... #


@attrs.define(slots=True, frozen=True, match_args=True)
class Field(Expr):
    name: str
    op: Op
    value: Scalar | Array
