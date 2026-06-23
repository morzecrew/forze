"""Value objects returned by the procedures port."""

from typing import Generic, TypeVar

import attrs

# ----------------------- #

Out = TypeVar("Out")

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ExecResult(Generic[Out]):
    """Result of one procedure execution.

    Narrow by design: a procedure returns a scalar, a single typed row, or
    an affected-row count — never a page of rows (that is analytics). Which field is set is
    driven by the spec's declared ``result`` cardinality:

    - ``result`` is a Pydantic model  -> :attr:`value` carries the single row.
    - ``result`` is a scalar type      -> :attr:`value` carries the scalar.
    - ``result`` is ``None``           -> side-effect only; :attr:`affected_count` carries the
      rows touched (when the backend reports it).
    """

    value: Out | None = attrs.field(default=None)
    """Scalar or single typed row returned by the procedure; ``None`` for a side-effect-only op."""

    affected_count: int | None = attrs.field(default=None)
    """Rows affected by a side-effect/DML procedure; ``None`` when the backend reports no count."""
