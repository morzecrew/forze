"""Numeric helpers shared across the application."""

from collections.abc import Iterable
from typing import Annotated, Any, TypeVar, final, get_args, get_origin

import attrs

# ----------------------- #

Numeric = TypeVar("Numeric", int, float)


@final
@attrs.define(slots=True, frozen=True)
class IntWidth:
    """How many bits an ``int`` field may need, read by a store that sizes its columns.

    Pydantic ignores it, so the value is validated as any ``int``. A store with fixed-width
    integers reads it to pick the width: forze's Postgres schema render emits ``bigint`` for
    64 bits and ``integer`` for an ``int`` without one.
    """

    bits: int
    """The width, in bits."""


Int64 = Annotated[int, IntWidth(64)]
"""An ``int`` that may need 64 bits, such as a packed HLC timestamp or a size in bytes."""


def int_width(annotation: Any, metadata: Iterable[Any] = ()) -> int | None:
    """The width, in bits, an :class:`IntWidth` states for a field, or ``None``.

    Pydantic moves an outer ``Annotated``'s items into a field's *metadata* and leaves its
    annotation a bare ``int``; inside ``Int64 | None`` the ``Annotated`` stays in the
    annotation. Pass both, and either form is read.
    """

    if get_origin(annotation) is Annotated:
        metadata = (*metadata, *get_args(annotation)[1:])

    for item in metadata:
        if isinstance(item, IntWidth):
            return item.bits

    return None


def clamp(value: Numeric, lo: Numeric, hi: Numeric) -> Numeric:
    """Constrain *value* to the inclusive ``[lo, hi]`` range.

    Returns the nearest legal value: *lo* when *value* falls below the range,
    *hi* when it rises above, and *value* unchanged when it already fits. This
    preserves caller intent (the closest permitted magnitude) rather than
    substituting an unrelated default. Works on ``int`` or ``float`` (the return
    type matches the argument type).

    Raises:
        ValueError: When ``lo > hi`` (an inverted, unsatisfiable range).
    """

    if lo > hi:
        raise ValueError(f"clamp bounds inverted: lo={lo} > hi={hi}")

    if value < lo:
        return lo

    if value > hi:
        return hi

    return value
