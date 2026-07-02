"""Numeric helpers shared across the application."""

from typing import TypeVar

# ----------------------- #

Numeric = TypeVar("Numeric", int, float)


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
