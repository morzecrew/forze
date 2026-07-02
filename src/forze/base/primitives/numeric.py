"""Numeric helpers shared across the application."""

# ----------------------- #


def clamp(value: int, lo: int, hi: int) -> int:
    """Constrain *value* to the inclusive ``[lo, hi]`` range.

    Returns the nearest legal value: *lo* when *value* falls below the range,
    *hi* when it rises above, and *value* unchanged when it already fits. This
    preserves caller intent (the closest permitted magnitude) rather than
    substituting an unrelated default.

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
