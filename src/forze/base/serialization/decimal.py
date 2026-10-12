"""The JSON text of a ``Decimal``."""

from decimal import Decimal

# ----------------------- #

FIXED_POINT_EXPONENT = 100
"""The largest exponent, either way, a ``Decimal`` is written out in fixed point for; past
it the fixed-point text would run long (``1E+999999999`` to a billion characters)."""


def decimal_text(value: Decimal) -> str:
    """*value* in fixed point (``0.00000000`` rather than ``0E-8``), unless its exponent is
    past :data:`FIXED_POINT_EXPONENT`; then, and for a non-finite value, as ``str`` writes it.

    How a forze model writes a ``Decimal`` field in JSON.
    """

    exponent = value.as_tuple().exponent

    if isinstance(exponent, int) and abs(exponent) <= FIXED_POINT_EXPONENT:
        return format(value, "f")

    return str(value)
