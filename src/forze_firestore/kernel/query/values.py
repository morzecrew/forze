"""Coercion of domain values to Firestore-encodable types (writes and filters)."""

from __future__ import annotations

import math
from decimal import Decimal
from typing import Any
from uuid import UUID

from forze.base.exceptions import exc

# ----------------------- #


def coerce_firestore_value(value: Any) -> Any:
    """Recursively coerce domain values to types the Firestore client can encode.

    A ``UUID`` becomes its canonical string (the representation document ids and
    ``tenant_id`` stamps already use). A ``Decimal`` becomes a ``float`` — Firestore has
    no decimal type, and a stringified decimal would compare lexically in range filters;
    ``double`` keeps numeric ordering at the cost of binary-float precision. Writes and
    filter values go through the same coercion so stored and compared values match.

    Non-finite numerics (``NaN``/``±Infinity``, native or produced by a ``Decimal``
    overflowing the double range) are refused: a stored non-finite double can never be
    matched by a filter — every filter seam already refuses non-finite operands — so
    persisting one would corrupt the system of record with an unqueryable value.
    """

    if isinstance(value, UUID):
        return str(value)

    if isinstance(value, Decimal):
        if not value.is_finite():
            raise exc.precondition(f"Non-finite numeric not allowed: {value!r}")

        result = float(value)

        if not math.isfinite(result):
            raise exc.precondition(f"Numeric exceeds the Firestore double range: {value!r}")

        return result

    if isinstance(value, float) and not math.isfinite(value):
        raise exc.precondition(f"Non-finite float not allowed: {value!r}")

    if isinstance(value, list):
        return [
            coerce_firestore_value(x)
            for x in value  # pyright: ignore[reportUnknownVariableType]
        ]

    if isinstance(value, dict):
        return {
            k: coerce_firestore_value(v)
            for k, v in value.items()  # pyright: ignore[reportUnknownVariableType]
        }

    return value
