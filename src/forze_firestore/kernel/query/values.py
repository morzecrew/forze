"""Coercion of domain values to Firestore-encodable types (writes and filters)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import UUID

# ----------------------- #


def coerce_firestore_value(value: Any) -> Any:
    """Recursively coerce domain values to types the Firestore client can encode.

    A ``UUID`` becomes its canonical string (the representation document ids and
    ``tenant_id`` stamps already use). A ``Decimal`` becomes a ``float`` — Firestore has
    no decimal type, and a stringified decimal would compare lexically in range filters;
    ``double`` keeps numeric ordering at the cost of binary-float precision. Writes and
    filter values go through the same coercion so stored and compared values match.
    """

    if isinstance(value, UUID):
        return str(value)

    if isinstance(value, Decimal):
        return float(value)

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
