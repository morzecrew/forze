"""Finiteness/magnitude guard on the shared Firestore value coercion.

``coerce_firestore_value`` is the single seam both writes and filter values pass
through. A non-finite ``Decimal``/``float`` — or a finite ``Decimal`` that overflows
the double range — must be refused here: filters refuse non-finite operands at the
shared query seam, so persisting such a value would put an unqueryable ``inf``/``nan``
into the system of record. In-range precision loss stays the documented trade-off of
the decimal→double representation.
"""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest

from forze.base.exceptions import CoreException, ExceptionKind
from forze_firestore.kernel.query.values import coerce_firestore_value


@pytest.mark.parametrize(
    "value",
    [
        Decimal("NaN"),
        Decimal("sNaN"),
        Decimal("Infinity"),
        Decimal("-Infinity"),
        float("nan"),
        float("inf"),
        float("-inf"),
    ],
)
def test_non_finite_numeric_refused(value: object) -> None:
    with pytest.raises(CoreException) as ei:
        coerce_firestore_value(value)
    assert ei.value.kind is ExceptionKind.PRECONDITION


@pytest.mark.parametrize(
    "value",
    [Decimal("1e400"), Decimal("-1e400"), Decimal("9" * 400)],
)
def test_decimal_overflowing_double_range_refused(value: Decimal) -> None:
    """A finite Decimal that ``float()`` turns into ``±inf`` must not be written."""

    with pytest.raises(CoreException) as ei:
        coerce_firestore_value(value)
    assert ei.value.kind is ExceptionKind.PRECONDITION


@pytest.mark.parametrize(
    "container",
    [
        [Decimal("NaN")],
        {"total": Decimal("Infinity")},
        {"nested": {"deep": [Decimal("1e400")]}},
    ],
)
def test_guard_applies_recursively(container: object) -> None:
    """Non-finite values nested in list/dict payloads are refused, not passed through."""

    with pytest.raises(CoreException) as ei:
        coerce_firestore_value(container)
    assert ei.value.kind is ExceptionKind.PRECONDITION


def test_finite_values_coerce_as_before() -> None:
    uid = uuid4()
    out = coerce_firestore_value(
        {"id": uid, "total": Decimal("19.99"), "ratio": 0.5, "tags": [Decimal("2")]}
    )
    assert out == {"id": str(uid), "total": 19.99, "ratio": 0.5, "tags": [2.0]}


def test_in_range_precision_loss_still_allowed() -> None:
    """Rounding within the double range is the representation's documented cost —
    only magnitude overflow and non-finite values are refused."""

    out = coerce_firestore_value(Decimal("12345678901234567890"))
    assert out == pytest.approx(1.2345678901234567e19)
