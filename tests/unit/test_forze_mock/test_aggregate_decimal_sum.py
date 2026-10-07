"""The mock's ``$sum`` keeps ``Decimal`` totals exact, as Postgres sums ``numeric``."""

from decimal import Decimal

import pytest

from forze_mock.query.matching import _aggregate_docs  # pyright: ignore[reportPrivateUsage]

pytestmark = pytest.mark.unit

_SUM = {"$computed": {"total": {"$sum": "amount"}}}


def test_a_total_past_the_default_context_precision_stays_exact() -> None:
    """Thirty integer digits plus nine fractional ones: the default 28-digit context rounds."""

    rows = _aggregate_docs(
        [{"amount": Decimal("1" * 30)}, {"amount": Decimal("0.000000001")}, {"amount": 2}],
        _SUM,
    )

    assert rows == [{"total": Decimal("1" * 29 + "3.000000001")}]


def test_floats_and_an_empty_set_keep_their_old_shape() -> None:
    assert _aggregate_docs([{"amount": 1.5}, {"amount": Decimal("1")}], _SUM) == [{"total": 2.5}]
    assert _aggregate_docs([], _SUM) == [{"total": None}]
