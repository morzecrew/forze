"""Shared fixtures for the analytics resolver differential (``compare_analytics_query``).

One document spec, one analytics query over it, the SQL that answers the query on Postgres, and
two mock resolvers standing in for that SQL: one that agrees with it and one that has drifted
(it forgot the ``HAVING``). The unit suite runs the differential mock-against-mock; the Postgres
leg runs the same plan and query against the real engine.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsQueryDefinition, AnalyticsSpec
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockAnalyticsView
from forze_mock.seeding import SeedPlan, spec_seed

# ----------------------- #


class Order(Document):
    customer: str
    amount: int


class OrderCreate(CreateDocumentCmd):
    customer: str
    amount: int


class OrderUpdate(BaseDTO):
    amount: int | None = None


class OrderRead(ReadDocument):
    customer: str
    amount: int


ORDERS = DocumentSpec(
    name="orders",
    read=OrderRead,
    write=DocumentWriteTypes(domain=Order, create_cmd=OrderCreate, update_cmd=OrderUpdate),
)


class CustomerTotal(BaseModel):
    customer: str
    total: int


class MinTotal(BaseModel):
    min_total: int = 0


TOTALS = AnalyticsSpec(
    name="order_totals",
    read=CustomerTotal,
    queries={"by_customer": AnalyticsQueryDefinition(params=MinTotal)},
)

PLAN = SeedPlan(
    specs=(
        spec_seed(
            ORDERS,
            fixtures=[
                {"customer": "ada", "amount": 10},
                {"customer": "ada", "amount": 5},
                {"customer": "bob", "amount": 7},
                {"customer": "cy", "amount": 2},
                {"customer": "cy", "amount": 30},
            ],
        ),
    ),
)
"""Five orders; at ``min_total=10`` the query keeps ada (15) and cy (32) and drops bob (7)."""

PARAMS = MinTotal(min_total=10)

BY_CUSTOMER_SQL = """
SELECT customer, SUM(amount)::int AS total
FROM {table}
GROUP BY customer
HAVING SUM(amount) >= %(min_total)s
"""
"""The query on Postgres; ``{table}`` is the qualified document table."""


# ....................... #


def _sums(view: MockAnalyticsView) -> dict[str, int]:
    sums: dict[str, int] = defaultdict(int)

    for row in view.rows(ORDERS):
        sums[row["customer"]] += row["amount"]

    return sums


def matching_resolver(view: MockAnalyticsView, params: BaseModel) -> Iterable[dict[str, Any]]:
    """The query as :data:`BY_CUSTOMER_SQL` computes it."""

    assert isinstance(params, MinTotal)

    return [
        {"customer": customer, "total": total}
        for customer, total in _sums(view).items()
        if total >= params.min_total
    ]


def drifting_resolver(view: MockAnalyticsView, params: BaseModel) -> Iterable[dict[str, Any]]:
    """The query with its ``HAVING`` forgotten: every customer, whatever the threshold."""

    return [{"customer": customer, "total": total} for customer, total in _sums(view).items()]
