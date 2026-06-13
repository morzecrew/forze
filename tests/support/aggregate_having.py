"""Cross-backend ``$having`` parity: post-group filtering of aggregate rows.

``$having`` filters the *aggregated* rows by the output aliases (group keys + computed
metrics) — the aggregate analogue of SQL ``HAVING``. The in-memory mock is the oracle
(it filters the computed group dicts directly); Postgres wraps the group query in a
subquery and filters its aliases, Mongo appends a ``$match`` after ``$group``. This
corpus exercises count/sum thresholds, multi-key groups, and a group-key + metric mix.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from forze.domain.models import CreateDocumentCmd, Document, ReadDocument

# ----------------------- #


class _AggFields(BaseModel):
    region: str
    tier: str
    amount: int


class AggCreate(CreateDocumentCmd, _AggFields):
    pass


class AggDoc(Document, _AggFields):
    pass


class AggRead(ReadDocument, _AggFields):
    pass


SEED: tuple[AggCreate, ...] = (
    AggCreate(region="east", tier="gold", amount=10),
    AggCreate(region="east", tier="gold", amount=20),
    AggCreate(region="east", tier="silver", amount=5),
    AggCreate(region="west", tier="gold", amount=100),
    AggCreate(region="west", tier="silver", amount=50),
    AggCreate(region="north", tier="gold", amount=3),
    AggCreate(region="north", tier="silver", amount=7),
    AggCreate(region="south", tier="gold", amount=40),
)
# Per-region: east cnt=3 sum=35 · west cnt=2 sum=150 · north cnt=2 sum=10 · south cnt=1 sum=40

CASES: tuple[dict[str, Any], ...] = (
    # count threshold on grouped rows
    {
        "$groups": {"region": "region"},
        "$computed": {"cnt": {"$count": None}, "total": {"$sum": "amount"}},
        "$having": {"$values": {"cnt": {"$gte": 2}}},
    },
    # sum threshold
    {
        "$groups": {"region": "region"},
        "$computed": {"total": {"$sum": "amount"}},
        "$having": {"$values": {"total": {"$gt": 50}}},
    },
    # multi-key group + metric threshold
    {
        "$groups": {"region": "region", "tier": "tier"},
        "$computed": {"total": {"$sum": "amount"}},
        "$having": {"$values": {"total": {"$gte": 20}}},
    },
    # combine a group key with a metric, under a combinator
    {
        "$groups": {"region": "region"},
        "$computed": {"cnt": {"$count": None}, "total": {"$sum": "amount"}},
        "$having": {
            "$and": [
                {"$values": {"region": {"$in": ["east", "west"]}}},
                {"$values": {"cnt": {"$gte": 2}}},
            ],
        },
    },
)


# ....................... #


def rowset(hits: list[Any]) -> set[tuple[tuple[str, Any], ...]]:
    """Order-independent set of aggregate rows as sorted ``(alias, value)`` tuples."""

    out: set[tuple[tuple[str, Any], ...]] = set()

    for row in hits:
        items = row.items() if isinstance(row, dict) else vars(row).items()
        out.add(tuple(sorted((str(k), v) for k, v in items)))

    return out


async def seed_aggregate_corpus(cmd: Any) -> None:
    for create in SEED:
        await cmd.create(create)


async def assert_aggregate_having_parity(
    real_query: Any,
    oracle: Any,
) -> None:
    """Assert *real_query* reproduces the mock *oracle* for every ``$having`` case.

    Both must already be seeded with :data:`SEED`.
    """

    for aggregates in CASES:
        real = await real_query.aggregate_page(
            aggregates=aggregates, pagination={"limit": 100}
        )
        expected = await oracle.aggregate_page(
            aggregates=aggregates, pagination={"limit": 100}
        )

        assert rowset(real.hits) == rowset(expected.hits), (
            f"$having mismatch for {aggregates}:\n"
            f" real={sorted(rowset(real.hits))}\n exp={sorted(rowset(expected.hits))}"
        )
