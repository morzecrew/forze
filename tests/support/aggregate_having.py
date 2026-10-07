"""Cross-backend ``$having`` parity: post-group filtering of aggregate rows.

``$having`` filters the *aggregated* rows by the output aliases (group keys + computed
metrics) — the aggregate analogue of SQL ``HAVING``. The in-memory mock is the oracle
(it filters the computed group dicts directly); Postgres wraps the group query in a
subquery and filters its aliases, Mongo appends a ``$match`` after ``$group``. This
corpus exercises count/sum thresholds, multi-key groups, and a group-key + metric mix.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel

from forze.application.contracts.querying import UNSUPPORTED_QUERY_FEATURE_CODE
from forze.base.exceptions import CoreException
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


# ....................... #
# Time buckets: a bucket compares as the instant it starts at, whatever its zone.


class _BucketFields(BaseModel):
    region: str
    at: datetime


class BucketCreate(CreateDocumentCmd, _BucketFields):
    pass


class BucketDoc(Document, _BucketFields):
    pass


class BucketRead(ReadDocument, _BucketFields):
    pass


BUCKET_SEED: tuple[BucketCreate, ...] = (
    BucketCreate(region="early", at=datetime(2026, 1, 1, 5, tzinfo=UTC)),
    BucketCreate(region="late", at=datetime(2026, 1, 1, 12, tzinfo=UTC)),
)

_CUTOFF = datetime(2026, 1, 1, 8, tzinfo=UTC)


def bucket_cases() -> list[tuple[dict[str, Any], list[str]]]:
    """``$having`` over an hour bucket in several zones, with the regions it keeps.

    Only ``late`` starts at or after 08:00Z: an aware bound is that instant, and a naive one
    is wall time in the bucket's zone, the form a bucket is cut and reported in.
    """

    cases: list[tuple[dict[str, Any], list[str]]] = []

    for zone, tz in (("UTC", UTC), ("Asia/Tokyo", ZoneInfo("Asia/Tokyo")), ("+09:00", None)):
        tz = tz or timezone(timedelta(hours=9))
        wall = _CUTOFF.astimezone(tz).replace(tzinfo=None)
        late_wall = BUCKET_SEED[1].at.astimezone(tz).replace(tzinfo=None)
        base = {
            "$groups": {
                "hour": {"$trunc": {"field": "at", "unit": "hour", "timezone": zone}},
                "region": "region",
            },
            "$computed": {"n": {"$count": None}, "last": {"$max": "at"}},
        }

        for having, kept in (
            ({"$values": {"hour": {"$gte": _CUTOFF}}}, ["late"]),
            ({"$values": {"hour": {"$gte": "2026-01-01T08:00:00Z"}}}, ["late"]),
            ({"$values": {"hour": {"$gte": wall}}}, ["late"]),
            ({"$values": {"hour": {"$lt": wall.isoformat()}}}, ["early"]),
            ({"$values": {"hour": {"$eq": late_wall}}}, ["late"]),
            ({"$values": {"hour": {"$in": [late_wall.isoformat()]}}}, ["late"]),
            ({"$fields": {"hour": {"$eq": "last"}}}, ["early", "late"]),
        ):
            cases.append(({**base, "$having": having}, kept))

    return cases


async def assert_bucket_having(query: Any) -> None:
    """Assert *query*, seeded with :data:`BUCKET_SEED`, keeps what :func:`bucket_cases` says."""

    for aggregates, kept in bucket_cases():
        page = await query.aggregate_page(aggregates=aggregates, pagination={"limit": 100})

        assert sorted(row["region"] for row in page.hits) == kept, aggregates

    # A bucket is a time: a text pattern on it is refused everywhere, not matched as text.
    aggregates, _ = bucket_cases()[0]

    try:
        await query.aggregate_page(
            aggregates={**aggregates, "$having": {"$values": {"hour": {"$like": "2026%"}}}},
            pagination={"limit": 100},
        )

    except CoreException as refused:
        assert refused.code == UNSUPPORTED_QUERY_FEATURE_CODE

    else:
        raise AssertionError("$like on a bucket ran")


# ....................... #
# A string bound is cast to the output's type, as a filter casts it.

_BY_REGION: dict[str, Any] = {
    "$groups": {"region": "region"},
    "$computed": {
        "cnt": {"$count": None},
        "total": {"$sum": "amount"},
        "top": {"$max": "amount"},
        "last": {"$max": "created_at"},
    },
}

STRING_BOUND_CASES: tuple[tuple[dict[str, Any], list[str]], ...] = (
    ({"cnt": {"$gte": "2"}}, ["east", "north", "west"]),
    ({"total": {"$gt": "40.5"}}, ["west"]),
    ({"top": {"$gte": "40"}}, ["south", "west"]),
    ({"last": {"$gt": "2000-01-01T00:00:00Z"}}, ["east", "north", "south", "west"]),
    ({"last": {"$lt": "2000-01-01T00:00:00Z"}}, []),
)
"""``$having`` bounds written as strings over :data:`SEED`, with the regions they keep."""


async def assert_string_bound_having(query: Any) -> None:
    """Assert *query*, seeded with :data:`SEED`, casts string bounds in ``$having`` and in a
    metric filter, and refuses an operator no number supports on a ``$max`` over a number,
    as it does on a ``$count``."""

    for having, kept in STRING_BOUND_CASES:
        page = await query.aggregate_page(
            aggregates={**_BY_REGION, "$having": {"$values": having}},
            pagination={"limit": 100},
        )

        assert sorted(row["region"] for row in page.hits) == kept, having

    # A metric filter is a filter over the documents and casts its bounds as one does.
    page = await query.aggregate_page(
        aggregates={
            "$groups": {"region": "region"},
            "$computed": {
                "big": {"$count": {"filter": {"$values": {"amount": {"$gt": "15"}}}}},
            },
        },
        pagination={"limit": 100},
    )
    assert {row["region"]: row["big"] for row in page.hits} == {
        "east": 1,
        "north": 0,
        "south": 1,
        "west": 2,
    }

    for misused in ("cnt", "top"):
        try:
            await query.aggregate_page(
                aggregates={**_BY_REGION, "$having": {"$values": {misused: {"$like": "1%"}}}},
                pagination={"limit": 100},
            )

        except CoreException as refused:
            assert refused.code == UNSUPPORTED_QUERY_FEATURE_CODE, misused

        else:
            raise AssertionError(f"$like on {misused!r} ran")
