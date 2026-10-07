"""Postgres ``$having`` parity: post-group filtering matches the in-memory oracle.

Postgres wraps the group query in a subquery and filters its output aliases; this checks
the result against the mock for count/sum thresholds, multi-key groups, and a group-key +
metric mix.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.contracts.querying import UNSUPPORTED_QUERY_FEATURE_CODE
from forze.base.exceptions import CoreException
from forze_mock.adapters import MockDocumentAdapter, MockState
from forze_postgres.kernel.client.client import PostgresClient
from tests.integration.test_forze_postgres._document_fixtures import document_context
from tests.support.aggregate_functions import assert_aggregate_function_parity
from tests.support.aggregate_having import (
    AggCreate,
    AggDoc,
    AggRead,
    assert_aggregate_having_parity,
    seed_aggregate_corpus,
)


def _mock_oracle() -> MockDocumentAdapter[Any, Any, Any, Any]:
    spec = DocumentSpec(
        name="agg",
        read=AggRead,
        write=DocumentWriteTypes(domain=AggDoc, create_cmd=AggCreate),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="agg",
        read_model=AggRead,
        domain_model=AggDoc,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_aggregate_having_postgres(pg_client: PostgresClient) -> None:
    t = f"agg_having_{uuid4().hex[:12]}"

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            region text NOT NULL,
            tier text NOT NULL,
            amount integer NOT NULL
        );
        """
    )

    spec = DocumentSpec(
        name="agg",
        read=AggRead,
        write=DocumentWriteTypes(domain=AggDoc, create_cmd=AggCreate),
    )
    ctx = document_context(pg_client, t)

    await seed_aggregate_corpus(ctx.document.command(spec))

    oracle = _mock_oracle()
    await seed_aggregate_corpus(oracle)

    await assert_aggregate_having_parity(ctx.document.query(spec), oracle)
    # Postgres percentile_cont is exact, so all functions are value-checked.
    await assert_aggregate_function_parity(
        ctx.document.query(spec), oracle, exclude_approx=False
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_having_operator_the_output_cannot_take_is_refused_up_front(
    pg_client: PostgresClient,
) -> None:
    """``$having`` renders against each output's type: ``$like`` on a count is the caller's
    error, not a server one, while a fractional threshold on a count and a pattern on a
    text group or a text ``$min`` still run."""

    t = f"agg_having_typed_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            region text NOT NULL,
            tier text NOT NULL,
            amount integer NOT NULL
        );
        """
    )
    spec = DocumentSpec(
        name="agg",
        read=AggRead,
        write=DocumentWriteTypes(domain=AggDoc, create_cmd=AggCreate),
    )
    ctx = document_context(pg_client, t)
    await seed_aggregate_corpus(ctx.document.command(spec))
    query = ctx.document.query(spec)
    by_region = {"$groups": {"region": "region"}, "$computed": {"cnt": {"$count": None}}}

    with pytest.raises(CoreException) as refused:
        await query.aggregate_many(
            {**by_region, "$having": {"$values": {"cnt": {"$like": "3%"}}}}
        )

    assert refused.value.code == UNSUPPORTED_QUERY_FEATURE_CODE

    fractional = await query.aggregate_many(
        {**by_region, "$having": {"$values": {"cnt": {"$gte": 2.5}}}}
    )
    assert [row["region"] for row in fractional.hits] == ["east"]

    patterned = await query.aggregate_many(
        {**by_region, "$having": {"$values": {"region": {"$like": "%st"}}}},
        sorts={"region": "asc"},
    )
    assert [row["region"] for row in patterned.hits] == ["east", "west"]

    first = await query.aggregate_many(
        {
            "$groups": {"tier": "tier"},
            "$computed": {"first": {"$min": "region"}},
            "$having": {"$values": {"first": {"$like": "e%"}}},
        },
        sorts={"tier": "asc"},
    )
    assert [(row["tier"], row["first"]) for row in first.hits] == [
        ("gold", "east"),
        ("silver", "east"),
    ]

    # Outputs of one kind compare with each other and take any threshold of that kind: an
    # integer ``$max`` against a count, a fractional bound on an integer ``$min``, and a day
    # bucket against a timestamp ``$max``.
    regions = {"east", "west", "north", "south"}
    counted_vs_max = await query.aggregate_many(
        {
            "$groups": {"region": "region"},
            "$computed": {"cnt": {"$count": None}, "top": {"$max": "amount"}},
            "$having": {"$fields": {"cnt": {"$lt": "top"}}},
        }
    )
    assert {row["region"] for row in counted_vs_max.hits} == regions

    fractional_min = await query.aggregate_many(
        {
            "$groups": {"region": "region"},
            "$computed": {"low": {"$min": "amount"}},
            "$having": {"$values": {"low": {"$gt": 1.5}}},
        }
    )
    assert {row["region"] for row in fractional_min.hits} == regions

    buckets = await query.aggregate_many(
        {
            "$groups": {"day": {"$trunc": {"field": "created_at", "unit": "day"}}},
            "$computed": {"last": {"$max": "created_at"}},
            "$having": {"$fields": {"day": {"$lte": "last"}}},
        }
    )
    assert buckets.hits


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_column_precision_does_not_change_an_output_kind(
    pg_client: PostgresClient,
) -> None:
    """A ``numeric(10,2)`` measure is a number like any other, so it compares with a sum."""

    t = f"agg_having_precision_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            region text NOT NULL,
            tier text NOT NULL,
            amount numeric(10, 2) NOT NULL
        );
        """
    )
    spec = DocumentSpec(
        name="agg",
        read=AggRead,
        write=DocumentWriteTypes(domain=AggDoc, create_cmd=AggCreate),
    )
    ctx = document_context(pg_client, t)
    await seed_aggregate_corpus(ctx.document.command(spec))

    # Only south has one row, whose largest amount is its whole total.
    rows = await ctx.document.query(spec).aggregate_many(
        {
            "$groups": {"region": "region"},
            "$computed": {"top": {"$max": "amount"}, "total": {"$sum": "amount"}},
            "$having": {"$fields": {"top": {"$lt": "total"}}},
        },
        sorts={"region": "asc"},
    )

    assert [row["region"] for row in rows.hits] == ["east", "north", "west"]


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("zone", ["UTC", "Asia/Tokyo"])
async def test_a_temporal_having_matches_the_filter_in_any_session_zone(
    postgres_container: Any, zone: str
) -> None:
    """A threshold on a temporal output coerces as a filter on that column does, so the
    session's time zone moves neither: ``$having`` keeps exactly the rows the filter keeps."""

    from urllib.parse import quote

    from forze_postgres.kernel.client.client import PostgresClient as _Client

    url = postgres_container.get_connection_url().replace(
        "postgresql+psycopg://", "postgresql://"
    )
    client = _Client()
    await client.initialize(f"{url}?options={quote(f'-c TimeZone={zone}')}")

    try:
        assert await client.fetch_value("SHOW timezone", []) == zone

        t = f"agg_having_tz_{uuid4().hex[:10]}"
        await client.execute(
            f"""
            CREATE TABLE {t} (
                id uuid PRIMARY KEY,
                rev integer NOT NULL,
                created_at timestamptz NOT NULL,
                last_update_at timestamptz NOT NULL,
                region text NOT NULL,
                tier text NOT NULL,
                amount integer NOT NULL
            );
            """
        )

        for region, at in (("early", "2026-01-01 05:00:00+00"), ("late", "2026-01-01 12:00:00+00")):
            await client.execute(
                f"INSERT INTO {t} VALUES (%s, 1, %s, %s, %s, 'g', 1)",
                [uuid4(), at, at, region],
            )

        spec = DocumentSpec(
            name="agg",
            read=AggRead,
            write=DocumentWriteTypes(domain=AggDoc, create_cmd=AggCreate),
        )
        query = document_context(client, t).document.query(spec)
        by_region = {"$groups": {"region": "region"}, "$computed": {"last": {"$max": "created_at"}}}

        for bound in ("2026-01-01T08:00:00", "2026-01-01T08:00:00Z", datetime(2026, 1, 1, 8)):
            having = await query.aggregate_many(
                {**by_region, "$having": {"$values": {"last": {"$gt": bound}}}}
            )
            filtered = await query.find_many({"$values": {"created_at": {"$gt": bound}}})

            assert sorted(r["region"] for r in having.hits) == sorted(
                r.region for r in filtered.hits
            ), (zone, bound)

        # A UTC hour bucket against an instant: only the 12:00 row is at or past 08:00Z.
        buckets = await query.aggregate_many(
            {
                "$groups": {
                    "hour": {"$trunc": {"field": "created_at", "unit": "hour"}},
                    "region": "region",
                },
                "$computed": {"n": {"$count": None}},
                "$having": {"$values": {"hour": {"$gte": datetime(2026, 1, 1, 8, tzinfo=UTC)}}},
            }
        )
        assert [r["region"] for r in buckets.hits] == ["late"], zone

    finally:
        await client.close()
