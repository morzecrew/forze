"""Integration coverage for bound query parameters on the Postgres read path.

The unit suite proves the gateway emits ``set_config('<ns>.<field>', …, true)`` inside a
transaction; this proves the *round-trip* against a real database: a plain view reads the bound
value through ``current_setting`` **deep inside** a window function, where an outer ``WHERE`` can't
reach. Binding a different value reshuffles what the view computes — the rank is taken over the
as-of-filtered set, not the result.

Mirrors ``test_pg_read_gateway_variants.py``: the gateway is built directly via ``read_gw`` and
bound with ``attrs.evolve(gw, bound_params=…)`` — exactly what
``PostgresDocumentAdapter.with_parameters`` does.
"""

from datetime import UTC, date, datetime
from uuid import uuid4

import attrs
import pytest
from pydantic import BaseModel

from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from forze.domain.models import Document
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.execution.deps.utils import read_gw
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps


class Standing(Document):
    region: str
    player: str
    score: int
    rank: int  # rank() over the as-of-filtered set, computed inside the view


class AsOf(BaseModel):
    as_of: date


# Raw results the view ranks over: (region, player, score, recorded_on).
_RESULTS = [
    ("eu", "ana", 30, date(2026, 1, 10)),
    ("eu", "bo", 50, date(2026, 2, 20)),
    ("eu", "cy", 40, date(2026, 4, 5)),  # after a March cutoff
    ("us", "dot", 70, date(2026, 1, 15)),
    ("us", "el", 60, date(2026, 3, 1)),
]


def _ctx(pg_client: PostgresClient):
    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )


async def _make_view(pg_client: PostgresClient, namespace: str = "forze") -> str:
    """Create a results table and a standings view that reads ``current_setting`` internally.

    The as-of cutoff lives in the view's own ``WHERE``, before the window function, so the rank is
    computed over the filtered set — a value an outer filter on the result could not reproduce.
    """

    suffix = uuid4().hex[:10]
    table = f"pg_qp_results_{suffix}"
    view = f"pg_qp_standings_{suffix}"

    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            region text NOT NULL,
            player text NOT NULL,
            score integer NOT NULL,
            recorded_on date NOT NULL
        );
        """
    )
    now = datetime.now(UTC)
    for region, player, score, recorded_on in _RESULTS:
        await pg_client.execute(
            f"""
            INSERT INTO public.{table}
            (id, rev, created_at, last_update_at, region, player, score, recorded_on)
            VALUES (%(id)s, 1, %(now)s, %(now)s, %(region)s, %(player)s, %(score)s, %(rec)s);
            """,
            {
                "id": uuid4(),
                "now": now,
                "region": region,
                "player": player,
                "score": score,
                "rec": recorded_on,
            },
        )

    await pg_client.execute(
        f"""
        CREATE VIEW public.{view} AS
        SELECT id, rev, created_at, last_update_at, region, player, score,
               rank() OVER (PARTITION BY region ORDER BY score DESC) AS rank
        FROM public.{table}
        WHERE recorded_on <= current_setting('{namespace}.as_of')::date;
        """
    )
    return view


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bound_parameter_round_trips_through_current_setting(
    pg_client: PostgresClient,
) -> None:
    """The view reads the bound value via ``current_setting``; the rank reflects the as-of set."""

    view = await _make_view(pg_client)
    gw = read_gw(
        _ctx(pg_client),
        read_type=Standing,
        read_relation=("public", view),
        tenant_aware=False,
        params_required=True,
    )

    march = attrs.evolve(gw, bound_params=AsOf(as_of=date(2026, 3, 1)))
    rows = await march.find_many({"$values": {"region": "eu"}}, sorts={"rank": "asc"})
    # cy (recorded in April) is excluded as of March → bo ranks first, ana second.
    assert [(r.player, r.rank) for r in rows] == [("bo", 1), ("ana", 2)]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_later_binding_reshuffles_the_ranking(
    pg_client: PostgresClient,
) -> None:
    """Binding a later date pulls cy into the set and recomputes the ranks inside the view."""

    view = await _make_view(pg_client)
    gw = read_gw(
        _ctx(pg_client),
        read_type=Standing,
        read_relation=("public", view),
        tenant_aware=False,
        params_required=True,
    )

    may = attrs.evolve(gw, bound_params=AsOf(as_of=date(2026, 5, 1)))
    rows = await may.find_many({"$values": {"region": "eu"}}, sorts={"rank": "asc"})
    assert [(r.player, r.rank) for r in rows] == [("bo", 1), ("cy", 2), ("ana", 3)]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bound_params_do_not_leak_past_the_read(
    pg_client: PostgresClient,
) -> None:
    """A param-bound read inside a caller transaction must not leak its ``SET LOCAL`` GUCs
    into the surrounding transaction (they are transaction-scoped, not savepoint-scoped)."""

    view = await _make_view(pg_client)
    gw = read_gw(
        _ctx(pg_client),
        read_type=Standing,
        read_relation=("public", view),
        tenant_aware=False,
        params_required=True,
    )
    march = attrs.evolve(gw, bound_params=AsOf(as_of=date(2026, 3, 1)))

    # An outer caller transaction: the read's own transaction becomes a savepoint on this
    # same connection, so a leaked SET LOCAL would be visible here after the read.
    async with pg_client.transaction():
        await march.find_many({"$values": {"region": "eu"}}, sorts={"rank": "asc"})

        leaked = await pg_client.fetch_value("SELECT current_setting('forze.as_of', true)", None)

    assert leaked != "2026-03-01"  # the param value did not survive the read


class _RawAsOf(BaseModel):
    as_of: str


@pytest.mark.integration
@pytest.mark.asyncio
async def test_failed_read_surfaces_the_query_error_not_the_reset_failure(
    pg_client: PostgresClient,
) -> None:
    """A bound value the view cannot cast fails the fetch and aborts the transaction; the
    follow-up GUC reset then fails too (every command does in an aborted transaction) and
    must never replace the query's own error."""

    view = await _make_view(pg_client)
    gw = read_gw(
        _ctx(pg_client),
        read_type=Standing,
        read_relation=("public", view),
        tenant_aware=False,
        params_required=True,
    )
    bad = attrs.evolve(gw, bound_params=_RawAsOf(as_of="not-a-date"))

    with pytest.raises(CoreException, match="Invalid value"):
        await bad.find_many({"$values": {"region": "eu"}})


@pytest.mark.integration
@pytest.mark.asyncio
async def test_failed_read_leaves_the_caller_transaction_usable(
    pg_client: PostgresClient,
) -> None:
    """Inside a caller transaction, the failed read's savepoint rolls back: the outer
    transaction stays usable, sees the original error, and does not inherit the GUC."""

    view = await _make_view(pg_client)
    gw = read_gw(
        _ctx(pg_client),
        read_type=Standing,
        read_relation=("public", view),
        tenant_aware=False,
        params_required=True,
    )
    bad = attrs.evolve(gw, bound_params=_RawAsOf(as_of="not-a-date"))

    async with pg_client.transaction():
        with pytest.raises(CoreException, match="Invalid value"):
            await bad.find_many({"$values": {"region": "eu"}})

        # The savepoint rolled back: the outer transaction still accepts statements
        # and the failed read's parameter did not merge into it.
        leaked = await pg_client.fetch_value("SELECT current_setting('forze.as_of', true)", None)

    assert leaked != "not-a-date"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_count_composes_over_the_bound_view(pg_client: PostgresClient) -> None:
    """``count`` runs in the same bound transaction, so the view still sees the setting."""

    view = await _make_view(pg_client)
    gw = read_gw(
        _ctx(pg_client),
        read_type=Standing,
        read_relation=("public", view),
        tenant_aware=False,
        params_required=True,
    )

    bound = attrs.evolve(gw, bound_params=AsOf(as_of=date(2026, 5, 1)))
    assert await bound.count(None) == 5  # 3 eu + 2 us as of May
    assert await bound.count({"$values": {"region": "eu"}}) == 3


@pytest.mark.integration
@pytest.mark.asyncio
async def test_required_parameter_unbound_fails_closed(
    pg_client: PostgresClient,
) -> None:
    """A view that needs the setting, read without binding, fails closed before touching the DB."""

    view = await _make_view(pg_client)
    gw = read_gw(
        _ctx(pg_client),
        read_type=Standing,
        read_relation=("public", view),
        tenant_aware=False,
        params_required=True,
    )

    with pytest.raises(CoreException, match="query_parameters_unbound"):
        await gw.find_many(None)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_custom_namespace_round_trips(pg_client: PostgresClient) -> None:
    """A per-route GUC prefix other than ``forze`` round-trips end to end."""

    view = await _make_view(pg_client, namespace="myapp")
    gw = read_gw(
        _ctx(pg_client),
        read_type=Standing,
        read_relation=("public", view),
        tenant_aware=False,
        param_namespace="myapp",
        params_required=True,
    )

    bound = attrs.evolve(gw, bound_params=AsOf(as_of=date(2026, 5, 1)))
    rows = await bound.find_many({"$values": {"region": "us"}}, sorts={"rank": "asc"})
    assert [(r.player, r.rank) for r in rows] == [("dot", 1), ("el", 2)]
