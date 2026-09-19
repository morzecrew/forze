"""Integration: an effective-dated aggregate against a real Postgres.

Three halves, all of which need the database.

**The rule** — the kit's reads assume no two periods under one key overlap, and the kit does not
enforce that: an exclusion constraint does. Whether the constraint refuses what the declaration
says it refuses is a question about Postgres, and only Postgres can answer it.

**Under concurrency** — the reason the rule is the store's and not the kit's. Two transactions
each read no conflict and each insert; a read-then-insert check in application code accepts
both, which is the accepted race in every hand-rolled version of this aggregate.

**The missing migration** — startup refuses a deployment whose constraint is absent, naming the
DDL, including the bounds the declaration asked for.
"""

from __future__ import annotations

from datetime import date
from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.guarantees import NonOverlapping
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, ReadDocument
from forze_kits.aggregates.temporal import EffectiveOn, EffectiveOnDTO, Timeline, TimelineDTO
from forze_kits.aggregates.temporal.policy import TemporalPolicy
from forze_kits.domain.temporal import CreateCmdWithTemporalFields, DocWithTemporal
from forze_postgres.execution.deps import PostgresDepsModule
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import PostgresIntrospectorDepKey
from forze_postgres.execution.lifecycle import PostgresDocumentSchemaValidationHook
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.catalog.validation.validate_schema import (
    PostgresDocumentSchemaSpec,
)
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #

POLICY = TemporalPolicy(key=("employee_id",), bounds="[]")
NO_OVERLAP = NonOverlapping(key=("employee_id",), period=("valid_from", "valid_to"), bounds="[]")


class Contract(DocWithTemporal):
    employee_id: str
    hours: int = 0


class ContractCreate(CreateCmdWithTemporalFields):
    employee_id: str
    hours: int = 0


class ContractUpdate(BaseDTO):
    hours: int | None = None
    valid_to: date | None = None


class ContractRead(ReadDocument):
    employee_id: str
    hours: int = 0
    valid_from: date
    valid_to: date | None = None


def _spec(name: str) -> DocumentSpec[ContractRead, Contract, ContractCreate, ContractUpdate]:
    return DocumentSpec[ContractRead, Contract, ContractCreate, ContractUpdate](
        name=name,
        read=ContractRead,
        write=DocumentWriteTypes(
            domain=Contract, create_cmd=ContractCreate, update_cmd=ContractUpdate
        ),
        guarantees=(NO_OVERLAP,),
    )


async def _table(pg_client: PostgresClient, *, constrained: bool = True) -> str:
    name = f"contracts_{uuid4().hex[:10]}"

    await pg_client.execute(
        f"""
        CREATE TABLE {name} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            employee_id text NOT NULL,
            hours integer NOT NULL DEFAULT 0,
            valid_from date NOT NULL,
            valid_to date
        );
        """
    )

    if constrained:
        # The guarantee, as the migration an operator writes.
        await pg_client.execute("CREATE EXTENSION IF NOT EXISTS btree_gist;")
        await pg_client.execute(
            f"ALTER TABLE {name} ADD EXCLUDE USING gist "
            f"(employee_id WITH =, daterange(valid_from, valid_to, '[]') WITH &&);"
        )

    return name


def _doc(table: str) -> PostgresDocumentConfig:
    return PostgresDocumentConfig(
        read=("public", table), write=("public", table), bookkeeping_strategy="application"
    )


def _ctx(pg_client: PostgresClient, table: str) -> ExecutionContext:
    return context_from_deps(
        PostgresDepsModule(client=pg_client, rw_documents={table: _doc(table)}, tx={"postgres"})()
    )


async def _add(
    ctx: ExecutionContext, table: str, *, employee: str, hours: int, start: date, end: date | None
) -> ContractRead:
    return await ctx.doc.command(_spec(table)).create(
        ContractCreate(employee_id=employee, hours=hours, valid_from=start, valid_to=end)
    )


# ....................... #


class TestTheStoreKeepsTheRule:
    async def test_an_overlapping_period_is_refused(self, pg_client: PostgresClient) -> None:
        table = await _table(pg_client)
        ctx = _ctx(pg_client, table)

        await _add(
            ctx,
            table,
            employee="e1",
            hours=40,
            start=date(2026, 1, 1),
            end=date(2026, 3, 31),
        )

        with pytest.raises(CoreException) as caught:
            await _add(
                ctx,
                table,
                employee="e1",
                hours=20,
                start=date(2026, 3, 1),
                end=date(2026, 5, 1),
            )

        # `conflict`, the same kind the in-memory store raises for the same declaration.
        assert caught.value.kind is ExceptionKind.CONFLICT

    async def test_the_declared_bounds_refuse_a_touching_period(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # Under `[]` the shared endpoint is in force on both sides, so consecutive periods must
        # not share a day. This is the case the startup check's bounds comparison protects.
        table = await _table(pg_client)
        ctx = _ctx(pg_client, table)

        await _add(
            ctx,
            table,
            employee="e1",
            hours=40,
            start=date(2026, 1, 1),
            end=date(2026, 4, 1),
        )

        with pytest.raises(CoreException):
            await _add(
                ctx,
                table,
                employee="e1",
                hours=20,
                start=date(2026, 4, 1),
                end=date(2026, 6, 1),
            )

    async def test_another_employee_never_conflicts(self, pg_client: PostgresClient) -> None:
        table = await _table(pg_client)
        ctx = _ctx(pg_client, table)

        await _add(
            ctx,
            table,
            employee="e1",
            hours=40,
            start=date(2026, 1, 1),
            end=date(2026, 3, 31),
        )
        row = await _add(
            ctx,
            table,
            employee="e2",
            hours=20,
            start=date(2026, 1, 1),
            end=date(2026, 3, 31),
        )

        assert row.employee_id == "e2"

    async def test_an_open_period_conflicts_with_everything_after_it(
        self,
        pg_client: PostgresClient,
    ) -> None:
        table = await _table(pg_client)
        ctx = _ctx(pg_client, table)

        await _add(ctx, table, employee="e1", hours=40, start=date(2026, 1, 1), end=None)

        with pytest.raises(CoreException):
            await _add(ctx, table, employee="e1", hours=20, start=date(2030, 1, 1), end=None)


# ....................... #


class TestTheReadsAgainstTheRealStore:
    async def test_effective_on_returns_the_row_in_force(
        self,
        pg_client: PostgresClient,
    ) -> None:
        table = await _table(pg_client)
        ctx = _ctx(pg_client, table)

        await _add(
            ctx,
            table,
            employee="e1",
            hours=40,
            start=date(2026, 1, 1),
            end=date(2026, 3, 31),
        )
        await _add(ctx, table, employee="e1", hours=20, start=date(2026, 4, 1), end=None)

        handler = EffectiveOn(query=ctx.doc.query(_spec(table)), policy=POLICY)
        row = await handler(EffectiveOnDTO(key={"employee_id": "e1"}, on=date(2026, 2, 15)))

        assert row.hours == 40

    async def test_the_last_day_is_in_force_under_closed_bounds(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # The predicate and the constraint read the same convention; against a real store the
        # boundary day is the one that would show them disagreeing.
        table = await _table(pg_client)
        ctx = _ctx(pg_client, table)

        await _add(
            ctx,
            table,
            employee="e1",
            hours=40,
            start=date(2026, 1, 1),
            end=date(2026, 3, 31),
        )

        handler = EffectiveOn(query=ctx.doc.query(_spec(table)), policy=POLICY)
        row = await handler(EffectiveOnDTO(key={"employee_id": "e1"}, on=date(2026, 3, 31)))

        assert row.hours == 40

    async def test_a_day_nothing_covers_is_not_found(self, pg_client: PostgresClient) -> None:
        table = await _table(pg_client)
        ctx = _ctx(pg_client, table)

        await _add(
            ctx,
            table,
            employee="e1",
            hours=40,
            start=date(2026, 1, 1),
            end=date(2026, 3, 31),
        )

        handler = EffectiveOn(query=ctx.doc.query(_spec(table)), policy=POLICY)

        with pytest.raises(CoreException) as caught:
            await handler(EffectiveOnDTO(key={"employee_id": "e1"}, on=date(2026, 4, 1)))

        assert caught.value.kind is ExceptionKind.NOT_FOUND

    async def test_the_timeline_answers_a_month_in_one_query(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # The operation the origin application did not have: it asked this once per day.
        table = await _table(pg_client)
        ctx = _ctx(pg_client, table)

        await _add(
            ctx,
            table,
            employee="e1",
            hours=40,
            start=date(2026, 1, 1),
            end=date(2026, 3, 31),
        )
        await _add(
            ctx,
            table,
            employee="e1",
            hours=30,
            start=date(2026, 4, 1),
            end=date(2026, 6, 30),
        )
        await _add(ctx, table, employee="e1", hours=20, start=date(2026, 7, 1), end=None)
        await _add(ctx, table, employee="e2", hours=10, start=date(2026, 1, 1), end=None)

        handler = Timeline(query=ctx.doc.query(_spec(table)), policy=POLICY)
        page = await handler(
            TimelineDTO(key={"employee_id": "e1"}, start=date(2026, 3, 1), end=date(2026, 5, 1))
        )

        assert [row.hours for row in page.hits] == [40, 30]


# ....................... #


class TestNeitherWriterHasToCheckFirst:
    """Two overlapping writes issued back to back leave exactly one row.

    What this pins is that **no writer reads before writing** — neither call looks for a
    conflicting period, and the store refuses the second anyway. That is the whole reason the
    rule is declared rather than implemented: a read-then-insert check in the kit would accept
    both under interleaving, which is the accepted race in every hand-rolled version of this.

    Deliberately *not* a two-connection race. These two writes share one client, so they
    serialize, and a test asserting "exactly one survived" would pass on a sequential store
    too. The interleaved case is Postgres's own property, which §6 of the design leaves to
    Postgres; what is testable here is that the kit never asks.
    """

    async def test_the_second_write_is_refused_without_anyone_looking(
        self,
        pg_client: PostgresClient,
    ) -> None:
        table = await _table(pg_client)
        ctx = _ctx(pg_client, table)

        async def _write(start: date, end: date) -> bool:
            try:
                await _add(ctx, table, employee="e1", hours=1, start=start, end=end)

            except CoreException:
                return False

            return True

        results = [
            await _write(date(2026, 1, 1), date(2026, 6, 30)),
            await _write(date(2026, 3, 1), date(2026, 9, 30)),
        ]

        rows = await pg_client.fetch_all(
            f"SELECT count(*) AS n FROM {table};", [], row_factory="dict", commit=False
        )

        assert results == [True, False]
        assert rows[0]["n"] == 1


# ....................... #


async def _validate(pg_client: PostgresClient, table: str) -> None:
    intro = PostgresIntrospector(client=pg_client)
    ctx = context_from_deps(Deps.plain({PostgresIntrospectorDepKey: intro}))
    hook = PostgresDocumentSchemaValidationHook(
        specs=(
            PostgresDocumentSchemaSpec(
                name="contracts",
                read_model=ContractRead,
                read_relation=("public", table),
                write_domain_model=Contract,
                write_create_model=ContractCreate,
                write_relation=("public", table),
                bookkeeping_strategy="application",
                guarantees=(NO_OVERLAP,),
            ),
        ),
    )

    await hook(ctx)


class TestTheMissingMigration:
    async def test_startup_refuses_a_table_without_the_constraint(
        self,
        pg_client: PostgresClient,
    ) -> None:
        table = await _table(pg_client, constrained=False)

        with pytest.raises(CoreException) as caught:
            await _validate(pg_client, table)

        message = caught.value.summary

        assert "EXCLUDE USING gist" in message
        assert "daterange(valid_from, valid_to, '[]')" in message

    async def test_startup_passes_once_it_is_migrated(
        self,
        pg_client: PostgresClient,
    ) -> None:
        await _validate(pg_client, await _table(pg_client))
