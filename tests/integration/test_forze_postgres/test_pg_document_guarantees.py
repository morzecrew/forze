"""Integration: a declared storage guarantee against a real Postgres.

Two halves, and both need the database.

**Validation** — reconciliation at wiring said Postgres *can* keep a uniqueness guarantee; only
the catalog can say whether this deployment's migration did. The refusal has to name the DDL,
because an operator reading "guarantee not satisfied" with no statement to run is an operator
who has to read the framework's source.

**Parity** — the in-memory store is the canonical superset, which is exactly the declaration most
easily wrong in the optimistic direction: a mock stricter than the backend makes a green
simulation meaningless. So the violating write is made against *both* stores and the two
refusals are compared, rather than asserted separately in files that never meet.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.guarantees import NonOverlapping, UniqueTogether
from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule
from forze_postgres.execution.deps.keys import PostgresIntrospectorDepKey
from forze_postgres.execution.lifecycle import PostgresDocumentSchemaValidationHook
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.catalog.validation.validate_schema import PostgresDocumentSchemaSpec
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps, context_from_modules

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #

ONE_CURRENT = UniqueTogether(fields=("root_id",), where={"$values": {"is_current": True}})
ONE_EVER = UniqueTogether(fields=("root_id",))


class _Read(BaseModel):
    id: UUID
    root_id: str
    is_current: bool


class _Domain(Document):
    root_id: str
    is_current: bool = True


class _Create(CreateDocumentCmd):
    root_id: str
    is_current: bool = True


async def _table(pg_client: PostgresClient) -> str:
    name = f"guarantee_{uuid4().hex[:12]}"

    await pg_client.execute(
        f"""
        CREATE TABLE {name} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            root_id text NOT NULL,
            is_current boolean NOT NULL
        );
        """
    )

    return name


async def _validate(
    pg_client: PostgresClient,
    table: str,
    *guarantees: UniqueTogether,
) -> None:
    intro = PostgresIntrospector(client=pg_client)
    ctx = context_from_deps(Deps.plain({PostgresIntrospectorDepKey: intro}))
    hook = PostgresDocumentSchemaValidationHook(
        specs=(
            PostgresDocumentSchemaSpec(
                name="fact",
                read_model=_Read,
                read_relation=("public", table),
                write_domain_model=_Domain,
                write_create_model=_Create,
                write_relation=("public", table),
                bookkeeping_strategy="application",
                guarantees=guarantees,
            ),
        ),
    )

    await hook(ctx)


# ....................... #


class TestStartupValidation:
    async def test_a_missing_partial_index_refuses_and_names_the_ddl(
        self,
        pg_client: PostgresClient,
    ) -> None:
        table = await _table(pg_client)

        with pytest.raises(CoreException) as caught:
            await _validate(pg_client, table, ONE_CURRENT)

        message = caught.value.summary

        assert "CREATE UNIQUE INDEX" in message
        assert table in message
        assert "root_id" in message
        # The doctrine, in the message a deployment actually reads.
        assert "The migration is what satisfies a guarantee" in message

    async def test_a_present_partial_index_passes(self, pg_client: PostgresClient) -> None:
        table = await _table(pg_client)
        await pg_client.execute(f"CREATE UNIQUE INDEX ON {table} (root_id) WHERE is_current;")

        await _validate(pg_client, table, ONE_CURRENT)

    async def test_a_plain_index_does_not_satisfy_a_filtered_guarantee(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # The axis the capability declaration splits, proved against the catalog: a plain
        # unique index is the wrong mechanism for "one *current* row per fact", and a check
        # that accepted it would pass a deployment that refuses every second history row.
        table = await _table(pg_client)
        await pg_client.execute(f"ALTER TABLE {table} ADD UNIQUE (root_id);")

        with pytest.raises(CoreException, match="no partial unique index"):
            await _validate(pg_client, table, ONE_CURRENT)

    async def test_a_plain_index_satisfies_an_unfiltered_guarantee(
        self,
        pg_client: PostgresClient,
    ) -> None:
        table = await _table(pg_client)
        await pg_client.execute(f"ALTER TABLE {table} ADD UNIQUE (root_id);")

        await _validate(pg_client, table, ONE_EVER)

    async def test_declaring_nothing_validates_nothing(self, pg_client: PostgresClient) -> None:
        # The inert case: a table with no unique index at all is fine for a spec that asks
        # for nothing, which is every spec that has not opted in.
        await _validate(pg_client, await _table(pg_client))

    async def test_a_member_no_store_maps_is_skipped_rather_than_crashing(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # Reconciliation refuses `NonOverlapping` today, so validation never meets one — but
        # it will the moment a store maps it, and the transition must not be a crash on a
        # member this function does not understand. Reached directly, since the hook takes a
        # schema spec without going through reconciliation.
        table = await _table(pg_client)
        spec = PostgresDocumentSchemaSpec(
            name="fact",
            read_model=_Read,
            read_relation=("public", table),
            write_domain_model=_Domain,
            write_create_model=_Create,
            write_relation=("public", table),
            bookkeeping_strategy="application",
            guarantees=(NonOverlapping(key=("root_id",), period=("id", "root_id")),),
        )
        intro = PostgresIntrospector(client=pg_client)
        ctx = context_from_deps(Deps.plain({PostgresIntrospectorDepKey: intro}))

        await PostgresDocumentSchemaValidationHook(specs=(spec,))(ctx)

    async def test_a_partial_index_on_other_columns_does_not_count(
        self,
        pg_client: PostgresClient,
    ) -> None:
        table = await _table(pg_client)
        await pg_client.execute(f"CREATE UNIQUE INDEX ON {table} (id) WHERE is_current;")

        with pytest.raises(CoreException, match="no partial unique index"):
            await _validate(pg_client, table, ONE_CURRENT)


# ....................... #


class _FactRead(ReadDocument):
    root_id: str
    is_current: bool = True


class _FactUpdate(BaseDTO):
    is_current: bool | None = None


def _spec() -> DocumentSpec[_FactRead, _Domain, _Create, _FactUpdate]:
    return DocumentSpec[_FactRead, _Domain, _Create, _FactUpdate](
        name="fact",
        read=_FactRead,
        write=DocumentWriteTypes(domain=_Domain, create_cmd=_Create, update_cmd=_FactUpdate),
        guarantees=(ONE_CURRENT,),
    )


class TestMockAndPostgresRefuseTheSameWay:
    async def test_both_stores_raise_conflict_for_the_same_violation(
        self,
        pg_client: PostgresClient,
    ) -> None:
        table = await _table(pg_client)
        await pg_client.execute(f"CREATE UNIQUE INDEX ON {table} (root_id) WHERE is_current;")

        # Postgres: the index refuses the second current row.
        await pg_client.execute(
            f"INSERT INTO {table} (id, rev, created_at, last_update_at, root_id, is_current)"
            " VALUES (%s, 1, now(), now(), 'r1', true);",
            [uuid4()],
        )

        with pytest.raises(CoreException) as from_postgres:
            await pg_client.execute(
                f"INSERT INTO {table} (id, rev, created_at, last_update_at, root_id, is_current)"
                " VALUES (%s, 1, now(), now(), 'r1', true);",
                [uuid4()],
            )

        # The in-memory store: the declared guarantee refuses it.
        command = context_from_modules(MockDepsModule()).doc.command(_spec())
        await command.create(_Create(root_id="r1"))

        with pytest.raises(CoreException) as from_mock:
            await command.create(_Create(root_id="r1"))

        # The comparison is the point: same kind, so a caller cannot tell the stores apart by
        # how they refuse, and a simulation that handles one handles the other.
        assert from_mock.value.kind is from_postgres.value.kind
        assert from_mock.value.kind.value == "conflict"

    async def test_neither_store_refuses_a_row_outside_the_filter(
        self,
        pg_client: PostgresClient,
    ) -> None:
        # The other direction, and the one that catches an over-strict mock: a non-current
        # duplicate is legal in both stores.
        table = await _table(pg_client)
        await pg_client.execute(f"CREATE UNIQUE INDEX ON {table} (root_id) WHERE is_current;")
        await pg_client.execute(
            f"INSERT INTO {table} (id, rev, created_at, last_update_at, root_id, is_current)"
            " VALUES (%s, 1, now(), now(), 'r1', true);",
            [uuid4()],
        )
        await pg_client.execute(
            f"INSERT INTO {table} (id, rev, created_at, last_update_at, root_id, is_current)"
            " VALUES (%s, 1, now(), now(), 'r1', false);",
            [uuid4()],
        )

        command = context_from_modules(MockDepsModule()).doc.command(_spec())
        await command.create(_Create(root_id="r1"))
        row = await command.create(_Create(root_id="r1", is_current=False))

        assert row.is_current is False
