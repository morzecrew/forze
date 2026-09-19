"""Integration: correction lineage against a real Postgres, and the parity that makes it useful.

Three halves, all of which need the database.

**Behaviour** — the command writes four times across two relations under one transaction, and
whether that produces the intended rows is a question about Postgres, not about the handler. In
particular the retire-then-insert order exists because of a partial unique index, and only the
index can say whether the order is right.

**The missing migration** — the kit declares guarantees, and startup refuses a deployment whose
index is absent. The refusal has to name the DDL, because an operator reading "guarantee not
satisfied" with no statement to run has to read the framework's source.

**Parity** — the in-memory store is the canonical superset, which is the declaration most easily
wrong in the optimistic direction. So the violating correction is made against *both* stores and
the two refusals are compared, rather than asserted separately in files that never meet.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import utcnow
from forze.domain.models import ReadDocument
from forze_kits.aggregates.versioned import (
    ONE_CURRENT_VERSION,
    ONE_SUCCESSOR,
    CorrectDocument,
    CorrectDocumentDTO,
)
from forze_kits.domain.versioned import (
    CorrectionDoc,
    CreateCmdWithVersioningFields,
    CreateCorrectionCmd,
    DocWithVersioning,
    UpdateCmdWithVersioning,
)
from forze_mock import MockDepsModule
from forze_postgres.execution.deps import PostgresDepsModule
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import PostgresIntrospectorDepKey
from forze_postgres.execution.lifecycle import PostgresDocumentSchemaValidationHook
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.catalog.validation.validate_schema import (
    PostgresDocumentSchemaSpec,
)
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps, context_from_modules

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #


class Reading(DocWithVersioning):
    meter: str
    kwh: int = 0


class ReadingCreate(CreateCmdWithVersioningFields):
    meter: str
    kwh: int = 0


class ReadingUpdate(UpdateCmdWithVersioning):
    meter: str | None = None
    kwh: int | None = None


class ReadingRead(ReadDocument):
    meter: str
    kwh: int = 0
    root_id: UUID
    version: int = 1
    supersedes_id: UUID | None = None
    is_current: bool = True
    superseded_at: datetime | None = None


class CorrectionRead(ReadDocument):
    root_id: UUID
    from_id: UUID
    to_id: UUID
    actor_id: UUID | None = None
    reason: str


def _spec(name: str) -> DocumentSpec[ReadingRead, Reading, ReadingCreate, ReadingUpdate]:
    return DocumentSpec[ReadingRead, Reading, ReadingCreate, ReadingUpdate](
        name=name,
        read=ReadingRead,
        write=DocumentWriteTypes(
            domain=Reading, create_cmd=ReadingCreate, update_cmd=ReadingUpdate
        ),
        guarantees=(ONE_CURRENT_VERSION, ONE_SUCCESSOR),
    )


def _corrections_spec(name: str) -> DocumentSpec:
    return DocumentSpec(
        name=name,
        read=CorrectionRead,
        write=DocumentWriteTypes(domain=CorrectionDoc, create_cmd=CreateCorrectionCmd),
    )


async def _tables(pg_client: PostgresClient) -> tuple[str, str]:
    """A readings table with both guarantees migrated, and a corrections table."""

    readings = f"readings_{uuid4().hex[:10]}"
    corrections = f"corrections_{uuid4().hex[:10]}"

    await pg_client.execute(
        f"""
        CREATE TABLE {readings} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            meter text NOT NULL,
            kwh integer NOT NULL DEFAULT 0,
            root_id uuid NOT NULL,
            version integer NOT NULL,
            supersedes_id uuid,
            is_current boolean NOT NULL DEFAULT true,
            superseded_at timestamptz
        );
        """
    )
    # The two guarantees, as the migration an operator writes.
    await pg_client.execute(f"CREATE UNIQUE INDEX ON {readings} (root_id) WHERE is_current;")
    await pg_client.execute(
        f"CREATE UNIQUE INDEX ON {readings} (supersedes_id) WHERE supersedes_id IS NOT NULL;"
    )
    await pg_client.execute(
        f"""
        CREATE TABLE {corrections} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            root_id uuid NOT NULL,
            from_id uuid NOT NULL,
            to_id uuid NOT NULL,
            actor_id uuid,
            reason text NOT NULL
        );
        """
    )

    return readings, corrections


def _ctx(pg_client: PostgresClient, readings: str, corrections: str) -> ExecutionContext:
    """A context with both relations wired, routed by spec name.

    Two routes because a correction writes two aggregates, which is the wiring fact an author
    inherits from the declaration: a versioned kit needs a second ``rw_documents`` entry.
    """

    return context_from_deps(
        PostgresDepsModule(
            client=pg_client,
            rw_documents={readings: _doc(readings), corrections: _doc(corrections)},
            tx={"postgres"},
        )()
    )


def _doc(table: str) -> PostgresDocumentConfig:
    # Application bookkeeping = the adapter manages `rev`, matching the mock's rev-OCC.
    return PostgresDocumentConfig(
        read=("public", table), write=("public", table), bookkeeping_strategy="application"
    )


def _correct_handler(ctx: ExecutionContext, readings: str, corrections: str) -> CorrectDocument:
    """The kit's command over the Postgres ports, built the way the factory builds it."""

    spec = _spec(readings)

    return CorrectDocument(
        doc=ctx.doc.command(spec),
        query=ctx.doc.query(spec),
        corrections=ctx.doc.command(_corrections_spec(corrections)),
        create_cmd=ReadingCreate,
        actor=ctx.inv_ctx.get_authn,
    )


# ....................... #


class TestACorrectionAgainstPostgres:
    async def test_it_supersedes_under_the_real_indexes(self, pg_client: PostgresClient) -> None:
        readings, corrections = await _tables(pg_client)
        ctx = _ctx(pg_client, readings, corrections)
        spec = _spec(readings)
        cmd = ctx.doc.command(spec)

        fact = uuid4()
        first = await cmd.create(
            ReadingCreate(meter="m-1", kwh=100, root_id=fact, version=1), id=fact
        )

        second = await _correct_handler(ctx, readings, corrections)(
            CorrectDocumentDTO(
                id=first.id,
                expected_version=1,
                dto=ReadingUpdate(kwh=120),
                reason="meter misread",
            )
        )

        assert second.version == 2
        assert second.root_id == fact
        assert second.kwh == 120
        # Carried, not patched — a correction asserts the whole fact.
        assert second.meter == "m-1"

        rows = await pg_client.fetch_all(
            f"SELECT version, is_current, superseded_at FROM {readings} ORDER BY version",
            row_factory="dict",
        )

        assert [row["is_current"] for row in rows] == [False, True]
        assert rows[0]["superseded_at"] is not None

        records = await pg_client.fetch_all(f"SELECT reason FROM {corrections}", row_factory="dict")

        assert [row["reason"] for row in records] == ["meter misread"]

    async def test_the_partial_index_is_what_orders_the_writes(
        self, pg_client: PostgresClient
    ) -> None:
        """Insert-then-retire is refused by the real index, which is why the kit retires first.

        The order in the handler is not a preference. Proving it against Postgres rather than
        against the mock is the point: an in-memory store could be made to permit either order,
        and the database cannot.
        """

        readings, corrections = await _tables(pg_client)
        ctx = _ctx(pg_client, readings, corrections)
        cmd = ctx.doc.command(_spec(readings))

        fact = uuid4()
        await cmd.create(ReadingCreate(meter="m-1", kwh=100, root_id=fact, version=1), id=fact)

        with pytest.raises(CoreException) as caught:
            # The successor, while the predecessor is still current — the order §5.2 reads as
            # natural and the index forbids.
            await cmd.create(
                ReadingCreate(meter="m-1", kwh=120, root_id=fact, version=2, supersedes_id=fact),
                id=uuid4(),
            )

        assert caught.value.kind is ExceptionKind.CONFLICT

    async def test_two_successors_of_one_predecessor_are_refused(
        self, pg_client: PostgresClient
    ) -> None:
        """The second guarantee, against the real index: a chain cannot fork.

        Without it two concurrent corrections both insert successors and both commit, and every
        chain walker then picks whichever row it saw first — the defect this kit replaces.
        """

        readings, corrections = await _tables(pg_client)
        ctx = _ctx(pg_client, readings, corrections)
        cmd = ctx.doc.command(_spec(readings))

        fact = uuid4()
        await cmd.create(ReadingCreate(meter="m-1", kwh=100, root_id=fact, version=1), id=fact)
        # Retire it, so the first guarantee is not what refuses the second insert.
        await cmd.update(
            pk=fact, rev=1, dto=ReadingUpdate(is_current=False, superseded_at=utcnow())
        )
        await cmd.create(
            ReadingCreate(meter="m-1", kwh=120, root_id=fact, version=2, supersedes_id=fact),
            id=uuid4(),
        )

        with pytest.raises(CoreException) as caught:
            await cmd.create(
                ReadingCreate(meter="m-1", kwh=130, root_id=fact, version=2, supersedes_id=fact),
                id=uuid4(),
            )

        assert caught.value.kind is ExceptionKind.CONFLICT

    async def test_first_versions_do_not_collide_on_a_null_pointer(
        self, pg_client: PostgresClient
    ) -> None:
        # The contrast for `skip_null`: every first version has `supersedes_id = NULL`, so a
        # guarantee counting nulls as values would refuse the second fact ever created.
        readings, corrections = await _tables(pg_client)
        ctx = _ctx(pg_client, readings, corrections)
        cmd = ctx.doc.command(_spec(readings))

        for _ in range(3):
            fact = uuid4()
            await cmd.create(ReadingCreate(meter="m", kwh=1, root_id=fact, version=1), id=fact)

        rows = await pg_client.fetch_all(f"SELECT id FROM {readings}", row_factory="dict")

        assert len(rows) == 3


# ....................... #


class TestAMissingMigrationIsABootFailure:
    """A declared guarantee with no index behind it refuses at startup, naming the DDL.

    That is the intent and it is still a new way to fail at deploy time, so the refusal has to
    carry everything an operator needs to fix it — otherwise the failure mode of the safety
    feature is a person reading framework source at 3am.
    """

    @staticmethod
    async def _validate(pg_client: PostgresClient, table: str) -> None:
        introspector = PostgresIntrospector(client=pg_client)
        ctx = context_from_deps(Deps.plain({PostgresIntrospectorDepKey: introspector}))
        hook = PostgresDocumentSchemaValidationHook(
            specs=(
                PostgresDocumentSchemaSpec(
                    name="readings",
                    read_model=ReadingRead,
                    read_relation=("public", table),
                    write_domain_model=Reading,
                    write_create_model=ReadingCreate,
                    write_relation=("public", table),
                    bookkeeping_strategy="application",
                    guarantees=(ONE_CURRENT_VERSION, ONE_SUCCESSOR),
                ),
            ),
        )

        await hook(ctx)

    async def test_both_indexes_present_passes(self, pg_client: PostgresClient) -> None:
        readings, _ = await _tables(pg_client)

        await self._validate(pg_client, readings)

    async def test_a_dropped_current_index_refuses_and_names_the_ddl(
        self, pg_client: PostgresClient
    ) -> None:
        readings, _ = await _tables(pg_client)
        await pg_client.execute(f"DROP INDEX {readings}_root_id_idx;")

        with pytest.raises(CoreException) as caught:
            await self._validate(pg_client, readings)

        message = caught.value.summary

        assert "CREATE UNIQUE INDEX" in message
        assert "root_id" in message
        assert "The migration is what satisfies a guarantee" in message

    async def test_a_dropped_successor_index_refuses_too(self, pg_client: PostgresClient) -> None:
        # Both guarantees are load-bearing, so both are checked; validating only the first
        # would leave the fork the second exists to prevent.
        readings, _ = await _tables(pg_client)
        await pg_client.execute(f"DROP INDEX {readings}_supersedes_id_idx;")

        with pytest.raises(CoreException) as caught:
            await self._validate(pg_client, readings)

        assert "supersedes_id" in caught.value.summary


# ....................... #


class TestBothStoresRefuseTheSameWay:
    async def test_a_second_current_version_is_a_conflict_on_both(
        self, pg_client: PostgresClient
    ) -> None:
        """The mock is the canonical superset, so its refusal has to match the real one.

        A mock stricter or looser than the backend makes a green simulation meaningless, and the
        only way to know is to provoke the same violation against both and compare.
        """

        readings, corrections = await _tables(pg_client)
        pg_ctx = _ctx(pg_client, readings, corrections)
        spec = _spec(readings)

        fact = uuid4()
        await pg_ctx.doc.command(spec).create(
            ReadingCreate(meter="m-1", kwh=100, root_id=fact, version=1), id=fact
        )

        with pytest.raises(CoreException) as from_postgres:
            await pg_ctx.doc.command(spec).create(
                ReadingCreate(meter="m-1", kwh=120, root_id=fact, version=2, supersedes_id=fact),
                id=uuid4(),
            )

        mock_ctx = context_from_modules(MockDepsModule())
        mock_cmd = mock_ctx.doc.command(spec)
        await mock_cmd.create(ReadingCreate(meter="m-1", kwh=100, root_id=fact, version=1), id=fact)

        with pytest.raises(CoreException) as from_mock:
            await mock_cmd.create(
                ReadingCreate(meter="m-1", kwh=120, root_id=fact, version=2, supersedes_id=fact),
                id=uuid4(),
            )

        assert from_postgres.value.kind is from_mock.value.kind is ExceptionKind.CONFLICT
