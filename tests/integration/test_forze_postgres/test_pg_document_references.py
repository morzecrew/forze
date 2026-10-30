"""Integration: a declared reference against a real Postgres.

**Validation** — startup checks that the foreign key keeping a declared reference is there with
the declared ``ON DELETE``, names the DDL when it is not, and warns about a cascading foreign key
no spec declares.

**Parity** — the in-memory store applies the same delete action, and refuses a ``restrict`` with
the same error kind the client maps Postgres's restrict violation to.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.guarantees import Reference, ReferenceAction
from forze.application.contracts.inventory import SpecRegistry
from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule
from forze_postgres.execution.deps import PostgresDepsModule
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import PostgresIntrospectorDepKey
from forze_postgres.execution.lifecycle import (
    PostgresDocumentSchemaValidationHook,
    postgres_document_schema_specs,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.catalog.validation.validate_schema import PostgresDocumentSchemaSpec
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps, context_from_modules

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #


class _LineRead(BaseModel):
    id: UUID
    stage_id: UUID | None = None


class _Line(Document):
    stage_id: UUID | None = None


class _LineCreate(CreateDocumentCmd):
    stage_id: UUID | None = None


class _StageRead(ReadDocument):
    pass


class _Stage(Document):
    pass


class _MockLineRead(ReadDocument):
    stage_id: UUID | None = None


def _reference(on_delete: ReferenceAction) -> Reference:
    return Reference(field="stage_id", spec="stages", on_delete=on_delete)


async def _tables(pg_client: PostgresClient, *, tenant: bool = False) -> tuple[str, str]:
    suffix = uuid4().hex[:10]
    stages, lines = f"ref_stages_{suffix}", f"ref_lines_{suffix}"
    bookkeeping = (
        "rev integer NOT NULL, created_at timestamptz NOT NULL, last_update_at timestamptz NOT NULL"
    )
    scoped = "tenant_id uuid NOT NULL, " if tenant else ""

    await pg_client.execute(
        f"CREATE TABLE {stages} (id uuid PRIMARY KEY, {scoped}{bookkeeping}"
        + (", UNIQUE (tenant_id, id)" if tenant else "")
        + ");"
    )
    await pg_client.execute(
        f"CREATE TABLE {lines} (id uuid PRIMARY KEY, {scoped}{bookkeeping}, stage_id uuid);"
    )

    return stages, lines


async def _foreign_key(pg_client: PostgresClient, stages: str, lines: str, tail: str) -> None:
    await pg_client.execute(
        f"ALTER TABLE {lines} ADD FOREIGN KEY (stage_id) REFERENCES {stages} (id) {tail};"
    )


async def _validate(
    pg_client: PostgresClient,
    stages: str,
    lines: str,
    *guarantees: Reference,
    bound: bool = True,
    warn_undeclared: bool = True,
) -> list[str]:
    intro = PostgresIntrospector(client=pg_client)
    ctx = context_from_deps(Deps.plain({PostgresIntrospectorDepKey: intro}))
    hook = PostgresDocumentSchemaValidationHook(
        specs=(
            PostgresDocumentSchemaSpec(
                name="lines",
                read_model=_LineRead,
                read_relation=("public", lines),
                write_domain_model=_Line,
                write_create_model=_LineCreate,
                write_relation=("public", lines),
                bookkeeping_strategy="application",
                guarantees=guarantees,
                warn_undeclared=warn_undeclared,
                bound_relations={"stages": ("public", stages)} if bound else {},
            ),
        ),
    )

    with patch("forze_postgres.kernel.catalog.validation.validate_schema.logger") as log:
        await hook(ctx)

    return [call.args[0] % call.args[1:] for call in log.warning.call_args_list]


# ....................... #


class TestValidation:
    async def test_a_matching_foreign_key_passes(self, pg_client: PostgresClient) -> None:
        stages, lines = await _tables(pg_client)
        await _foreign_key(pg_client, stages, lines, "ON DELETE CASCADE")

        assert await _validate(pg_client, stages, lines, _reference("cascade")) == []

    async def test_a_missing_foreign_key_refuses_and_names_the_ddl(
        self, pg_client: PostgresClient
    ) -> None:
        stages, lines = await _tables(pg_client)

        with pytest.raises(CoreException) as caught:
            await _validate(pg_client, stages, lines, _reference("cascade"))

        ddl = (
            f"ALTER TABLE public.{lines} ADD FOREIGN KEY (stage_id) REFERENCES public.{stages} "
            "(id) ON DELETE CASCADE;"
        )
        assert ddl in caught.value.summary

        await pg_client.execute(ddl)

        assert await _validate(pg_client, stages, lines, _reference("cascade")) == []

    async def test_another_delete_action_does_not_count(self, pg_client: PostgresClient) -> None:
        stages, lines = await _tables(pg_client)
        await _foreign_key(pg_client, stages, lines, "ON DELETE RESTRICT")

        with pytest.raises(CoreException, match="ON DELETE CASCADE") as caught:
            await _validate(pg_client, stages, lines, _reference("cascade"))

        # Dropped first, or the old ON DELETE stays in force beside the new key.
        drop = f"ALTER TABLE public.{lines} DROP CONSTRAINT {lines}_stage_id_fkey;"
        assert drop in caught.value.summary

    async def test_set_null_is_kept_by_set_null(self, pg_client: PostgresClient) -> None:
        stages, lines = await _tables(pg_client)
        await _foreign_key(pg_client, stages, lines, "ON DELETE SET NULL")

        assert await _validate(pg_client, stages, lines, _reference("set_null")) == []

    async def test_restrict_is_kept_by_restrict(self, pg_client: PostgresClient) -> None:
        stages, lines = await _tables(pg_client)
        await _foreign_key(pg_client, stages, lines, "ON DELETE RESTRICT")

        assert await _validate(pg_client, stages, lines, _reference("restrict")) == []

    async def test_restrict_is_kept_by_an_immediate_no_action(
        self, pg_client: PostgresClient
    ) -> None:
        # A plain key refuses the delete within its statement, mapped to the same `conflict`.
        stages, lines = await _tables(pg_client)
        await _foreign_key(pg_client, stages, lines, "")

        assert await _validate(pg_client, stages, lines, _reference("restrict")) == []

    async def test_restrict_is_not_kept_by_a_deferred_no_action(
        self, pg_client: PostgresClient
    ) -> None:
        # Checked at commit: a transaction deleting the children after the parent passes there.
        stages, lines = await _tables(pg_client)
        await _foreign_key(pg_client, stages, lines, "DEFERRABLE INITIALLY DEFERRED")

        with pytest.raises(CoreException, match="deferred to commit"):
            await _validate(pg_client, stages, lines, _reference("restrict"))

    async def test_set_null_over_a_tenant_key_must_clear_the_field_alone(
        self, pg_client: PostgresClient
    ) -> None:
        # Without a column list it clears tenant_id too, and the row leaves its tenant.
        stages, lines = await _tables(pg_client, tenant=True)
        key = (
            f"ALTER TABLE {lines} ADD CONSTRAINT {lines}_stage FOREIGN KEY (tenant_id, stage_id) "
            f"REFERENCES {stages} (tenant_id, id) ON DELETE SET NULL"
        )
        await pg_client.execute(key + ";")

        with pytest.raises(CoreException) as caught:
            await _validate(pg_client, stages, lines, _reference("set_null"))

        assert "clears every column of the key" in caught.value.summary
        assert f"DROP CONSTRAINT {lines}_stage;" in caught.value.summary
        assert "ON DELETE SET NULL (stage_id);" in caught.value.summary

        await pg_client.execute(f"ALTER TABLE {lines} DROP CONSTRAINT {lines}_stage;")
        await pg_client.execute(key + " (stage_id);")

        reported = await _validate(pg_client, stages, lines, _reference("set_null"))

        assert not [warning for warning in reported if "foreign key" in warning]

    async def test_set_null_over_a_not_null_column_is_refused(
        self, pg_client: PostgresClient
    ) -> None:
        # Postgres would refuse every delete that reaches a row.
        stages, lines = await _tables(pg_client)
        await _foreign_key(pg_client, stages, lines, "ON DELETE SET NULL")
        await pg_client.execute(f"ALTER TABLE {lines} ALTER COLUMN stage_id SET NOT NULL;")

        with pytest.raises(CoreException, match="is NOT NULL"):
            await _validate(pg_client, stages, lines, _reference("set_null"))

    async def test_a_key_beside_the_tenant_column_counts(self, pg_client: PostgresClient) -> None:
        stages, lines = await _tables(pg_client, tenant=True)
        await pg_client.execute(
            f"ALTER TABLE {lines} ADD FOREIGN KEY (tenant_id, stage_id) "
            f"REFERENCES {stages} (tenant_id, id) ON DELETE CASCADE;"
        )

        reported = await _validate(pg_client, stages, lines, _reference("cascade"))

        assert not [warning for warning in reported if "foreign key" in warning]

    async def test_a_referenced_spec_the_module_does_not_bind_is_refused(
        self, pg_client: PostgresClient
    ) -> None:
        stages, lines = await _tables(pg_client)
        await _foreign_key(pg_client, stages, lines, "ON DELETE CASCADE")

        with pytest.raises(CoreException, match="does not bind to a fixed relation"):
            await _validate(pg_client, stages, lines, _reference("cascade"), bound=False)

    async def test_an_undeclared_cascading_key_is_reported(self, pg_client: PostgresClient) -> None:
        stages, lines = await _tables(pg_client)
        await _foreign_key(pg_client, stages, lines, "ON DELETE CASCADE")

        reported = await _validate(pg_client, stages, lines)

        assert len(reported) == 1
        assert "declared by no guarantee" in reported[0]
        assert "Reference(field='stage_id', spec='stages', on_delete='cascade')" in reported[0]
        # A spec the framework owns is not reported: the application cannot declare on it.
        assert await _validate(pg_client, stages, lines, warn_undeclared=False) == []

    async def test_an_undeclared_restricting_key_is_not_reported(
        self, pg_client: PostgresClient
    ) -> None:
        # It changes no row the in-memory store keeps.
        stages, lines = await _tables(pg_client)
        await _foreign_key(pg_client, stages, lines, "ON DELETE RESTRICT")

        assert await _validate(pg_client, stages, lines) == []

    async def test_specs_from_the_catalogue_find_the_referenced_relation(
        self, pg_client: PostgresClient
    ) -> None:
        stages, lines = await _tables(pg_client)
        write: Any = {"domain": _Line, "create_cmd": _LineCreate}
        registry = (
            SpecRegistry()
            .register(
                DocumentSpec(
                    name="stages",
                    read=_StageRead,
                    write={"domain": _Stage, "create_cmd": CreateDocumentCmd},
                ),
                DocumentSpec(
                    name="lines", read=_LineRead, write=write, guarantees=(_reference("cascade"),)
                ),
            )
            .freeze()
        )
        deps = PostgresDepsModule(
            client=pg_client,
            rw_documents={
                name: PostgresDocumentConfig(
                    read=("public", table),
                    write=("public", table),
                    bookkeeping_strategy="application",
                )
                for name, table in (("stages", stages), ("lines", lines))
            },
        )
        intro = PostgresIntrospector(client=pg_client)
        ctx = context_from_deps(Deps.plain({PostgresIntrospectorDepKey: intro}))
        hook = PostgresDocumentSchemaValidationHook(
            specs=postgres_document_schema_specs(registry, deps)
        )

        with pytest.raises(CoreException, match=f"REFERENCES public.{stages} \\(id\\)"):
            await hook(ctx)

        await _foreign_key(pg_client, stages, lines, "ON DELETE CASCADE")
        await hook(ctx)


# ....................... #


def _mock_specs(on_delete: ReferenceAction) -> tuple[DocumentSpec[Any, Any, Any, Any], ...]:
    stages = DocumentSpec(
        name="stages",
        read=_StageRead,
        write=DocumentWriteTypes(domain=_Stage, create_cmd=CreateDocumentCmd),
    )
    lines = DocumentSpec(
        name="lines",
        read=_MockLineRead,
        write=DocumentWriteTypes(domain=_Line, create_cmd=_LineCreate),
        guarantees=(_reference(on_delete),),
    )

    return stages, lines


async def _rows(pg_client: PostgresClient, stages: str, lines: str) -> UUID:
    stage = uuid4()
    await pg_client.execute(
        f"INSERT INTO {stages} (id, rev, created_at, last_update_at) VALUES (%s, 1, now(), now());",
        [stage],
    )
    await pg_client.execute(
        f"INSERT INTO {lines} (id, rev, created_at, last_update_at, stage_id)"
        " VALUES (%s, 1, now(), now(), %s);",
        [uuid4(), stage],
    )

    return stage


class TestMockAndPostgresAgree:
    @pytest.mark.parametrize("tail", ["ON DELETE RESTRICT", ""])
    async def test_both_stores_refuse_a_restricted_delete_the_same_way(
        self, pg_client: PostgresClient, tail: str
    ) -> None:
        # RESTRICT raises a restrict violation and a plain key a foreign-key violation; a hard
        # delete through the document port refuses both as the in-memory store does.
        stages, lines = await _tables(pg_client)
        await _foreign_key(pg_client, stages, lines, tail)
        stage = await _rows(pg_client, stages, lines)
        stage_spec, line_spec = _mock_specs("restrict")
        postgres = context_from_deps(
            PostgresDepsModule(
                client=pg_client,
                rw_documents={
                    "stages": PostgresDocumentConfig(
                        read=("public", stages),
                        write=("public", stages),
                        bookkeeping_strategy="application",
                    )
                },
            )()
        )

        with pytest.raises(CoreException) as from_postgres:
            await postgres.doc.command(stage_spec).kill(stage)

        with pytest.raises(CoreException) as from_postgres_many:
            await postgres.doc.command(stage_spec).kill_many([stage])

        ctx = context_from_modules(MockDepsModule())
        parent = await ctx.doc.command(stage_spec).create(CreateDocumentCmd())
        await ctx.doc.command(line_spec).create(_LineCreate(stage_id=parent.id))

        with pytest.raises(CoreException) as from_mock:
            await ctx.doc.command(stage_spec).kill(parent.id)

        assert from_mock.value.kind.value == "conflict"
        assert from_postgres.value.kind is from_mock.value.kind
        assert from_postgres_many.value.kind is from_mock.value.kind

    @pytest.mark.parametrize("cascade_first", [True, False])
    async def test_postgres_answers_a_restrict_on_a_cascaded_row_by_key_order(
        self, pg_client: PostgresClient, cascade_first: bool
    ) -> None:
        # One row both cascades from the parent and restricts it. Postgres allows the delete when
        # the cascading key was created first and refuses it otherwise; the mock always refuses.
        suffix = uuid4().hex[:10]
        parent, child = f"ref_p_{suffix}", f"ref_c_{suffix}"
        await pg_client.execute(f"CREATE TABLE {parent} (id uuid PRIMARY KEY);")
        await pg_client.execute(f"CREATE TABLE {child} (id uuid PRIMARY KEY, a uuid, b uuid);")
        keys = [
            f"ALTER TABLE {child} ADD FOREIGN KEY (a) REFERENCES {parent} (id) ON DELETE CASCADE;",
            f"ALTER TABLE {child} ADD FOREIGN KEY (b) REFERENCES {parent} (id) ON DELETE RESTRICT;",
        ]

        for key in keys if cascade_first else keys[::-1]:
            await pg_client.execute(key)

        row = uuid4()
        await pg_client.execute(f"INSERT INTO {parent} VALUES (%s);", [row])
        await pg_client.execute(f"INSERT INTO {child} VALUES (%s, %s, %s);", [uuid4(), row, row])

        if cascade_first:
            await pg_client.execute(f"DELETE FROM {parent} WHERE id = %s;", [row])

        else:
            with pytest.raises(CoreException):
                await pg_client.execute(f"DELETE FROM {parent} WHERE id = %s;", [row])

    async def test_both_stores_take_the_children_with_a_cascade(
        self, pg_client: PostgresClient
    ) -> None:
        stages, lines = await _tables(pg_client)
        await _foreign_key(pg_client, stages, lines, "ON DELETE CASCADE")
        stage = await _rows(pg_client, stages, lines)

        await pg_client.execute(f"DELETE FROM {stages} WHERE id = %s;", [stage])
        left = await pg_client.fetch_all(f"SELECT id FROM {lines};", [], row_factory="dict")

        ctx = context_from_modules(MockDepsModule())
        stage_spec, line_spec = _mock_specs("cascade")
        parent = await ctx.doc.command(stage_spec).create(CreateDocumentCmd())
        await ctx.doc.command(line_spec).create(_LineCreate(stage_id=parent.id))
        await ctx.doc.command(stage_spec).kill(parent.id)

        assert left == []
        assert (await ctx.doc.query(line_spec).find_many()).hits == []
