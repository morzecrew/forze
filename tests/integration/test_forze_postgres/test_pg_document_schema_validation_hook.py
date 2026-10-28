"""Integration: PostgresDocumentSchemaValidationHook against real relations."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.inventory import SpecRegistry
from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from forze.domain.models import CreateDocumentCmd, Document
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
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _Read(BaseModel):
    id: UUID
    name: str


class _Domain(Document):
    name: str


class _Create(CreateDocumentCmd):
    name: str


async def test_schema_validation_hook_accepts_history_table(
    pg_client: PostgresClient,
) -> None:
    suf = uuid4().hex[:12]
    main = f"schema_val_{suf}"
    hist = f"schema_val_h_{suf}"

    await pg_client.execute(
        f"""
        CREATE TABLE {main} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )
    await pg_client.execute(
        f"""
        CREATE TABLE {hist} (
            source text NOT NULL,
            id uuid NOT NULL,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            data jsonb NOT NULL,
            PRIMARY KEY (source, id, rev)
        );
        """
    )

    intro = PostgresIntrospector(client=pg_client)
    ctx = context_from_deps(Deps.plain({PostgresIntrospectorDepKey: intro}),
    )
    hook = PostgresDocumentSchemaValidationHook(
        specs=(
            PostgresDocumentSchemaSpec(
                name="schema_val",
                read_model=_Read,
                read_relation=("public", main),
                write_domain_model=_Domain,
                write_create_model=_Create,
                write_relation=("public", main),
                history_enabled=True,
                history_relation=("public", hist),
                bookkeeping_strategy="application",
            ),
        ),
    )

    await hook(ctx)


async def test_specs_from_the_catalogue_check_every_bound_document(
    pg_client: PostgresClient,
) -> None:
    # The document nobody remembered to list is the one with the drifted table: derived from
    # the wiring, it is checked anyway and startup names it.
    suf = uuid4().hex[:12]
    good, drifted = f"cat_good_{suf}", f"cat_drift_{suf}"

    for table, name_column in ((good, "name text NOT NULL,"), (drifted, "")):
        await pg_client.execute(
            f"CREATE TABLE {table} (id uuid PRIMARY KEY, rev integer NOT NULL, "
            f"created_at timestamptz NOT NULL, {name_column} "
            "last_update_at timestamptz NOT NULL);"
        )

    write: Any = {"domain": _Domain, "create_cmd": _Create}
    registry = (
        SpecRegistry()
        .register(
            DocumentSpec(name="good", read=_Read, write=write),
            DocumentSpec(name="drifted", read=_Read, write=write),
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
            for name, table in (("good", good), ("drifted", drifted))
        },
    )
    intro = PostgresIntrospector(client=pg_client)
    ctx = context_from_deps(Deps.plain({PostgresIntrospectorDepKey: intro}))

    with pytest.raises(CoreException, match="missing columns") as caught:
        await PostgresDocumentSchemaValidationHook(
            specs=postgres_document_schema_specs(registry, deps)
        )(ctx)

    assert caught.value.details is not None
    assert caught.value.details["document"] == "drifted read"

    await PostgresDocumentSchemaValidationHook(
        specs=postgres_document_schema_specs(registry, deps, exclude=("drifted",))
    )(ctx)
