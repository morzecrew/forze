"""Integration: PostgresDocumentSchemaValidationHook against real relations."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import CreateDocumentCmd, Document
from forze_postgres.execution.deps.keys import PostgresIntrospectorDepKey
from forze_postgres.execution.document_schema import PostgresDocumentSchemaValidationHook
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient
from forze_postgres.kernel.validate_schema import PostgresDocumentSchemaSpec

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
    ctx = ExecutionContext(
        deps=Deps.plain({PostgresIntrospectorDepKey: intro}),
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
