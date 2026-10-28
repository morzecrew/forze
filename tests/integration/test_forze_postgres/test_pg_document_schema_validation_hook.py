"""Integration: PostgresDocumentSchemaValidationHook against real relations."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.guarantees import UniqueTogether
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


# ....................... #


class _ReadWithCustomer(_Read):
    customer: str | None = None


@pytest.mark.parametrize("kind", ["VIEW", "MATERIALIZED VIEW"])
async def test_a_view_passes_its_required_fields(pg_client: PostgresClient, kind: str) -> None:
    # The read view of an aggregate with joined display fields. Postgres records NOT NULL on
    # tables only, so every column of a view or a materialized view reads nullable.
    suf = uuid4().hex[:12]
    orders, customers, view = (f"view_{part}_{suf}" for part in ("orders", "customers", "read"))

    await pg_client.execute(f"CREATE TABLE {customers} (id uuid PRIMARY KEY, label text NOT NULL);")
    await pg_client.execute(
        f"CREATE TABLE {orders} (id uuid PRIMARY KEY, name text NOT NULL, customer_id uuid);"
    )
    await pg_client.execute(
        f"CREATE {kind} {view} AS SELECT o.id, o.name, c.label AS customer "
        f"FROM {orders} o LEFT JOIN {customers} c ON c.id = o.customer_id;"
    )

    intro = PostgresIntrospector(client=pg_client)
    columns = await intro.get_column_types(schema="public", relation=view)
    assert not any(column.not_null for column in columns.values())

    await PostgresDocumentSchemaValidationHook(
        specs=(
            PostgresDocumentSchemaSpec(
                name="orders", read_model=_ReadWithCustomer, read_relation=("public", view)
            ),
        ),
    )(context_from_deps(Deps.plain({PostgresIntrospectorDepKey: intro})))


async def test_every_failing_document_is_reported_in_one_error(
    pg_client: PostgresClient,
) -> None:
    suf = uuid4().hex[:12]
    good, nullable, missing, unguarded = (
        f"agg_{kind}_{suf}" for kind in ("good", "nullable", "missing", "unguarded")
    )

    await pg_client.execute(f"CREATE TABLE {good} (id uuid PRIMARY KEY, name text NOT NULL);")
    await pg_client.execute(f"CREATE TABLE {nullable} (id uuid PRIMARY KEY, name text);")
    await pg_client.execute(f"CREATE TABLE {missing} (id uuid PRIMARY KEY);")
    await pg_client.execute(
        f"CREATE TABLE {unguarded} (id uuid PRIMARY KEY, rev integer NOT NULL, "
        "created_at timestamptz NOT NULL, last_update_at timestamptz NOT NULL, "
        "name text NOT NULL);"
    )

    specs = [
        PostgresDocumentSchemaSpec(name=name, read_model=_Read, read_relation=("public", table))
        for name, table in (("good", good), ("nullable", nullable), ("missing", missing))
    ]
    specs.append(
        PostgresDocumentSchemaSpec(
            name="unguarded",
            read_model=_Read,
            read_relation=("public", unguarded),
            write_domain_model=_Domain,
            write_create_model=_Create,
            write_relation=("public", unguarded),
            bookkeeping_strategy="application",
            guarantees=(UniqueTogether(fields=("name",)),),
        )
    )
    intro = PostgresIntrospector(client=pg_client)
    ctx = context_from_deps(Deps.plain({PostgresIntrospectorDepKey: intro}))

    with pytest.raises(CoreException) as caught:
        await PostgresDocumentSchemaValidationHook(specs=specs)(ctx)

    assert caught.value.code == "postgres_schema_validation_failed"
    assert caught.value.details is not None
    failures = caught.value.details["failures"]
    assert [f["document"] for f in failures] == ["nullable", "missing", "unguarded"]
    assert "nullable column" in failures[0]["summary"]
    assert "missing columns" in failures[1]["summary"]
    assert "no valid unique index" in failures[2]["summary"]

    for name in ("nullable", "missing", "unguarded"):
        assert f"\n  - {name}: " in caught.value.summary
