"""Integration test: equality search over a deterministically-encrypted Postgres field."""

import pytest

from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import CryptoDepsModule, Deps, ExecutionContext
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockKeyManagement
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

# ----------------------- #


class _Person(Document):
    name: str
    email: str


class _PersonCreate(CreateDocumentCmd):
    name: str
    email: str


class _PersonUpdate(BaseDTO):
    name: str | None = None
    email: str | None = None


class _PersonRead(ReadDocument):
    name: str
    email: str


_SPEC = DocumentSpec(
    name="people_ns",
    read=_PersonRead,
    write={  # type: ignore[arg-type]
        "domain": _Person,
        "create_cmd": _PersonCreate,
        "update_cmd": _PersonUpdate,
    },
    searchable_fields=frozenset({"email"}),
)


def _ctx(pg_client: PostgresClient) -> ExecutionContext:
    configurable = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", "people"),
            write=("public", "people"),
            bookkeeping_strategy="application",
        )
    )
    deps = Deps.merge(
        CryptoDepsModule(
            kms=MockKeyManagement(),
            directory=StaticKeyDirectory(KeyRef(key_id="people-cmk")),
            deterministic_root=b"searchable-root-secret-32-bytes!",
        )(),
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        ),
    )
    return context_from_deps(deps)


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_equality_search_on_encrypted_field(pg_client: PostgresClient) -> None:
    await pg_client.execute("DROP TABLE IF EXISTS people CASCADE;")
    await pg_client.execute(
        """
        CREATE TABLE people (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL,
            email text NOT NULL
        );
        """
    )

    ctx = _ctx(pg_client)
    await ctx.document.command(_SPEC).create(
        _PersonCreate(name="Alice", email="alice@example.com")
    )
    await ctx.document.command(_SPEC).create(
        _PersonCreate(name="Bob", email="bob@example.com")
    )

    # Stored deterministically: equal plaintext would store equal ciphertext.
    row = await pg_client.fetch_one(
        "SELECT email FROM people WHERE name = %s", ["Alice"], row_factory="dict"
    )
    assert row is not None and row["email"] != "alice@example.com"

    # Equality query on the encrypted field — the filter is rewritten to match.
    page = await ctx.document.query(_SPEC).find_page(
        filters={"$values": {"email": "alice@example.com"}},
    )
    assert page.count == 1
    assert page.hits[0].name == "Alice"
    assert page.hits[0].email == "alice@example.com"  # decrypted on read

    # A non-matching value finds nothing.
    empty = await ctx.document.query(_SPEC).find_page(
        filters={"$values": {"email": "nobody@example.com"}},
    )
    assert empty.count == 0
