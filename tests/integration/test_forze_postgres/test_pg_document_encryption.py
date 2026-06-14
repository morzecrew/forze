"""Integration test: Postgres document field encryption end-to-end (real Postgres).

Covers both the same-process path (encrypt seeds the decrypt cache) and a
cross-process cold read (a fresh keyring whose decrypt cache is empty, forcing the
gateway's async ``ensure_unwrapped`` pre-pass before the synchronous decode).
"""

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
    encrypted_fields=frozenset({"email"}),
)


def _ctx(pg_client: PostgresClient) -> ExecutionContext:
    """Fresh execution context with its OWN keyring (simulates a separate process)."""

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
async def test_pg_document_field_encryption(pg_client: PostgresClient) -> None:
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

    writer = _ctx(pg_client).document.command(_SPEC)
    created = await writer.create(_PersonCreate(name="Alice", email="alice@example.com"))

    # The email column holds ciphertext at rest; name stays plaintext (queryable).
    row = await pg_client.fetch_one(
        "SELECT name, email FROM people WHERE id = %s",
        [created.id],
        row_factory="dict",
    )
    assert row is not None
    assert row["name"] == "Alice"
    assert row["email"] != "alice@example.com"

    # Same-process read decrypts transparently.
    same = await writer.get(created.id)
    assert same.email == "alice@example.com"

    # Cross-process read: a brand-new context/keyring (cold decrypt cache) must run
    # the async ensure_unwrapped pre-pass before the synchronous decode.
    reader = _ctx(pg_client).document.query(_SPEC)
    fresh = await reader.get(created.id)
    assert fresh.name == "Alice"
    assert fresh.email == "alice@example.com"
