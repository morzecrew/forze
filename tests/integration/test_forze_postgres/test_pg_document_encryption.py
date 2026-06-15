"""Integration test: Postgres document field encryption end-to-end (real Postgres).

Covers both the same-process path (encrypt seeds the decrypt cache) and a
cross-process cold read (a fresh keyring whose decrypt cache is empty, forcing the
gateway's async ``ensure_unwrapped`` pre-pass before the synchronous decode).
"""

from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import (
    FieldEncryption,
    KeyRef,
    StaticKeyDirectory,
)
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


class _PersonEmailView(BaseModel):
    id: UUID
    email: str


_SPEC = DocumentSpec(
    name="people_ns",
    read=_PersonRead,
    write={  # type: ignore[arg-type]
        "domain": _Person,
        "create_cmd": _PersonCreate,
        "update_cmd": _PersonUpdate,
    },
    encryption=FieldEncryption(encrypted=frozenset({"email"})),
)

_SPEC_BOUND = DocumentSpec(
    name="people_ns",
    read=_PersonRead,
    write={  # type: ignore[arg-type]
        "domain": _Person,
        "create_cmd": _PersonCreate,
        "update_cmd": _PersonUpdate,
    },
    encryption=FieldEncryption(encrypted=frozenset({"email"}), binds_record_id=True),
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

    # Typed projection (`select_page`) over the encrypted field decrypts too.
    page = await _ctx(pg_client).document.query(_SPEC).select_page(
        _PersonEmailView,
        filters={"$values": {"name": "Alice"}},
    )
    assert page.count == 1
    assert page.hits[0].email == "alice@example.com"

    # Raw field-dict projections (`project` / `project_page`) decrypt too — and on a
    # fresh context (cold keyring), exercising the projection decrypt pre-pass.
    one = await _ctx(pg_client).document.query(_SPEC).project(
        {"$values": {"name": "Alice"}}, ["id", "email"]
    )
    assert one is not None
    assert one["email"] == "alice@example.com"

    raw_page = await _ctx(pg_client).document.query(_SPEC).project_page(
        ["id", "email"],
        filters={"$values": {"name": "Alice"}},
    )
    assert raw_page.count == 1
    assert raw_page.hits[0]["email"] == "alice@example.com"

    # A projection of only plaintext fields is unaffected (no decryption needed).
    name_only = await _ctx(pg_client).document.query(_SPEC).project(
        {"$values": {"name": "Alice"}}, ["name"]
    )
    assert name_only == {"name": "Alice"}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_reencrypt_documents_refreshes_envelope(
    pg_client: PostgresClient,
) -> None:
    from forze.application.integrations.crypto import reencrypt_documents

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
    created = await ctx.document.command(_SPEC).create(
        _PersonCreate(name="Alice", email="alice@example.com")
    )

    before = (
        await pg_client.fetch_one(
            "SELECT email FROM people WHERE id = %s", [created.id], row_factory="dict"
        )
    )["email"]

    # Re-encrypt sweep (fresh context/keyring, like a maintenance job).
    sweep = _ctx(pg_client)
    count = await reencrypt_documents(
        sweep.document.query(_SPEC),
        sweep.document.command(_SPEC),
        to_update=lambda d: _PersonUpdate(email=d.email),
    )

    after = (
        await pg_client.fetch_one(
            "SELECT email FROM people WHERE id = %s", [created.id], row_factory="dict"
        )
    )["email"]

    assert count == 1
    assert after != before  # re-encrypted under a fresh data key
    # Still decrypts to the original value.
    assert (await _ctx(pg_client).document.query(_SPEC).get(created.id)).email == (
        "alice@example.com"
    )


# ....................... #


async def _create_people_table(pg_client: PostgresClient) -> None:
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_record_id_binding_update_threads_pk(
    pg_client: PostgresClient,
) -> None:
    """The patch path threads the pk, so an updated bound field reads back (cold)."""

    await _create_people_table(pg_client)

    writer = _ctx(pg_client).document.command(_SPEC_BOUND)
    created = await writer.create(_PersonCreate(name="Alice", email="alice@example.com"))
    await writer.update(created.id, created.rev, _PersonUpdate(email="alice2@example.com"))

    # Cross-process cold read decrypts the id-bound ciphertext written via the patch.
    fresh = await _ctx(pg_client).document.query(_SPEC_BOUND).get(created.id)
    assert fresh.email == "alice2@example.com"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_record_id_binding_rejects_transplant(
    pg_client: PostgresClient,
) -> None:
    """A ciphertext moved to a different row fails to decrypt under the new id."""

    from forze.base.exceptions import CoreException

    await _create_people_table(pg_client)

    writer = _ctx(pg_client).document.command(_SPEC_BOUND)
    a = await writer.create(_PersonCreate(name="Alice", email="alice@example.com"))
    b = await writer.create(_PersonCreate(name="Bob", email="bob@example.com"))

    a_cipher = (
        await pg_client.fetch_one(
            "SELECT email FROM people WHERE id = %s", [a.id], row_factory="dict"
        )
    )["email"]
    # Transplant A's ciphertext onto B's row (an attacker with DB write access).
    await pg_client.execute(
        "UPDATE people SET email = %s WHERE id = %s", [a_cipher, b.id]
    )

    with pytest.raises(CoreException):
        await _ctx(pg_client).document.query(_SPEC_BOUND).get(b.id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_record_id_binding_refuses_bulk_update_matching(
    pg_client: PostgresClient,
) -> None:
    """A filter-based bulk update of a bound encrypted field is refused (no per-row id)."""

    from forze.base.exceptions import CoreException, ExceptionKind

    await _create_people_table(pg_client)

    writer = _ctx(pg_client).document.command(_SPEC_BOUND)
    await writer.create(_PersonCreate(name="Alice", email="alice@example.com"))

    with pytest.raises(CoreException) as ei:
        await writer.update_matching(
            {"$values": {"name": "Alice"}},
            _PersonUpdate(email="hacked@example.com"),
        )

    assert ei.value.kind is ExceptionKind.PRECONDITION
    assert ei.value.code == "core.crypto.record_id_required"
