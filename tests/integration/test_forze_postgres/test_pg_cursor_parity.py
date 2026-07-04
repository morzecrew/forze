"""Cross-backend keyset cursor parity: real Postgres must match the in-memory oracle.

Exercises multi-key, mixed ``asc``/``desc`` direction, and **nullable** sort keys — the
combination that used to be impossible (mixed directions were rejected) or wrong
(Postgres' plain ``col > ?`` seek silently dropped null-keyed rows, and its default null
placement disagreed with the oracle). Postgres now emits ``NULLS FIRST/LAST`` and a
null-aware seek, so a full forward traversal reproduces the oracle order and covers every
row exactly once.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.execution import Deps, ExecutionContext
from forze_mock.adapters import MockDocumentAdapter, MockState
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.cursor_parity import (
    CursorCreate,
    CursorDoc,
    CursorRead,
    _seq,
    assert_cursor_parity,
    seed_cursor_corpus,
)
from tests.support.execution_context import context_from_deps


@attrs.define
class _CursorPort:
    """A create + find_cursor port adapting the split command/query pair."""

    command: Any
    query: Any

    async def create(self, cmd: Any) -> Any:
        return await self.command.create(cmd)

    async def find_cursor(self, *, filters: Any, cursor: Any, sorts: Any) -> Any:
        return await self.query.find_cursor(filters, cursor=cursor, sorts=sorts)


def _ctx(pg_client: PostgresClient, table: str) -> ExecutionContext:
    doc = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
        )
    )
    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: doc,
                DocumentCommandDepKey: doc,
            }
        )
    )


def _mock_port() -> MockDocumentAdapter[Any, Any, Any, Any]:
    spec = DocumentSpec(
        name="cursor_parity",
        read=CursorRead,
        write=DocumentWriteTypes(domain=CursorDoc, create_cmd=CursorCreate),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="cursor_parity",
        read_model=CursorRead,
        domain_model=CursorDoc,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_cursor_parity_postgres(pg_client: PostgresClient) -> None:
    t = f"cursor_corpus_{uuid4().hex[:12]}"

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            grp integer NOT NULL,
            score integer,
            seq integer NOT NULL
        );
        """
    )

    spec = DocumentSpec(
        name="cursor_parity",
        read=CursorRead,
        write=DocumentWriteTypes(domain=CursorDoc, create_cmd=CursorCreate),
    )
    ctx = _ctx(pg_client, t)
    real = _CursorPort(
        command=ctx.document.command(spec),
        query=ctx.document.query(spec),
    )

    await assert_cursor_parity(real, _mock_port())


@pytest.mark.integration
@pytest.mark.asyncio
async def test_signed_cursor_binding_on_real_postgres(pg_client: PostgresClient) -> None:
    # The document path splits mint (pagination mixin) from verify (read gateway); this proves
    # both frames derive an *identical* (tenant, filter) binding on real Postgres — the
    # same-filter page-2 advance only works if the minted and re-checked digests agree — and
    # that a signed cursor replayed under a different filter is rejected.
    from forze.application.contracts.querying import (
        CursorTokenSigner,
        configure_cursor_signer,
    )
    from forze.base.exceptions import CoreException

    t = f"cursor_signed_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            grp integer NOT NULL,
            score integer,
            seq integer NOT NULL
        );
        """
    )

    spec = DocumentSpec(
        name="cursor_signed",
        read=CursorRead,
        write=DocumentWriteTypes(domain=CursorDoc, create_cmd=CursorCreate),
    )
    ctx = _ctx(pg_client, t)
    real = _CursorPort(
        command=ctx.document.command(spec),
        query=ctx.document.query(spec),
    )
    await seed_cursor_corpus(real)

    filters_all = {"$values": {"grp": {"$in": [0, 1, 2]}}}
    sorts = {"seq": "asc"}

    previous = configure_cursor_signer(CursorTokenSigner(secret=b"k" * 32))

    try:
        page1 = await real.find_cursor(
            filters=filters_all, cursor={"limit": 3}, sorts=sorts
        )
        assert page1.next_cursor is not None
        assert "." in page1.next_cursor  # signed <payload>.<hmac>

        # Same filter -> the split-frame bindings match and the cursor advances.
        page2 = await real.find_cursor(
            filters=filters_all,
            cursor={"limit": 3, "after": page1.next_cursor},
            sorts=sorts,
        )
        assert [_seq(h) for h in page2.hits] == [3, 4, 5]

        # Different filter -> the bound cursor is rejected on real Postgres.
        with pytest.raises(CoreException):
            await real.find_cursor(
                filters={"$values": {"grp": {"$in": [1, 2]}}},
                cursor={"limit": 3, "after": page1.next_cursor},
                sorts=sorts,
            )

    finally:
        configure_cursor_signer(previous)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_encrypted_cursor_on_real_postgres(pg_client: PostgresClient) -> None:
    # Same split mint/verify as the signed test, but AEAD-encrypted: proves the document
    # cursor is opaque, decrypts + advances across pages on real Postgres, and still rejects a
    # replay under a different filter (the binding rides inside the ciphertext).
    from forze.application.contracts.querying import (
        CursorTokenCipher,
        configure_cursor_cipher,
    )
    from forze.base.exceptions import CoreException

    t = f"cursor_enc_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            grp integer NOT NULL,
            score integer,
            seq integer NOT NULL
        );
        """
    )

    spec = DocumentSpec(
        name="cursor_enc",
        read=CursorRead,
        write=DocumentWriteTypes(domain=CursorDoc, create_cmd=CursorCreate),
    )
    ctx = _ctx(pg_client, t)
    real = _CursorPort(
        command=ctx.document.command(spec),
        query=ctx.document.query(spec),
    )
    await seed_cursor_corpus(real)

    filters_all = {"$values": {"grp": {"$in": [0, 1, 2]}}}
    sorts = {"seq": "asc"}

    previous = configure_cursor_cipher(CursorTokenCipher(secret=b"z" * 32))

    try:
        page1 = await real.find_cursor(
            filters=filters_all, cursor={"limit": 3}, sorts=sorts
        )
        assert page1.next_cursor is not None
        assert page1.next_cursor.startswith("~")  # AEAD-encrypted, opaque

        # Same filter -> decrypts, the split-frame bindings match, and the page advances.
        page2 = await real.find_cursor(
            filters=filters_all,
            cursor={"limit": 3, "after": page1.next_cursor},
            sorts=sorts,
        )
        assert [_seq(h) for h in page2.hits] == [3, 4, 5]

        # Different filter -> rejected after decryption on real Postgres.
        with pytest.raises(CoreException):
            await real.find_cursor(
                filters={"$values": {"grp": {"$in": [1, 2]}}},
                cursor={"limit": 3, "after": page1.next_cursor},
                sorts=sorts,
            )

    finally:
        configure_cursor_cipher(previous)
