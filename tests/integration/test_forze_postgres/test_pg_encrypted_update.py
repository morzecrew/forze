"""Postgres updates a field-encrypted document of every field type."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.contracts.guarantees import SerializedBy
from forze.application.execution import CryptoDepsModule, Deps
from forze_mock import MockKeyManagement
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.encrypted_update import (
    ENCRYPTION,
    KEY,
    SEED,
    EncCreate,
    EncDoc,
    EncRead,
    EncUpdate,
    assert_encrypted_updates,
)
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _documents(pg_client: PostgresClient, *guarantees: SerializedBy) -> tuple[Any, Any, Any]:
    t = f"pg_enc_upd_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL,
            pin text NOT NULL,
            profile text NOT NULL,
            note text NOT NULL,
            hint text
        );
        """
    )
    doc = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", t), write=("public", t), bookkeeping_strategy="application"
        )
    )
    ctx = context_from_deps(
        Deps.merge(
            CryptoDepsModule(kms=MockKeyManagement(), directory=KEY)(),
            Deps.plain(
                {
                    PostgresClientDepKey: pg_client,
                    PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                    DocumentQueryDepKey: doc,
                    DocumentCommandDepKey: doc,
                }
            ),
        )
    )
    spec = DocumentSpec(
        name="enc",
        read=EncRead,
        write=DocumentWriteTypes(domain=EncDoc, create_cmd=EncCreate, update_cmd=EncUpdate),
        encryption=ENCRYPTION,
        guarantees=guarantees,
    )

    async def raw(pk: UUID) -> dict[str, Any]:
        row = await pg_client.fetch_one(f"SELECT pin, profile, note FROM {t} WHERE id = %s", [pk])
        assert row is not None
        return dict(row)

    return ctx.document.command(spec), ctx.document.query(spec), raw


async def test_a_sealed_field_of_any_type_updates(pg_client: PostgresClient) -> None:
    await assert_encrypted_updates(*await _documents(pg_client))


async def test_a_serialized_matching_update_sets_a_sealed_int(pg_client: PostgresClient) -> None:
    # A serialization guarantee works out where the patch moves each matched row, by merging
    # the patch into it: an open one, or a sealed ``int`` fails validation.
    command, query, raw = await _documents(pg_client, SerializedBy(key=("name",)))
    created = await command.create(SEED)

    (updated,) = await command.update_matching({"$values": {"name": "n"}}, EncUpdate(pin=5))

    assert updated.pin == 5 and (await query.get(created.id, skip_cache=True)).pin == 5
    assert (await raw(created.id))["pin"] != "5"
