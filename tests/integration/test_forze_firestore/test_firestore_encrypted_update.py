"""Firestore updates a field-encrypted document of every field type."""

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
from forze.application.execution import CryptoDepsModule, Deps
from forze_firestore.execution.deps import ConfigurableFirestoreDocument
from forze_firestore.execution.deps.configs import FirestoreDocumentConfig
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.client import FirestoreClient
from forze_mock import MockKeyManagement
from tests.support.encrypted_update import (
    ENCRYPTION,
    KEY,
    EncCreate,
    EncDoc,
    EncRead,
    EncUpdate,
    assert_a_stale_update_seals_afresh,
    assert_encrypted_updates,
)
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _documents(
    firestore_client: FirestoreClient, *, history: bool = False
) -> tuple[Any, Any, Any]:
    collection = f"enc_upd_{uuid4().hex[:8]}"
    doc = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection),
            write=("(default)", collection),
            history=("(default)", f"{collection}_history") if history else None,
        )
    )
    ctx = context_from_deps(
        Deps.merge(
            CryptoDepsModule(kms=MockKeyManagement(), directory=KEY)(),
            Deps.plain(
                {
                    FirestoreClientDepKey: firestore_client,
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
        history_enabled=history,
    )

    async def raw(pk: UUID) -> dict[str, Any]:
        found = await firestore_client.get_document(
            await firestore_client.collection(collection), str(pk)
        )
        assert found is not None
        return found

    return ctx.document.command(spec), ctx.document.query(spec), raw


async def test_a_sealed_field_of_any_type_updates(firestore_client: FirestoreClient) -> None:
    await assert_encrypted_updates(*await _documents(firestore_client))


async def test_a_stale_update_seals_afresh(firestore_client: FirestoreClient) -> None:
    await assert_a_stale_update_seals_afresh(*await _documents(firestore_client, history=True))
