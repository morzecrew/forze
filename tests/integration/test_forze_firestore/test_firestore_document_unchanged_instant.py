"""A stale resend of an instant on Firestore keeps a concurrent change to it."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.execution import Deps
from forze_firestore.execution.deps import ConfigurableFirestoreDocument
from forze_firestore.execution.deps.configs import FirestoreDocumentConfig
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.client import FirestoreClient
from tests.support.document_unchanged_instant import (
    InstantCreate,
    InstantDoc,
    InstantRead,
    InstantUpdate,
    assert_a_stale_resend_keeps_a_concurrent_change,
)
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_a_stale_resend_keeps_a_concurrent_change(firestore_client: FirestoreClient) -> None:
    collection = f"instant_{uuid4().hex[:8]}"
    configurable = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection),
            write=("(default)", collection),
            history=("(default)", f"{collection}_history"),
        ),
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                FirestoreClientDepKey: firestore_client,
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        )
    )
    spec = DocumentSpec(
        name="instant",
        read=InstantRead,
        write=DocumentWriteTypes(
            domain=InstantDoc, create_cmd=InstantCreate, update_cmd=InstantUpdate
        ),
        history_enabled=True,
    )

    await assert_a_stale_resend_keeps_a_concurrent_change(
        ctx.document.command(spec), ctx.document.query(spec)
    )
