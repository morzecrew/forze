"""Firestore writes a mirrored and a ``skip_none`` upsert as the other stores do; with
``set_based`` it takes its usual path."""

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
from tests.support.execution_context import context_from_deps
from tests.support.upsert_modes import (
    ModeCreate,
    ModeDoc,
    ModeRead,
    ModeUpdate,
    assert_upsert_modes,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.mark.parametrize("set_based", [False, True])
async def test_a_mirror_and_skip_none_write_what_the_other_stores_write(
    firestore_client: FirestoreClient, set_based: bool
) -> None:
    collection = f"modes_{uuid4().hex[:8]}"
    configurable = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection), write=("(default)", collection)
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
        name="modes",
        read=ModeRead,
        write=DocumentWriteTypes(domain=ModeDoc, create_cmd=ModeCreate, update_cmd=ModeUpdate),
    )

    await assert_upsert_modes(
        ctx.document.command(spec),
        ctx.document.query(spec),
        [uuid4() for _ in range(3)],
        set_based=set_based,
    )
