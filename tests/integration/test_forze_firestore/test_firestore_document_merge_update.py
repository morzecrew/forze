"""Firestore stores an update merged into a mapping or nested model, not the patch.

Firestore has no ``update_matching``, so only the merge itself is checked here.
"""

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
from tests.support.document_merge_update import (
    MergeCreate,
    MergeDoc,
    MergeRead,
    MergeUpdate,
    assert_updates_merge,
)
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_an_update_merges_into_a_stored_mapping_and_model(
    firestore_client: FirestoreClient,
) -> None:
    collection = f"merge_{uuid4().hex[:8]}"
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
        name="merge",
        read=MergeRead,
        write=DocumentWriteTypes(domain=MergeDoc, create_cmd=MergeCreate, update_cmd=MergeUpdate),
    )

    await assert_updates_merge(ctx.document.command(spec), ctx.document.query(spec))
