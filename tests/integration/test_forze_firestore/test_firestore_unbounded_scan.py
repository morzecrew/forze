"""A read with no limit on Firestore reads past its first batch.

Firestore refuses an offset past zero, so a read drained by offset failed on its second
batch. A read in ``id`` order now seeks past each batch instead.
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
from forze_firestore.execution.deps import (
    ConfigurableFirestoreDocument,
    FirestoreDocumentConfig,
)
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.client import FirestoreClient
from tests.support.execution_context import context_from_deps
from tests.support.unbounded_scan_parity import (
    ScanCreate,
    ScanDoc,
    ScanRead,
    run_unbounded_scan_parity,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_a_read_without_a_limit_reads_every_batch(
    firestore_client: FirestoreClient,
) -> None:
    collection = f"scan_{uuid4().hex[:8]}"
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
        name="scan",
        read=ScanRead,
        write=DocumentWriteTypes(domain=ScanDoc, create_cmd=ScanCreate),
    )

    # Its cursor seeks on `id` alone and it refuses offsets, so only `id` order drains here.
    await run_unbounded_scan_parity(
        ctx.document.command(spec), ctx.document.query(spec), custom_sorts=False
    )
