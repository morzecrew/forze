"""A read with no limit on Firestore reads past its first batch.

Firestore refuses an offset past zero, so a read drained by offset failed on its second
batch. A read in ``id`` order now seeks past each batch instead.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from forze_firestore.execution.deps import (
    ConfigurableFirestoreDocument,
    FirestoreDocumentConfig,
)
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.client import FirestoreClient
from tests.support.execution_context import context_from_deps
from tests.support.unbounded_scan_parity import (
    SORTS,
    ScanCreate,
    ScanDoc,
    ScanRead,
    expected_order,
    run_id_first_cursor_parity,
    run_unbounded_scan_parity,
    seed,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _ports(client: FirestoreClient) -> tuple[Any, Any]:
    collection = f"scan_{uuid4().hex[:8]}"
    configurable = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection), write=("(default)", collection)
        ),
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                FirestoreClientDepKey: client,
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

    return ctx.document.command(spec), ctx.document.query(spec)


async def test_a_read_without_a_limit_reads_every_batch(
    firestore_client: FirestoreClient,
) -> None:
    # Its cursor seeks on `id` alone and it refuses offsets, so only `id` order drains here.
    await run_unbounded_scan_parity(*_ports(firestore_client), custom_sorts=False)


async def test_a_sorted_read_within_one_batch_breaks_ties_by_document_name(
    firestore_client: FirestoreClient,
) -> None:
    # The tie-breaker orders by `__name__`, the document name forze sets to the id; both
    # directions must come back in the same order as the `id` field would give.
    command, query = _ports(firestore_client)
    created = await command.create_many(seed()[:150])

    for sorts in SORTS:
        page = await query.find_many(sorts=sorts)

        assert [hit.id for hit in page.hits] == expected_order(created, sorts), sorts


async def test_a_sorted_read_past_one_batch_is_refused_not_cut_short(
    firestore_client: FirestoreClient,
) -> None:
    command, query = _ports(firestore_client)
    await command.create_many(seed()[:250])

    with pytest.raises(CoreException, match="offset"):
        await query.find_many(sorts={"grp": "asc"})


async def test_a_cursor_sorted_by_id_first_orders_by_id(firestore_client: FirestoreClient) -> None:
    await run_id_first_cursor_parity(*_ports(firestore_client))


async def test_a_page_sorted_by_id_descending_reads(firestore_client: FirestoreClient) -> None:
    command, query = _ports(firestore_client)
    created = await command.create_many(seed()[:12])
    expected = sorted((row.id for row in created), reverse=True)

    page = await query.find_page(sorts={"id": "desc"}, pagination={"limit": 5})

    assert ([hit.id for hit in page.hits], page.count) == (expected[:5], 12)
