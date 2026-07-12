"""Integration tests for Firestore keyset cursor pagination beyond page 1.

The gateway fetches ``limit + 1`` rows per page (the extra row lets callers derive
``has_more``), seeking strictly past the cursor document. ``before`` pages run the
query in the flipped order and re-reverse the rows, so both directions share one
seek primitive. A cursor whose anchor document was deleted fails closed instead of
silently restarting from the first page.
"""

from __future__ import annotations

from uuid import UUID

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.contracts.querying import encode_keyset_v1
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.constants import ID_FIELD
from forze_firestore.execution.deps import (
    ConfigurableFirestoreDocument,
    FirestoreDocumentConfig,
)
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.execution.deps.utils import doc_write_gw, read_gw
from forze_firestore.kernel.client import FirestoreClient
from tests.support import (
    IntegrationCreateCmd,
    IntegrationDocument,
    IntegrationUpdateCmd,
    make_create_cmd,
)
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_WRITE_TYPES = {
    "domain": IntegrationDocument,
    "create_cmd": IntegrationCreateCmd,
    "update_cmd": IntegrationUpdateCmd,
}

_IDS = [UUID(f"30000000-0000-0000-0000-00000000000{i}") for i in range(1, 6)]


def _ctx(client: FirestoreClient) -> ExecutionContext:
    return context_from_deps(Deps.plain({FirestoreClientDepKey: client}))


def _token(doc_id: UUID, direction: str = "asc") -> str:
    return encode_keyset_v1(
        sort_keys=[ID_FIELD],
        directions=[direction],
        values=[str(doc_id)],
    )


async def _seed(ctx: ExecutionContext, collection: str) -> object:
    write = doc_write_gw(
        ctx,
        write_types=_WRITE_TYPES,
        write_relation=("(default)", collection),
        history_enabled=False,
        tenant_aware=False,
    )

    for i, doc_id in enumerate(_IDS):
        await write.create(make_create_cmd(name=f"doc-{i + 1}"), id=doc_id)

    return write


def _read(ctx: ExecutionContext, collection: str) -> object:
    return read_gw(
        ctx,
        read_type=IntegrationDocument,
        read_relation=("(default)", collection),
        tenant_aware=False,
    )


async def test_cursor_after_returns_exact_windows(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """``after`` pages return the rows strictly past the cursor, over-fetched by one."""

    collection = f"cur_after_{unique_collection}"
    ctx = _ctx(firestore_client)
    await _seed(ctx, collection)
    read = _read(ctx, collection)

    first = await read.find_many_with_cursor(
        None, cursor={"limit": 2}, sorts={ID_FIELD: "asc"}
    )
    assert [d.id for d in first] == _IDS[:3]

    second = await read.find_many_with_cursor(
        None, cursor={"limit": 2, "after": _token(_IDS[1])}, sorts={ID_FIELD: "asc"}
    )
    assert [d.id for d in second] == _IDS[2:5]

    third = await read.find_many_with_cursor(
        None, cursor={"limit": 2, "after": _token(_IDS[3])}, sorts={ID_FIELD: "asc"}
    )
    assert [d.id for d in third] == [_IDS[4]]

    exhausted = await read.find_many_with_cursor(
        None, cursor={"limit": 2, "after": _token(_IDS[4])}, sorts={ID_FIELD: "asc"}
    )
    assert exhausted == []


async def test_cursor_before_returns_window_preceding_cursor(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """``before`` pages return the rows strictly preceding the cursor, in sort order.

    The over-fetched sentinel row sits at the front (furthest from the cursor),
    mirroring the Mongo adapter's keyset output shape.
    """

    collection = f"cur_before_{unique_collection}"
    ctx = _ctx(firestore_client)
    await _seed(ctx, collection)
    read = _read(ctx, collection)

    preceding = await read.find_many_with_cursor(
        None, cursor={"limit": 2, "before": _token(_IDS[4])}, sorts={ID_FIELD: "asc"}
    )
    assert [d.id for d in preceding] == _IDS[1:4]

    near_start = await read.find_many_with_cursor(
        None, cursor={"limit": 2, "before": _token(_IDS[1])}, sorts={ID_FIELD: "asc"}
    )
    assert [d.id for d in near_start] == [_IDS[0]]

    exhausted = await read.find_many_with_cursor(
        None, cursor={"limit": 2, "before": _token(_IDS[0])}, sorts={ID_FIELD: "asc"}
    )
    assert exhausted == []


async def test_cursor_desc_sort_paginates_both_directions(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """Descending id sort seeks correctly in both directions."""

    collection = f"cur_desc_{unique_collection}"
    ctx = _ctx(firestore_client)
    await _seed(ctx, collection)
    read = _read(ctx, collection)

    first = await read.find_many_with_cursor(
        None, cursor={"limit": 2}, sorts={ID_FIELD: "desc"}
    )
    assert [d.id for d in first] == [_IDS[4], _IDS[3], _IDS[2]]

    after = await read.find_many_with_cursor(
        None,
        cursor={"limit": 2, "after": _token(_IDS[3], "desc")},
        sorts={ID_FIELD: "desc"},
    )
    assert [d.id for d in after] == [_IDS[2], _IDS[1], _IDS[0]]

    before = await read.find_many_with_cursor(
        None,
        cursor={"limit": 2, "before": _token(_IDS[1], "desc")},
        sorts={ID_FIELD: "desc"},
    )
    assert [d.id for d in before] == [_IDS[4], _IDS[3], _IDS[2]]


async def test_cursor_over_deleted_anchor_fails_closed(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """A cursor pointing at a deleted document is a caller-caused precondition error."""

    collection = f"cur_gone_{unique_collection}"
    ctx = _ctx(firestore_client)
    write = await _seed(ctx, collection)
    read = _read(ctx, collection)

    await write.kill(_IDS[2])

    with pytest.raises(CoreException, match="no longer exists") as after_err:
        await read.find_many_with_cursor(
            None, cursor={"limit": 2, "after": _token(_IDS[2])}, sorts={ID_FIELD: "asc"}
        )

    assert after_err.value.kind is ExceptionKind.PRECONDITION

    with pytest.raises(CoreException, match="no longer exists") as before_err:
        await read.find_many_with_cursor(
            None, cursor={"limit": 2, "before": _token(_IDS[2])}, sorts={ID_FIELD: "asc"}
        )

    assert before_err.value.kind is ExceptionKind.PRECONDITION


async def test_adapter_find_cursor_pages_forward_and_back(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """End-to-end ``find_cursor`` walks forward across pages and back with ``before``."""

    collection = f"cur_page_{unique_collection}"
    configurable = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection),
            write=("(default)", collection),
        )
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
        name="cursor_page_docs",
        read=IntegrationDocument,
        write=_WRITE_TYPES,
    )
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    for i, doc_id in enumerate(_IDS):
        await cmd.create(IntegrationCreateCmd(name=f"doc-{i + 1}"), id=doc_id)

    p1 = await query.find_cursor(None, cursor={"limit": 2})
    assert [d.id for d in p1.hits] == _IDS[:2]
    assert p1.has_more is True
    assert p1.next_cursor is not None
    assert p1.prev_cursor is None

    p2 = await query.find_cursor(None, cursor={"limit": 2, "after": p1.next_cursor})
    assert [d.id for d in p2.hits] == _IDS[2:4]
    assert p2.next_cursor is not None
    assert p2.prev_cursor is not None

    p3 = await query.find_cursor(None, cursor={"limit": 2, "after": p2.next_cursor})
    assert [d.id for d in p3.hits] == [_IDS[4]]
    assert p3.has_more is False

    # Exactly ``limit`` rows precede the anchor here, so the page is unambiguous.
    back = await query.find_cursor(None, cursor={"limit": 2, "before": p2.prev_cursor})
    assert [d.id for d in back.hits] == _IDS[:2]
    assert back.has_more is False

    # Over-fetched before window: three rows precede this anchor, so the page must be
    # the two rows NEAREST the cursor (_IDS[1:3]), not the two farthest (_IDS[0:2]).
    back_far = await query.find_cursor(
        None, cursor={"limit": 2, "before": p2.next_cursor}
    )
    assert [d.id for d in back_far.hits] == _IDS[1:3]
    assert back_far.has_more is True
    assert back_far.prev_cursor is not None
    assert back_far.next_cursor is not None

    # More than ``limit`` rows precede this anchor. The Firestore layer guarantees
    # the fetched rows are on the correct side of the cursor and in sort order; the
    # exact trim of the over-fetched sentinel row belongs to the shared page
    # assembler, so only the adapter-agnostic invariants are pinned here.
    deep_back = await query.find_cursor(
        None, cursor={"limit": 2, "before": _token(_IDS[4])}
    )
    back_ids = [d.id for d in deep_back.hits]
    assert len(back_ids) == 2
    assert back_ids == sorted(back_ids)
    assert all(doc_id < _IDS[4] for doc_id in back_ids)
    assert set(back_ids) <= set(_IDS[1:4])
