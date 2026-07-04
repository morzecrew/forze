"""Keyset cursor pagination for the mock document adapter.

The mock mirrors the real gateways: cursors carry the last returned row's
sort values (encoded with the shared ``encode_keyset_v1`` token machinery)
and pages seek past those values instead of slicing by index, so pagination
stays stable under concurrent inserts and deletes.
"""

from typing import Any
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.contracts.querying import (
    decode_keyset_v1,
    validate_cursor_token,
)
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, ReadDocument
from forze_kits.domain.soft_deletion.models import DocWithSoftDeletion
from forze_mock.adapters import MockDocumentAdapter, MockState

# ----------------------- #


class _ItemDoc(DocWithSoftDeletion):
    title: str


class _ItemCreate(CreateDocumentCmd):
    title: str


class _ItemUpdate(BaseDTO):
    title: str | None = None


class _ItemRead(ReadDocument):
    title: str
    is_deleted: bool = False


def _adapter() -> MockDocumentAdapter[_ItemRead, _ItemDoc, _ItemCreate, _ItemUpdate]:
    spec = DocumentSpec(
        name="items",
        read=_ItemRead,
        write=DocumentWriteTypes(
            domain=_ItemDoc,
            create_cmd=_ItemCreate,
            update_cmd=_ItemUpdate,
        ),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="items",
        read_model=_ItemRead,
        domain_model=_ItemDoc,
    )


# ....................... #


@pytest.mark.asyncio
async def test_cursor_page_stable_under_insert_before_position() -> None:
    """THE keyset regression: an insert before the cursor neither duplicates nor skips rows."""

    doc = _adapter()
    for title in ["b", "d", "f", "h"]:
        await doc.create(_ItemCreate(title=title))

    page1 = await doc.find_cursor(sorts={"title": "asc"}, cursor={"limit": 2})
    assert [h.title for h in page1.hits] == ["b", "d"]
    assert page1.has_more
    assert page1.next_cursor is not None

    # New row sorting BEFORE the current position: an offset cursor would now
    # re-deliver "d" and skip "h"; a keyset cursor is unaffected.
    await doc.create(_ItemCreate(title="a"))

    page2 = await doc.find_cursor(
        sorts={"title": "asc"},
        cursor={"limit": 2, "after": page1.next_cursor},
    )
    assert [h.title for h in page2.hits] == ["f", "h"]
    assert not page2.has_more


@pytest.fixture
def cursor_signing() -> Any:
    from forze.application.contracts.querying import (
        CursorTokenSigner,
        configure_cursor_signer,
    )

    previous = configure_cursor_signer(CursorTokenSigner(secret=b"k" * 32))

    try:
        yield

    finally:
        configure_cursor_signer(previous)


@pytest.mark.asyncio
async def test_signed_cursor_pagination_round_trips(cursor_signing: Any) -> None:
    # With a signer configured, the minted cursor is HMAC-signed and drives the next page.
    doc = _adapter()
    for title in ["b", "d", "f", "h"]:
        await doc.create(_ItemCreate(title=title))

    page1 = await doc.find_cursor(sorts={"title": "asc"}, cursor={"limit": 2})
    assert [h.title for h in page1.hits] == ["b", "d"]
    assert page1.next_cursor is not None
    assert "." in page1.next_cursor  # <payload>.<signature>

    page2 = await doc.find_cursor(
        sorts={"title": "asc"},
        cursor={"limit": 2, "after": page1.next_cursor},
    )
    assert [h.title for h in page2.hits] == ["f", "h"]


@pytest.mark.asyncio
async def test_signed_cursor_rejects_tampered_token(cursor_signing: Any) -> None:
    doc = _adapter()
    for title in ["b", "d", "f", "h"]:
        await doc.create(_ItemCreate(title=title))

    page1 = await doc.find_cursor(sorts={"title": "asc"}, cursor={"limit": 2})
    assert page1.next_cursor is not None
    last = page1.next_cursor[-1]
    tampered = page1.next_cursor[:-1] + ("x" if last != "x" else "y")

    with pytest.raises(CoreException):
        await doc.find_cursor(
            sorts={"title": "asc"}, cursor={"limit": 2, "after": tampered}
        )


@pytest.fixture
def cursor_encryption() -> Any:
    from forze.application.contracts.querying import (
        CursorTokenCipher,
        configure_cursor_cipher,
    )

    previous = configure_cursor_cipher(CursorTokenCipher(secret=b"z" * 32))

    try:
        yield

    finally:
        configure_cursor_cipher(previous)


@pytest.mark.asyncio
async def test_encrypted_cursor_pagination_hides_payload_and_round_trips(
    cursor_encryption: Any,
) -> None:
    # With a cipher configured, the minted cursor is AEAD-encrypted: opaque, yet it still
    # drives the next page end-to-end through the mock adapter.
    doc = _adapter()
    for title in ["b", "d", "f", "h"]:
        await doc.create(_ItemCreate(title=title))

    page1 = await doc.find_cursor(sorts={"title": "asc"}, cursor={"limit": 2})
    assert [h.title for h in page1.hits] == ["b", "d"]
    assert page1.next_cursor is not None
    assert page1.next_cursor.startswith("~")  # encrypted marker

    page2 = await doc.find_cursor(
        sorts={"title": "asc"},
        cursor={"limit": 2, "after": page1.next_cursor},
    )
    assert [h.title for h in page2.hits] == ["f", "h"]


@pytest.mark.asyncio
async def test_encrypted_cursor_rejects_a_previously_plaintext_cursor() -> None:
    # Hard cutover: mint plaintext (no cipher), then enable encryption and reuse it -> rejected.
    from forze.application.contracts.querying import (
        CursorTokenCipher,
        configure_cursor_cipher,
    )

    doc = _adapter()
    for title in ["b", "d", "f", "h"]:
        await doc.create(_ItemCreate(title=title))

    page1 = await doc.find_cursor(sorts={"title": "asc"}, cursor={"limit": 2})
    plaintext = page1.next_cursor
    assert plaintext is not None
    assert not plaintext.startswith("~")

    previous = configure_cursor_cipher(CursorTokenCipher(secret=b"z" * 32))
    try:
        with pytest.raises(CoreException):
            await doc.find_cursor(
                sorts={"title": "asc"}, cursor={"limit": 2, "after": plaintext}
            )
    finally:
        configure_cursor_cipher(previous)


@pytest.mark.asyncio
async def test_signed_cursor_rejects_replay_against_a_different_filter(
    cursor_signing: Any,
) -> None:
    # The P2 context binding: a signed cursor minted for one filter must not drive a page
    # under a different filter — the embedded (spec, tenant, filter) binding won't match.
    doc = _adapter()
    for title in ["b", "d", "f", "h", "j", "l"]:
        await doc.create(_ItemCreate(title=title))

    # Page 1 under a membership filter that keeps every title.
    filters_a = {"$values": {"title": {"$in": ["b", "d", "f", "h", "j", "l"]}}}
    page1 = await doc.find_cursor(
        filters=filters_a, sorts={"title": "asc"}, cursor={"limit": 2}
    )
    assert [h.title for h in page1.hits] == ["b", "d"]
    assert page1.next_cursor is not None

    # Same filter -> the cursor advances (positive control).
    page2 = await doc.find_cursor(
        filters=filters_a,
        sorts={"title": "asc"},
        cursor={"limit": 2, "after": page1.next_cursor},
    )
    assert [h.title for h in page2.hits] == ["f", "h"]

    # Different filter -> the bound cursor is rejected, not silently honored.
    with pytest.raises(CoreException):
        await doc.find_cursor(
            filters={"$values": {"title": {"$in": ["d", "f", "h", "j", "l"]}}},
            sorts={"title": "asc"},
            cursor={"limit": 2, "after": page1.next_cursor},
        )


@pytest.mark.asyncio
async def test_signing_hard_cutover_rejects_a_previously_unsigned_cursor() -> None:
    # Mint unsigned (no signer), then enable signing and try to reuse it: rejected.
    from forze.application.contracts.querying import (
        CursorTokenSigner,
        configure_cursor_signer,
    )

    doc = _adapter()
    for title in ["b", "d", "f", "h"]:
        await doc.create(_ItemCreate(title=title))

    page1 = await doc.find_cursor(sorts={"title": "asc"}, cursor={"limit": 2})
    unsigned = page1.next_cursor
    assert unsigned is not None
    assert "." not in unsigned  # unsigned by default

    previous = configure_cursor_signer(CursorTokenSigner(secret=b"k" * 32))

    try:
        with pytest.raises(CoreException):
            await doc.find_cursor(
                sorts={"title": "asc"}, cursor={"limit": 2, "after": unsigned}
            )

    finally:
        configure_cursor_signer(previous)


@pytest.mark.asyncio
async def test_cursor_page_stable_under_delete_of_returned_row() -> None:
    doc = _adapter()
    created = {
        title: await doc.create(_ItemCreate(title=title))
        for title in ["b", "d", "f", "h"]
    }

    page1 = await doc.find_cursor(sorts={"title": "asc"}, cursor={"limit": 2})
    assert [h.title for h in page1.hits] == ["b", "d"]

    # Hard-delete a row that was already returned on page 1.
    await doc.kill(created["b"].id)

    page2 = await doc.find_cursor(
        sorts={"title": "asc"},
        cursor={"limit": 2, "after": page1.next_cursor},
    )
    assert [h.title for h in page2.hits] == ["f", "h"]


@pytest.mark.asyncio
async def test_cursor_pages_concatenate_to_full_sorted_query() -> None:
    doc = _adapter()
    titles = ["e", "a", "c", "b", "d"]
    for title in titles:
        await doc.create(_ItemCreate(title=title))

    collected: list[str] = []
    cursor: dict[str, Any] = {"limit": 2}

    while True:
        page = await doc.find_cursor(sorts={"title": "asc"}, cursor=cursor)
        collected.extend(h.title for h in page.hits)
        if not page.has_more or page.next_cursor is None:
            break
        cursor = {"limit": 2, "after": page.next_cursor}

    full = await doc.find_many(sorts={"title": "asc"})
    assert collected == [h.title for h in full.hits] == sorted(titles)


@pytest.mark.asyncio
async def test_cursor_descending_sort_pages() -> None:
    doc = _adapter()
    for title in ["b", "d", "f", "h"]:
        await doc.create(_ItemCreate(title=title))

    page1 = await doc.find_cursor(sorts={"title": "desc"}, cursor={"limit": 2})
    assert [h.title for h in page1.hits] == ["h", "f"]
    assert page1.has_more

    page2 = await doc.find_cursor(
        sorts={"title": "desc"},
        cursor={"limit": 2, "after": page1.next_cursor},
    )
    assert [h.title for h in page2.hits] == ["d", "b"]
    assert not page2.has_more
    assert page2.next_cursor is None


@pytest.mark.asyncio
async def test_cursor_before_navigates_back_to_previous_page() -> None:
    doc = _adapter()
    for title in ["b", "d", "f", "h"]:
        await doc.create(_ItemCreate(title=title))

    page1 = await doc.find_cursor(sorts={"title": "asc"}, cursor={"limit": 2})
    page2 = await doc.find_cursor(
        sorts={"title": "asc"},
        cursor={"limit": 2, "after": page1.next_cursor},
    )
    assert page2.prev_cursor is not None

    back = await doc.find_cursor(
        sorts={"title": "asc"},
        cursor={"limit": 2, "before": page2.prev_cursor},
    )
    assert [h.title for h in back.hits] == ["b", "d"]


@pytest.mark.asyncio
async def test_cursor_before_with_more_returns_nearest_previous_page() -> None:
    # A 'before' page that over-fetches (rows remain before the window) must return the rows
    # NEAREST the cursor, not the far end. Walk to the last page, then page back: from the
    # last page's start the nearest previous page is the middle one, and more remain before it.
    doc = _adapter()
    for title in ["b", "d", "f", "h", "j", "l"]:
        await doc.create(_ItemCreate(title=title))

    page1 = await doc.find_cursor(sorts={"title": "asc"}, cursor={"limit": 2})
    page2 = await doc.find_cursor(
        sorts={"title": "asc"},
        cursor={"limit": 2, "after": page1.next_cursor},
    )
    page3 = await doc.find_cursor(
        sorts={"title": "asc"},
        cursor={"limit": 2, "after": page2.next_cursor},
    )
    assert [h.title for h in page3.hits] == ["j", "l"]
    assert page3.prev_cursor is not None

    back = await doc.find_cursor(
        sorts={"title": "asc"},
        cursor={"limit": 2, "before": page3.prev_cursor},
    )
    # Nearest previous page is [f, h] (not [d, f]); b, d still remain before it.
    assert [h.title for h in back.hits] == ["f", "h"]


@pytest.mark.asyncio
async def test_cursor_end_of_results_and_empty_page() -> None:
    doc = _adapter()

    empty = await doc.find_cursor(cursor={"limit": 3})
    assert empty.hits == []
    assert not empty.has_more
    assert empty.next_cursor is None

    for title in ["a", "b"]:
        await doc.create(_ItemCreate(title=title))

    exact = await doc.find_cursor(sorts={"title": "asc"}, cursor={"limit": 2})
    assert [h.title for h in exact.hits] == ["a", "b"]
    assert not exact.has_more
    assert exact.next_cursor is None


@pytest.mark.asyncio
async def test_cursor_token_decodes_via_shared_keyset_machinery() -> None:
    """Machinery parity: mock tokens validate with the shared v1 keyset codec."""

    doc = _adapter()
    for title in ["b", "d", "f"]:
        await doc.create(_ItemCreate(title=title))

    page = await doc.find_cursor(sorts={"title": "asc"}, cursor={"limit": 2})
    assert page.next_cursor is not None

    keys, dirs, nulls, vals = decode_keyset_v1(page.next_cursor)
    assert keys == ["title", "id"]
    assert dirs == ["asc", "asc"]
    assert nulls == ["first", "first"]

    last = page.hits[-1]
    assert vals == [last.title, str(last.id)]

    # Same validation entrypoint the real gateways use before seeking.
    assert validate_cursor_token(
        page.next_cursor,
        sort_keys=["title", "id"],
        directions=["asc", "asc"],
    ) == [last.title, str(last.id)]


@pytest.mark.asyncio
async def test_cursor_rejects_stale_or_invalid_tokens() -> None:
    doc = _adapter()
    for title in ["a", "b", "c"]:
        await doc.create(_ItemCreate(title=title))

    page = await doc.find_cursor(sorts={"title": "asc"}, cursor={"limit": 1})
    assert page.next_cursor is not None

    # Token minted under a different sort spec does not validate.
    with pytest.raises(CoreException):
        await doc.find_cursor(cursor={"limit": 1, "after": page.next_cursor})

    with pytest.raises(CoreException):
        await doc.find_cursor(
            sorts={"title": "asc"},
            cursor={"limit": 1, "after": "not-a-token"},
        )


@pytest.mark.asyncio
async def test_select_cursor_uses_python_mode_like_offset_path() -> None:
    """Audit follow-up: cursor and offset select paths round-trip the same value types."""

    class _Probe(BaseModel):
        id: object
        title: str

    doc = _adapter()
    await doc.create(_ItemCreate(title="x"))

    via_offset = await doc.select_many(_Probe)
    via_cursor = await doc.select_cursor(_Probe, cursor={"limit": 5})

    assert isinstance(via_offset.hits[0].id, UUID)
    assert isinstance(via_cursor.hits[0].id, UUID)
    assert via_cursor.hits[0].id == via_offset.hits[0].id


@pytest.mark.asyncio
async def test_project_cursor_requires_sort_keys_in_projection() -> None:
    doc = _adapter()
    await doc.create(_ItemCreate(title="x"))

    page = await doc.project_cursor(
        ("title", "id"),
        sorts={"title": "asc"},
        cursor={"limit": 5},
    )
    assert page.hits[0]["title"] == "x"

    with pytest.raises(CoreException):
        await doc.project_cursor(
            ("title",),
            sorts={"title": "asc"},
            cursor={"limit": 5},
        )


@pytest.mark.asyncio
async def test_cursor_limit_is_coerced_and_clamped() -> None:
    # The mock keyset path routes its limit through the shared, hardened parser (like the
    # offset path and the real backends): a non-integer is a clean validation error, and a
    # huge value is clamped rather than materializing an unbounded in-memory page.
    from forze.application.contracts.querying.pagination.cursor_page import (
        MAX_CURSOR_LIMIT,
    )

    doc = _adapter()
    for title in ["a", "b", "c"]:
        await doc.create(_ItemCreate(title=title))

    with pytest.raises(CoreException, match="must be an integer"):
        await doc.find_cursor(sorts={"title": "asc"}, cursor={"limit": "abc"})

    # An enormous limit is clamped (no error, no unbounded page): all rows fit under the cap.
    page = await doc.find_cursor(
        sorts={"title": "asc"}, cursor={"limit": MAX_CURSOR_LIMIT + 10_000}
    )
    assert [h.title for h in page.hits] == ["a", "b", "c"]
    assert page.has_more is False
