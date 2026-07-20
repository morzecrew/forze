"""Unit tests for resumable multipart upload sessions on the mock adapter.

Drives the mock end-to-end through the ``deposit_part`` test seam (the mock has
no real HTTP, so a presigned part URL is informational and tests deposit bytes
directly, standing in for the client's PUT to that URL).
"""

from datetime import UTC, datetime, timedelta

import pytest

from forze.application.contracts.storage import UploadPart
from forze.base.exceptions import CoreException
from forze.base.primitives import FrozenTimeSource, bind_time_source
from forze_mock.adapters.storage import MockStorageAdapter
from forze_mock.state import MockState

# ----------------------- #

INSTANT = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def state() -> MockState:
    return MockState()


@pytest.fixture
def adapter(state: MockState) -> MockStorageAdapter:
    return MockStorageAdapter(state=state, bucket="files")


# ----------------------- #


@pytest.mark.asyncio
async def test_presign_part_records_intent_and_validates(
    adapter: MockStorageAdapter,
) -> None:
    session = await adapter.begin_upload("big.bin", content_type="application/octet-stream")

    url = await adapter.presign_part(session, 2, expires_in=timedelta(minutes=5))
    assert url.method == "PUT"
    assert "part=2" in url.url
    assert session.upload_id in url.url

    # Recorded on the state for test observability.
    rec = adapter.state.storage_presigns[-1]
    assert rec["part_number"] == 2
    assert rec["upload_id"] == session.upload_id

    with pytest.raises(CoreException):
        await adapter.presign_part(session, 0, expires_in=timedelta(minutes=5))


@pytest.mark.asyncio
async def test_multipart_assembles_out_of_order_parts(
    adapter: MockStorageAdapter,
) -> None:
    with bind_time_source(FrozenTimeSource(INSTANT)):
        session = await adapter.begin_upload("assembled.bin")

        # Deposit 3 parts OUT OF ORDER (parallel-style).
        p3 = adapter.deposit_part(session, 3, b"ccc")
        p1 = adapter.deposit_part(session, 1, b"aaa")
        p2 = adapter.deposit_part(session, 2, b"bbb")

        # list_parts reports all three, ascending.
        listed = await adapter.list_parts(session)
        assert [p.part_number for p in listed] == [1, 2, 3]
        assert all(p.etag for p in listed)
        assert [p.size for p in listed] == [3, 3, 3]

        head = await adapter.complete_upload(session, [p1, p2, p3])

    assert head.size == 9
    assert head.last_modified == INSTANT

    # Stored bytes == concatenation in part-number order.
    dl = await adapter.download("assembled.bin")
    assert dl.data == b"aaabbbccc"


@pytest.mark.asyncio
async def test_resume_lists_landed_parts(adapter: MockStorageAdapter) -> None:
    session = await adapter.begin_upload("resume.bin")

    # Only 2 of 3 parts land before "interruption".
    adapter.deposit_part(session, 1, b"11111")
    adapter.deposit_part(session, 2, b"22222")

    landed = await adapter.list_parts(session)
    assert [p.part_number for p in landed] == [1, 2]

    # Resume: deposit the missing 3rd part, then complete with all three.
    p3 = adapter.deposit_part(session, 3, b"33333")
    parts = await adapter.list_parts(session)
    assert [p.part_number for p in parts] == [1, 2, 3]

    head = await adapter.complete_upload(session, parts)
    assert head.size == 15

    dl = await adapter.download("resume.bin")
    assert dl.data == b"111112222233333"
    _ = p3


@pytest.mark.asyncio
async def test_complete_requires_parts(adapter: MockStorageAdapter) -> None:
    session = await adapter.begin_upload("empty.bin")

    with pytest.raises(CoreException, match="at least one"):
        await adapter.complete_upload(session, [])


@pytest.mark.asyncio
async def test_complete_rejects_duplicate_part_numbers(
    adapter: MockStorageAdapter,
) -> None:
    session = await adapter.begin_upload("dup.bin")
    p1 = adapter.deposit_part(session, 1, b"aaa")

    # Two parts sharing a part_number would silently corrupt the assembly.
    with pytest.raises(CoreException, match="[Dd]uplicate"):
        await adapter.complete_upload(
            session,
            [p1, UploadPart(part_number=1, etag="other", size=3)],
        )


@pytest.mark.asyncio
async def test_complete_unknown_part_raises(adapter: MockStorageAdapter) -> None:
    session = await adapter.begin_upload("partial.bin")
    adapter.deposit_part(session, 1, b"aaa")

    # Part 2 was never deposited.
    with pytest.raises(CoreException):
        await adapter.complete_upload(
            session,
            [UploadPart(part_number=1), UploadPart(part_number=2)],
        )


@pytest.mark.asyncio
async def test_abort_discards_session(adapter: MockStorageAdapter) -> None:
    session = await adapter.begin_upload("aborted.bin")
    adapter.deposit_part(session, 1, b"aaa")

    await adapter.abort_upload(session)

    # The session is gone: listing or completing now errors.
    with pytest.raises(CoreException):
        await adapter.list_parts(session)

    with pytest.raises(CoreException):
        await adapter.complete_upload(session, [UploadPart(part_number=1)])

    # Abort is best-effort idempotent.
    await adapter.abort_upload(session)


@pytest.mark.asyncio
async def test_completed_session_cannot_be_reused(
    adapter: MockStorageAdapter,
) -> None:
    session = await adapter.begin_upload("once.bin")
    p1 = adapter.deposit_part(session, 1, b"data")

    await adapter.complete_upload(session, [p1])

    # The session is consumed; a second complete errors.
    with pytest.raises(CoreException):
        await adapter.complete_upload(session, [p1])

    with pytest.raises(CoreException):
        await adapter.list_parts(session)


@pytest.mark.asyncio
async def test_deposit_unknown_session_raises(
    adapter: MockStorageAdapter,
) -> None:
    from forze.application.contracts.storage import UploadSession

    bogus = UploadSession(key="x", upload_id="does-not-exist")

    with pytest.raises(CoreException):
        adapter.deposit_part(bogus, 1, b"x")
