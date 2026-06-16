"""Unit tests: the mock storage adapter records requested SSE (no real crypto).

The mock runs no encryption; it only records what server-side encryption the
route was asked to apply at rest, into ``MockState.storage_sse`` (and the
presign log), so tests can assert "SSE was requested" without a live backend.
"""

from datetime import datetime, timedelta, timezone

import pytest

from forze.application.integrations.storage.client import ObjectStorageSSE
from forze.application.contracts.storage import UploadedObject
from forze.base.primitives import FrozenTimeSource, bind_time_source
from forze_mock.adapters.storage import MockStorageAdapter
from forze_mock.state import MockState

# ----------------------- #

INSTANT = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def state() -> MockState:
    return MockState()


def _adapter(state: MockState, sse: ObjectStorageSSE | None) -> MockStorageAdapter:
    return MockStorageAdapter(state=state, bucket="files", sse=sse)


# ----------------------- #


@pytest.mark.asyncio
async def test_upload_records_requested_sse(state: MockState) -> None:
    adapter = _adapter(state, ObjectStorageSSE(mode="kms", key_id="kid"))

    stored = await adapter.upload(UploadedObject(filename="f.txt", data=b"x"))

    assert state.storage_sse["files"][stored.key] == {"mode": "kms", "key_id": "kid"}


@pytest.mark.asyncio
async def test_upload_records_none_when_sse_off(state: MockState) -> None:
    adapter = _adapter(state, None)

    stored = await adapter.upload(UploadedObject(filename="f.txt", data=b"x"))

    assert state.storage_sse["files"][stored.key] is None


@pytest.mark.asyncio
async def test_copy_records_sse_on_destination(state: MockState) -> None:
    adapter = _adapter(state, ObjectStorageSSE(mode="s3"))
    src = await adapter.upload(UploadedObject(filename="f.txt", data=b"x"))

    await adapter.copy(src.key, "dst/key")

    assert state.storage_sse["files"]["dst/key"] == {"mode": "s3", "key_id": None}


@pytest.mark.asyncio
async def test_presign_upload_records_sse_and_kms_headers(state: MockState) -> None:
    adapter = _adapter(state, ObjectStorageSSE(mode="kms", key_id="kid"))

    with bind_time_source(FrozenTimeSource(INSTANT)):
        vo = await adapter.presign_upload("docs/k1", expires_in=timedelta(minutes=5))

    # KMS presigned PUTs surface the SSE request headers (mirrors real S3).
    assert vo.headers["x-amz-server-side-encryption"] == "aws:kms"
    assert vo.headers["x-amz-server-side-encryption-aws-kms-key-id"] == "kid"

    record = state.storage_presigns[-1]
    assert record["sse"] == {"mode": "kms", "key_id": "kid"}


@pytest.mark.asyncio
async def test_multipart_complete_records_sse(state: MockState) -> None:
    adapter = _adapter(state, ObjectStorageSSE(mode="kms", key_id="kid"))

    session = await adapter.begin_upload("multi/key")
    part = adapter.deposit_part(session, 1, b"chunk")
    await adapter.complete_upload(session, [part])

    assert state.storage_sse["files"]["multi/key"] == {"mode": "kms", "key_id": "kid"}
