"""Tests for the resumable multipart upload-session contract surface."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from forze.application.contracts.storage import (
    ObjectHead,
    PresignedUrl,
    StorageUploadSessionPort,
    UploadPart,
    UploadSession,
)
from forze.application.contracts.storage.deps import StorageDeps
from forze.base.exceptions import CoreException

# ----------------------- #
# Value objects


def test_upload_session_masks_upload_id_in_repr() -> None:
    session = UploadSession(key="files/big.bin", upload_id="s3-upload-id-SECRET")

    text = repr(session)

    assert "files/big.bin" in text
    assert "SECRET" not in text
    assert "upload_id" not in text


def test_upload_session_carries_bucket_and_content_type() -> None:
    session = UploadSession(
        key="k",
        upload_id="u",
        bucket="resolved-bucket",
        content_type="application/octet-stream",
    )

    assert session.bucket == "resolved-bucket"
    assert session.content_type == "application/octet-stream"


def test_upload_part_defaults() -> None:
    part = UploadPart(part_number=1)

    assert part.part_number == 1
    assert part.etag == ""
    assert part.size == 0


def test_upload_part_number_must_be_positive() -> None:
    for bad in (0, -1):
        with pytest.raises(ValueError):
            UploadPart(part_number=bad)


# ----------------------- #
# Port protocol


class _StubUploads:
    async def begin_upload(
        self, key: str, *, content_type: str | None = None
    ) -> UploadSession:
        return UploadSession(key=key, upload_id="uid", content_type=content_type)

    async def presign_part(
        self,
        session: UploadSession,
        part_number: int,
        *,
        expires_in: timedelta,
    ) -> PresignedUrl:
        return PresignedUrl(
            url=f"https://stub/{session.key}?part={part_number}",
            method="PUT",
            expires_at=datetime.now(UTC) + expires_in,
        )

    async def list_parts(self, session: UploadSession) -> list[UploadPart]:
        return [UploadPart(part_number=1, etag="e1", size=5)]

    async def complete_upload(
        self,
        session: UploadSession,
        parts: Sequence[UploadPart],
    ) -> ObjectHead:
        return ObjectHead(content_type="application/octet-stream", size=10)

    async def abort_upload(self, session: UploadSession) -> None:
        pass


def test_stub_is_runtime_checkable() -> None:
    assert isinstance(_StubUploads(), StorageUploadSessionPort)


def test_non_conforming_not_instance() -> None:
    class Bad:
        pass

    assert not isinstance(Bad(), StorageUploadSessionPort)


@pytest.mark.asyncio
async def test_stub_lifecycle() -> None:
    stub = _StubUploads()

    session = await stub.begin_upload("files/a", content_type="text/plain")
    assert session.content_type == "text/plain"

    url = await stub.presign_part(session, 1, expires_in=timedelta(minutes=5))
    assert url.method == "PUT"

    parts = await stub.list_parts(session)
    assert parts[0].part_number == 1

    head = await stub.complete_upload(session, parts)
    assert head.size == 10

    await stub.abort_upload(session)


# ----------------------- #
# Deps accessor write-guard (mirrors AnalyticsDeps.command)


def test_uploads_resolves_via_command_guard() -> None:
    port = object()
    ctx = MagicMock()
    ctx.inv_ctx.is_read_only.return_value = False
    ctx.deps.resolve_configurable.return_value = port

    deps = StorageDeps()
    deps.lock(ctx)

    assert deps.uploads(MagicMock()) is port
    assert ctx.deps.resolve_configurable.call_count == 1


def test_uploads_guarded_in_read_only_operation() -> None:
    ctx = MagicMock()
    ctx.inv_ctx.is_read_only.return_value = True

    deps = StorageDeps()
    deps.lock(ctx)

    with pytest.raises(CoreException, match="read-only"):
        deps.uploads(MagicMock())
