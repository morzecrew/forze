"""Unit tests for forze_kits.aggregates.storage.handlers."""

from datetime import timedelta

import pytest

from forze.application.contracts.storage import StorageSpec, UploadSession
from forze.base.exceptions import CoreException
from forze_kits.aggregates.storage import (
    BeginUploadRequestDTO,
    CompleteUploadRequestDTO,
    ListObjectsRequestDTO,
    PresignDownloadRequestDTO,
    PresignPartRequestDTO,
    PresignUploadRequestDTO,
    UploadObjectRequestDTO,
    UploadPartDTO,
    UploadSessionRequestDTO,
)
from forze_kits.aggregates.storage.handlers import (
    AbortUpload,
    BeginUpload,
    CompleteUpload,
    DeleteObject,
    DownloadObject,
    ListObjects,
    ListParts,
    PresignDownload,
    PresignPart,
    PresignUpload,
    UploadObject,
)

# ----------------------- #


def _session_dto_to_session(dto) -> UploadSession:
    return UploadSession(
        key=dto.key,
        upload_id=dto.upload_id,
        bucket=dto.bucket,
        content_type=dto.content_type,
    )


class TestStorageHandlers:
    @pytest.mark.asyncio
    async def test_upload_and_download(self, stub_ctx) -> None:
        spec = StorageSpec(name="files")
        query = stub_ctx.storage.query(spec)
        command = stub_ctx.storage.command(spec)

        upload_handler = UploadObject(storage=command)
        dto = UploadObjectRequestDTO(filename="hello.txt", data=b"hello", prefix="docs")
        uploaded = await upload_handler(dto)

        download_handler = DownloadObject(storage=query)
        downloaded = await download_handler(uploaded.key)

        assert uploaded.filename == "hello.txt"
        assert downloaded.data == b"hello"

    @pytest.mark.asyncio
    async def test_list_returns_paginated_objects(self, stub_ctx) -> None:
        spec = StorageSpec(name="files")
        upload_handler = UploadObject(storage=stub_ctx.storage.command(spec))

        await upload_handler(
            UploadObjectRequestDTO(filename="a.txt", data=b"a", prefix="docs")
        )
        await upload_handler(
            UploadObjectRequestDTO(filename="b.txt", data=b"b", prefix="docs")
        )
        await upload_handler(
            UploadObjectRequestDTO(filename="c.txt", data=b"c", prefix="tmp")
        )

        list_handler = ListObjects(storage=stub_ctx.storage.query(spec))
        result = await list_handler(
            ListObjectsRequestDTO(page=1, size=10, prefix="docs")
        )

        assert result.count == 2
        assert len(result.hits) == 2

    @pytest.mark.asyncio
    async def test_delete_removes_object(self, stub_ctx) -> None:
        spec = StorageSpec(name="files")
        command = stub_ctx.storage.command(spec)
        upload_handler = UploadObject(storage=command)
        uploaded = await upload_handler(
            UploadObjectRequestDTO(filename="gone.txt", data=b"x")
        )

        delete_handler = DeleteObject(storage=command)
        await delete_handler(uploaded.key)

        list_handler = ListObjects(storage=stub_ctx.storage.query(spec))
        result = await list_handler(ListObjectsRequestDTO(page=1, size=10))

        assert result.count == 0


# ....................... #


class TestStoragePresignHandlers:
    @pytest.mark.asyncio
    async def test_presign_download_returns_get_url(self, stub_ctx) -> None:
        spec = StorageSpec(name="files")
        handler = PresignDownload(storage=stub_ctx.storage.query(spec))

        result = await handler(
            PresignDownloadRequestDTO(
                key="docs/report.pdf", expires_in=timedelta(minutes=5)
            )
        )

        assert result.method == "GET"
        assert result.url  # the credential is in the response body the client needs
        assert "docs/report.pdf" in result.url

    @pytest.mark.asyncio
    async def test_presign_upload_returns_put_url_with_headers(self, stub_ctx) -> None:
        spec = StorageSpec(name="files")
        handler = PresignUpload(storage=stub_ctx.storage.command(spec))

        result = await handler(
            PresignUploadRequestDTO(
                key="docs/new.pdf",
                expires_in=timedelta(minutes=5),
                content_type="application/pdf",
            )
        )

        assert result.method == "PUT"
        assert result.url
        assert result.headers["Content-Type"] == "application/pdf"


# ....................... #


class TestStorageMultipartHandlers:
    @pytest.mark.asyncio
    async def test_full_multipart_flow(self, stub_ctx) -> None:
        spec = StorageSpec(name="files")
        uploads = stub_ctx.storage.uploads(spec)

        # begin
        begin = BeginUpload(storage=uploads)
        session_dto = await begin(
            BeginUploadRequestDTO(
                key="big/blob.bin", content_type="application/octet-stream"
            )
        )
        assert session_dto.key == "big/blob.bin"
        assert session_dto.upload_id

        # presign each part
        presign = PresignPart(storage=uploads)
        for n in (1, 2, 3):
            part_url = await presign(
                PresignPartRequestDTO(
                    session=session_dto,
                    part_number=n,
                    expires_in=timedelta(minutes=5),
                )
            )
            assert part_url.method == "PUT"
            assert f"part={n}" in part_url.url

        # deposit the part bytes out-of-band via the mock seam (stands in for the
        # client PUT to each presigned part URL).
        session = _session_dto_to_session(session_dto)
        chunks = {1: b"aaaa", 2: b"bbbb", 3: b"cccc"}
        for n, data in chunks.items():
            uploads.deposit_part(session, n, data)

        # list_parts (resume primitive)
        list_parts = ListParts(storage=uploads)
        listed = await list_parts(UploadSessionRequestDTO(session=session_dto))
        assert [p.part_number for p in listed.parts] == [1, 2, 3]
        assert all(p.etag for p in listed.parts)

        # complete -> ObjectHead
        complete = CompleteUpload(storage=uploads)
        head = await complete(
            CompleteUploadRequestDTO(session=session_dto, parts=listed.parts)
        )
        assert head.size == len(b"aaaabbbbcccc")
        assert head.etag

        # the assembled object is now downloadable
        downloaded = await DownloadObject(storage=stub_ctx.storage.query(spec))(
            "big/blob.bin"
        )
        assert downloaded.data == b"aaaabbbbcccc"

    @pytest.mark.asyncio
    async def test_abort_then_complete_errors(self, stub_ctx) -> None:
        spec = StorageSpec(name="files")
        uploads = stub_ctx.storage.uploads(spec)

        session_dto = await BeginUpload(storage=uploads)(
            BeginUploadRequestDTO(key="big/blob.bin")
        )
        session = _session_dto_to_session(session_dto)
        part = uploads.deposit_part(session, 1, b"data")

        await AbortUpload(storage=uploads)(UploadSessionRequestDTO(session=session_dto))

        complete = CompleteUpload(storage=uploads)
        with pytest.raises(CoreException):
            await complete(
                CompleteUploadRequestDTO(
                    session=session_dto,
                    parts=[
                        # round-trip the part the client returned
                        UploadPartDTO(
                            part_number=part.part_number,
                            etag=part.etag,
                            size=part.size,
                        )
                    ],
                )
            )
