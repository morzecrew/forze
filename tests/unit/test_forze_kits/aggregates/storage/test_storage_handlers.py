"""Unit tests for forze_kits.aggregates.storage.handlers."""

import pytest

from forze.application.contracts.storage import StorageSpec
from forze_kits.aggregates.storage import (
    ListObjectsRequestDTO,
    UploadObjectRequestDTO,
)
from forze_kits.aggregates.storage.handlers import (
    DeleteObject,
    DownloadObject,
    ListObjects,
    UploadObject,
)

# ----------------------- #


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
