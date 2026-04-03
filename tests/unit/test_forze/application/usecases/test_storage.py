"""Unit tests for forze.application.usecases.storage."""

import pytest

from forze.application.contracts.storage import StorageSpec
from forze.application.dto import ListObjectsRequestDTO, UploadObjectRequestDTO
from forze.application.usecases.storage import (
    DeleteObject,
    DownloadObject,
    ListObjects,
    UploadObject,
)

# ----------------------- #


class TestStorageUsecases:
    """Tests for storage usecase workflows."""

    @pytest.mark.asyncio
    async def test_upload_and_download(
        self,
        stub_ctx,
    ) -> None:
        storage = stub_ctx.storage(StorageSpec(name="files"))

        upload_uc = UploadObject(ctx=stub_ctx, storage=storage)
        uploaded = await upload_uc(
            UploadObjectRequestDTO(filename="hello.txt", data=b"hello", prefix="docs")
        )

        download_uc = DownloadObject(ctx=stub_ctx, storage=storage)
        downloaded = await download_uc(uploaded["key"])

        assert uploaded["filename"] == "hello.txt"
        assert downloaded["data"] == b"hello"

    @pytest.mark.asyncio
    async def test_list_returns_paginated_objects(
        self,
        stub_ctx,
    ) -> None:
        storage = stub_ctx.storage(StorageSpec(name="files"))
        upload_uc = UploadObject(ctx=stub_ctx, storage=storage)

        await upload_uc(
            UploadObjectRequestDTO(filename="a.txt", data=b"a", prefix="docs")
        )
        await upload_uc(
            UploadObjectRequestDTO(filename="b.txt", data=b"b", prefix="docs")
        )
        await upload_uc(
            UploadObjectRequestDTO(filename="c.txt", data=b"c", prefix="tmp")
        )

        list_uc = ListObjects(ctx=stub_ctx, storage=storage)
        result = await list_uc(ListObjectsRequestDTO(page=1, size=10, prefix="docs"))

        assert result.count == 2
        assert len(result.hits) == 2

    @pytest.mark.asyncio
    async def test_delete_removes_object(
        self,
        stub_ctx,
    ) -> None:
        storage = stub_ctx.storage(StorageSpec(name="files"))
        upload_uc = UploadObject(ctx=stub_ctx, storage=storage)
        uploaded = await upload_uc(
            UploadObjectRequestDTO(filename="gone.txt", data=b"x")
        )

        delete_uc = DeleteObject(ctx=stub_ctx, storage=storage)
        await delete_uc(uploaded["key"])

        list_uc = ListObjects(ctx=stub_ctx, storage=storage)
        result = await list_uc(ListObjectsRequestDTO(page=1, size=10))

        assert result.count == 0
