"""Unit tests for forze.application.usecases.storage."""

import pytest

from forze.application.usecases.storage import (
    DeleteObject,
    DeleteObjectArgs,
    DownloadObject,
    DownloadObjectArgs,
    ListObjects,
    ListObjectsArgs,
    UploadObject,
    UploadObjectArgs,
)

# ----------------------- #


class TestStorageUsecases:
    """Tests for storage usecase workflows."""

    @pytest.mark.asyncio
    async def test_upload_and_download(
        self,
        stub_ctx,
    ) -> None:
        storage = stub_ctx.storage("files")

        upload_uc = UploadObject(ctx=stub_ctx, storage=storage)
        uploaded = await upload_uc(
            UploadObjectArgs(filename="hello.txt", data=b"hello", prefix="docs")
        )

        download_uc = DownloadObject(ctx=stub_ctx, storage=storage)
        downloaded = await download_uc(DownloadObjectArgs(key=uploaded["key"]))

        assert uploaded["filename"] == "hello.txt"
        assert downloaded["data"] == b"hello"

    @pytest.mark.asyncio
    async def test_list_returns_paginated_objects(
        self,
        stub_ctx,
    ) -> None:
        storage = stub_ctx.storage("files")
        upload_uc = UploadObject(ctx=stub_ctx, storage=storage)

        await upload_uc(UploadObjectArgs(filename="a.txt", data=b"a", prefix="docs"))
        await upload_uc(UploadObjectArgs(filename="b.txt", data=b"b", prefix="docs"))
        await upload_uc(UploadObjectArgs(filename="c.txt", data=b"c", prefix="tmp"))

        list_uc = ListObjects(ctx=stub_ctx, storage=storage)
        result = await list_uc(ListObjectsArgs(page=1, size=10, prefix="docs"))

        assert result.count == 2
        assert len(result.hits) == 2

    @pytest.mark.asyncio
    async def test_delete_removes_object(
        self,
        stub_ctx,
    ) -> None:
        storage = stub_ctx.storage("files")
        upload_uc = UploadObject(ctx=stub_ctx, storage=storage)
        uploaded = await upload_uc(UploadObjectArgs(filename="gone.txt", data=b"x"))

        delete_uc = DeleteObject(ctx=stub_ctx, storage=storage)
        await delete_uc(DeleteObjectArgs(key=uploaded["key"]))

        list_uc = ListObjects(ctx=stub_ctx, storage=storage)
        result = await list_uc(ListObjectsArgs(page=1, size=10))

        assert result.count == 0
