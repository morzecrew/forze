"""Tests for forze.application.contracts.storage.ports."""

from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime, timedelta

from forze.application.contracts.storage import (
    DownloadedObject,
    ObjectHead,
    PresignedUrl,
    RangedDownload,
    StoredObject,
    StreamedDownload,
    UploadedObject,
)
from forze.application.contracts.storage.ports import (
    StorageCommandPort,
    StorageQueryPort,
)


class _StubStorage:
    """Concrete implementation for testing the storage query/command ports."""

    async def upload(self, obj: UploadedObject) -> StoredObject:
        return StoredObject(
            key=f"{obj.prefix or ''}/{obj.filename}",
            filename=obj.filename,
            description=obj.description,
            content_type="application/octet-stream",
            size=len(obj.data),
            created_at=datetime.now(),
        )

    async def download(self, key: str) -> DownloadedObject:
        return DownloadedObject(
            data=b"content",
            content_type="application/octet-stream",
            filename=key.split("/")[-1],
        )

    async def download_stream(self, key: str) -> StreamedDownload:
        async def _body() -> AsyncIterator[bytes]:
            yield b"content"

        return StreamedDownload(
            content_type="application/octet-stream",
            filename=key.split("/")[-1],
            chunks=_body(),
        )

    async def upload_stream(
        self,
        chunks: AsyncIterator[bytes],
        *,
        filename: str,
        prefix: str | None = None,
        description: str | None = None,
        tags: Mapping[str, str] | None = None,
        content_type: str | None = None,
        chunk_size: int = 1 << 20,
    ) -> StoredObject:
        total = 0
        async for piece in chunks:
            total += len(piece)

        return StoredObject(
            key=f"{prefix or ''}/{filename}",
            filename=filename,
            description=description,
            content_type=content_type or "application/octet-stream",
            size=total,
            created_at=datetime.now(),
        )

    async def overwrite_stream(
        self,
        key: str,
        chunks: AsyncIterator[bytes],
        *,
        content_type: str | None = None,
        metadata: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        chunk_size: int = 1 << 20,
        if_match: str | None = None,
    ) -> StoredObject:
        total = 0
        async for piece in chunks:
            total += len(piece)

        return StoredObject(
            key=key,
            filename=key.rsplit("/", 1)[-1],
            description=None,
            content_type=content_type or "application/octet-stream",
            size=total,
            created_at=datetime.now(),
            tags=dict(tags) if tags else None,
        )

    async def presign_download(
        self,
        key: str,
        *,
        expires_in: timedelta,
    ) -> PresignedUrl:
        return PresignedUrl(
            url=f"https://stub/{key}",
            method="GET",
            expires_at=datetime.now(UTC) + expires_in,
        )

    async def presign_upload(
        self,
        key: str,
        *,
        expires_in: timedelta,
        content_type: str | None = None,
    ) -> PresignedUrl:
        return PresignedUrl(
            url=f"https://stub/{key}",
            method="PUT",
            expires_at=datetime.now(UTC) + expires_in,
            headers={"Content-Type": content_type} if content_type else {},
        )

    async def head(self, key: str, *, include_tags: bool = False) -> ObjectHead:
        return ObjectHead(content_type="application/octet-stream", size=7)

    async def download_range(
        self,
        key: str,
        *,
        start: int,
        end: int | None = None,
    ) -> RangedDownload:
        return RangedDownload(
            data=b"con",
            content_type="application/octet-stream",
            content_range="bytes 0-2/7",
            total_size=7,
        )

    async def download_if_changed(
        self,
        key: str,
        *,
        if_none_match: str | None = None,
        if_modified_since: datetime | None = None,
    ) -> DownloadedObject | None:
        return None

    async def delete(self, key: str) -> None:
        pass

    async def copy(self, src_key: str, dst_key: str) -> ObjectHead:
        return ObjectHead(content_type="application/octet-stream", size=7)

    async def move(self, src_key: str, dst_key: str) -> ObjectHead:
        return ObjectHead(content_type="application/octet-stream", size=7)

    async def put_object_tags(self, key: str, tags: Mapping[str, str]) -> None:
        pass

    async def list(
        self,
        limit: int,
        offset: int,
        *,
        prefix: str | None = None,
        include_tags: bool = False,
    ) -> tuple[list[StoredObject], int]:
        return [], 0


class TestStoragePorts:
    def test_is_runtime_checkable(self) -> None:
        stub = _StubStorage()
        assert isinstance(stub, StorageQueryPort)
        assert isinstance(stub, StorageCommandPort)

    async def test_upload(self) -> None:
        stub = _StubStorage()
        result = await stub.upload(
            UploadedObject(filename="test.txt", data=b"data", prefix="files"),
        )
        assert result.filename == "test.txt"
        assert result.size == 4

    async def test_download(self) -> None:
        stub = _StubStorage()
        result = await stub.download("files/test.txt")
        assert result.data == b"content"

    async def test_delete(self) -> None:
        stub = _StubStorage()
        await stub.delete("key")

    async def test_list(self) -> None:
        stub = _StubStorage()
        items, total = await stub.list(10, 0)
        assert items == []
        assert total == 0

    async def test_presign_download(self) -> None:
        stub = _StubStorage()
        vo = await stub.presign_download("files/a", expires_in=timedelta(minutes=5))
        assert vo.method == "GET"

    async def test_presign_upload(self) -> None:
        stub = _StubStorage()
        vo = await stub.presign_upload(
            "files/a",
            expires_in=timedelta(minutes=5),
            content_type="text/plain",
        )
        assert vo.method == "PUT"
        assert dict(vo.headers) == {"Content-Type": "text/plain"}

    async def test_upload_with_description(self) -> None:
        stub = _StubStorage()
        result = await stub.upload(
            UploadedObject(filename="doc.pdf", data=b"pdf", description="My doc"),
        )
        assert result.description == "My doc"

    def test_non_conforming_not_instance(self) -> None:
        class Bad:
            pass

        assert not isinstance(Bad(), StorageQueryPort)
        assert not isinstance(Bad(), StorageCommandPort)
