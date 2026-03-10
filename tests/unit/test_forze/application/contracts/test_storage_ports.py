"""Tests for forze.application.contracts.storage.ports."""

from datetime import datetime
from typing import Optional

from forze.application.contracts.storage.ports import StoragePort
from forze.application.contracts.storage.types import DownloadedObject, StoredObject


class _StubStorage:
    """Concrete implementation for testing StoragePort."""

    async def upload(
        self,
        filename: str,
        data: bytes,
        description: Optional[str] = None,
        *,
        prefix: Optional[str] = None,
    ) -> StoredObject:
        return StoredObject(
            key=f"{prefix or ''}/{filename}",
            filename=filename,
            description=description,
            content_type="application/octet-stream",
            size=len(data),
            created_at=datetime.now(),
        )

    async def download(self, key: str) -> DownloadedObject:
        return DownloadedObject(
            data=b"content",
            content_type="application/octet-stream",
            filename=key.split("/")[-1],
        )

    async def delete(self, key: str) -> None:
        pass

    async def list(
        self,
        limit: int,
        offset: int,
        *,
        prefix: Optional[str] = None,
    ) -> tuple[list[StoredObject], int]:
        return [], 0


class TestStoragePort:
    def test_is_runtime_checkable(self) -> None:
        stub = _StubStorage()
        assert isinstance(stub, StoragePort)

    async def test_upload(self) -> None:
        stub = _StubStorage()
        result = await stub.upload("test.txt", b"data", prefix="files")
        assert result["filename"] == "test.txt"
        assert result["size"] == 4

    async def test_download(self) -> None:
        stub = _StubStorage()
        result = await stub.download("files/test.txt")
        assert result["data"] == b"content"

    async def test_delete(self) -> None:
        stub = _StubStorage()
        await stub.delete("key")

    async def test_list(self) -> None:
        stub = _StubStorage()
        items, total = await stub.list(10, 0)
        assert items == []
        assert total == 0

    async def test_upload_with_description(self) -> None:
        stub = _StubStorage()
        result = await stub.upload("doc.pdf", b"pdf", description="My doc")
        assert result["description"] == "My doc"

    def test_non_conforming_not_instance(self) -> None:
        class Bad:
            pass

        assert not isinstance(Bad(), StoragePort)
