"""Unit tests for forze_kits.aggregates.storage."""

import pytest

from forze.application.contracts.storage import StorageSpec
from forze.application.execution.operations.registry import OperationRegistry
from forze_kits.aggregates.storage import (
    DownloadObjectRange,
    DownloadObjectStream,
    DownloadRangeArgs,
    HeadObject,
    StorageFacade,
    StorageKernelOp,
    UploadObject,
    UploadObjectRequestDTO,
    build_storage_registry,
)
from forze_mock import MockState
from forze_mock.adapters import MockStorageAdapter

from .registry_helpers import registry_has_handler

# ----------------------- #

_FILES = StorageSpec(name="files")


class TestBuildStorageRegistry:
    """Tests for build_storage_registry."""

    def test_returns_registry(self) -> None:
        reg = build_storage_registry(_FILES)
        assert isinstance(reg, OperationRegistry)

    def test_has_core_operations(self) -> None:
        reg = build_storage_registry(_FILES)
        ns = _FILES.default_namespace
        assert registry_has_handler(reg, ns.key(StorageKernelOp.UPLOAD))
        assert registry_has_handler(reg, ns.key(StorageKernelOp.LIST))
        assert registry_has_handler(reg, ns.key(StorageKernelOp.DOWNLOAD))
        assert registry_has_handler(reg, ns.key(StorageKernelOp.DELETE))

    def test_has_streaming_read_operations(self) -> None:
        reg = build_storage_registry(_FILES)
        ns = _FILES.default_namespace
        assert registry_has_handler(reg, ns.key(StorageKernelOp.HEAD))
        assert registry_has_handler(reg, ns.key(StorageKernelOp.DOWNLOAD_STREAM))
        assert registry_has_handler(reg, ns.key(StorageKernelOp.DOWNLOAD_RANGE))

    def test_streaming_read_ops_are_read_only_queries(self) -> None:
        # They acquire only the query port, so they must dispatch as QUERY (read-only guard).
        cat = build_storage_registry(_FILES).freeze().catalog()
        ns = _FILES.default_namespace
        for op in (
            StorageKernelOp.HEAD,
            StorageKernelOp.DOWNLOAD_STREAM,
            StorageKernelOp.DOWNLOAD_RANGE,
        ):
            assert cat[ns.key(op)].is_read_only

    def test_resolve_upload_returns_handler(
        self,
        composition_ctx,
    ) -> None:
        reg = build_storage_registry(_FILES).freeze()
        op = _FILES.default_namespace.key(StorageKernelOp.UPLOAD)
        resolved = reg.resolve(op, composition_ctx)
        assert resolved is not None


class TestStorageFacadeWithRegistry:
    """Tests for StorageFacade with build_storage_registry."""

    def test_facade_resolves_upload(
        self,
        composition_ctx,
    ) -> None:
        reg = build_storage_registry(_FILES).freeze()
        facade = StorageFacade(
            ctx=composition_ctx,
            registry=reg,
            namespace=_FILES.default_namespace,
        )
        assert facade.upload is not None


class TestUploadObjectHandler:
    """Tests for the UploadObject handler against the mock storage adapter."""

    @pytest.mark.asyncio
    async def test_upload_carries_tags_end_to_end(self) -> None:
        storage = MockStorageAdapter(state=MockState(), bucket="files")
        handler = UploadObject(storage=storage)

        dto = await handler(
            UploadObjectRequestDTO(
                filename="a.txt",
                data=b"payload",
                tags={"env": "dev"},
            ),
        )

        assert dto.tags == {"env": "dev"}
        assert dto.filename == "a.txt"
        assert dto.size == len(b"payload")


class TestStreamingReadHandlers:
    """The head / stream / range handlers against the mock storage adapter (plaintext)."""

    @staticmethod
    async def _upload(storage: MockStorageAdapter, data: bytes) -> str:
        dto = await UploadObject(storage=storage)(
            UploadObjectRequestDTO(filename="blob.bin", data=data)
        )
        return dto.key

    @pytest.mark.asyncio
    async def test_head_returns_metadata_without_body(self) -> None:
        storage = MockStorageAdapter(state=MockState(), bucket="files")
        key = await self._upload(storage, b"hello world")

        head = await HeadObject(storage=storage)(key)

        assert head.size == len(b"hello world")
        assert head.content_type  # a content type is resolved

    @pytest.mark.asyncio
    async def test_download_stream_yields_the_full_body(self) -> None:
        storage = MockStorageAdapter(state=MockState(), bucket="files")
        payload = b"streamed payload bytes"
        key = await self._upload(storage, payload)

        streamed = await DownloadObjectStream(storage=storage)(key)
        body = b"".join([chunk async for chunk in streamed.chunks])

        assert body == payload
        # Enriched with the cache validators so a plain download needs no separate head.
        assert streamed.etag
        assert streamed.last_modified is not None
        assert streamed.size == len(payload)

    @pytest.mark.asyncio
    async def test_download_range_returns_the_inclusive_window(self) -> None:
        storage = MockStorageAdapter(state=MockState(), bucket="files")
        payload = b"0123456789"
        key = await self._upload(storage, payload)

        ranged = await DownloadObjectRange(storage=storage)(
            DownloadRangeArgs(key=key, start=2, end=5)
        )

        assert ranged.data == payload[2:6]  # end is inclusive
        assert ranged.content_range == "bytes 2-5/10"
        assert ranged.total_size == len(payload)
