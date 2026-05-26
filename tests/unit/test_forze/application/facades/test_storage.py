"""Unit tests for StorageKernelOp and StorageFacade."""

from datetime import datetime

import attrs
import pytest

from forze.application.composition.storage import StorageFacade, StorageKernelOp
from forze.application.contracts.execution import Handler
from forze.application.execution.registry import OperationRegistry
from forze.application.handlers.storage.dto import (
    StoredObjectDTO,
    UploadObjectRequestDTO,
)
from forze.base.primitives import StrKeyNamespace

# ----------------------- #

_STORAGE_KEYS = StrKeyNamespace(prefix="storage")


class TestStorageKernelOp:
    def test_upload_kernel_suffix(self) -> None:
        assert str(StorageKernelOp.UPLOAD) == "upload"
        assert _STORAGE_KEYS.key(StorageKernelOp.UPLOAD) == "storage.upload"


@attrs.define(slots=True, kw_only=True, frozen=True)
class StubUpload(Handler[UploadObjectRequestDTO, StoredObjectDTO]):
    async def __call__(self, args: UploadObjectRequestDTO) -> StoredObjectDTO:
        return StoredObjectDTO(
            key="k",
            filename=args.filename,
            created_at=datetime.now(),
            size=len(args.data),
            content_type="application/octet-stream",
        )


class TestStorageFacade:
    @pytest.fixture
    def mock_upload_registry(self) -> OperationRegistry:
        return OperationRegistry(
            handlers={
                _STORAGE_KEYS.key(StorageKernelOp.UPLOAD): lambda _ctx: StubUpload(),
            }
        )

    def test_upload_returns_resolved_operation(
        self,
        stub_ctx,
        mock_upload_registry: OperationRegistry,
    ) -> None:
        frozen = mock_upload_registry.freeze()
        facade = StorageFacade(
            ctx=stub_ctx,
            registry=frozen,
            namespace=_STORAGE_KEYS,
        )
        assert facade.upload is not None

    def test_list_not_supported_raises(
        self,
        stub_ctx,
        mock_upload_registry: OperationRegistry,
    ) -> None:
        frozen = mock_upload_registry.freeze()
        facade = StorageFacade(
            ctx=stub_ctx,
            registry=frozen,
            namespace=_STORAGE_KEYS,
        )
        with pytest.raises(exc.internal, match="Handler factory not found"):
            _ = facade.list
