"""Unit tests for document kernel op keys and DocumentUsecasesFacade (composition.document)."""

from datetime import datetime, timezone
from uuid import UUID

import pytest

from forze.application.composition.document import (
    DocumentKernelOp,
    DocumentUsecasesFacade,
)
from forze.application.execution import ExecutionContext, OperationNamespace, UsecaseRegistry
from forze.domain.models import ReadDocument

# ----------------------- #

_DOC_KEYS = OperationNamespace(prefix="document")


class TestDocumentKernelOp:
    """Tests for :class:`DocumentKernelOp` suffix enum and default keyspace."""

    def test_kernel_suffixes(self) -> None:
        assert str(DocumentKernelOp.GET) == "get"
        assert str(DocumentKernelOp.CREATE) == "create"
        assert str(DocumentKernelOp.UPDATE) == "update"
        assert str(DocumentKernelOp.KILL) == "kill"
        assert str(DocumentKernelOp.DELETE) == "delete"
        assert str(DocumentKernelOp.RESTORE) == "restore"
        assert str(DocumentKernelOp.LIST_CURSOR) == "list_cursor"
        assert str(DocumentKernelOp.RAW_LIST_CURSOR) == "raw_list_cursor"

    def test_op_key_space_composes_full_keys(self) -> None:
        assert _DOC_KEYS.op(DocumentKernelOp.GET) == "document.get"
        assert _DOC_KEYS.op(DocumentKernelOp.CREATE) == "document.create"


class TestDocumentUsecasesFacade:
    """Tests for DocumentUsecasesFacade."""

    @pytest.fixture
    def mock_get_usecase(self) -> UsecaseRegistry:
        """Registry with GET operation only."""
        from forze.application.execution import Usecase

        class StubGetUsecase(Usecase[UUID, ReadDocument]):
            async def main(self, args: UUID) -> ReadDocument:
                now = datetime.now(timezone.utc)
                return ReadDocument(id=args, rev=1, created_at=now, last_update_at=now)

        reg = UsecaseRegistry().register(
            _DOC_KEYS.op(DocumentKernelOp.GET),
            lambda ctx: StubGetUsecase(ctx=ctx),
        )
        reg.finalize("document_facade")
        return reg

    def test_get_returns_usecase(
        self,
        stub_ctx: ExecutionContext,
        mock_get_usecase: UsecaseRegistry,
    ) -> None:
        facade = DocumentUsecasesFacade(
            ctx=stub_ctx,
            registry=mock_get_usecase,
            namespace=_DOC_KEYS,
        )
        uc = facade.get
        assert uc is not None

    def test_update_not_supported_raises(
        self,
        stub_ctx: ExecutionContext,
        mock_get_usecase: UsecaseRegistry,
    ) -> None:
        from forze.base.errors import CoreError

        facade = DocumentUsecasesFacade(
            ctx=stub_ctx,
            registry=mock_get_usecase,
            namespace=_DOC_KEYS,
        )
        with pytest.raises(
            CoreError, match="not registered for operation: document.update"
        ):
            facade.update()

    def test_delete_not_supported_raises(
        self,
        stub_ctx: ExecutionContext,
        mock_get_usecase: UsecaseRegistry,
    ) -> None:
        from forze.base.errors import CoreError

        facade = DocumentUsecasesFacade(
            ctx=stub_ctx,
            registry=mock_get_usecase,
            namespace=_DOC_KEYS,
        )
        with pytest.raises(
            CoreError, match="not registered for operation: document.delete"
        ):
            facade.delete()

    def test_restore_not_supported_raises(
        self,
        stub_ctx: ExecutionContext,
        mock_get_usecase: UsecaseRegistry,
    ) -> None:
        from forze.base.errors import CoreError

        facade = DocumentUsecasesFacade(
            ctx=stub_ctx,
            registry=mock_get_usecase,
            namespace=_DOC_KEYS,
        )
        with pytest.raises(
            CoreError, match="not registered for operation: document.restore"
        ):
            facade.restore()
