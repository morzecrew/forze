"""Unit tests for DocumentOperation and DocumentUsecasesFacade (composition.document)."""

from datetime import datetime, timezone
from uuid import UUID

import pytest

from forze.application.execution import ExecutionContext, UsecaseRegistry
from forze.application.composition.document import (
    DocumentOperation,
    DocumentUsecasesFacade,
)
from forze.domain.models import ReadDocument

# ----------------------- #


class TestDocumentOperation:
    """Tests for DocumentOperation enum."""

    def test_all_operation_values(self) -> None:
        assert DocumentOperation.GET == "document.get"
        assert DocumentOperation.CREATE == "document.create"
        assert DocumentOperation.UPDATE == "document.update"
        assert DocumentOperation.KILL == "document.kill"
        assert DocumentOperation.DELETE == "document.delete"
        assert DocumentOperation.RESTORE == "document.restore"
        assert DocumentOperation.LIST_CURSOR == "document.list_cursor"
        assert DocumentOperation.RAW_LIST_CURSOR == "document.raw_list_cursor"


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
            DocumentOperation.GET,
            lambda ctx: StubGetUsecase(ctx=ctx),
        )
        reg.finalize("document_facade", inplace=True)
        return reg

    def test_get_returns_usecase(
        self,
        stub_ctx: ExecutionContext,
        mock_get_usecase: UsecaseRegistry,
    ) -> None:
        facade = DocumentUsecasesFacade(ctx=stub_ctx, reg=mock_get_usecase)
        uc = facade.get
        assert uc is not None

    def test_update_not_supported_raises(
        self,
        stub_ctx: ExecutionContext,
        mock_get_usecase: UsecaseRegistry,
    ) -> None:
        from forze.base.errors import CoreError

        facade = DocumentUsecasesFacade(ctx=stub_ctx, reg=mock_get_usecase)
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

        facade = DocumentUsecasesFacade(ctx=stub_ctx, reg=mock_get_usecase)
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

        facade = DocumentUsecasesFacade(ctx=stub_ctx, reg=mock_get_usecase)
        with pytest.raises(
            CoreError, match="not registered for operation: document.restore"
        ):
            facade.restore()
