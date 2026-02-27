"""Unit tests for forze.application.facades.document."""

from datetime import datetime, timezone
from uuid import UUID

import pytest

from forze.application.execution import ExecutionContext, UsecaseRegistry
from forze.application.facades.document import DocumentOperation, DocumentUsecasesFacade
from forze.domain.models import ReadDocument

# ----------------------- #


class TestDocumentOperation:
    """Tests for DocumentOperation enum."""

    def test_all_operation_values(self) -> None:
        assert DocumentOperation.GET == "get"
        assert DocumentOperation.SEARCH == "search"
        assert DocumentOperation.RAW_SEARCH == "raw_search"
        assert DocumentOperation.CREATE == "create"
        assert DocumentOperation.UPDATE == "update"
        assert DocumentOperation.KILL == "kill"
        assert DocumentOperation.DELETE == "delete"
        assert DocumentOperation.RESTORE == "restore"


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

        return UsecaseRegistry().register(
            DocumentOperation.GET,
            lambda ctx: StubGetUsecase(),
        )

    def test_get_returns_usecase(
        self,
        stub_ctx: ExecutionContext,
        mock_get_usecase: UsecaseRegistry,
    ) -> None:
        facade = DocumentUsecasesFacade(ctx=stub_ctx, reg=mock_get_usecase)
        uc = facade.get()
        assert uc is not None

    def test_update_not_supported_raises(
        self,
        stub_ctx: ExecutionContext,
        mock_get_usecase: UsecaseRegistry,
    ) -> None:
        from forze.base.errors import CoreError

        facade = DocumentUsecasesFacade(ctx=stub_ctx, reg=mock_get_usecase)
        with pytest.raises(CoreError, match="Update operation is not supported"):
            facade.update()

    def test_delete_not_supported_raises(
        self,
        stub_ctx: ExecutionContext,
        mock_get_usecase: UsecaseRegistry,
    ) -> None:
        from forze.base.errors import CoreError

        facade = DocumentUsecasesFacade(ctx=stub_ctx, reg=mock_get_usecase)
        with pytest.raises(CoreError, match="Delete operation is not supported"):
            facade.delete()

    def test_restore_not_supported_raises(
        self,
        stub_ctx: ExecutionContext,
        mock_get_usecase: UsecaseRegistry,
    ) -> None:
        from forze.base.errors import CoreError

        facade = DocumentUsecasesFacade(ctx=stub_ctx, reg=mock_get_usecase)
        with pytest.raises(CoreError, match="Restore operation is not supported"):
            facade.restore()
