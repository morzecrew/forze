"""Unit tests for document kernel op keys and DocumentFacade."""

from datetime import datetime, timezone
from uuid import UUID

import attrs
import pytest

from forze.application.composition.document import DocumentFacade, DocumentKernelOp
from forze.application.contracts.execution import Handler
from forze.application.execution.registry import OperationRegistry
from forze.base.primitives import StrKeyNamespace
from forze.domain.models import ReadDocument

# ----------------------- #

_DOC_KEYS = StrKeyNamespace(prefix="document")


class TestDocumentKernelOp:
    def test_kernel_suffixes(self) -> None:
        assert str(DocumentKernelOp.GET) == "get"
        assert str(DocumentKernelOp.CREATE) == "create"
        assert str(DocumentKernelOp.UPDATE) == "update"
        assert str(DocumentKernelOp.KILL) == "kill"
        assert str(DocumentKernelOp.LIST_CURSOR) == "list_cursor"
        assert str(DocumentKernelOp.RAW_LIST_CURSOR) == "raw_list_cursor"

    def test_op_key_space_composes_full_keys(self) -> None:
        assert _DOC_KEYS.key(DocumentKernelOp.GET) == "document.get"
        assert _DOC_KEYS.key(DocumentKernelOp.CREATE) == "document.create"


@attrs.define(slots=True, kw_only=True, frozen=True)
class StubGetHandler(Handler[UUID, ReadDocument]):
    async def __call__(self, args: UUID) -> ReadDocument:
        now = datetime.now(timezone.utc)
        return ReadDocument(id=args, rev=1, created_at=now, last_update_at=now)


class TestDocumentFacade:
    @pytest.fixture
    def mock_get_registry(self) -> OperationRegistry:
        return OperationRegistry(
            handlers={
                _DOC_KEYS.key(DocumentKernelOp.GET): lambda _ctx: StubGetHandler(),
            }
        )

    def test_get_returns_resolved_operation(
        self,
        stub_ctx,
        mock_get_registry: OperationRegistry,
    ) -> None:
        frozen = mock_get_registry.freeze()
        facade = DocumentFacade(
            ctx=stub_ctx,
            registry=frozen,
            namespace=_DOC_KEYS,
        )
        op = facade.get
        assert op is not None

    def test_update_not_supported_raises(
        self,
        stub_ctx,
        mock_get_registry: OperationRegistry,
    ) -> None:
        frozen = mock_get_registry.freeze()
        facade = DocumentFacade(
            ctx=stub_ctx,
            registry=frozen,
            namespace=_DOC_KEYS,
        )
        with pytest.raises(exc.internal, match="Handler factory not found"):
            _ = facade.update

    def test_create_not_supported_raises(
        self,
        stub_ctx,
        mock_get_registry: OperationRegistry,
    ) -> None:
        frozen = mock_get_registry.freeze()
        facade = DocumentFacade(
            ctx=stub_ctx,
            registry=frozen,
            namespace=_DOC_KEYS,
        )
        with pytest.raises(exc.internal, match="Handler factory not found"):
            _ = facade.create

    def test_kill_not_supported_raises(
        self,
        stub_ctx,
        mock_get_registry: OperationRegistry,
    ) -> None:
        frozen = mock_get_registry.freeze()
        facade = DocumentFacade(
            ctx=stub_ctx,
            registry=frozen,
            namespace=_DOC_KEYS,
        )
        with pytest.raises(exc.internal, match="Handler factory not found"):
            _ = facade.kill
