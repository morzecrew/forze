"""Unit tests for SearchKernelOp and SearchUsecasesFacade."""

import pytest

from forze.application.composition.search import (
    SearchKernelOp,
    SearchUsecasesFacade,
)
from forze.application.execution import ExecutionContext, OperationNamespace, UsecaseRegistry

# ----------------------- #

_SEARCH_KEYS = OperationNamespace(prefix="search")


class TestSearchKernelOp:
    """Tests for :class:`SearchKernelOp` and default keyspace."""

    def test_typed_search_wire_key(self) -> None:
        assert _SEARCH_KEYS.op(SearchKernelOp.TYPED) == "search.typed"

    def test_raw_search_wire_key(self) -> None:
        assert _SEARCH_KEYS.op(SearchKernelOp.RAW) == "search.raw"

    def test_typed_search_cursor_wire_key(self) -> None:
        assert _SEARCH_KEYS.op(SearchKernelOp.TYPED_CURSOR) == "search.typed_cursor"

    def test_raw_search_cursor_wire_key(self) -> None:
        assert _SEARCH_KEYS.op(SearchKernelOp.RAW_CURSOR) == "search.raw_cursor"

    def test_all_members_string_values(self) -> None:
        for op in SearchKernelOp:
            assert isinstance(op.value, str)
            assert len(op.value) > 0


class TestSearchUsecasesFacade:
    """Tests for SearchUsecasesFacade."""

    @pytest.fixture
    def mock_raw_search_usecase(self) -> UsecaseRegistry:
        """Registry with raw search operation only."""
        from forze.application.execution import Usecase

        class StubRawSearchUsecase(Usecase[dict, dict]):
            async def main(self, args: dict) -> dict:
                return {"hits": [], "count": 0}

        reg = UsecaseRegistry().register(
            _SEARCH_KEYS.op(SearchKernelOp.RAW),
            lambda ctx: StubRawSearchUsecase(ctx=ctx),
        )
        reg.finalize("search_facade")
        return reg

    def test_raw_search_returns_usecase(
        self,
        stub_ctx: ExecutionContext,
        mock_raw_search_usecase: UsecaseRegistry,
    ) -> None:
        facade = SearchUsecasesFacade(
            ctx=stub_ctx,
            registry=mock_raw_search_usecase,
            namespace=_SEARCH_KEYS,
        )
        uc = facade.raw_search
        assert uc is not None

    def test_search_not_supported_raises(
        self,
        stub_ctx: ExecutionContext,
        mock_raw_search_usecase: UsecaseRegistry,
    ) -> None:
        from forze.base.errors import CoreError

        facade = SearchUsecasesFacade(
            ctx=stub_ctx,
            registry=mock_raw_search_usecase,
            namespace=_SEARCH_KEYS,
        )
        with pytest.raises(
            CoreError, match="not registered for operation: search.typed"
        ):
            facade.search()
