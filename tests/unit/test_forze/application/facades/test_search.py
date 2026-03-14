"""Unit tests for SearchOperation and SearchUsecasesFacade."""

import pytest

from forze.application.composition.search import (
    SearchOperation,
    SearchUsecasesFacade,
)
from forze.application.execution import ExecutionContext, UsecaseRegistry

# ----------------------- #


class TestSearchOperation:
    """Tests for SearchOperation enum."""

    def test_typed_search_value(self) -> None:
        assert SearchOperation.TYPED_SEARCH == "search.typed"

    def test_raw_search_value(self) -> None:
        assert SearchOperation.RAW_SEARCH == "search.raw"

    def test_all_members_string_values(self) -> None:
        for op in SearchOperation:
            assert isinstance(op.value, str)
            assert len(op.value) > 0


class TestSearchUsecasesFacade:
    """Tests for SearchUsecasesFacade."""

    @pytest.fixture
    def mock_raw_search_usecase(self) -> UsecaseRegistry:
        """Registry with RAW_SEARCH operation only."""
        from forze.application.execution import Usecase

        class StubRawSearchUsecase(Usecase[dict, dict]):
            async def main(self, args: dict) -> dict:
                return {"hits": [], "count": 0}

        return UsecaseRegistry().register(
            SearchOperation.RAW_SEARCH,
            lambda ctx: StubRawSearchUsecase(ctx=ctx),
        )

    def test_raw_search_returns_usecase(
        self,
        stub_ctx: ExecutionContext,
        mock_raw_search_usecase: UsecaseRegistry,
    ) -> None:
        facade = SearchUsecasesFacade(ctx=stub_ctx, reg=mock_raw_search_usecase)
        uc = facade.raw_search()
        assert uc is not None

    def test_search_not_supported_raises(
        self,
        stub_ctx: ExecutionContext,
        mock_raw_search_usecase: UsecaseRegistry,
    ) -> None:
        from forze.base.errors import CoreError

        facade = SearchUsecasesFacade(ctx=stub_ctx, reg=mock_raw_search_usecase)
        with pytest.raises(
            CoreError, match="not registered for operation: search.typed"
        ):
            facade.search()
