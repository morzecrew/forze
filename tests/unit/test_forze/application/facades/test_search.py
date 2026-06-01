"""Unit tests for SearchKernelOp and SearchFacade."""

import attrs
import pytest

from forze.base.exceptions import CoreException

from forze_kits.aggregates.search import SearchFacade, SearchKernelOp
from forze.application.contracts.execution import Handler
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.handlers.search.dto import SearchRequestDTO
from forze.base.primitives import StrKeyNamespace

# ----------------------- #

_SEARCH_KEYS = StrKeyNamespace(prefix="search")


class TestSearchKernelOp:
    def test_typed_kernel_suffix(self) -> None:
        assert str(SearchKernelOp.TYPED) == "typed"
        assert _SEARCH_KEYS.key(SearchKernelOp.TYPED) == "search.typed"

    def test_raw_kernel_suffix(self) -> None:
        assert str(SearchKernelOp.RAW) == "raw"
        assert _SEARCH_KEYS.key(SearchKernelOp.RAW) == "search.raw"


@attrs.define(slots=True, kw_only=True, frozen=True)
class StubProjectedSearch(Handler[SearchRequestDTO, list]):
    async def __call__(self, args: SearchRequestDTO) -> list:
        return []


class TestSearchFacade:
    @pytest.fixture
    def mock_raw_registry(self) -> OperationRegistry:
        return OperationRegistry(
            handlers={
                _SEARCH_KEYS.key(
                    SearchKernelOp.RAW
                ): lambda _ctx: StubProjectedSearch(),
            }
        )

    def test_projected_search_returns_resolved_operation(
        self,
        stub_ctx,
        mock_raw_registry: OperationRegistry,
    ) -> None:
        frozen = mock_raw_registry.freeze()
        facade = SearchFacade(
            ctx=stub_ctx,
            registry=frozen,
            namespace=_SEARCH_KEYS,
        )
        assert facade.projected_search is not None

    def test_search_not_supported_raises(
        self,
        stub_ctx,
        mock_raw_registry: OperationRegistry,
    ) -> None:

        frozen = mock_raw_registry.freeze()
        facade = SearchFacade(
            ctx=stub_ctx,
            registry=frozen,
            namespace=_SEARCH_KEYS,
        )
        with pytest.raises(CoreException, match="Handler factory not found"):
            _ = facade.search
