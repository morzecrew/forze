"""Unit tests for search contract (SearchSpec, SearchReadDepKey)."""

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchReadDepKey, SearchSpec
from forze.base.errors import CoreError

# ----------------------- #


class _MinimalSearchModel(BaseModel):
    """Minimal model for search tests."""

    title: str = ""


def _minimal_search_spec() -> SearchSpec[_MinimalSearchModel]:
    """Build a minimal SearchSpec for testing."""
    return SearchSpec(
        name="test",
        model_type=_MinimalSearchModel,
        fields=["title"],
    )


class TestSearchSpec:
    """Tests for SearchSpec."""

    def test_minimal_spec(self) -> None:
        spec = _minimal_search_spec()
        assert list(spec.fields) == ["title"]

    def test_duplicate_fields_raise(self) -> None:
        with pytest.raises(CoreError, match="unique"):
            SearchSpec(
                name="test",
                model_type=_MinimalSearchModel,
                fields=["title", "title"],
            )

    def test_default_weights_must_cover_all_fields(self) -> None:
        with pytest.raises(CoreError, match="Default weights"):
            SearchSpec(
                name="test",
                model_type=_MinimalSearchModel,
                fields=["title", "body"],
                default_weights={"title": 0.5},
            )


class TestSearchReadDepKey:
    """Tests for SearchReadDepKey."""

    def test_search_read_dep_key_name(self) -> None:
        assert SearchReadDepKey.name == "search_read"


class TestExecutionContextSearchRead:
    """Tests for ExecutionContext.search_read() resolution."""

    def test_search_read_resolves_registered_port(
        self,
        stub_ctx,
    ) -> None:
        """ctx.search_read(spec) returns SearchReadPort from SearchReadDepKey."""
        spec = _minimal_search_spec()
        port = stub_ctx.search_read(spec)
        assert port is not None
        assert hasattr(port, "search")
