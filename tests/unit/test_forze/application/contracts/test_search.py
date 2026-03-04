"""Unit tests for search contract (SearchSpec, SearchReadDepKey)."""

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchFieldSpec,
    SearchIndexSpec,
    SearchReadDepKey,
    SearchSpec,
    parse_search_spec,
)

# ----------------------- #


class _MinimalSearchModel(BaseModel):
    """Minimal model for search tests."""

    title: str = ""


def _minimal_search_spec() -> SearchSpec[_MinimalSearchModel]:
    """Build a minimal SearchSpec for testing."""
    return SearchSpec(
        namespace="test",
        model=_MinimalSearchModel,
        indexes={
            "default": SearchIndexSpec(
                fields=[SearchFieldSpec(path="title")],
            ),
        },
    )


class TestSearchSpec:
    """Tests for SearchSpec."""

    def test_minimal_spec(self) -> None:
        spec = _minimal_search_spec()
        assert len(spec.indexes) == 1
        assert "default" in spec.indexes

    def test_stable_default_index(self) -> None:
        internal = parse_search_spec(_minimal_search_spec())
        assert internal.stable_default_index == "default"

    def test_empty_indexes_raises(self) -> None:
        from forze.base.errors import CoreError

        spec = SearchSpec(
            namespace="test",
            model=_MinimalSearchModel,
            indexes={},
        )
        with pytest.raises(CoreError, match="At least one index"):
            parse_search_spec(spec)


class TestSearchReadDepKey:
    """Tests for SearchReadDepKey."""

    def test_search_read_dep_key_name(self) -> None:
        assert SearchReadDepKey.name == "search_read"


class TestExecutionContextSearch:
    """Tests for ExecutionContext.search() resolution."""

    def test_search_resolves_registered_port(
        self,
        stub_ctx,
    ) -> None:
        """ctx.search(spec) returns SearchReadPort from SearchReadDepKey."""
        spec = _minimal_search_spec()
        port = stub_ctx.search(spec)
        assert port is not None
        assert hasattr(port, "search")
