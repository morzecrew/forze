"""Unit tests for search contract (SearchSpec, SearchReadDepKey)."""

import pytest

from forze.application.contracts.search import (
    SearchIndexSpec,
    SearchReadDepKey,
    SearchSpec,
)
from forze.application.contracts.search.internal.specs import SearchFieldSpec

# ----------------------- #


def _minimal_search_spec() -> SearchSpec:
    """Build a minimal SearchSpec for testing."""
    return SearchSpec(
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
        spec = _minimal_search_spec()
        assert spec.stable_default_index == "default"

    def test_empty_indexes_raises(self) -> None:
        from forze.base.errors import CoreError

        with pytest.raises(CoreError, match="At least one index"):
            SearchSpec(indexes={})


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
