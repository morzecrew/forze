"""Unit tests for search contract (SearchSpec, SearchQueryDepKey)."""

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    FederatedSearchSpec,
    HubSearchQueryDepKey,
    HubSearchSpec,
    SearchQueryDepKey,
    SearchSpec,
)
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


class TestSearchQueryDepKey:
    """Tests for SearchQueryDepKey."""

    def test_search_query_dep_key_name(self) -> None:
        assert SearchQueryDepKey.name == "search_query"

    def test_hub_search_query_dep_key_name(self) -> None:
        assert HubSearchQueryDepKey.name == "hub_search_query"


class TestHubSearchSpec:
    """Tests for HubSearchSpec."""

    def test_hub_duplicate_leg_search_names_raise(self) -> None:
        a = SearchSpec(
            name="same_leg",
            model_type=_MinimalSearchModel,
            fields=["title"],
        )
        b = SearchSpec(
            name="same_leg",
            model_type=_MinimalSearchModel,
            fields=["title"],
        )
        with pytest.raises(CoreError, match="distinct name"):
            HubSearchSpec(
                name="h",
                model_type=_MinimalSearchModel,
                members=(a, b),
            )


class TestFederatedSearchSpec:
    """Tests for FederatedSearchSpec including nested hub members."""

    def test_federated_accepts_hub_and_search_members(self) -> None:
        leg_a = SearchSpec(
            name="leg_a",
            model_type=_MinimalSearchModel,
            fields=["title"],
        )
        leg_b = SearchSpec(
            name="leg_b",
            model_type=_MinimalSearchModel,
            fields=["title"],
        )
        hub = HubSearchSpec(
            name="hub_leg",
            model_type=_MinimalSearchModel,
            members=(leg_a, leg_b),
        )
        standalone = SearchSpec(
            name="standalone",
            model_type=_MinimalSearchModel,
            fields=["title"],
        )
        fed = FederatedSearchSpec(
            name="fed",
            members=(hub, standalone),
        )
        assert [m.name for m in fed.members] == ["hub_leg", "standalone"]

    def test_federated_rejects_duplicate_member_names(self) -> None:
        a = SearchSpec(
            name="dup",
            model_type=_MinimalSearchModel,
            fields=["title"],
        )
        b = SearchSpec(
            name="dup",
            model_type=_MinimalSearchModel,
            fields=["title"],
        )
        with pytest.raises(CoreError, match="distinct name"):
            FederatedSearchSpec(name="fed", members=(a, b))


class TestExecutionContextSearchQuery:
    """Tests for ExecutionContext.search_query() resolution."""

    def test_search_query_resolves_registered_port(
        self,
        stub_ctx,
    ) -> None:
        """ctx.search_query(spec) returns SearchQueryPort from SearchQueryDepKey."""
        spec = _minimal_search_spec()
        port = stub_ctx.search_query(spec)
        assert port is not None
        assert hasattr(port, "search")
