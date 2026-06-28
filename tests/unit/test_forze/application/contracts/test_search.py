"""Unit tests for search contract (SearchSpec, SearchQueryDepKey)."""

import pytest

from forze.base.exceptions import CoreException
from pydantic import BaseModel

from forze.application.contracts.search import (
    FederatedSearchSpec,
    HubSearchQueryDepKey,
    HubSearchSpec,
    SearchQueryDepKey,
    SearchSpec,
)

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

    def test_sensitive_defaults_to_false(self) -> None:
        assert _minimal_search_spec().sensitive is False

    def test_sensitive_flag_round_trips(self) -> None:
        spec = SearchSpec(
            name="test",
            model_type=_MinimalSearchModel,
            fields=["title"],
            sensitive=True,
        )

        assert spec.sensitive is True

    def test_duplicate_fields_raise(self) -> None:
        with pytest.raises(CoreException, match="unique"):
            SearchSpec(
                name="test",
                model_type=_MinimalSearchModel,
                fields=["title", "title"],
            )

    def test_default_weights_must_cover_all_fields(self) -> None:
        with pytest.raises(CoreException, match="Default weights"):
            SearchSpec(
                name="test",
                model_type=_MinimalSearchModel,
                fields=["title", "body"],
                default_weights={"title": 0.5},
            )


class _LenientSearchModel(BaseModel):
    id: str = ""
    title: str
    author: str  # required, not indexed
    summary: str = ""  # returned, not indexed; eligible for leniency


class TestSearchLenientReadFields:
    """Lenient read fields on a SearchSpec (storage conformity)."""

    def test_lenient_field_round_trips(self) -> None:
        spec = SearchSpec(
            name="docs",
            model_type=_LenientSearchModel,
            fields=["title"],
            lenient_read_fields={"summary"},
        )
        assert spec.lenient_read_fields == frozenset({"summary"})

    def test_indexed_field_cannot_be_lenient(self) -> None:
        # An indexed (searchable) field needs a real column.
        with pytest.raises(CoreException, match="indexed .* and cannot be lenient"):
            SearchSpec(
                name="docs",
                model_type=_LenientSearchModel,
                fields=["title", "summary"],
                lenient_read_fields={"summary"},
            )

    def test_required_field_cannot_be_lenient(self) -> None:
        with pytest.raises(CoreException, match="has no default"):
            SearchSpec(
                name="docs",
                model_type=_LenientSearchModel,
                fields=["title"],
                lenient_read_fields={"author"},
            )

    def test_identity_field_cannot_be_lenient(self) -> None:
        with pytest.raises(CoreException, match="identity/audit fields"):
            SearchSpec(
                name="docs",
                model_type=_LenientSearchModel,
                fields=["title"],
                lenient_read_fields={"id"},
            )

    def test_lenient_field_rejected_as_default_sort(self) -> None:
        # A lenient field has no column, so it cannot be a sort key.
        with pytest.raises(CoreException, match="[Ss]ort field"):
            SearchSpec(
                name="docs",
                model_type=_LenientSearchModel,
                fields=["title"],
                lenient_read_fields={"summary"},
                default_sort={"summary": "asc"},
            )


class TestSearchQueryDepKey:
    """Tests for SearchQueryDepKey."""

    def test_search_query_dep_key_name(self) -> None:
        assert SearchQueryDepKey.name == "search_query"

    def test_hub_search_query_dep_key_name(self) -> None:
        assert HubSearchQueryDepKey.name == "hub_search_query"


class TestHubSearchSpec:
    """Tests for HubSearchSpec."""

    def test_hub_accepts_single_member(self) -> None:
        one = SearchSpec(
            name="only_leg",
            model_type=_MinimalSearchModel,
            fields=["title"],
        )
        hub = HubSearchSpec(name="h", model_type=_MinimalSearchModel, members=(one,))
        assert len(hub.members) == 1

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
        with pytest.raises(CoreException, match="distinct name"):
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
        with pytest.raises(CoreException, match="distinct name"):
            FederatedSearchSpec(name="fed", members=(a, b))


class TestExecutionContextSearchQuery:
    """Tests for ExecutionContext.search.query() resolution."""

    def test_search_query_resolves_registered_port(
        self,
        stub_ctx,
    ) -> None:
        """ctx.search.query(spec) returns SearchQueryPort from SearchQueryDepKey."""
        spec = _minimal_search_spec()
        port = stub_ctx.search.query(spec)
        assert port is not None
        assert hasattr(port, "search")
