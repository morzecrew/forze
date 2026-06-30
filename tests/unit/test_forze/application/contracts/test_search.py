"""Unit tests for search contract (SearchSpec, SearchQueryDepKey)."""

import pytest

from forze.base.exceptions import CoreException
from pydantic import BaseModel, computed_field

from forze.application.contracts.search import (
    FederatedSearchSpec,
    HubSearchQueryDepKey,
    HubSearchSpec,
    SearchFuzzySpec,
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

    def test_read_conformity_defaults_to_strict(self) -> None:
        spec = SearchSpec(
            name="docs", model_type=_LenientSearchModel, fields=["title"]
        )
        assert spec.read_conformity == "strict"
        assert spec.resolved_lenient_read_fields == frozenset()

    def test_read_conformity_lenient_auto_derives(self) -> None:
        spec = SearchSpec(
            name="docs",
            model_type=_LenientSearchModel,
            fields=["title"],
            read_conformity="lenient",
        )
        resolved = spec.resolved_lenient_read_fields
        # ``summary``/``id`` are defaulted and not indexed → derived; identity ``id``
        # is excluded; required ``author`` and indexed ``title`` are not derived.
        assert "summary" in resolved
        assert "id" not in resolved
        assert "author" not in resolved
        assert "title" not in resolved

    def test_read_conformity_lenient_excludes_indexed_fields(self) -> None:
        # An indexed field is never auto-derived (it needs a real column).
        spec = SearchSpec(
            name="docs",
            model_type=_LenientSearchModel,
            fields=["title", "summary"],
            read_conformity="lenient",
        )
        assert "summary" not in spec.resolved_lenient_read_fields


class _MaterializedSearchModel(BaseModel):
    id: str = ""
    qty: int
    unit_price: float

    @computed_field  # type: ignore[prop-decorator]
    @property
    def total(self) -> float:
        return self.qty * self.unit_price


class TestSearchMaterialized:
    """Materialized computed fields on a SearchSpec."""

    def test_materialized_persisted_in_read_codec(self) -> None:
        spec = SearchSpec(
            name="orders",
            model_type=_MaterializedSearchModel,
            fields=["id"],
            materialized={"total"},
        )
        assert spec.materialized == frozenset({"total"})
        # The derived value becomes a projected/queryable column.
        assert "total" in spec.resolved_read_codec.persisted_field_names()

    def test_materialized_allows_default_sort_on_derived_field(self) -> None:
        spec = SearchSpec(
            name="orders",
            model_type=_MaterializedSearchModel,
            fields=["id"],
            materialized={"total"},
            default_sort={"total": "desc"},
        )
        assert spec.default_sort == {"total": "desc"}

    def test_materialized_non_computed_field_rejected(self) -> None:
        with pytest.raises(CoreException, match="not ``@computed_field``"):
            SearchSpec(
                name="orders",
                model_type=_MaterializedSearchModel,
                fields=["id"],
                materialized={"qty"},
            )

    def test_materialized_unknown_field_rejected(self) -> None:
        with pytest.raises(CoreException, match="not ``@computed_field``"):
            SearchSpec(
                name="orders",
                model_type=_MaterializedSearchModel,
                fields=["id"],
                materialized={"ghost"},
            )

    def test_materialized_and_lenient_overlap_rejected(self) -> None:
        with pytest.raises(CoreException, match="both"):
            SearchSpec(
                name="orders",
                model_type=_MaterializedSearchModel,
                fields=["id"],
                materialized={"total"},
                lenient_read_fields={"total"},
            )

    def test_materialized_field_is_facetable(self) -> None:
        # A materialized computed field is a persisted real column, so it may be faceted.
        spec = SearchSpec(
            name="orders",
            model_type=_MaterializedSearchModel,
            fields=["id"],
            materialized={"total"},
            facetable_fields={"total"},
        )
        assert "total" in spec.facetable_fields

    def test_materialized_excluded_from_lenient_auto_derive(self) -> None:
        # A materialized field is stored, so read_conformity="lenient" must not derive it.
        spec = SearchSpec(
            name="orders",
            model_type=_MaterializedSearchModel,
            fields=["id"],
            materialized={"total"},
            read_conformity="lenient",
        )
        assert "total" not in spec.resolved_lenient_read_fields


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

    def test_hub_lenient_read_fields_round_trip(self) -> None:
        leg = SearchSpec(
            name="leg", model_type=_LenientSearchModel, fields=["title"]
        )
        hub = HubSearchSpec(
            name="h",
            model_type=_LenientSearchModel,
            members=(leg,),
            lenient_read_fields={"summary"},
        )
        assert hub.resolved_lenient_read_fields == frozenset({"summary"})

    def test_hub_read_conformity_lenient_auto_derives(self) -> None:
        leg = SearchSpec(
            name="leg", model_type=_LenientSearchModel, fields=["title"]
        )
        hub = HubSearchSpec(
            name="h",
            model_type=_LenientSearchModel,
            members=(leg,),
            read_conformity="lenient",
        )
        resolved = hub.resolved_lenient_read_fields
        assert "summary" in resolved
        assert "id" not in resolved  # identity excluded
        assert "author" not in resolved  # required excluded

    def test_hub_identity_field_cannot_be_lenient(self) -> None:
        leg = SearchSpec(
            name="leg", model_type=_LenientSearchModel, fields=["title"]
        )
        with pytest.raises(CoreException, match="identity/audit fields"):
            HubSearchSpec(
                name="h",
                model_type=_LenientSearchModel,
                members=(leg,),
                lenient_read_fields={"id"},
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


# ----------------------- #


class TestSearchFuzzySpec:
    """`SearchFuzzySpec` is an immutable value object."""

    def test_default_ratio(self) -> None:
        assert SearchFuzzySpec().max_distance_ratio == 0.34

    def test_explicit_ratio(self) -> None:
        assert SearchFuzzySpec(max_distance_ratio=0.1).max_distance_ratio == 0.1

    def test_frozen(self) -> None:
        spec = SearchFuzzySpec()
        with pytest.raises(AttributeError):
            spec.max_distance_ratio = 0.9  # type: ignore[misc]

    @pytest.mark.parametrize("ratio", [-0.1, 1.1, 2.0])
    def test_rejects_out_of_range_ratio(self, ratio: float) -> None:
        with pytest.raises(CoreException, match="between 0.0 and 1.0"):
            SearchFuzzySpec(max_distance_ratio=ratio)

    def test_usable_on_search_spec(self) -> None:
        spec = SearchSpec(
            name="fz",
            model_type=_MinimalSearchModel,
            fields=["title"],
            fuzzy=SearchFuzzySpec(max_distance_ratio=0.2),
        )
        assert spec.fuzzy is not None
        assert spec.fuzzy.max_distance_ratio == 0.2
