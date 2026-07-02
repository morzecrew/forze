"""Search retrieval capabilities: declared per backend, requests fail clean.

:class:`SearchCapabilities` makes vector / fusion / filtered-ANN support declarative
(mirroring :class:`QueryCapabilities`). The ``validate_*`` helpers reject an unsupported
request with a ``precondition`` (code ``query_feature_unsupported``), never a silent empty
page. The in-memory mock is the reference superset.
"""

from __future__ import annotations

import attrs
import pytest

from forze.application.contracts.querying import UNSUPPORTED_QUERY_FEATURE_CODE
from forze.application.contracts.search import (
    DEFAULT_SEARCH_CAPABILITIES,
    FULL_SEARCH_CAPABILITIES,
    SearchCapabilities,
    validate_fusion_supported,
    validate_stream_supported,
    validate_vector_supported,
)
from forze.base.exceptions import CoreException, ExceptionKind

pytestmark = pytest.mark.unit


# ....................... #


class TestDefaults:
    def test_default_is_plain_keyword_surface(self) -> None:
        caps = DEFAULT_SEARCH_CAPABILITIES
        assert caps.supports_vector is False
        assert caps.hybrid_fusion == frozenset()
        assert caps.filtered_ann == "none"
        assert caps.auto_embed is False

    def test_bare_construction_matches_default(self) -> None:
        assert SearchCapabilities() == DEFAULT_SEARCH_CAPABILITIES

    def test_full_superset_advertises_every_axis(self) -> None:
        caps = FULL_SEARCH_CAPABILITIES
        assert caps.supports_vector is True
        assert caps.hybrid_fusion == frozenset({"rrf", "weighted"})
        assert caps.filtered_ann == "integrated"

    def test_frozen(self) -> None:
        with pytest.raises(attrs.exceptions.FrozenInstanceError):
            FULL_SEARCH_CAPABILITIES.supports_vector = False  # type: ignore[misc]

    def test_filtered_ann_requires_vector(self) -> None:
        # A keyword-only adapter cannot declare a filtered-ANN strategy.
        with pytest.raises(CoreException, match="requires supports_vector") as ei:
            SearchCapabilities(supports_vector=False, filtered_ann="prefilter")

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_vector_without_filtered_ann_is_allowed(self) -> None:
        # A vector adapter that does not support filtering is a valid declaration.
        caps = SearchCapabilities(supports_vector=True, filtered_ann="none")
        assert caps.supports_vector is True


class TestStreamValidation:
    def test_default_off_full_on(self) -> None:
        assert DEFAULT_SEARCH_CAPABILITIES.supports_stream is False
        assert FULL_SEARCH_CAPABILITIES.supports_stream is True

    def test_supported_passes(self) -> None:
        validate_stream_supported(
            SearchCapabilities(supports_stream=True), backend="pg"
        )

    def test_unsupported_fails_clean(self) -> None:
        with pytest.raises(CoreException, match="result streaming") as ei:
            validate_stream_supported(DEFAULT_SEARCH_CAPABILITIES, backend="meili")

        assert ei.value.kind is ExceptionKind.PRECONDITION
        assert ei.value.code == UNSUPPORTED_QUERY_FEATURE_CODE


# ....................... #


class TestVectorValidation:
    def test_supported_passes(self) -> None:
        validate_vector_supported(FULL_SEARCH_CAPABILITIES, backend="mock")

    def test_unsupported_fails_clean(self) -> None:
        with pytest.raises(CoreException, match="vector search") as ei:
            validate_vector_supported(DEFAULT_SEARCH_CAPABILITIES, backend="keyword")

        assert ei.value.kind is ExceptionKind.PRECONDITION
        assert ei.value.code == UNSUPPORTED_QUERY_FEATURE_CODE
        assert "keyword" in str(ei.value)


# ....................... #


class TestFusionValidation:
    def test_rrf_supported_passes(self) -> None:
        caps = SearchCapabilities(hybrid_fusion=frozenset({"rrf"}))
        validate_fusion_supported(caps, "rrf", backend="federated")

    def test_weighted_unsupported_fails_clean(self) -> None:
        caps = SearchCapabilities(hybrid_fusion=frozenset({"rrf"}))
        with pytest.raises(CoreException, match="weighted fusion") as ei:
            validate_fusion_supported(caps, "weighted", backend="federated")

        assert ei.value.kind is ExceptionKind.PRECONDITION
        assert ei.value.code == UNSUPPORTED_QUERY_FEATURE_CODE

    def test_single_index_rejects_any_fusion(self) -> None:
        with pytest.raises(CoreException, match="rrf fusion"):
            validate_fusion_supported(DEFAULT_SEARCH_CAPABILITIES, "rrf", backend="pg")


# ....................... #


class TestAdapterDeclarations:
    """Adapters surface their capabilities via the port's ``search_capabilities`` property.

    Real backends inherit the plain keyword default from ``SimpleSearchPortMixin``; the
    vector / federated variants widen it. These assert the declared truths that P1 owns.
    """

    def test_mixin_default(self) -> None:
        from forze.application.integrations.search.port import SimpleSearchPortMixin

        assert SimpleSearchPortMixin.search_capabilities.fget(  # type: ignore[attr-defined]
            object()
        ) == DEFAULT_SEARCH_CAPABILITIES

    def test_pg_vector_declares_vector(self) -> None:
        from forze_postgres.adapters.search.vector import PostgresVectorSearchAdapter

        caps = PostgresVectorSearchAdapter.search_capabilities.fget(object())  # type: ignore[attr-defined]
        assert caps.supports_vector is True
        assert caps.filtered_ann == "postfilter"
        assert caps.auto_embed is False

    def test_mongo_vector_declares_prefilter(self) -> None:
        from forze_mongo.adapters.search.vector import MongoVectorSearchAdapter

        caps = MongoVectorSearchAdapter.search_capabilities.fget(object())  # type: ignore[attr-defined]
        assert caps.supports_vector is True
        assert caps.filtered_ann == "prefilter"

    def test_federated_declares_rrf(self) -> None:
        from forze_postgres.adapters.search.federated import (
            PostgresFederatedSearchAdapter,
        )

        caps = PostgresFederatedSearchAdapter.search_capabilities.fget(  # type: ignore[attr-defined]
            object()
        )
        assert caps.hybrid_fusion == frozenset({"rrf"})
        assert caps.supports_vector is False
