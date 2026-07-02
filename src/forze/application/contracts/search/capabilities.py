"""Per-backend search capabilities + fail-closed validators.

The search port presents one surface, but backends diverge on which *retrieval*
features they can serve: a keyword-only engine cannot do vector similarity; a
federated adapter can fuse by RRF but not always by weighted (relative-score)
fusion; filtered vector search executes — and recalls — differently depending on
whether the engine pre-filters, post-filters, or filters inside the ANN traversal.

:class:`SearchCapabilities` makes that surface **declarative**, mirroring
:class:`~forze.application.contracts.querying.capabilities.QueryCapabilities`: each
adapter publishes what it can serve (:attr:`~.SearchQueryPort.search_capabilities`),
and a request that strays is rejected up front with a clean
:func:`~forze.base.exceptions.exc.precondition` (code ``query_feature_unsupported``)
naming the feature and backend — never a silent empty result. The in-memory mock is
the canonical superset (:data:`FULL_SEARCH_CAPABILITIES`).

Scope: this declares *retrieval-topology* capabilities (vector support, which fusion
strategies, the filtered-ANN strategy, engine-side vs bring-your-own embedding). Index
build parameters (HNSW ``m`` / ``ef_construction``, IVFFlat ``lists``, quantization) are
DDL/adapter config and never appear here — only what a portable caller can request at
query time does.
"""

from typing import Final, Literal, TypeAlias

import attrs

from forze.base.exceptions import exc

from ..querying.capabilities import UNSUPPORTED_QUERY_FEATURE_CODE

# ----------------------- #

FusionStrategy: TypeAlias = Literal["rrf", "weighted"]
"""How a multi-source (hub / federated) search fuses ranked legs into one order.

``rrf``: Reciprocal Rank Fusion — rank-only, scale-invariant, needs no score
normalization across heterogeneous legs (BM25 vs cosine). The portable default; every
capable engine expresses it. ``weighted``: normalized weighted (relative-score) fusion —
preserves score magnitude so a high-confidence leg can dominate; often better, but not
universally available, so it is capability-gated.
"""

FilteredAnnKind: TypeAlias = Literal["none", "postfilter", "prefilter", "integrated"]
"""How an adapter applies a filter predicate to an approximate vector (ANN) search.

``none``: no vector search (filter honesty is a keyword concern only). ``postfilter``:
run ANN, then discard non-matching hits — recall can collapse under selective filters
unless the candidate pool is over-fetched. ``prefilter``: restrict the set, then search —
correct, but degrades toward brute force on large filtered sets. ``integrated``:
filter-aware graph traversal (e.g. filterable HNSW / ACORN) — keeps recall under
selective filters. The same filtered request yields different recall per kind, so it is
declared rather than pretended uniform.
"""

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchCapabilities:
    """What a search adapter can serve, declared per backend.

    A request that stays within these is guaranteed (by the adapter's contract) to be
    servable; one that strays is rejected up front by the ``validate_*`` helpers with a
    clean precondition, instead of a silent empty page or a deep render-time failure.

    Defaults describe a plain keyword/full-text single-index adapter: no vector, no
    multi-source fusion, filters do not interact with an ANN stage, and the app owns any
    embedding. Richer adapters widen the surface via :func:`attrs.evolve`.
    """

    supports_vector: bool = False
    """Whether this adapter can serve vector / semantic similarity retrieval (a ``vector``
    engine or a hybrid leg over an embedded field). Keyword-only adapters leave it off."""

    hybrid_fusion: frozenset[FusionStrategy] = frozenset()
    """Fusion strategies this adapter can apply when combining multiple ranked legs
    (hub / federated). Empty for a single-index adapter that never fuses. A multi-source
    adapter advertises at least ``{"rrf"}``; ``"weighted"`` only when it can honor
    relative-score fusion."""

    filtered_ann: FilteredAnnKind = "none"
    """How a filter predicate combines with this adapter's vector stage (see
    :data:`FilteredAnnKind`). ``none`` for keyword-only adapters."""

    auto_embed: bool = False
    """Whether the engine embeds query text itself (engine-side embedders), rather than the
    app embedding via :class:`~forze.application.contracts.embeddings.EmbeddingsProviderPort`
    before the call. ``False`` is the bring-your-own-vector default (e.g. pgvector)."""

    def __attrs_post_init__(self) -> None:
        # A filtered-ANN strategy only exists where there is a vector stage; a keyword-only
        # adapter (supports_vector=False) must keep filtered_ann="none".
        if self.filtered_ann != "none" and not self.supports_vector:
            raise exc.configuration(
                f"SearchCapabilities.filtered_ann={self.filtered_ann!r} requires "
                "supports_vector=True.",
            )


# ....................... #

FULL_SEARCH_CAPABILITIES: Final[SearchCapabilities] = SearchCapabilities(
    supports_vector=True,
    hybrid_fusion=frozenset({"rrf", "weighted"}),
    filtered_ann="integrated",
    auto_embed=False,
)
"""The canonical full retrieval surface every backend is a subset of.

The in-memory mock is the reference: it evaluates keyword scoring, mixes legs by both
RRF and weighted fusion, and applies filters exactly (an ``integrated`` filter that never
loses recall), so it advertises the superset. ``auto_embed`` stays ``False`` — the mock
(like pgvector) is bring-your-own-vector; engine-side embedding is a per-adapter opt-in,
not part of the reference surface.
"""

DEFAULT_SEARCH_CAPABILITIES: Final[SearchCapabilities] = SearchCapabilities()
"""The plain single-index keyword adapter surface (all off) — the mixin default a backend
overrides only when it serves more."""


# ....................... #


def _search_cap_fail(backend: str, feature: str) -> None:
    raise exc.precondition(
        f"Search feature {feature} is not supported by the {backend!r} backend.",
        code=UNSUPPORTED_QUERY_FEATURE_CODE,
    )


def validate_vector_supported(caps: SearchCapabilities, *, backend: str) -> None:
    """Raise cleanly if a vector query is asked of a *backend* that cannot serve it."""

    if not caps.supports_vector:
        _search_cap_fail(backend, "vector search")


def validate_fusion_supported(
    caps: SearchCapabilities,
    strategy: FusionStrategy,
    *,
    backend: str,
) -> None:
    """Raise cleanly if *strategy* fusion is requested of a *backend* that lacks it.

    Call it where a multi-source adapter resolves the requested fusion, before merging.
    A backend that only does RRF rejects ``"weighted"`` up front instead of silently
    falling back to a different ranking than the caller asked for.
    """

    if strategy not in caps.hybrid_fusion:
        _search_cap_fail(backend, f"{strategy} fusion")


def resolve_fusion(
    requested: FusionStrategy | None,
    caps: SearchCapabilities,
    *,
    backend: str,
) -> FusionStrategy:
    """Default the requested fusion to ``rrf`` and fail closed if the backend lacks it.

    The one place a multi-source adapter turns the optional ``fusion`` request key into the
    strategy it will actually run: an omitted key means the portable ``rrf`` default; an
    explicit strategy the backend does not advertise raises ``query_feature_unsupported``."""

    strategy: FusionStrategy = requested or "rrf"
    validate_fusion_supported(caps, strategy, backend=backend)
    return strategy


# ....................... #

__all__ = [
    "DEFAULT_SEARCH_CAPABILITIES",
    "FULL_SEARCH_CAPABILITIES",
    "FilteredAnnKind",
    "FusionStrategy",
    "SearchCapabilities",
    "resolve_fusion",
    "validate_fusion_supported",
    "validate_vector_supported",
]
