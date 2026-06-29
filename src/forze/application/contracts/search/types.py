from typing import Literal, Sequence, TypeAlias, TypedDict

# ----------------------- #

PhraseCombine = Literal["any", "all"]
"""``any``: at least one list phrase matches (disjunction). ``all``: every phrase must match."""

PgroongaPlan = Literal["filter_first", "index_first", "auto"]
"""PGroonga ranked search SQL shape (Postgres adapter)."""

SearchCountPolicy = Literal["exact", "approximate", "none"]
"""How ranked search populates page totals when ``return_count=True``."""

# ....................... #

ResultSnapshotMode: TypeAlias = bool | Literal["auto"]
"""Whether to materialize an ordered result snapshot for paged follow-up.

``True`` always (when the adapter supports it), ``False`` never, ``\"auto\"`` defers
to the search surface defaults (e.g. :class:`~.SearchResultSnapshotSpec`).
"""

# ....................... #


class HighlightOptions(TypedDict, total=False):
    """Per-request highlighting options (see RFC 0006).

    Used as the value of :attr:`SearchOptions.highlight` when finer control than
    ``True`` is needed. Omitted keys fall back to backend defaults; the marker tags
    default to the cross-industry ``<em>`` / ``</em>`` and are always passed to the
    backend explicitly so marked-up output is uniform across adapters.
    """

    fields: Sequence[str]
    """Subset of highlightable fields to highlight; omit to highlight all of them
    (the spec's :attr:`~.SearchSpec.highlightable_fields`, defaulting to searchable ``fields``)."""

    pre_tag: str
    """Opening marker inserted before each matched fragment. Default ``\"<em>\"``."""

    post_tag: str
    """Closing marker inserted after each matched fragment. Default ``\"</em>\"``."""

    fragment_size: int
    """Approximate maximum length (characters) of each returned snippet fragment."""

    max_fragments: int
    """Maximum number of snippet fragments returned per field."""


# ....................... #


class SearchResultSnapshotOptions(TypedDict, total=False):
    """Per-request options for result-ID snapshotting.

    Omitted keys fall back to the search surface :class:`.SearchResultSnapshotSpec` where applicable.
    """

    mode: ResultSnapshotMode
    """``True`` / ``False`` / ``\"auto\"``; omit defers to the surface :class:`.SearchResultSnapshotSpec` ``enabled``."""

    id: str
    """Opaque handle; when set, read the next page from a stored ordered ID list."""

    ttl_seconds: int
    """Override TTL in seconds for the stored ID list and metadata."""

    max_ids: int
    """Override maximum number of document IDs in this snapshot."""

    chunk_size: int
    """Override KV chunk size for materializing ``ordered_ids``."""

    fingerprint: str
    """If set, must match the stored snapshot or the read is treated as a miss."""


# ....................... #


class SearchOptions(TypedDict, total=False):
    """Optional tuning parameters for search backends."""

    fuzzy: bool
    """Whether fuzzy matching is enabled."""

    weights: dict[str, float]
    """Field weights (between 0.0 and 1.0). If field weight is not specified, it will be set to 0.0."""

    fields: Sequence[str]
    """Simple alternative to weights for specifying fields to search on.

    For specified fields weights will be set to 1.0, for other fields weights will be set to 0.0.
    Ignored if weights are provided.
    """

    member_weights: dict[str, float]
    """Weights for hub / federation members."""

    members: Sequence[str]
    """Simple alternative to member_weights for specifying hub / federation members to search on.

    For specified members weights will be set to 1.0, for other members weights will be set to 0.0.
    Ignored if member_weights are provided.
    """

    phrase_combine: PhraseCombine
    """When ``query`` is a list of strings, how to combine them.

    ``any`` (default): disjunction (match if any phrase matches).
    ``all``: conjunction (match every phrase).
    """

    pgroonga_plan: PgroongaPlan
    """Override Postgres PGroonga plan (``filter_first``, ``index_first``, ``auto``)."""

    candidate_limit: int
    """Cap ranked heap rows per PGroonga leg or simple search pipeline."""

    groonga_query: str
    """Raw Groonga query string for ``pgroonga_condition`` (skips phrase combiner)."""

    search_count: SearchCountPolicy
    """Ranked search total: ``exact`` (``COUNT(*)``), ``approximate`` (planner/stats), ``none``."""

    combo_limit: int
    """Cap rows in hub ``combo_top`` before outer pagination (Postgres hub search)."""

    facets: Sequence[str]
    """Field names to compute term (value) distributions over for this query (see RFC 0006).

    Each must be a :attr:`~.SearchSpec.facetable_fields` member; an unservable field
    fails with a ``precondition``. Distributions are returned on the page
    (:attr:`~forze.application.contracts.base.value_objects.CountlessPage.facets`),
    computed over the full matching set, independent of the page window."""

    facet_size: int
    """Maximum number of buckets returned per faceted field (caps buckets, not the match
    count). Backend default applies when omitted."""

    highlight: bool | HighlightOptions
    """Request highlighting of matched fragments per hit (see RFC 0006). ``True`` highlights
    all highlightable fields with default markers; a :class:`HighlightOptions` narrows the
    fields / customizes markers. Returned index-aligned on the page
    (:attr:`~forze.application.contracts.base.value_objects.CountlessPage.highlights`)."""
