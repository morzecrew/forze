from typing import Literal, Sequence, TypedDict, TypeAlias

# ----------------------- #

PhraseCombine = Literal["any", "all"]
"""``any``: at least one list phrase matches (disjunction). ``all``: every phrase must match."""

# ....................... #

ResultSnapshotMode: TypeAlias = bool | Literal["auto"]
"""Whether to materialize an ordered result snapshot for paged follow-up.

``True`` always (when the adapter supports it), ``False`` never, ``\"auto\"`` defers
to the search surface defaults (e.g. :class:`~.SearchResultSnapshotSpec`).
"""

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

    result_snapshot: SearchResultSnapshotOptions
    """Result-ID snapshot controls (mode, handle, overrides). Used by the outer search adapter only."""
