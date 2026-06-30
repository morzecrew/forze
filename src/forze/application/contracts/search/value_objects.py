from typing import Any, Mapping, Tuple, TypeAlias, final

import attrs

from forze.base.exceptions import exc

# ----------------------- #
# Result-level search metadata: the snapshot continuation handle plus facet and
# highlight value objects carried by the SearchPage family (see .pages). These are
# search concepts, so they live here rather than in the base pagination contract.


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchSnapshotHandle:
    """Opaque handle to continue paged search without re-running the full query (KV snapshot)."""

    id: str
    """Snapshot run id; send back as :attr:`~.types.SearchResultSnapshotOptions.id`."""

    fingerprint: str
    """Stable request fingerprint; clients should echo for validation."""

    total: int
    """Number of entries materialized in the snapshot (after cap)."""

    capped: bool = False
    """``True`` if the result set was truncated to ``max_ids`` when the snapshot was written."""

    expires_at: int | None = None
    """Unix timestamp (UTC seconds) when the snapshot expires and replay stops serving it, or
    ``None`` when unknown (e.g. a run written before this was tracked). Lets a client tell how
    long the snapshot id stays valid before the query must be re-run."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FacetBucket:
    """One value in a facet (term) distribution: a field value and its document count."""

    value: Any
    """The field value (scalar: str / int / float / bool / ...)."""

    count: int
    """Number of matching documents carrying this value."""


FacetResults: TypeAlias = Mapping[str, Tuple[FacetBucket, ...]]
"""Facet distributions keyed by facetable field name → buckets ordered count-descending.

Result-level metadata attached to a paged search response (:attr:`~.pages.SearchCountlessPage.facets`)."""

HitHighlights: TypeAlias = Mapping[str, Tuple[str, ...]]
"""Highlighted fragments for a single hit, keyed by field name → marked-up snippets.

Each fragment already carries the requested ``pre_tag`` / ``post_tag`` markers. A field
with no match is absent; a hit with no highlights maps to an empty mapping (never ``None``),
so the per-hit highlight list stays index-aligned with ``hits`` and non-sparse."""

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class Rrf:
    """Reciprocal Rank Fusion settings for merging ranked result legs.

    Shared across the federated search integrations (Postgres, Meilisearch) so the
    fusion knobs are expressed the same way everywhere.
    """

    k: int = 60
    """RRF smoothing constant."""

    per_leg_limit: int = 5000
    """Max hits fetched per member leg before merging."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.k < 1:
            raise exc.configuration("RRF k must be at least 1.")

        if self.per_leg_limit < 1:
            raise exc.configuration("RRF per_leg_limit must be at least 1.")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchResultSnapshotMeta:
    """Metadata for a stored ordered-ID snapshot (read model)."""

    run_id: str
    """Opaque run identifier (same key used in :class:`SearchResultSnapshotPort` methods)."""

    fingerprint: str
    """Request fingerprint the snapshot was built from (e.g. hash of query + sort + surface)."""

    total: int
    """Number of document IDs in the snapshot (after any materialization cap)."""

    chunk_size: int
    """Chunk size used when writing ID lists to the backing store."""

    complete: bool
    """True when all chunks and meta were written for this run."""

    expires_at: int | None = None
    """Unix timestamp (UTC seconds) when the run's store keys expire, or ``None`` for a run
    written before this was tracked. Computed at write time as ``now + ttl``."""


# ....................... #

__all__ = [
    "FacetBucket",
    "FacetResults",
    "HitHighlights",
    "Rrf",
    "SearchResultSnapshotMeta",
    "SearchSnapshotHandle",
]
