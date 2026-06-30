from typing import final

import attrs

from forze.base.exceptions import exc

# Facets & highlights are defined in the base contract (next to the page value
# objects that carry them, same rationale as SearchSnapshotHandle) and re-exported
# here so they are reachable from the search contract surface.
from ..base.value_objects import FacetBucket, FacetResults, HitHighlights

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
]
