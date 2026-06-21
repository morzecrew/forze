from typing import final

import attrs

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
