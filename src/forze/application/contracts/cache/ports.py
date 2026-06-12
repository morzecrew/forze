from datetime import timedelta
from typing import Any, Awaitable, Protocol, Sequence, runtime_checkable, Mapping

from forze.base.primitives import JsonDict

# ----------------------- #


@runtime_checkable
class CacheQueryPort(Protocol):  # pragma: no cover
    """Contract for reading values from a cache backend."""

    def get(self, key: str) -> Awaitable[Any | None]:
        """Return the cached value for *key*, or ``None`` on miss."""
        ...

    def get_many(self, keys: Sequence[str]) -> Awaitable[tuple[JsonDict, list[str]]]:
        """Return found entries and a list of missing keys."""
        ...

    def exists(self, key: str) -> Awaitable[bool]:
        """Whether a live entry exists for *key* — a presence check without
        transferring or decoding the payload."""
        ...


# ....................... #


@runtime_checkable
class CacheCommandPort(Protocol):  # pragma: no cover
    """Contract for writing, versioning, and deleting cached values.

    Every setter takes an optional per-entry ``ttl`` overriding the adapter's
    configured default lifetime for that entry alone — the seam adaptive-TTL
    policies (e.g. age-proportional document caching) write through. ``None``
    keeps the configured default.

    **Value contract:** values must be JSON-serializable, or pre-encoded
    ``bytes`` (stored verbatim and returned for the caller to decode).
    Adapters may serialize with any JSON codec — do not rely on key ordering
    or non-JSON types surviving a round trip.
    """

    def set(
        self,
        key: str,
        value: Any,
        *,
        ttl: timedelta | None = None,
    ) -> Awaitable[None]:
        """Store *value* under *key* (optionally with a per-entry *ttl*)."""
        ...

    def set_many(
        self,
        key_mapping: Mapping[str, Any],
        *,
        ttl: timedelta | None = None,
    ) -> Awaitable[None]:
        """Bulk-store multiple key/value pairs (one *ttl* for the batch).

        Values follow the same contract as :meth:`set` — JSON-serializable,
        or pre-encoded ``bytes``.
        """
        ...

    def set_versioned(
        self,
        key: str,
        version: str,
        value: Any,
        *,
        ttl: timedelta | None = None,
    ) -> Awaitable[None]:
        """Store *value* under *key* tagged with a *version* identifier."""
        ...

    def set_many_versioned(
        self,
        key_version_mapping: Mapping[tuple[str, str], Any],
        *,
        ttl: timedelta | None = None,
    ) -> Awaitable[None]:
        """Bulk-store multiple versioned key/value pairs (one *ttl* for the batch)."""
        ...

    def delete(self, key: str, *, hard: bool) -> Awaitable[None]:
        """Delete a single cache entry. When ``hard`` is ``True``, bypass soft-deletion."""
        ...

    def delete_many(self, keys: Sequence[str], *, hard: bool) -> Awaitable[None]:
        """Delete multiple cache entries at once."""
        ...


# ....................... #


@runtime_checkable
class CachePort(CacheQueryPort, CacheCommandPort, Protocol):
    """Combined read/write cache contract."""
