from typing import (
    Any,
    Awaitable,
    Protocol,
    Sequence,
    runtime_checkable,
)

# ----------------------- #


@runtime_checkable
class CacheReadPort(Protocol):  # pragma: no cover
    """Contract for reading values from a cache backend."""

    def get(self, key: str) -> Awaitable[Any | None]:
        """Return the cached value for *key*, or ``None`` on miss."""
        ...

    def get_many(
        self,
        keys: Sequence[str],
    ) -> Awaitable[tuple[dict[str, Any], list[str]]]:
        """Return found entries and a list of missing keys."""
        ...


# ....................... #


@runtime_checkable
class CacheWritePort(Protocol):  # pragma: no cover
    """Contract for writing, versioning, and deleting cached values."""

    def set(self, key: str, value: Any) -> Awaitable[None]:
        """Store *value* under *key*."""
        ...

    def set_versioned(self, key: str, version: str, value: Any) -> Awaitable[None]:
        """Store *value* under *key* tagged with a *version* identifier."""
        ...

    def set_many(
        self,
        key_mapping: dict[str, Any],
    ) -> Awaitable[None]:
        """Bulk-store multiple key/value pairs."""
        ...

    def set_many_versioned(
        self,
        key_version_mapping: dict[tuple[str, str], Any],
    ) -> Awaitable[None]:
        """Bulk-store multiple versioned key/value pairs."""
        ...

    def delete(self, key: str, *, hard: bool) -> Awaitable[None]:
        """Delete a single cache entry. When ``hard`` is ``True``, bypass soft-deletion."""
        ...

    def delete_many(self, keys: Sequence[str], *, hard: bool) -> Awaitable[None]:
        """Delete multiple cache entries at once."""
        ...


# ....................... #


@runtime_checkable
class CachePort(CacheReadPort, CacheWritePort, Protocol):
    """Combined read/write cache contract."""
