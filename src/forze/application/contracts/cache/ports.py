from typing import (
    Any,
    Awaitable,
    Optional,
    Protocol,
    Sequence,
    runtime_checkable,
)

# ----------------------- #


@runtime_checkable
class CacheReadPort(Protocol):  # pragma: no cover
    """Cache read operations."""

    def get(self, key: str) -> Awaitable[Optional[Any]]: ...
    def get_many(
        self,
        keys: Sequence[str],
    ) -> Awaitable[tuple[dict[str, Any], list[str]]]: ...


# ....................... #


@runtime_checkable
class CacheWritePort(Protocol):  # pragma: no cover
    """Cache write operations."""

    def set(self, key: str, value: Any) -> Awaitable[None]: ...
    def set_versioned(self, key: str, version: str, value: Any) -> Awaitable[None]: ...

    def set_many(
        self,
        key_mapping: dict[str, Any],
    ) -> Awaitable[None]: ...
    def set_many_versioned(
        self,
        key_version_mapping: dict[tuple[str, str], Any],
    ) -> Awaitable[None]: ...

    def delete(self, key: str, *, hard: bool) -> Awaitable[None]: ...
    def delete_many(self, keys: Sequence[str], *, hard: bool) -> Awaitable[None]: ...


# ....................... #


@runtime_checkable
class CachePort(CacheReadPort, CacheWritePort, Protocol):
    """Cache operations."""
