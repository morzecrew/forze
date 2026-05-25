"""Port for HashiCorp Vault KV access."""

from typing import Any, Awaitable, Protocol

# ----------------------- #


class VaultClientPort(Protocol):
    """Low-level Vault KV v2 client."""

    def initialize(self) -> Awaitable[None]:
        """Create the underlying client and authenticate."""

        ...  # pragma: no cover

    def close(self) -> Awaitable[None]:
        """Release the underlying client."""

        ...  # pragma: no cover

    def read_kv_data(self, path: str) -> Awaitable[dict[str, Any]]:
        """Read secret data for a logical KV path (without mount prefix)."""

        ...  # pragma: no cover

    def kv_exists(self, path: str) -> Awaitable[bool]:
        """Return whether a secret exists at *path*."""

        ...  # pragma: no cover
