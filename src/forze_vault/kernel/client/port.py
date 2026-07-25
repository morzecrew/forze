"""Port for HashiCorp Vault KV access."""

from collections.abc import Awaitable
from typing import Any, Protocol

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

    def read_kv_data_versioned(self, path: str) -> Awaitable[tuple[dict[str, Any], int]]:
        """Read secret data and its KV v2 version in one request (no TOCTOU)."""

        ...  # pragma: no cover

    def read_kv_metadata(self, path: str) -> Awaitable[dict[str, Any]]:
        """Read KV v2 metadata for *path* (``current_version``, timestamps) without
        the secret payload."""

        ...  # pragma: no cover

    def write_kv_data(self, path: str, data: dict[str, Any]) -> Awaitable[int]:
        """Write *data* as the new current version at *path*, returning the version."""

        ...  # pragma: no cover

    def db_generate_credentials(self, role: str) -> Awaitable[dict[str, Any]]:
        """Mint dynamic database credentials for *role*, returning the raw lease
        response (``lease_id``, ``lease_duration``, ``renewable``, ``data``)."""

        ...  # pragma: no cover

    def renew_lease(self, lease_id: str, increment_seconds: int) -> Awaitable[int]:
        """Renew a lease via ``sys/leases/renew``, returning the granted TTL in
        seconds (backends may grant less than asked)."""

        ...  # pragma: no cover

    def revoke_lease(self, lease_id: str) -> Awaitable[None]:
        """Revoke a lease via ``sys/leases/revoke``, dropping what it minted."""

        ...  # pragma: no cover

    def health(self) -> Awaitable[tuple[str, bool]]:
        """Return ``(message, ok)``; must not raise."""

        ...  # pragma: no cover

    def transit_generate_data_key(
        self,
        key_name: str,
    ) -> Awaitable[tuple[bytes, str]]:
        """Generate a Transit data key, returning ``(plaintext, wrapped_ciphertext)``.

        ``plaintext`` is the raw data key; ``wrapped_ciphertext`` is Vault's
        ``vault:vN:...`` token, which only the named Transit key can decrypt.
        """

        ...  # pragma: no cover

    def transit_decrypt(
        self,
        key_name: str,
        ciphertext: str,
    ) -> Awaitable[bytes]:
        """Decrypt a Transit ``vault:vN:...`` token, returning the raw plaintext."""

        ...  # pragma: no cover

    def transit_sign(
        self,
        key_name: str,
        data: bytes,
        *,
        signature_algorithm: str | None = "pkcs1v15",
        marshaling_algorithm: str | None = None,
    ) -> Awaitable[bytes]:
        """Sign *data* with a Transit signing key, returning the raw JWS signature.

        RSA (RS256) by default; for an ECDSA (ES256) key pass
        ``signature_algorithm=None, marshaling_algorithm="jws"``.
        """

        ...  # pragma: no cover

    def transit_public_key(self, key_name: str) -> Awaitable[str]:
        """Return the PEM public key of a Transit signing key's latest version."""

        ...  # pragma: no cover

    def transit_rewrap(self, key_name: str, ciphertext: str) -> Awaitable[str]:
        """Re-wrap a ``vault:vN:...`` ciphertext under the key's latest version."""

        ...  # pragma: no cover

    def transit_create_key(self, key_name: str, *, key_type: str) -> Awaitable[None]:
        """Create a Transit key of *key_type* (idempotent — existing key is a no-op)."""

        ...  # pragma: no cover

    def transit_delete_key(self, key_name: str) -> Awaitable[None]:
        """Delete a Transit key (enabling deletion first); a no-op if already absent."""

        ...  # pragma: no cover
