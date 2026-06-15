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

    def transit_create_key(
        self, key_name: str, *, key_type: str
    ) -> Awaitable[None]:
        """Create a Transit key of *key_type* (idempotent — existing key is a no-op)."""

        ...  # pragma: no cover

    def transit_delete_key(self, key_name: str) -> Awaitable[None]:
        """Delete a Transit key (enabling deletion first); a no-op if already absent."""

        ...  # pragma: no cover
