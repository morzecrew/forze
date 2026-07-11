"""Port for the low-level AWS KMS client."""

from typing import Awaitable, Protocol

# ----------------------- #


class AwsKmsClientPort(Protocol):
    """Low-level AWS KMS client (the two envelope operations plus lifecycle)."""

    def initialize(self) -> Awaitable[None]:
        """Open the underlying long-lived ``aioboto3`` KMS client."""

        ...  # pragma: no cover

    def close(self) -> Awaitable[None]:
        """Release the underlying client."""

        ...  # pragma: no cover

    def health(self) -> Awaitable[tuple[str, bool]]:
        """Return ``(message, ok)``; must not raise."""

        ...  # pragma: no cover

    def generate_data_key(
        self,
        key_id: str,
        *,
        key_spec: str = "AES_256",
    ) -> Awaitable[tuple[bytes, bytes]]:
        """Generate a data key under the CMK *key_id*.

        Returns ``(plaintext, ciphertext_blob)`` — the raw data key and the
        opaque KMS-wrapped blob that only the CMK can decrypt. ``key_spec`` is
        the AWS ``KeySpec`` (``AES_256`` → a 32-byte key, ``AES_128`` → 16).
        """

        ...  # pragma: no cover

    def decrypt(
        self,
        ciphertext_blob: bytes,
        *,
        key_id: str | None = None,
    ) -> Awaitable[bytes]:
        """Decrypt a KMS ``ciphertext_blob``, returning the raw plaintext.

        When *key_id* is given it is passed to ``Decrypt`` so KMS rejects a blob
        that was not wrapped under that CMK (a server-side confused-deputy guard
        beside the keyring's own check); for a symmetric CMK the blob already
        names its key, so *key_id* is a constraint, not a selector.
        """

        ...  # pragma: no cover

    # ....................... #
    # Key administration (per-tenant provisioning)

    def find_key_id_by_alias(self, alias: str) -> Awaitable[str | None]:
        """Return the CMK id the *alias* points at, or ``None`` when it does not exist."""

        ...  # pragma: no cover

    def create_key_with_alias(
        self,
        alias: str,
        *,
        description: str | None = None,
    ) -> Awaitable[str]:
        """Create a symmetric CMK and point *alias* at it, returning the new CMK id.

        A CMK id is minted by KMS, so an alias is the only caller-chosen name a key
        directory can address a tenant's key by.
        """

        ...  # pragma: no cover

    def delete_alias(self, alias: str) -> Awaitable[None]:
        """Delete *alias* (a no-op when it does not exist). The CMK itself survives."""

        ...  # pragma: no cover

    def schedule_key_deletion(
        self,
        key_id: str,
        *,
        pending_window_days: int = 30,
    ) -> Awaitable[None]:
        """Schedule the CMK for deletion after a waiting period (7–30 days).

        KMS never deletes a key immediately — the window is a last chance to cancel,
        since destroying a KEK makes every value wrapped under it unrecoverable.
        """

        ...  # pragma: no cover
