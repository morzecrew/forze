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
