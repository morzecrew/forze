"""Port for the low-level Yandex Cloud KMS client."""

from typing import Awaitable, Protocol

# ----------------------- #


class YcKmsClientPort(Protocol):
    """Low-level Yandex Cloud KMS client (the two envelope operations plus lifecycle).

    Yandex Cloud KMS has a native data-key API (``SymmetricCrypto.GenerateDataKey``),
    so — like AWS KMS and unlike GCP — the backend mints the data key.
    """

    def initialize(self) -> Awaitable[None]:
        """Build the underlying SDK client / stub."""

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
        algorithm: str = "AES_256",
    ) -> Awaitable[tuple[bytes, bytes]]:
        """Generate a data key under the symmetric key *key_id*.

        Returns ``(plaintext, ciphertext)`` — the raw data key and the wrapped
        blob only that key can decrypt. *algorithm* is a Yandex Cloud
        ``SymmetricAlgorithm`` name (``AES_256`` → a 32-byte key, ``AES_128`` → 16).
        """

        ...  # pragma: no cover

    def decrypt(self, key_id: str, ciphertext: bytes) -> Awaitable[bytes]:
        """Decrypt a wrapped data key under *key_id*, returning the raw plaintext.

        The ciphertext names its own key version, so rotation is transparent.
        """

        ...  # pragma: no cover
