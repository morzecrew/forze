"""Port for the low-level GCP KMS client."""

from typing import Awaitable, Protocol

# ----------------------- #


class GcpKmsClientPort(Protocol):
    """Low-level GCP KMS client (symmetric encrypt/decrypt plus lifecycle).

    GCP KMS has no ``GenerateDataKey``; the envelope adapter generates the data
    key itself and wraps it with :meth:`encrypt`, so this port exposes the two
    raw symmetric operations rather than a data-key API.
    """

    def initialize(self) -> Awaitable[None]:
        """Open the underlying async KMS client / channel."""

        ...  # pragma: no cover

    def close(self) -> Awaitable[None]:
        """Release the underlying client / channel."""

        ...  # pragma: no cover

    def health(self) -> Awaitable[tuple[str, bool]]:
        """Return ``(message, ok)``; must not raise."""

        ...  # pragma: no cover

    def encrypt(self, key_name: str, plaintext: bytes) -> Awaitable[bytes]:
        """Encrypt *plaintext* under the CryptoKey *key_name*, returning ciphertext.

        *key_name* is a CryptoKey resource name
        (``projects/…/locations/…/keyRings/…/cryptoKeys/…``); KMS uses the key's
        primary version.
        """

        ...  # pragma: no cover

    def decrypt(self, key_name: str, ciphertext: bytes) -> Awaitable[bytes]:
        """Decrypt *ciphertext* under the CryptoKey *key_name*, returning plaintext.

        KMS selects the version from the ciphertext, so rotation is transparent.
        """

        ...  # pragma: no cover
