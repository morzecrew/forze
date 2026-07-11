"""Port for the low-level Yandex Cloud KMS client."""

from typing import Awaitable, Protocol

from .value_objects import YcGeneratedDataKey

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
    ) -> Awaitable[YcGeneratedDataKey]:
        """Generate a data key under the symmetric key *key_id*.

        Returns the raw data key, the wrapped blob only that key can decrypt, and the
        key version that wrapped it. *algorithm* is a Yandex Cloud
        ``SymmetricAlgorithm`` name (``AES_256`` → a 32-byte key, ``AES_128`` → 16).
        """

        ...  # pragma: no cover

    def decrypt(self, key_id: str, ciphertext: bytes) -> Awaitable[bytes]:
        """Decrypt a wrapped data key under *key_id*, returning the raw plaintext.

        The ciphertext names its own key version, so rotation is transparent.
        """

        ...  # pragma: no cover

    # ....................... #
    # Key administration (per-tenant provisioning)

    def find_key_id_by_name(self, folder_id: str, name: str) -> Awaitable[str | None]:
        """Return the id of the symmetric key called *name* in *folder_id*, or ``None``.

        Yandex Cloud mints a key id itself and the crypto API addresses keys by id, so a
        caller-chosen *name* has to be resolved to an id before a key can be used.
        """

        ...  # pragma: no cover

    def create_key(
        self,
        folder_id: str,
        name: str,
        *,
        algorithm: str = "AES_256",
        description: str | None = None,
    ) -> Awaitable[str]:
        """Create a symmetric key called *name* in *folder_id*, returning its new id."""

        ...  # pragma: no cover

    def delete_key(self, key_id: str) -> Awaitable[None]:
        """Delete the symmetric key *key_id* (destructive and irreversible)."""

        ...  # pragma: no cover
