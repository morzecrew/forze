"""Ports for key management (the async half of envelope encryption)."""

from typing import Awaitable, Protocol

from .value_objects import DataKey, KeyRef

# ----------------------- #


class KeyManagementPort(Protocol):
    """Port for a key manager (KMS / HSM / transit engine).

    Implementations call out to a key backend; the key-encryption key never
    leaves it. The framework only ever holds wrapped data keys plus short-lived
    plaintext data keys. This is the BYOK seam: a customer-managed backend
    resolves the customer's own key, while a framework-managed backend resolves a
    key the deployment provisions.
    """

    def generate_data_key(self, key_ref: KeyRef) -> Awaitable[DataKey]:
        """Generate a fresh data-encryption key, returned plaintext + wrapped.

        :param key_ref: Reference to the key-encryption key to wrap under. A
            ``None`` :attr:`~KeyRef.version` requests the backend's current
            version, which is echoed back in the result.
        :raises CoreException: ``infrastructure`` / ``not_found`` when the key
            cannot be resolved or the backend is unavailable.
        """

        ...  # pragma: no cover

    def unwrap_data_key(
        self,
        *,
        wrapped: bytes,
        key_ref: KeyRef,
    ) -> Awaitable[bytes]:
        """Unwrap a previously wrapped data-encryption key.

        :param wrapped: The wrapped data key taken from a stored envelope.
        :param key_ref: Reference (including version) identifying the
            key-encryption key that wrapped it.
        :returns: The raw plaintext data-encryption key.
        :raises CoreException: ``infrastructure`` / ``not_found`` when the key
            version cannot be resolved or unwrapping fails.
        """

        ...  # pragma: no cover
