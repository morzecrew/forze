"""Ports for key management and value-level encryption."""

from typing import Awaitable, Protocol

from forze.application.contracts.tenancy import TenantIdentity

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


# ....................... #


class BytesCipherPort(Protocol):
    """Encrypt/decrypt opaque byte values, resolving the tenant's key internally.

    The async value-encryption seam used by integration adapters (object storage,
    later message/field paths). Implementations (see the keyring) resolve the
    key-encryption key per tenant, perform envelope encryption, and cache data
    keys; callers pass associated data to bind ciphertext to its context.
    """

    def encrypt(
        self,
        plaintext: bytes,
        *,
        tenant: TenantIdentity | None,
        aad: bytes = b"",
    ) -> Awaitable[bytes]:
        """Encrypt *plaintext* under *tenant*'s key, returning a packed envelope."""

        ...  # pragma: no cover

    def decrypt(self, blob: bytes, *, aad: bytes = b"") -> Awaitable[bytes]:
        """Decrypt a packed envelope; the key is resolved from the envelope itself.

        :param aad: Must equal the value passed to :meth:`encrypt`.
        :raises CoreException: ``validation`` on a malformed envelope or an
            authentication failure (tamper / wrong ``aad`` / wrong tenant).
        """

        ...  # pragma: no cover
