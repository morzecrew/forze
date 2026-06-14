"""Ports for key management and value-level encryption."""

from typing import Awaitable, Iterable, Protocol

from forze.application.contracts.tenancy import TenantIdentity
from forze.base.crypto import EncryptedEnvelope

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


# ....................... #


class FieldCipherPort(Protocol):
    """Cipher with a synchronous fast path, for the field-encrypting codec.

    The codec runs inside synchronous ``ModelCodec`` methods, so it cannot await
    a key backend. Instead, an async *pre-pass* primes the data-key cache and the
    sync methods then operate purely against it:

    - before a synchronous *encode*, :meth:`warm` resolves the tenant's active
      data key;
    - before a synchronous *decode*, :meth:`ensure_unwrapped` unwraps the data
      keys named by the stored envelopes (only needed across processes / after a
      key rotation — a same-process read-after-write already hits the cache).

    A sync call with a cold cache raises rather than blocking, surfacing a
    missing pre-pass as a wiring bug.
    """

    def warm(self, tenant: TenantIdentity | None) -> Awaitable[None]:
        """Pre-resolve *tenant*'s active data key for subsequent sync encrypts."""

        ...  # pragma: no cover

    def ensure_unwrapped(
        self,
        envelopes: Iterable[EncryptedEnvelope],
    ) -> Awaitable[None]:
        """Unwrap and cache the data keys for *envelopes* for sync decrypts."""

        ...  # pragma: no cover

    def encrypt_sync(
        self,
        plaintext: bytes,
        *,
        tenant: TenantIdentity | None,
        aad: bytes = b"",
    ) -> bytes:
        """Encrypt synchronously against the warmed cache.

        :raises CoreException: ``internal`` ``core.crypto.cipher_not_warm`` when
            the tenant's active data key has not been :meth:`warm`-ed.
        """

        ...  # pragma: no cover

    def decrypt_sync(self, blob: bytes, *, aad: bytes = b"") -> bytes:
        """Decrypt synchronously against the warmed cache.

        :raises CoreException: ``internal`` ``core.crypto.cipher_not_warm`` when
            the envelope's data key is not cached (run :meth:`ensure_unwrapped`).
        """

        ...  # pragma: no cover
