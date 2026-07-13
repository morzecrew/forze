"""Ports for key management and value-level encryption."""

from collections.abc import AsyncIterator, Awaitable, Iterable
from typing import Protocol, runtime_checkable

from forze.application.contracts.tenancy import TenantIdentity
from forze.base.crypto import DEFAULT_CHUNK_SIZE, ChunkFrame, EncryptedEnvelope

from .value_objects import DataKey, KeyRef

# ----------------------- #


class KeyManagementPort(Protocol):
    """Port for a key manager (KMS / HSM / transit engine).

    Implementations call out to a key backend; the key-encryption key never
    leaves it. The framework only ever holds wrapped data keys plus short-lived
    plaintext data keys. This is the BYOK seam: a customer-managed backend
    resolves the customer's own key, while a framework-managed backend resolves a
    key the deployment provisions.

    **Two methods, deliberately** — there is no ``rotate``, ``list_versions``, or
    ``rewrap`` here, and adding one would be a mistake:

    - *Rotating a key version* is the backend's own concern and is already
      transparent: a wrapped data key is decryptable by the backend without being told
      which version sealed it, so data written before a rotation still decrypts
      afterwards and new writes pick up the new version by themselves. Nothing to sweep,
      nothing to call. (:attr:`DataKey.key_version` records the version only where the
      provider reports one — it never *drives* the unwrap.)
    - *Retiring* old key material is a re-encryption, not a rewrap:
      ``reencrypt_documents`` / ``reencrypt_objects`` already re-seal under the
      current key as a side effect of their read→write round-trip. A rewrap that
      swapped only the wrapped data key would save no I/O (the row or object is
      rewritten either way), and no backend agrees on how to express it.
    - *Replacing a key* is a directory concern, not a backend one — see
      :class:`~forze.application.contracts.crypto.KeyDirectoryWithPrevious`.

    Keeping the surface at two methods is what lets any KMS — including one this
    project has never seen — be a few lines of adapter.
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

    def decrypt(
        self,
        blob: bytes,
        *,
        aad: bytes = b"",
        tenant: TenantIdentity | None = None,
    ) -> Awaitable[bytes]:
        """Decrypt a packed envelope; the key is resolved from the envelope itself.

        :param aad: Must equal the value passed to :meth:`encrypt`.
        :param tenant: When given, the envelope's key id is checked against the
            tenant's own key-encryption key *before* any KMS unwrap, so a caller
            cannot make the backend unwrap under a key id it names but does not own
            (a cross-tenant confused-deputy). ``None`` skips the check (single-key).
        :raises CoreException: ``validation`` on a malformed envelope, an
            authentication failure (tamper / wrong ``aad`` / wrong tenant), or a
            ``core.crypto.key_id_unauthorized`` key-id/tenant mismatch.
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
        *,
        tenant: TenantIdentity | None = None,
    ) -> Awaitable[None]:
        """Unwrap and cache the data keys for *envelopes* for sync decrypts.

        :param tenant: When given, each envelope's key id is checked against the
            tenant's key-encryption key before it is unwrapped, so a foreign key id
            fails closed (``core.crypto.key_id_unauthorized``) with no KMS call.
            The check runs whether or not the data key is already cached — a warm
            cache never bypasses it.
        """

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


# ....................... #


@runtime_checkable
class ChunkedStreamOpener(Protocol):
    """Random-access reader over one chunked-AEAD object (its data key already unwrapped).

    Returned by :meth:`StreamingBytesCipherPort.open_chunked_stream` after the header is
    parsed and the tenant authorized: it exposes the layout a caller needs to seek
    (:attr:`chunk_size`, :attr:`header_len`) and opens an individual parsed frame at its
    index. Lets a storage adapter fetch and decrypt only the chunks a byte range covers.
    """

    @property
    def chunk_size(self) -> int:
        """Plaintext bytes per (non-final) chunk — the range→chunk mapping unit."""
        ...  # pragma: no cover

    @property
    def header_len(self) -> int:
        """Byte length of the stream header (where the first frame begins)."""
        ...  # pragma: no cover

    def open_frame(self, index: int, frame: ChunkFrame) -> bytes:
        """Verify and decrypt the frame at position *index*.

        :raises CoreException: ``validation`` on an authentication failure (tampered,
            reordered — wrong *index* — or mis-flagged chunk).
        """
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class StreamingBytesCipherPort(Protocol):
    """Encrypt/decrypt a byte value chunk-by-chunk for bounded-memory large blobs.

    The whole-value :class:`BytesCipherPort` needs the entire plaintext (and its
    ciphertext) in memory at once. This seam frames the value into independently
    sealed chunks (see :mod:`forze.base.crypto.chunked`) so a producer/consumer holds
    only one chunk at a time — the basis for streaming a large object through the
    object store. One data key is generated per stream and KMS-wrapped in the stream
    header; the reader unwraps it once, then opens each chunk against it.
    """

    def encrypt_stream(
        self,
        plaintext: AsyncIterator[bytes],
        *,
        tenant: TenantIdentity | None,
        aad: bytes = b"",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> AsyncIterator[bytes]:
        """Yield a chunked-AEAD stream (header then sealed chunks) for *plaintext*.

        *plaintext* is re-chunked to *chunk_size* internally, so the caller may feed
        arbitrary byte runs. *aad* is the base associated data (typically the object's
        bucket/key/tenant binding); each chunk additionally binds its position and a
        terminator flag, giving reordering and truncation resistance.
        """

        ...  # pragma: no cover

    def decrypt_stream(
        self,
        ciphertext: AsyncIterator[bytes],
        *,
        aad: bytes = b"",
        tenant: TenantIdentity | None = None,
    ) -> AsyncIterator[bytes]:
        """Yield plaintext chunks for a chunked-AEAD stream produced by :meth:`encrypt_stream`.

        *aad* must equal the base value passed to :meth:`encrypt_stream`. When *tenant*
        is given, the header's key id is authorized against the tenant's own key before
        the data key is unwrapped (confused-deputy guard). A stream that ends without a
        terminating chunk (truncation) or carries trailing bytes is rejected.

        :raises CoreException: ``validation`` on a malformed/truncated stream, an
            authentication failure, an algorithm mismatch, or a
            ``core.crypto.key_id_unauthorized`` key-id/tenant mismatch.
        """

        ...  # pragma: no cover

    def open_chunked_stream(
        self,
        header_bytes: bytes,
        *,
        aad: bytes = b"",
        tenant: TenantIdentity | None = None,
    ) -> Awaitable["ChunkedStreamOpener"]:
        """Parse a chunked stream's *header_bytes* and return a random-access opener.

        Authorizes the header's key id against *tenant* (confused-deputy guard) and
        unwraps the data key **once**, so the caller can then fetch and decrypt only the
        chunks a byte range covers (see :class:`ChunkedStreamOpener`). *header_bytes* must
        contain at least the full header; *aad* is the object's base associated data.

        :raises CoreException: ``validation`` on a malformed/truncated header, an
            algorithm mismatch, or a ``core.crypto.key_id_unauthorized`` mismatch.
        """

        ...  # pragma: no cover


# ....................... #


class KeyringPort(BytesCipherPort, FieldCipherPort, StreamingBytesCipherPort, Protocol):
    """The keyring's full surface: the async value cipher, the sync field path, and streaming.

    A single registration (``KeyringDepKey``) serves every consumer — object storage
    uses the async :class:`BytesCipherPort` and :class:`StreamingBytesCipherPort` halves,
    the field codec uses the :class:`FieldCipherPort` half. The
    :class:`~forze.application.integrations.crypto.Keyring` implements all of it.
    """


# ....................... #


class DeterministicFieldCipherPort(Protocol):
    """Synchronous deterministic cipher for equality-searchable encrypted fields.

    Same ``(tenant, field, plaintext)`` always maps to the same ciphertext, so an
    equality filter can be rewritten to match the value stored at rest. Fully sync
    (a stable key, no KMS round-trip) — no warm/pre-pass needed.
    """

    def encrypt(
        self,
        *,
        tenant: TenantIdentity | None,
        field: str,
        plaintext: bytes,
    ) -> bytes:
        """Deterministically encrypt *plaintext* for ``(tenant, field)``."""

        ...  # pragma: no cover

    def decrypt(
        self,
        *,
        tenant: TenantIdentity | None,
        field: str,
        ciphertext: bytes,
    ) -> bytes:
        """Decrypt a value produced by :meth:`encrypt`.

        :raises CoreException: ``validation`` when authentication fails.
        """

        ...  # pragma: no cover

    def search_variants(
        self,
        *,
        tenant: TenantIdentity | None,
        field: str,
        plaintext: bytes,
    ) -> tuple[bytes, ...]:
        """Every ciphertext an equality query must match for *plaintext*.

        The current ciphertext in steady state; during a key-rotation overlap also
        the ciphertext under the prior key, so a query still matches values written
        before rotation. Used by the filter rewrite to lower equality predicates.
        """

        ...  # pragma: no cover
