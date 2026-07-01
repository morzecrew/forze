"""Deterministic field cipher — equality-searchable encryption via AES-SIV.

Randomized envelope encryption (the keyring) hides everything but is not
queryable. For fields that must support equality lookups, this cipher uses
AES-SIV (RFC 5297, deterministic AEAD): the same plaintext under the same key
always yields the same ciphertext, so an equality filter can be rewritten to
match the stored ciphertext — no separate blind-index column.

The key is derived per ``(tenant, field)`` via HKDF from a single stable root
secret, so it is **synchronous** (no KMS round-trip) and stable across processes
and restarts — which equality search requires. That stability is also the trade:
the root secret is long-lived (rotating it requires re-encrypting searchable
fields) and deterministic ciphertext leaks equality/frequency *within* a tenant
(distinct keys per tenant prevent cross-tenant correlation). Use it only for
fields where equality search is worth that exposure.
"""

from collections import OrderedDict
from typing import Iterable, final

import attrs
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESSIV
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import exc

# ----------------------- #


def _aessiv_decrypt(key: bytes, previous: bytes | None, ciphertext: bytes) -> bytes:
    """Decrypt an AES-SIV *ciphertext* under *key*, falling back to *previous* on auth failure.

    Shared by the live cipher and its frozen decrypt snapshot so both apply identical
    current-then-previous-root semantics."""

    try:
        return AESSIV(key).decrypt(ciphertext, [])

    except InvalidTag:
        pass

    if previous is not None:
        try:
            return AESSIV(previous).decrypt(ciphertext, [])

        except InvalidTag:
            pass

    raise exc.validation(
        "Deterministic decrypt failed (wrong key, tenant, or field)",
        code="core.crypto.deterministic_auth_failed",
    )


# ....................... #

# AES-SIV uses a double-length key: 64 bytes selects AES-256-SIV (two 256-bit
# subkeys), matching the AES-256 security level of the randomized envelope path.
_DERIVED_KEY_BYTES = 64
# Entropy floor for the root secret; HKDF expands it to the derived key length, so
# 32 bytes (256 bits) of input is sufficient — it need not equal the derived size.
_MIN_ROOT_BYTES = 32
# Fixed, non-secret HKDF salt — a stable salt strengthens extraction over a nil
# salt (NIST SP 800-56C Rev. 2 §4.1). Changing it re-derives every key.
_HKDF_SALT = b"forze.det.hkdf.v1"
_NONE_TENANT = "\x00none"


def _tenant_key(tenant: TenantIdentity | None) -> str:
    return _NONE_TENANT if tenant is None else str(tenant.tenant_id)


# ....................... #


@final
@attrs.define(slots=True)
class DeterministicFieldCipher:
    """Per-``(tenant, field)`` deterministic cipher derived from a stable root.

    A :class:`~forze.application.contracts.crypto.DeterministicFieldCipherPort`.

    Rotation: set :attr:`previous_root` to the prior secret during the overlap
    window. New writes encrypt under :attr:`root`; reads decrypt under either; and
    :meth:`search_variants` returns the ciphertext under *both* roots so an equality
    query still matches values written under the old key. Once a re-index sweep
    (``reencrypt_documents``) has rewritten every searchable value under the new
    root, drop :attr:`previous_root` and queries collapse back to a single key.
    """

    root: bytes = attrs.field(repr=False)
    """Stable root secret (>= 32 bytes). Long-lived; rotating it re-indexes data."""

    previous_root: bytes | None = attrs.field(default=None, repr=False)
    """Prior root, set only during a rotation overlap. Reads + queries still match
    values written under it; writes always use :attr:`root`."""

    key_cache_max: int = 1024
    """Max derived ``(tenant, field)`` keys to retain per root (LRU). Bounds memory
    under many tenants/fields; eviction just re-derives (cheap, no KMS round-trip)."""

    _keys: OrderedDict[tuple[str, str], bytes] = attrs.field(
        factory=OrderedDict, init=False, repr=False
    )
    _prev_keys: OrderedDict[tuple[str, str], bytes] = attrs.field(
        factory=OrderedDict, init=False, repr=False
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if len(self.root) < _MIN_ROOT_BYTES:
            raise exc.configuration(
                f"Deterministic cipher root secret must be at least {_MIN_ROOT_BYTES} bytes",
            )

        if self.previous_root is not None and len(self.previous_root) < _MIN_ROOT_BYTES:
            raise exc.configuration(
                f"Deterministic cipher previous root must be at least {_MIN_ROOT_BYTES} bytes",
            )

    # ....................... #

    def _derive(self, tenant: TenantIdentity | None, field: str, root: bytes) -> bytes:
        return HKDF(
            algorithm=hashes.SHA256(),
            length=_DERIVED_KEY_BYTES,
            salt=_HKDF_SALT,
            info=f"forze.det|{_tenant_key(tenant)}|{field}".encode("utf-8"),
        ).derive(root)

    def _cached_key(
        self,
        cache: OrderedDict[tuple[str, str], bytes],
        tenant: TenantIdentity | None,
        field: str,
        root: bytes,
    ) -> bytes:
        cache_key = (_tenant_key(tenant), field)
        cached = cache.get(cache_key)

        if cached is not None:
            cache.move_to_end(cache_key)
            return cached

        cached = cache[cache_key] = self._derive(tenant, field, root)

        while len(cache) > self.key_cache_max:
            cache.popitem(last=False)

        return cached

    def _key(self, tenant: TenantIdentity | None, field: str) -> bytes:
        return self._cached_key(self._keys, tenant, field, self.root)

    def _previous_key(self, tenant: TenantIdentity | None, field: str) -> bytes | None:
        if self.previous_root is None:
            return None

        return self._cached_key(self._prev_keys, tenant, field, self.previous_root)

    # ....................... #

    def encrypt(
        self,
        *,
        tenant: TenantIdentity | None,
        field: str,
        plaintext: bytes,
    ) -> bytes:
        """Deterministically encrypt *plaintext* for ``(tenant, field)`` under the root."""

        return AESSIV(self._key(tenant, field)).encrypt(plaintext, [])

    # ....................... #

    def decrypt(
        self,
        *,
        tenant: TenantIdentity | None,
        field: str,
        ciphertext: bytes,
    ) -> bytes:
        """Decrypt a value produced by :meth:`encrypt`, under the current or previous root.

        :raises CoreException: ``validation`` when authentication fails under every
            available key (the value was not produced by this cipher for this
            ``(tenant, field)``).
        """

        return _aessiv_decrypt(
            self._key(tenant, field),
            self._previous_key(tenant, field),
            ciphertext,
        )

    # ....................... #

    def freeze_decryptor(
        self,
        tenant: TenantIdentity | None,
        fields: Iterable[str],
    ) -> "_FrozenDeterministicFieldCipher":
        """Pre-derive the ``(tenant, field)`` keys for *fields* into a thread-local snapshot.

        Resolves every key on the event loop (mutating the shared derivation LRU here, where
        it is safe) so the returned cipher's :meth:`decrypt` derives nothing and touches no
        shared cache — safe to call from a worker thread (e.g. under ``run_cpu_map``). A batch
        is single-tenant (the codec resolves one tenant per decode), so keys are held by
        field alone."""

        keys = {field: self._key(tenant, field) for field in fields}
        prev = {
            field: pk
            for field in keys
            if (pk := self._previous_key(tenant, field)) is not None
        }

        return _FrozenDeterministicFieldCipher(keys=keys, prev=prev)

    # ....................... #

    def search_variants(
        self,
        *,
        tenant: TenantIdentity | None,
        field: str,
        plaintext: bytes,
    ) -> tuple[bytes, ...]:
        """Every ciphertext an equality query must match for *plaintext*.

        Just the current-root ciphertext in steady state; during a rotation overlap
        (``previous_root`` set) the previous-root ciphertext is appended too, so a
        query matches values written under either key.
        """

        primary = AESSIV(self._key(tenant, field)).encrypt(plaintext, [])
        previous = self._previous_key(tenant, field)

        if previous is None:
            return (primary,)

        return (primary, AESSIV(previous).encrypt(plaintext, []))


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class _FrozenDeterministicFieldCipher:
    """Thread-safe, decrypt-only snapshot of a :class:`DeterministicFieldCipher`.

    Built by :meth:`DeterministicFieldCipher.freeze_decryptor` from pre-derived per-field
    keys, so :meth:`decrypt` derives nothing and reads no shared LRU — safe to call off the
    event loop (e.g. under ``run_cpu_map``). A :class:`~forze.application.contracts.crypto.\
DeterministicFieldCipherPort` structurally; encrypt/search are not on the decrypt path and
    are unsupported."""

    keys: dict[str, bytes]
    """Field → current-root derived key (single-tenant batch)."""

    prev: dict[str, bytes]
    """Field → previous-root derived key, present only during a rotation overlap."""

    # ....................... #

    def decrypt(
        self,
        *,
        tenant: TenantIdentity | None,
        field: str,
        ciphertext: bytes,
    ) -> bytes:
        key = self.keys.get(field)

        if key is None:
            # Not in the snapshot (freeze resolves every searchable field, so this is
            # defensive): fail closed like an auth failure, so the codec treats the value as
            # legacy plaintext exactly as the live path would.
            raise exc.validation(
                "Deterministic decrypt failed (field not resolved in snapshot)",
                code="core.crypto.deterministic_auth_failed",
            )

        return _aessiv_decrypt(key, self.prev.get(field), ciphertext)

    # ....................... #

    def encrypt(
        self,
        *,
        tenant: TenantIdentity | None,
        field: str,
        plaintext: bytes,
    ) -> bytes:
        raise exc.internal("frozen deterministic cipher is decrypt-only")

    # ....................... #

    def search_variants(
        self,
        *,
        tenant: TenantIdentity | None,
        field: str,
        plaintext: bytes,
    ) -> tuple[bytes, ...]:
        raise exc.internal("frozen deterministic cipher is decrypt-only")
