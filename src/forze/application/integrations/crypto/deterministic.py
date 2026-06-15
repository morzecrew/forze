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

from typing import final

import attrs
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESSIV
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import exc

# ----------------------- #

_KEY_BYTES = 32  # AES-SIV key length (AES-128-SIV)
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

    _keys: dict[tuple[str, str], bytes] = attrs.field(factory=dict, init=False)
    _prev_keys: dict[tuple[str, str], bytes] = attrs.field(factory=dict, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if len(self.root) < _KEY_BYTES:
            raise exc.configuration(
                f"Deterministic cipher root secret must be at least {_KEY_BYTES} bytes",
            )

        if self.previous_root is not None and len(self.previous_root) < _KEY_BYTES:
            raise exc.configuration(
                f"Deterministic cipher previous root must be at least {_KEY_BYTES} bytes",
            )

    # ....................... #

    def _derive(self, tenant: TenantIdentity | None, field: str, root: bytes) -> bytes:
        return HKDF(
            algorithm=hashes.SHA256(),
            length=_KEY_BYTES,
            salt=None,
            info=f"forze.det|{_tenant_key(tenant)}|{field}".encode("utf-8"),
        ).derive(root)

    def _key(self, tenant: TenantIdentity | None, field: str) -> bytes:
        cache_key = (_tenant_key(tenant), field)
        cached = self._keys.get(cache_key)

        if cached is None:
            cached = self._keys[cache_key] = self._derive(tenant, field, self.root)

        return cached

    def _previous_key(self, tenant: TenantIdentity | None, field: str) -> bytes | None:
        if self.previous_root is None:
            return None

        cache_key = (_tenant_key(tenant), field)
        cached = self._prev_keys.get(cache_key)

        if cached is None:
            cached = self._prev_keys[cache_key] = self._derive(
                tenant, field, self.previous_root
            )

        return cached

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

        try:
            return AESSIV(self._key(tenant, field)).decrypt(ciphertext, [])

        except InvalidTag:
            pass

        previous = self._previous_key(tenant, field)

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
