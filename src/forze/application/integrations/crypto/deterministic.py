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
    """

    root: bytes = attrs.field(repr=False)
    """Stable root secret (>= 32 bytes). Long-lived; rotating it re-indexes data."""

    _keys: dict[tuple[str, str], bytes] = attrs.field(factory=dict, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if len(self.root) < _KEY_BYTES:
            raise exc.configuration(
                f"Deterministic cipher root secret must be at least {_KEY_BYTES} bytes",
            )

    # ....................... #

    def _key(self, tenant: TenantIdentity | None, field: str) -> bytes:
        cache_key = (_tenant_key(tenant), field)
        cached = self._keys.get(cache_key)

        if cached is not None:
            return cached

        derived = HKDF(
            algorithm=hashes.SHA256(),
            length=_KEY_BYTES,
            salt=None,
            info=f"forze.det|{cache_key[0]}|{field}".encode("utf-8"),
        ).derive(self.root)
        self._keys[cache_key] = derived
        return derived

    # ....................... #

    def encrypt(
        self,
        *,
        tenant: TenantIdentity | None,
        field: str,
        plaintext: bytes,
    ) -> bytes:
        """Deterministically encrypt *plaintext* for ``(tenant, field)``."""

        return AESSIV(self._key(tenant, field)).encrypt(plaintext, [])

    # ....................... #

    def decrypt(
        self,
        *,
        tenant: TenantIdentity | None,
        field: str,
        ciphertext: bytes,
    ) -> bytes:
        """Decrypt a value produced by :meth:`encrypt`.

        :raises CoreException: ``validation`` when authentication fails (the value
            was not produced by this cipher for this ``(tenant, field)``).
        """

        try:
            return AESSIV(self._key(tenant, field)).decrypt(ciphertext, [])

        except InvalidTag as error:
            raise exc.validation(
                "Deterministic decrypt failed (wrong key, tenant, or field)",
                code="core.crypto.deterministic_auth_failed",
            ) from error
