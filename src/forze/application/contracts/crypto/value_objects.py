"""Crypto contract value objects: key references and data keys."""

from typing import final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class KeyRef:
    """Logical reference to a key-encryption key (CMK) in a key manager.

    Backends interpret :attr:`key_id` per their own rules (a KMS key ARN, a Vault
    transit key name, an alias, …). A deployment that mints one key per tenant
    resolves the tenant to a distinct :class:`KeyRef`; a single-key deployment
    uses the same reference for everyone (the degenerate, single-tenant case).
    """

    key_id: str
    """Opaque key-encryption-key identifier."""

    version: str | None = None
    """Optional key version. ``None`` asks the backend for its current version on
    encrypt; on decrypt it is taken from the stored envelope."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DataKey:
    """A freshly generated data-encryption key in both plaintext and wrapped form.

    The plaintext key seals the payload locally and is then discarded; the
    wrapped key is stored in the envelope and can only be unwrapped by the key
    manager. This is the heart of envelope encryption — the key-encryption key
    never leaves the backend.
    """

    plaintext: bytes = attrs.field(repr=False)
    """Raw data-encryption key. Never logged or persisted (``repr`` suppressed)."""

    wrapped: bytes
    """Data-encryption key encrypted under the key-encryption key, safe to store."""

    key_id: str
    """Identifier of the key-encryption key that wrapped :attr:`plaintext`."""

    key_version: str | None = None
    """Version of the key-encryption key, when the backend exposes one."""
