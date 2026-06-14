""":class:`~forze.application.contracts.crypto.KeyManagementPort` adapter for Vault Transit.

Implements envelope key management on the Vault Transit secrets engine: the
key-encryption key (the named Transit key) never leaves Vault. ``generate_data_key``
maps to Transit ``datakey/plaintext`` and ``unwrap_data_key`` to ``decrypt``.

:class:`~forze.application.contracts.crypto.KeyRef.key_id` is the Transit key name
(the mount is configured on the client). Wire it with a keyring, e.g.::

    Keyring(
        kms=VaultTransitKeyManagement(client=vault_client),
        aead=AesGcmAead(),
        directory=TenantTemplateKeyDirectory(template="tenant-{tenant_id}", default_key_id="app"),
    )
"""

import re
from typing import final

import attrs

from forze.application.contracts.crypto import DataKey, KeyRef

from ..kernel.client import VaultClientPort

# ----------------------- #

_VERSION = re.compile(r"^vault:(v\d+):")
"""Matches the key-version token Vault prefixes onto Transit ciphertext."""


def _parse_version(ciphertext: str) -> str | None:
    match = _VERSION.match(ciphertext)
    return match.group(1) if match is not None else None


# ....................... #


@final
@attrs.define(slots=True)
class VaultTransitKeyManagement:
    """Envelope key management backed by the Vault Transit engine."""

    client: VaultClientPort
    """Vault client (carries the Transit mount in its config)."""

    # ....................... #

    async def generate_data_key(self, key_ref: KeyRef) -> DataKey:
        plaintext, ciphertext = await self.client.transit_generate_data_key(
            key_ref.key_id,
        )

        return DataKey(
            plaintext=plaintext,
            wrapped=ciphertext.encode("ascii"),
            key_id=key_ref.key_id,
            key_version=_parse_version(ciphertext),
        )

    # ....................... #

    async def unwrap_data_key(self, *, wrapped: bytes, key_ref: KeyRef) -> bytes:
        return await self.client.transit_decrypt(
            key_ref.key_id,
            wrapped.decode("ascii"),
        )
