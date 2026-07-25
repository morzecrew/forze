""":class:`~forze.application.contracts.secrets.SecretsPort` adapter for Vault KV v2."""

from typing import final

import attrs
import orjson

from forze.application.contracts.secrets import (
    SecretRef,
    SecretsCapabilities,
    SecretValue,
    SecretVersion,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from ..kernel.client import VaultClientPort

# ----------------------- #

_VAULT_SECRETS_CAPABILITIES = SecretsCapabilities(
    versioned_reads=True,
    native_versions=True,
    writes=True,
)
"""KV v2 assigns integer version tokens and accepts control-plane writes; there is
no native watch endpoint (poll over ``current_version`` instead)."""


def _encode_kv_payload(data: JsonDict) -> str:
    if set(data.keys()) == {"value"} and isinstance(data["value"], str):
        return data["value"]

    return orjson.dumps(data).decode()


# ....................... #


@final
@attrs.define(slots=True)
class VaultKvSecrets:
    """Resolve secrets via :class:`~forze_vault.kernel.client.VaultClient`.

    :attr:`~forze.application.contracts.secrets.SecretRef.path` is the logical KV path
    (without mount prefix; mount is configured on the client).
    """

    client: VaultClientPort
    """Vault client."""

    # ....................... #

    @property
    def secrets_capabilities(self) -> SecretsCapabilities:
        return _VAULT_SECRETS_CAPABILITIES

    # ....................... #

    async def resolve_str(self, ref: SecretRef) -> str:
        data = await self.client.read_kv_data(ref.path)

        return _encode_kv_payload(data)

    # ....................... #

    async def exists(self, ref: SecretRef) -> bool:
        return await self.client.kv_exists(ref.path)

    # ....................... #

    async def resolve_versioned(self, ref: SecretRef) -> SecretValue:
        # One KV response carries both payload and version — value and version can
        # never be torn against each other.
        data, version = await self.client.read_kv_data_versioned(ref.path)

        return SecretValue(text=_encode_kv_payload(data), version=SecretVersion(str(version)))

    # ....................... #

    async def current_version(self, ref: SecretRef) -> SecretVersion:
        metadata = await self.client.read_kv_metadata(ref.path)
        version = metadata.get("current_version")

        if not isinstance(version, int):
            raise exc.infrastructure(
                f"Vault metadata at {ref.path!r} carries no current_version",
            )

        return SecretVersion(str(version))

    # ....................... #

    async def put(self, ref: SecretRef, value: str) -> SecretVersion:
        # The single-value shape round-trips through _encode_kv_payload, so a
        # rotator's put is read back verbatim by resolve_str.
        version = await self.client.write_kv_data(ref.path, {"value": value})

        return SecretVersion(str(version))
