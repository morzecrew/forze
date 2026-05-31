""":class:`~forze.application.contracts.secrets.SecretsPort` adapter for Vault KV v2."""

from typing import final

import attrs
import orjson

from forze.application.contracts.secrets import SecretRef
from forze.base.primitives import JsonDict

from ..kernel.client import VaultClientPort

# ----------------------- #


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

    async def resolve_str(self, ref: SecretRef) -> str:
        data = await self.client.read_kv_data(ref.path)

        return _encode_kv_payload(data)

    # ....................... #

    async def exists(self, ref: SecretRef) -> bool:
        return await self.client.kv_exists(ref.path)
