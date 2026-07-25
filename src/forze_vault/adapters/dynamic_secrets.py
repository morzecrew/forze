""":class:`~forze.application.contracts.secrets.DynamicSecretsPort` adapter over
Vault's database secrets engine."""

from datetime import timedelta
from typing import final

import attrs
import orjson

from forze.application.contracts.secrets import (
    LeasedSecret,
    SecretRef,
    SecretsCapabilities,
)
from forze.base.exceptions import exc

from ..kernel.client import VaultClientPort

# ----------------------- #

_VAULT_DYNAMIC_CAPABILITIES = SecretsCapabilities(dynamic_credentials=True)
"""This adapter serves only the lease plane; static reads/writes live on
:class:`~forze_vault.adapters.VaultKvSecrets`."""


@final
@attrs.define(slots=True)
class VaultDynamicSecrets:
    """Mint, renew, and revoke leased database credentials via Vault.

    :attr:`~forze.application.contracts.secrets.SecretRef.path` names the database
    engine *role* (e.g. ``app-readwrite``), not a stored value. Issued credentials
    are serialized as a JSON object (``{"username": ..., "password": ...}``) so they
    validate through the same structured-credentials path as static secrets.
    """

    client: VaultClientPort
    """Vault client."""

    # ....................... #

    @property
    def secrets_capabilities(self) -> SecretsCapabilities:
        return _VAULT_DYNAMIC_CAPABILITIES

    # ....................... #

    async def issue(self, ref: SecretRef) -> LeasedSecret:
        response = await self.client.db_generate_credentials(ref.path)

        lease_id = response.get("lease_id")
        duration = response.get("lease_duration")
        data = response.get("data")

        if (
            not isinstance(lease_id, str)
            or not isinstance(duration, int)
            or not isinstance(data, dict)
        ):
            raise exc.infrastructure(
                f"Vault lease for role {ref.path!r} has unexpected payload shape",
            )

        return LeasedSecret(
            text=orjson.dumps(data).decode(),
            lease_id=lease_id,
            ttl=timedelta(seconds=duration),
            renewable=bool(response.get("renewable", False)),
        )

    # ....................... #

    async def renew(self, lease_id: str, increment: timedelta) -> timedelta:
        granted = await self.client.renew_lease(lease_id, int(increment.total_seconds()))

        return timedelta(seconds=granted)

    # ....................... #

    async def revoke(self, lease_id: str) -> None:
        await self.client.revoke_lease(lease_id)
