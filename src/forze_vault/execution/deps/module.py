"""Dependency module for Vault client and secrets adapter."""

from typing import final

import attrs

from forze.application.contracts.secrets import SecretsDepKey, SecretsPort
from forze.application.execution import Deps, DepsModule

from ...adapters import VaultKvSecrets
from ...kernel.client import VaultClientPort
from .keys import VaultClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class VaultDepsModule(DepsModule):
    """Register Vault client and :class:`~forze_vault.adapters.VaultKvSecrets` under deps keys."""

    client: VaultClientPort
    """Pre-constructed Vault client (initialized via :func:`~forze_vault.execution.vault_lifecycle_step`)."""

    secrets: SecretsPort | None = attrs.field(default=None)
    """Optional secrets adapter; defaults to :class:`~forze_vault.adapters.VaultKvSecrets`."""

    # ....................... #

    def __call__(self) -> Deps:
        adapter = (
            self.secrets
            if self.secrets is not None
            else VaultKvSecrets(client=self.client)
        )

        return Deps.plain(
            {
                VaultClientDepKey: self.client,
                SecretsDepKey: adapter,
            },
        )
