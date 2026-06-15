"""Dependency module for Vault client and secrets adapter."""

from typing import Any, final

import attrs

from forze.application.contracts.crypto import KeyManagementDepKey, KeyManagementPort
from forze.application.contracts.deps import DepKey
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

    key_management: KeyManagementPort | None = attrs.field(default=None)
    """Optional envelope key manager (e.g. :class:`~forze_vault.adapters.VaultTransitKeyManagement`).
    Registered under ``KeyManagementDepKey`` only when set, so KV-only deployments
    need not enable the Transit engine."""

    # ....................... #

    def __call__(self) -> Deps:
        adapter = (
            self.secrets
            if self.secrets is not None
            else VaultKvSecrets(client=self.client)
        )

        deps: dict[DepKey[Any], Any] = {
            VaultClientDepKey: self.client,
            SecretsDepKey: adapter,
        }

        if self.key_management is not None:
            deps[KeyManagementDepKey] = self.key_management

        return Deps.plain(deps)
