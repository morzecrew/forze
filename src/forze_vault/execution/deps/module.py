"""Dependency module for Vault client and secrets adapter."""

from typing import Any, final

import attrs

from forze.application.contracts.crypto import KeyManagementDepKey, KeyManagementPort
from forze.application.contracts.deps import DepKey, Deps, DepsModule
from forze.application.contracts.secrets import (
    DynamicSecretsPort,
    SecretsAdminDepKey,
    SecretsAdminPort,
    SecretsDepKey,
    SecretsLeaseDepKey,
    SecretsPort,
)

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

    secrets_admin: SecretsAdminPort | None = attrs.field(default=None)
    """Optional control-plane write surface. Defaults to the (write-capable)
    :class:`~forze_vault.adapters.VaultKvSecrets` adapter when :attr:`secrets` is
    defaulted; a custom read-only :attr:`secrets` leaves the admin key unregistered
    unless set explicitly."""

    key_management: KeyManagementPort | None = attrs.field(default=None)
    """Optional envelope key manager (e.g. :class:`~forze_vault.adapters.VaultTransitKeyManagement`).
    Registered under ``KeyManagementDepKey`` only when set, so KV-only deployments
    need not enable the Transit engine."""

    dynamic_secrets: DynamicSecretsPort | None = attrs.field(default=None)
    """Optional leased-credentials port (e.g. :class:`~forze_vault.adapters.VaultDynamicSecrets`).
    Registered under ``SecretsLeaseDepKey`` only when set, so KV-only deployments
    need not enable a database engine."""

    # ....................... #

    def __call__(self) -> Deps:
        adapter = self.secrets if self.secrets is not None else VaultKvSecrets(client=self.client)

        deps: dict[DepKey[Any], Any] = {
            VaultClientDepKey: self.client,
            SecretsDepKey: adapter,
        }

        admin = self.secrets_admin

        if admin is None and isinstance(adapter, VaultKvSecrets):
            admin = adapter

        if admin is not None:
            deps[SecretsAdminDepKey] = admin

        if self.key_management is not None:
            deps[KeyManagementDepKey] = self.key_management

        if self.dynamic_secrets is not None:
            deps[SecretsLeaseDepKey] = self.dynamic_secrets

        return Deps.plain(deps)
