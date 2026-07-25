"""Dependency module for registering a :class:`~forze.application.contracts.secrets.SecretsPort`."""

from typing import Any, final

import attrs

from forze.application.contracts.deps import DepKey, Deps, DepsModule
from forze.application.contracts.secrets import (
    SecretsAdminDepKey,
    SecretsAdminPort,
    SecretsDepKey,
    SecretsPort,
)

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SecretsDepsModule(DepsModule):
    """Register a pre-constructed secrets backend under :data:`~forze.application.contracts.secrets.SecretsDepKey`."""

    secrets: SecretsPort
    """Secrets backend (mapping, env, directory, Vault adapter, etc.)."""

    secrets_admin: SecretsAdminPort | None = attrs.field(default=None)
    """Optional control-plane write surface (rotator-facing). Registered under
    ``SecretsAdminDepKey`` only when set, so the data path never acquires write
    access by accident."""

    # ....................... #

    def __call__(self) -> Deps:
        deps: dict[DepKey[Any], Any] = {SecretsDepKey: self.secrets}

        if self.secrets_admin is not None:
            deps[SecretsAdminDepKey] = self.secrets_admin

        return Deps.plain(deps)
