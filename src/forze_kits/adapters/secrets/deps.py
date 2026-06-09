"""Dependency module for registering a :class:`~forze.application.contracts.secrets.SecretsPort`."""

from typing import final

import attrs

from forze.application.contracts.secrets import SecretsDepKey, SecretsPort
from forze.application.execution import Deps, DepsModule

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SecretsDepsModule(DepsModule):
    """Register a pre-constructed secrets backend under :data:`~forze.application.contracts.secrets.SecretsDepKey`."""

    secrets: SecretsPort
    """Secrets backend (mapping, env, directory, Vault adapter, etc.)."""

    # ....................... #

    def __call__(self) -> Deps:
        return Deps.plain({SecretsDepKey: self.secrets})
