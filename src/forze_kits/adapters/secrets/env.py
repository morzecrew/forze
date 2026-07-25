"""Environment-variable backend for :class:`~forze.application.contracts.secrets.SecretsPort`."""

import os
from typing import final

import attrs

from forze.application.contracts.secrets import (
    SecretRef,
    SecretsCapabilities,
    SecretsPort,
    SecretValue,
    SecretVersion,
    content_secret_version,
)
from forze.base.exceptions import exc

# ----------------------- #

_ENV_SECRETS_CAPABILITIES = SecretsCapabilities(versioned_reads=True)
"""Content-hash pseudo-versions only; process env refuses writes by design."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class EnvSecrets(SecretsPort):
    """Resolve secrets from process environment variables.

    :attr:`~forze.application.contracts.secrets.SecretRef.path` is the env var name.
    """

    @property
    def secrets_capabilities(self) -> SecretsCapabilities:
        return _ENV_SECRETS_CAPABILITIES

    # ....................... #

    async def resolve_str(self, ref: SecretRef) -> str:
        value = os.environ.get(ref.path)

        if value is None:
            raise exc.not_found(
                f"No secret for {ref.path!r}",
                details={"ref": ref.path},
            )

        return value

    # ....................... #

    async def exists(self, ref: SecretRef) -> bool:
        return ref.path in os.environ

    # ....................... #

    async def resolve_versioned(self, ref: SecretRef) -> SecretValue:
        text = await self.resolve_str(ref)

        return SecretValue(text=text, version=content_secret_version(text))

    # ....................... #

    async def current_version(self, ref: SecretRef) -> SecretVersion:
        return content_secret_version(await self.resolve_str(ref))
