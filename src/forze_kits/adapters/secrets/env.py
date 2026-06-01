"""Environment-variable backend for :class:`~forze.application.contracts.secrets.SecretsPort`."""

import os
from typing import final

import attrs

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class EnvSecrets(SecretsPort):
    """Resolve secrets from process environment variables.

    :attr:`~forze.application.contracts.secrets.SecretRef.path` is the env var name.
    """

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
