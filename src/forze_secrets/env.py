"""Environment-variable backend for :class:`~forze.application.contracts.secrets.SecretsPort`."""

import os
from typing import final

import attrs

from forze.application.contracts.secrets import SecretRef
from forze.base.errors import SecretNotFoundError

# ----------------------- #


@final
@attrs.define(slots=True)
class EnvSecrets:
    """Resolve secrets from process environment variables.

    :attr:`~forze.application.contracts.secrets.SecretRef.path` is the env var name.
    """

    async def resolve_str(self, ref: SecretRef) -> str:
        value = os.environ.get(ref.path)

        if value is None:
            raise SecretNotFoundError(
                f"No secret for {ref.path!r}",
                details={"ref": ref.path},
            )

        return value

    # ....................... #

    async def exists(self, ref: SecretRef) -> bool:
        return ref.path in os.environ
