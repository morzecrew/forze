"""In-memory mapping backend for :class:`~forze.application.contracts.secrets.SecretsPort`."""

from typing import Mapping, final

import attrs

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MappingSecrets(SecretsPort):
    """Resolve secrets from a static ``path -> value`` mapping.

    :attr:`~forze.application.contracts.secrets.SecretRef.path` is the dict key.
    """

    _data: Mapping[str, str] = attrs.field(factory=dict[str, str], alias="data")
    """Mapping of paths to secret values."""

    # ....................... #

    async def resolve_str(self, ref: SecretRef) -> str:
        try:
            return self._data[ref.path]

        except KeyError as e:
            raise exc.not_found(
                f"No secret for {ref.path!r}",
                details={"ref": ref.path},
            ) from e

    # ....................... #

    async def exists(self, ref: SecretRef) -> bool:
        return ref.path in self._data
