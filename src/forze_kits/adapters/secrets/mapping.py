"""In-memory mapping backend for :class:`~forze.application.contracts.secrets.SecretsPort`."""

from collections.abc import Mapping, MutableMapping
from typing import final

import attrs

from forze.application.contracts.secrets import (
    SecretRef,
    SecretsAdminPort,
    SecretsCapabilities,
    SecretsPort,
    SecretValue,
    SecretVersion,
    VersionedSecretsPort,
    content_secret_version,
    validate_secret_writes_supported,
)
from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MappingSecrets(SecretsPort, VersionedSecretsPort, SecretsAdminPort):
    """Resolve secrets from a ``path -> value`` mapping.

    :attr:`~forze.application.contracts.secrets.SecretRef.path` is the dict key.
    Versioned reads derive content-hash pseudo-versions; :meth:`put` is honored when
    the backing mapping is mutable (declared through ``secrets_capabilities``).
    """

    _data: Mapping[str, str] = attrs.field(factory=dict[str, str], alias="data")
    """Mapping of paths to secret values."""

    # ....................... #

    @property
    def secrets_capabilities(self) -> SecretsCapabilities:
        return SecretsCapabilities(
            versioned_reads=True,
            writes=isinstance(self._data, MutableMapping),
        )

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

    # ....................... #

    async def resolve_versioned(self, ref: SecretRef) -> SecretValue:
        text = await self.resolve_str(ref)

        return SecretValue(text=text, version=content_secret_version(text))

    # ....................... #

    async def current_version(self, ref: SecretRef) -> SecretVersion:
        return content_secret_version(await self.resolve_str(ref))

    # ....................... #

    async def put(self, ref: SecretRef, value: str) -> SecretVersion:
        validate_secret_writes_supported(self.secrets_capabilities, backend=type(self).__name__)

        data = self._data

        if not isinstance(data, MutableMapping):  # pragma: no cover - guarded above
            raise exc.internal("Mapping secrets store is not mutable")

        data[ref.path] = value

        return content_secret_version(value)
