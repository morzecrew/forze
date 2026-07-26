"""Filesystem directory backend for :class:`~forze.application.contracts.secrets.SecretsPort`."""

from pathlib import Path
from typing import final

import attrs

from forze.application.contracts.secrets import (
    SecretRef,
    SecretsCapabilities,
    SecretsPort,
    SecretValue,
    SecretVersion,
    VersionedSecretsPort,
    content_secret_version,
)
from forze.base.exceptions import exc

# ----------------------- #

_DIRECTORY_SECRETS_CAPABILITIES = SecretsCapabilities(versioned_reads=True, change_feed=True)
"""Content-hash pseudo-versions plus a native file change source
(``forze_kits.integrations.secrets.DirectorySecretsChangeSource``).
Writes are refused by design: mounted secret files are rotated through the
platform (e.g. the Kubernetes API), never by the app."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DirectorySecrets(SecretsPort, VersionedSecretsPort):
    """Resolve secrets as UTF-8 text files under a configured root directory.

    :attr:`~forze.application.contracts.secrets.SecretRef.path` is a relative path
    under :attr:`root` (POSIX-style, no ``..`` traversal).
    """

    root: Path
    """Root directory for secrets."""

    # ....................... #

    @property
    def secrets_capabilities(self) -> SecretsCapabilities:
        return _DIRECTORY_SECRETS_CAPABILITIES

    # ....................... #

    def _resolve_path(self, ref: SecretRef) -> Path:
        root = self.root.resolve()
        candidate = (root / ref.path).resolve()

        if not candidate.is_relative_to(root):
            raise exc.internal(
                f"Secret path {ref.path!r} escapes configured root",
                code="secret_path_invalid",
                details={"ref": ref.path},
            )

        return candidate

    # ....................... #

    async def resolve_str(self, ref: SecretRef) -> str:
        path = self._resolve_path(ref)

        if not path.is_file():
            raise exc.not_found(
                f"No secret for {ref.path!r}",
                details={"ref": ref.path},
            )

        return path.read_text(encoding="utf-8")

    # ....................... #

    async def exists(self, ref: SecretRef) -> bool:
        try:
            path = self._resolve_path(ref)

        except exc:
            return False

        return path.is_file()

    # ....................... #

    async def resolve_versioned(self, ref: SecretRef) -> SecretValue:
        # Read once and hash — text and version can never be torn against each other.
        text = await self.resolve_str(ref)

        return SecretValue(text=text, version=content_secret_version(text))

    # ....................... #

    async def current_version(self, ref: SecretRef) -> SecretVersion:
        # A hash-based backend necessarily reads the value to hash it; files are local.
        return content_secret_version(await self.resolve_str(ref))
