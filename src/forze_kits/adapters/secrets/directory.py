"""Filesystem directory backend for :class:`~forze.application.contracts.secrets.SecretsPort`."""

from pathlib import Path
from typing import final

import attrs

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DirectorySecrets(SecretsPort):
    """Resolve secrets as UTF-8 text files under a configured root directory.

    :attr:`~forze.application.contracts.secrets.SecretRef.path` is a relative path
    under :attr:`root` (POSIX-style, no ``..`` traversal).
    """

    root: Path
    """Root directory for secrets."""

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
