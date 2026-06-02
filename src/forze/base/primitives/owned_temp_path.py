"""Owned temporary filesystem paths for credential materialization."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import attrs

# ----------------------- #


@attrs.define(slots=True)
class OwnedTempPath:
    """A filesystem path that may be deleted when :meth:`release` is called.

    Use :meth:`materialize_text` when an integration receives inline secret text
    but the underlying SDK requires a file path. Use :meth:`unowned` for paths
    managed outside Forze.
    """

    path: str | None = None
    owned: bool = False

    @classmethod
    def empty(cls) -> OwnedTempPath:
        """Return a placeholder with no path."""

        return cls()

    @classmethod
    def unowned(cls, path: str | None) -> OwnedTempPath:
        """Wrap an existing path that Forze must not delete."""

        return cls(path=path, owned=False)

    @classmethod
    def materialize_text(
        cls,
        content: str,
        *,
        prefix: str,
        suffix: str = ".json",
    ) -> OwnedTempPath:
        """Write *content* to a new temp file and mark it owned by Forze."""

        fd, path = tempfile.mkstemp(prefix=prefix, suffix=suffix)

        try:
            os.write(fd, content.encode("utf-8"))

        finally:
            os.close(fd)

        return cls(path=path, owned=True)

    def release(self) -> None:
        """Remove the path when owned; reset to :meth:`empty` afterwards."""

        if not self.owned or self.path is None:
            self.path = None
            self.owned = False

            return

        try:
            Path(self.path).unlink(missing_ok=True)

        except OSError:
            pass

        self.path = None
        self.owned = False
