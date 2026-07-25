"""Directory change source — poll-based file watching with Kubernetes semantics.

Kubernetes facts this source encodes, which any naive inotify watcher gets wrong:

- kubelet updates a mounted Secret by **atomically swapping the ``..data``
  symlink** — so this source re-stats the *path* every tick (following symlinks),
  never a held inode: the inode you watched is the old version, forever;
- **``subPath`` mounts never update** — a secret mounted via ``subPath`` will never
  produce a change here no matter the watcher; this is a deployment fact the source
  cannot detect at runtime, so it is an ops warning, not a runtime check;
- kubelet's propagation cadence is its sync period (~1 minute by default) — which
  is why polling at 30s is *not* a compromise versus inotify, and why a native-event
  upgrade is an optional nicety behind the same seam, never a requirement.

A stat signature (inode, mtime, size) gates the content read: an unchanged file
costs one ``stat`` per tick; only a swapped or rewritten file is re-read and
re-hashed. Corollary: an in-place same-size rewrite inside the filesystem's
timestamp granularity is invisible to the gate — irrelevant for kubelet (swaps
change the inode) and covered by the ``fingerprint_ttl`` floor everywhere else.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Collection
from datetime import timedelta
from pathlib import Path
from typing import final

import attrs

from forze.application.contracts.execution import LifecycleStep
from forze.application.contracts.secrets import (
    SecretChanged,
    SecretRef,
    SecretVersion,
    content_secret_version,
)
from forze.application.execution.background import periodic_lifecycle_step
from forze.base.exceptions import exc
from forze.base.primitives import StrKey
from forze_kits.integrations._logger import logger

from ._fanout import ChangeFanout
from .watcher import DEFAULT_SECRETS_WATCH_INTERVAL

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class DirectorySecretsChangeSource:
    """Poll-based change source over secret files beneath a root directory.

    Versions are content hashes — identical to what
    :class:`~forze_kits.adapters.secrets.DirectorySecrets` reports for the same
    file, so watcher-observed and directly-resolved versions always agree. The
    first tick primes without emitting; a file that appears later does emit.
    """

    root: Path
    """Root directory (typically the Secret mount point)."""

    refs: tuple[SecretRef, ...] = attrs.field(converter=tuple)
    """Relative file paths under :attr:`root` to watch (no ``..`` traversal)."""

    # ....................... #

    _stat_signatures: dict[str, tuple[int, int, int]] = attrs.field(
        factory=dict, init=False, repr=False
    )
    _versions: dict[str, SecretVersion] = attrs.field(factory=dict, init=False, repr=False)
    _primed: bool = attrs.field(default=False, init=False)
    _fanout: ChangeFanout = attrs.field(factory=ChangeFanout, init=False, repr=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.refs:
            raise exc.configuration("Directory change source needs at least one ref")

        root = self.root.resolve()

        for ref in self.refs:
            if not (root / ref.path).resolve().is_relative_to(root):
                raise exc.configuration(
                    f"Secret path {ref.path!r} escapes configured root",
                )

    # ....................... #

    async def tick(self) -> None:
        """Re-stat every watched path; re-hash and emit only what actually moved."""

        root = self.root.resolve()

        for ref in self.refs:
            path = (root / ref.path).resolve()

            try:
                # Follows symlinks by design: kubelet's ..data swap changes the
                # resolved inode, which is exactly the signal.
                stat = os.stat(path)

            except OSError:
                logger.warning(
                    "Secrets file source could not stat %s; skipping this tick",
                    ref.path,
                )
                continue

            signature = (stat.st_ino, stat.st_mtime_ns, stat.st_size)

            if signature == self._stat_signatures.get(ref.path):
                continue

            try:
                text = path.read_text(encoding="utf-8")

            except OSError:
                # Raced a swap between stat and read — the next tick sees the settled state.
                logger.warning(
                    "Secrets file source could not read %s; skipping this tick",
                    ref.path,
                )
                continue

            self._stat_signatures[ref.path] = signature
            version = content_secret_version(text)
            previous = self._versions.get(ref.path)
            self._versions[ref.path] = version

            if self._primed and previous != version:
                self._fanout.emit(SecretChanged(ref=ref, version=version))

        self._primed = True

    # ....................... #

    def subscribe(
        self,
        refs: Collection[SecretRef] | None = None,
    ) -> AsyncIterator[SecretChanged]:
        """Yield changes for *refs* (``None`` = every ref this source covers)."""

        return self._fanout.stream(refs)

    # ....................... #

    def lifecycle_step(
        self,
        *,
        interval: timedelta = DEFAULT_SECRETS_WATCH_INTERVAL,
        step_id: StrKey = "secrets_file_source",
    ) -> LifecycleStep:
        """Run the file source as a supervised periodic lifecycle step."""

        return periodic_lifecycle_step(
            tick=self.tick,
            interval=interval,
            name="secrets_file_source",
            step_id=step_id,
        )
