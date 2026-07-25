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

**Native-event upgrade** (``forze[watchfiles]`` extra): an optional second
lifecycle step that watches the root directory with OS-native events and triggers
the *same* stat+hash tick immediately on activity. Events are only ever an
accelerator — their paths are never trusted (that would re-import every inotify
trap this module exists to avoid), a spurious or duplicate event costs one cheap
tick, and the poll step stays wired as the floor; native events let you *raise*
its interval, not remove it.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Collection
from datetime import timedelta
from pathlib import Path
from typing import final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.secrets import (
    SecretChanged,
    SecretRef,
    SecretVersion,
    content_secret_version,
)
from forze.application.execution.background import (
    DEFAULT_STOP_GRACE_SECONDS,
    BackgroundLoopControl,
    periodic_lifecycle_step,
    run_supervised,
)
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import StrKey
from forze_kits.integrations._logger import logger

from ._fanout import ChangeFanout
from .watcher import DEFAULT_SECRETS_WATCH_INTERVAL

try:  # the native-event upgrade is optional by design (decision: poll-first)
    from watchfiles import awatch as _awatch  # pyright: ignore[reportUnknownVariableType]

except ImportError:  # pragma: no cover - exercised via the fail-closed test
    _awatch = None  # type: ignore[assignment]

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

    # ....................... #

    def native_events_lifecycle_step(
        self,
        *,
        debounce: timedelta = timedelta(milliseconds=1600),
        step_id: StrKey = "secrets_file_source_native",
    ) -> LifecycleStep:
        """OS-native event accelerator — an *additional* step beside the poll step.

        Watches :attr:`root` recursively and runs :meth:`tick` immediately on
        activity. Event paths are never trusted: kubelet's ``..data`` swap makes
        per-file event interpretation a trap, so every event just triggers the same
        stat-gated diff (a duplicate or irrelevant event costs one ``stat`` per
        ref). Keep :meth:`lifecycle_step` wired too — native events accelerate, the
        poll floor guarantees; wiring this step lets you raise the poll interval,
        not remove it.

        Fails closed at wiring when ``watchfiles`` is not installed
        (``forze[watchfiles]`` extra).

        :param debounce: Event coalescing window before a tick fires (native
            bursts — a kubelet swap touches several entries — become one tick).
        """

        if _awatch is None:
            raise exc.configuration(
                "Native file events need the 'watchfiles' package; install the "
                "forze[watchfiles] extra, or rely on the poll step alone.",
            )

        if debounce.total_seconds() <= 0:
            raise exc.configuration("Debounce must be positive")

        startup = _NativeEventsStartup(source=self, debounce=debounce)

        return LifecycleStep(
            id=step_id,
            startup=startup,
            shutdown=_NativeEventsShutdown(startup=startup),
            requires_long_running=True,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _NativeEventsStartup(LifecycleHook):
    """Run a supervised native-event loop that ticks the source on activity."""

    source: DirectorySecretsChangeSource
    debounce: timedelta

    control: BackgroundLoopControl = attrs.field(
        default=attrs.Factory(
            lambda: BackgroundLoopControl(name="secrets_file_source_native"),
        ),
        init=False,
    )

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        if self.control.running:
            return

        stop = self.control.arm()
        root = self.source.root
        debounce_ms = int(self.debounce.total_seconds() * 1000)

        async def _watch_once() -> None:
            if _awatch is None:  # pragma: no cover - wiring already failed closed
                raise exc.configuration("watchfiles is not installed")

            # stop_event ends the generator at its next step, so a stop request
            # never waits out a quiet directory; rust_timeout bounds how long a
            # quiet directory can defer that check (the default 5s equals the stop
            # grace and would push every shutdown into the cancel backstop). The
            # events themselves are discarded — the tick re-derives truth from
            # stat+hash.
            async for _events in _awatch(
                root, debounce=debounce_ms, stop_event=stop, rust_timeout=1_000
            ):
                await self.source.tick()

                if stop.is_set():
                    return

        self.control.task = asyncio.create_task(
            run_supervised(
                _watch_once,
                stop=stop,
                name=self.control.loop_name,
            ),
            name=self.control.loop_name,
        )
        ctx.drainables.register(self.control)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _NativeEventsShutdown(LifecycleHook):
    """Stop the native-event loop; normally a no-op after the runtime drains it."""

    startup: _NativeEventsStartup

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        clock = asyncio.get_running_loop()
        await self.startup.control.stop(deadline=clock.time() + DEFAULT_STOP_GRACE_SECONDS)
