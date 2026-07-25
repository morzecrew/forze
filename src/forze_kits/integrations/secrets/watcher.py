"""Poll watcher — the universal change source over any versioned secrets backend.

Every shipped backend implements versioned reads (content-hash pseudo-versions where
the store has no native concept), so polling ``current_version`` works over files,
env, mappings, mock, and Vault alike — no store-native watch required. The default
30-second tick is deliberate: kubelet's own secret-sync cadence is minute-granular,
and against Vault a tick is one metadata read per ref.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Collection
from datetime import timedelta
from typing import final

import attrs

from forze.application.contracts.execution import LifecycleStep
from forze.application.contracts.secrets import (
    SecretChanged,
    SecretRef,
    SecretVersion,
    VersionedSecretsPort,
    secrets_capabilities_of,
    validate_versioned_reads_supported,
)
from forze.application.execution.background import periodic_lifecycle_step
from forze.base.exceptions import exc
from forze.base.primitives import StrKey
from forze_kits.integrations._logger import logger

from ._fanout import ChangeFanout

# ----------------------- #

DEFAULT_SECRETS_WATCH_INTERVAL = timedelta(seconds=30)
"""Default poll cadence — matched to kubelet's minute-granular sync, one Vault
metadata read per ref per tick."""


@final
@attrs.define(slots=True, kw_only=True)
class SecretsPollWatcher:
    """A change source that diffs ``current_version`` snapshots on a periodic tick.

    Snapshots hold **versions only** — never values. The first tick primes the
    snapshot without emitting, so a fresh container does not "detect" every secret
    as changed and evict every pool at boot. A ref that appears after priming (a
    created secret) does emit. One failing ref is logged and skipped; the rest of
    the tick proceeds.
    """

    secrets: VersionedSecretsPort
    """Versioned backend to poll. Fails closed at construction when the backend
    refuses versioned reads."""

    refs: tuple[SecretRef, ...] = attrs.field(converter=tuple)
    """The refs this watcher covers."""

    backend: str = "secrets"
    """Backend label for capability failures and logs."""

    # ....................... #

    _snapshot: dict[str, SecretVersion] = attrs.field(factory=dict, init=False, repr=False)
    _primed: bool = attrs.field(default=False, init=False)
    _fanout: ChangeFanout = attrs.field(factory=ChangeFanout, init=False, repr=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.refs:
            raise exc.configuration("Secrets poll watcher needs at least one ref")

        validate_versioned_reads_supported(
            secrets_capabilities_of(self.secrets), backend=self.backend
        )

    # ....................... #

    async def tick(self) -> None:
        """Diff one round of ``current_version`` reads against the snapshot."""

        for ref in self.refs:
            try:
                version = await self.secrets.current_version(ref)

            except exc:
                # Missing or unreadable ref: keep the old snapshot entry so a
                # re-appearance with the same value stays a non-event.
                logger.warning(
                    "Secrets watcher could not read a version for %s; skipping this tick",
                    ref.path,
                )
                continue

            previous = self._snapshot.get(ref.path)
            self._snapshot[ref.path] = version

            if self._primed and previous != version:
                self._fanout.emit(SecretChanged(ref=ref, version=version))

        self._primed = True

    # ....................... #

    def subscribe(
        self,
        refs: Collection[SecretRef] | None = None,
    ) -> AsyncIterator[SecretChanged]:
        """Yield changes for *refs* (``None`` = every ref this watcher covers)."""

        return self._fanout.stream(refs)

    # ....................... #

    def lifecycle_step(
        self,
        *,
        interval: timedelta = DEFAULT_SECRETS_WATCH_INTERVAL,
        step_id: StrKey = "secrets_poll_watcher",
    ) -> LifecycleStep:
        """Run the watcher as a supervised periodic lifecycle step."""

        return periodic_lifecycle_step(
            tick=self.tick,
            interval=interval,
            name="secrets_poll_watcher",
            step_id=step_id,
        )
