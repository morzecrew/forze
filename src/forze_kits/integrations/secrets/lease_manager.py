"""Lease manager — keeps leased credentials alive: renew early, reissue before
expiry, escalate-never-abandon.

One supervised loop per wired role:

- renew each lease at **~⅔ elapsed TTL with jitter** (herd avoidance across a fleet);
- when renewal is refused, the grant shrinks below the reissue floor, or the lease
  is non-renewable, **reissue** — the new credential flows through the same delivery
  callback as any rotation (evict/rebuild or connect-time pickup), then the old
  lease is revoked after a drain grace;
- on renewal failure: retry with backoff **indefinitely** while escalating log
  severity as expiry approaches — abandoning means certain credential death at TTL;
- on clean shutdown: revoke held leases (leaving orphaned principals to the store's
  TTL is correct but noisy).

Revocation is hard-edged (the store drops the principal, killing established
connections), which is why reissue-then-drain always precedes revoke.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import timedelta
from typing import final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.secrets import (
    DynamicSecretsPort,
    LeasedSecret,
    SecretRef,
    secrets_capabilities_of,
    validate_dynamic_credentials_supported,
)
from forze.application.execution.background import (
    DEFAULT_STOP_GRACE_SECONDS,
    BackgroundLoopControl,
    run_supervised,
)
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, monotonic
from forze.base.primitives.entropy_source import current_entropy_source
from forze_kits.integrations._logger import logger

# ----------------------- #

RENEW_FRACTION = 2.0 / 3.0
"""Renew when this fraction of the granted TTL has elapsed."""


async def _wait_or_stop(stop: asyncio.Event, delay: float) -> bool:
    """Interruptible sleep; ``True`` when a stop was requested."""

    with suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=max(0.0, delay))

    return stop.is_set()


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class SecretsLeaseManager:
    """Holds one live lease per wired role for the lifetime of the container."""

    dynamic: DynamicSecretsPort
    """Lease-issuing backend. Fails closed at construction when the backend
    refuses dynamic credentials."""

    roles: tuple[SecretRef, ...] = attrs.field(converter=tuple)
    """Role refs to hold leases for."""

    on_credential: Callable[[SecretRef, LeasedSecret], Awaitable[None]]
    """Delivery callback, invoked at issuance and every reissuance — the same
    hot-reload path as a rotation (rebuild the pool, or let connect-time
    re-resolution pick the credential up). Failures are logged and retried at the
    next lifecycle event; they never kill the loop."""

    reissue_below: timedelta = timedelta(seconds=30)
    """Reissue when a renewal grant falls below this (``max_ttl`` is near)."""

    drain_grace: timedelta = timedelta(seconds=30)
    """How long the old credential drains after a reissue before its lease is
    revoked (revocation kills established connections)."""

    retry_backoff: timedelta = timedelta(seconds=5)
    """Base backoff between renewal retries after a failure."""

    restart_backoff: timedelta = timedelta(seconds=5)
    """Backoff between supervised restarts of a crashed role loop."""

    backend: str = "secrets_lease"
    """Backend label for capability failures and logs."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.roles:
            raise exc.configuration("Lease manager needs at least one role")

        validate_dynamic_credentials_supported(
            secrets_capabilities_of(self.dynamic), backend=self.backend
        )

    # ....................... #

    async def _deliver(self, ref: SecretRef, leased: LeasedSecret) -> None:
        try:
            await self.on_credential(ref, leased)

        except Exception:
            logger.critical(
                "Lease delivery callback failed for role %s; holding the lease and continuing",
                ref.path,
                exc_info=True,
            )

    # ....................... #

    async def _reissue(
        self, ref: SecretRef, old: LeasedSecret, stop: asyncio.Event
    ) -> LeasedSecret:
        fresh = await self.dynamic.issue(ref)
        await self._deliver(ref, fresh)

        # Reissue-then-drain: the fleet moves to the fresh credential before the
        # old principal is dropped (revocation kills its live connections). The
        # drain is capped by the fresh lease's own TTL — sitting out a long grace
        # would let the credential we just issued expire unrenewed.
        drain = min(self.drain_grace.total_seconds(), fresh.ttl.total_seconds() / 3.0)
        await _wait_or_stop(stop, drain)

        try:
            await self.dynamic.revoke(old.lease_id)

        except Exception:
            logger.warning(
                "Failed to revoke drained lease %s for role %s; the store's TTL will collect it",
                old.lease_id,
                ref.path,
                exc_info=True,
            )

        return fresh

    # ....................... #

    async def _run_role(self, ref: SecretRef, stop: asyncio.Event) -> None:
        leased = await self.dynamic.issue(ref)
        await self._deliver(ref, leased)
        deadline = monotonic() + leased.ttl.total_seconds()

        while True:
            jitter = current_entropy_source().as_random().uniform(0.9, 1.1)
            renew_at = deadline - leased.ttl.total_seconds() * (1.0 - RENEW_FRACTION) * jitter

            if await _wait_or_stop(stop, renew_at - monotonic()):
                break

            if not leased.renewable:
                leased = await self._reissue(ref, leased, stop)
                deadline = monotonic() + leased.ttl.total_seconds()
                continue

            try:
                granted = await self.dynamic.renew(leased.lease_id, leased.ttl)

            except Exception:
                remaining = deadline - monotonic()

                if remaining <= leased.ttl.total_seconds() * 0.25:
                    logger.critical(
                        "Lease renewal for role %s failing with %.0fs to expiry",
                        ref.path,
                        remaining,
                        exc_info=True,
                    )

                else:
                    logger.warning(
                        "Lease renewal for role %s failed; retrying (%.0fs to expiry)",
                        ref.path,
                        remaining,
                        exc_info=True,
                    )

                backoff = self.retry_backoff.total_seconds() * jitter

                if await _wait_or_stop(stop, min(backoff, max(remaining / 4.0, 1.0))):
                    break

                if monotonic() >= deadline:
                    # The lease is dead; only a fresh issuance recovers.
                    leased = await self._reissue(ref, leased, stop)
                    deadline = monotonic() + leased.ttl.total_seconds()

                continue

            if granted < leased.ttl and granted < self.reissue_below:
                # The store granted less than asked AND the grant is near the floor:
                # max_ttl is close — reissue early. A short-but-full grant is just a
                # short lease cadence, not a cap.
                leased = await self._reissue(ref, leased, stop)
                deadline = monotonic() + leased.ttl.total_seconds()
                continue

            leased = attrs.evolve(leased, ttl=granted)
            deadline = monotonic() + granted.total_seconds()

        # Clean shutdown: revoke rather than orphan.
        try:
            await self.dynamic.revoke(leased.lease_id)

        except Exception:
            logger.warning(
                "Failed to revoke lease %s for role %s at shutdown",
                leased.lease_id,
                ref.path,
                exc_info=True,
            )

    # ....................... #

    def lifecycle_step(self, *, step_id: StrKey = "secrets_lease_manager") -> LifecycleStep:
        """Run one supervised lease loop per role as a lifecycle step."""

        startup = _LeaseManagerStartup(manager=self)

        return LifecycleStep(
            id=step_id,
            startup=startup,
            shutdown=_LeaseManagerShutdown(startup=startup),
            requires_long_running=True,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _LeaseManagerStartup(LifecycleHook):
    """Start one supervised lease loop per role."""

    manager: SecretsLeaseManager

    # ....................... #

    controls: list[BackgroundLoopControl] = attrs.field(
        factory=list,
        init=False,
        repr=False,
    )

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        if any(control.running for control in self.controls):
            return

        self.controls.clear()

        for ref in self.manager.roles:
            control = BackgroundLoopControl(name=f"secrets_lease:{ref.path}")
            stop = control.arm()

            async def _run_once(ref: SecretRef = ref, stop: asyncio.Event = stop) -> None:
                await self.manager._run_role(ref, stop)  # pyright: ignore[reportPrivateUsage]

            control.task = asyncio.create_task(
                run_supervised(
                    _run_once,
                    stop=stop,
                    name=control.loop_name,
                    restart_backoff=self.manager.restart_backoff,
                ),
                name=control.loop_name,
            )
            self.controls.append(control)
            ctx.drainables.register(control)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _LeaseManagerShutdown(LifecycleHook):
    """Stop every lease loop; normally a no-op after the runtime drains them."""

    startup: _LeaseManagerStartup

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        clock = asyncio.get_running_loop()

        for control in self.startup.controls:
            await control.stop(deadline=clock.time() + DEFAULT_STOP_GRACE_SECONDS)
