"""Hot-reload binder — translates secret-change events into pool evictions.

**Routed clients.** For each registered pool, the binder recomputes every cached
tenant's ref via the client's own ``secret_ref_for_tenant`` and evicts the matches —
reverse mapping without new state, O(cached tenants) per event, and events are rare.
Over-notification is free by design: eviction on an unchanged secret re-resolves,
recomputes an equal fingerprint, and rebuilds nothing.

**Non-routed pools — the doctrine.** A singleton pool should re-resolve its secret
**at connection-establishment time**: a Postgres/MySQL password is checked only at
connect and established connections survive rotation, so a connect-time
``resolve_str`` makes the rotation window race-free with or without a signal. The
registered ``on_change`` callbacks only *accelerate* draining of old connections
(e.g. a soft pool recycle); they are never the correctness mechanism.

**Degradation.** Signals accelerate, the ``fingerprint_ttl`` floor guarantees:
wiring this binder does not remove the TTL — it lets you raise it. A dead source
degrades to "rotation observed within TTL", loudly, never to frozen credentials.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import timedelta
from typing import Any, final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.secrets import (
    SecretChanged,
    SecretRef,
    SecretsChangeSource,
    secret_ref_for_tenant,
)
from forze.application.contracts.tenancy.routed_client_base import RoutedTenantClientBase
from forze.application.execution.background import (
    DEFAULT_STOP_GRACE_SECONDS,
    BackgroundLoopControl,
    run_supervised,
)
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import StrKey
from forze_kits.integrations._logger import logger

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class SecretsHotReloadBinder:
    """Subscribes to change sources and evicts affected pools.

    Sources are interchangeable behind the seam — a poll watcher, a directory
    source, and a pub/sub subscriber all look the same from here. Dispatch is
    idempotent and duplicate-tolerant, so at-least-once unordered delivery needs no
    dedup in front of it.
    """

    sources: tuple[SecretsChangeSource, ...] = attrs.field(converter=tuple)
    """Change sources to consume (each in its own supervised loop)."""

    routed_clients: tuple[RoutedTenantClientBase[Any], ...] = attrs.field(
        default=(), converter=tuple
    )
    """Routed pools whose cached tenants are evicted when their ref changes."""

    on_change: tuple[Callable[[SecretRef], Awaitable[None]], ...] = attrs.field(
        default=(), converter=tuple
    )
    """Opt-in callbacks for non-routed pools (typically a soft pool recycle). See
    the module doctrine: connect-time re-resolution is the correctness mechanism;
    these only accelerate draining."""

    restart_backoff: timedelta = timedelta(seconds=5)
    """Backoff between supervised restarts of a crashed source loop."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.sources:
            raise exc.configuration("Hot-reload binder needs at least one change source")

        if self.restart_backoff.total_seconds() <= 0:
            raise exc.configuration("Restart backoff must be positive")

    # ....................... #

    async def dispatch(self, change: SecretChanged) -> None:
        """Evict every cached pool the changed ref affects; never raises."""

        for client in self.routed_clients:
            for tenant_id in client.cached_tenant_ids():
                try:
                    ref = secret_ref_for_tenant(client.secret_ref_for_tenant, tenant_id)

                except Exception:
                    # A tenant the resolver no longer knows can't match the change.
                    logger.trace(
                        "Hot-reload binder skipped tenant %s: no ref resolvable",
                        tenant_id,
                    )
                    continue

                if ref == change.ref:
                    await client.evict_tenant(tenant_id)

        for callback in self.on_change:
            try:
                await callback(change.ref)

            except Exception:
                logger.warning(
                    "Secrets hot-reload callback failed for %s",
                    change.ref.path,
                    exc_info=True,
                )

    # ....................... #

    def lifecycle_step(self, *, step_id: StrKey = "secrets_hot_reload") -> LifecycleStep:
        """Run one supervised consume loop per source as a lifecycle step."""

        startup = _BinderStartup(binder=self)

        return LifecycleStep(
            id=step_id,
            startup=startup,
            shutdown=_BinderShutdown(startup=startup),
            requires_long_running=True,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _BinderStartup(LifecycleHook):
    """Start one supervised consume task per change source."""

    binder: SecretsHotReloadBinder

    controls: list[BackgroundLoopControl] = attrs.field(factory=list, init=False, repr=False)

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        if any(control.running for control in self.controls):
            return

        self.controls.clear()

        for index, source in enumerate(self.binder.sources):
            control = BackgroundLoopControl(name=f"secrets_hot_reload:{index}")
            stop = control.arm()

            async def _run_once(
                source: SecretsChangeSource = source,
                stop: asyncio.Event = stop,
            ) -> None:
                await _consume_until_stopped(self.binder, source, stop)

            control.task = asyncio.create_task(
                run_supervised(
                    _run_once,
                    stop=stop,
                    name=control.loop_name,
                    restart_backoff=self.binder.restart_backoff,
                ),
                name=control.loop_name,
            )
            self.controls.append(control)
            ctx.drainables.register(control)


# ....................... #


async def _consume_until_stopped(
    binder: SecretsHotReloadBinder,
    source: SecretsChangeSource,
    stop: asyncio.Event,
) -> None:
    """Dispatch a source's changes until it ends or a stop is requested.

    The stop is raced against the next change so shutdown never waits out a quiet
    subscription (a bare ``async for`` would block in the source until the runtime's
    cancel backstop fired).
    """

    iterator = aiter(source.subscribe())
    stop_task = asyncio.create_task(stop.wait())

    async def _next() -> SecretChanged:
        return await anext(iterator)

    try:
        while not stop.is_set():
            next_task = asyncio.create_task(_next())
            done, _ = await asyncio.wait(
                {next_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
            )

            if next_task not in done:
                next_task.cancel()

                with suppress(asyncio.CancelledError, StopAsyncIteration):
                    await next_task

                return

            try:
                change = next_task.result()

            except StopAsyncIteration:
                # Source ended without a stop — run_supervised restarts after backoff.
                return

            await binder.dispatch(change)

    finally:
        stop_task.cancel()

        with suppress(asyncio.CancelledError):
            await stop_task

        aclose = getattr(iterator, "aclose", None)

        if aclose is not None:
            with suppress(Exception):
                await aclose()


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _BinderShutdown(LifecycleHook):
    """Stop every consume loop; normally a no-op after the runtime drains them."""

    startup: _BinderStartup

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        clock = asyncio.get_running_loop()

        for control in self.startup.controls:
            await control.stop(deadline=clock.time() + DEFAULT_STOP_GRACE_SECONDS)
