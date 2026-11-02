"""The startup step that subscribes a process to other processes' grants-cache invalidations, and
the call app code makes after changing bindings outside role assignment."""

from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.cache import Unsubscribe
from forze.application.contracts.execution import LifecycleStep
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc

from ..services.grants_cache import GrantsCache, GrantsInvalidation, invalidate_after_commit
from .deps import AuthzKernelConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class _Subscribe:
    invalidation: GrantsInvalidation
    cache: GrantsCache
    unsubscribe: Unsubscribe | None = attrs.field(default=None, init=False)

    async def __call__(self, ctx: ExecutionContext) -> None:
        self.unsubscribe = await self.invalidation.subscribe(ctx, self.cache)


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _Unsubscribe:
    startup: _Subscribe

    async def __call__(self, ctx: ExecutionContext) -> None:
        unsubscribe, self.startup.unsubscribe = self.startup.unsubscribe, None

        if unsubscribe is not None:
            await unsubscribe()


def grants_invalidation_lifecycle_step(
    kernel: AuthzKernelConfig,
    *,
    name: str = "authz_grants_invalidation",
) -> LifecycleStep:
    """A step subscribing this process's grants cache to the kernel's broadcast at startup, and
    detaching it at shutdown.

    Pass the kernel given to ``AuthzDepsModule(kernel=...)``, and order the step after the one
    that starts the cache backend. Startup fails when the broadcast cannot run: a cache port
    that cannot push invalidations, declines to, or does not bring its stream up within ten
    seconds, or a tenant-aware one. It needs a process that keeps running, so a ``SERVERLESS``
    runtime refuses it.

    :raises CoreException: ``configuration`` (``authz_grants_invalidation_missing``) when the
        kernel configures no ``grants_invalidation``.
    """

    invalidation = kernel.grants_invalidation

    if invalidation is None or kernel.grants_cache is None:
        raise exc.configuration(
            "grants_invalidation_lifecycle_step subscribes to the kernel's grants_invalidation, "
            "and the kernel configures none: set AuthzKernelConfig(grants_invalidation=...).",
            code="authz_grants_invalidation_missing",
        )

    startup = _Subscribe(invalidation=invalidation, cache=kernel.grants_cache)

    return LifecycleStep(
        id=name,
        startup=startup,
        shutdown=_Unsubscribe(startup=startup),
        requires_long_running=True,
    )


async def invalidate_grants(
    ctx: ExecutionContext,
    kernel: AuthzKernelConfig,
    principal_id: UUID | None = None,
) -> None:
    """Forget *principal_id*'s cached grants, or everyone's for ``None``, once the current
    transaction commits, or now outside one: in this process, and in every subscribed process
    when the kernel configures ``grants_invalidation``.

    Call it after a binding changes through a plain document command, which nothing announces:
    with *principal_id* for one principal's own role, permission or group-membership binding,
    without it for a change that reaches many principals. No-op without ``grants_cache``.
    """

    if kernel.grants_cache is None:
        return

    await invalidate_after_commit(
        ctx, kernel.grants_cache, kernel.grants_invalidation, principal_id
    )
