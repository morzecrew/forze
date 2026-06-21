"""Resilience policy wrapper for resolved dependency ports.

Mirrors the tracing proxy in :mod:`forze.application.execution.tracing.port_proxy`
but routes public coroutine methods through the registered resilience executor
(``ctx.resilience().run(...)``) instead of recording events. The two compose:
the policy proxy wraps **outside** the tracing proxy, so port trace events
correspond 1:1 with real invocations of the underlying port — each retry
attempt is traced, and a rejected (throttled / bulkhead-full / breaker-open)
call records no phantom port event; the executor emits its own
``domain="resilience"`` events for rejections.
"""

from functools import wraps
from typing import TYPE_CHECKING, Any, cast

import attrs

from forze.application.contracts.resilience import PortPolicy
from forze.base.primitives import StrKey

from ..port_proxy_base import PortProxy

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #


@attrs.define(slots=True)
class ResiliencePortProxy(PortProxy):
    """Wrap a port so its public coroutine methods run under a named policy.

    Method calls go through ``ctx.resilience().run(fn, policy=..., route=...)``
    lazily at call time, so the proxy never resolves the executor during
    dependency resolution. Skipped (returned unwrapped): non-callables,
    private/dunder attributes, methods outside an explicit ``methods`` tuple,
    async-generator methods (a stream cannot run inside one ``run()`` call —
    the base default), and plain sync methods (the base default).
    """

    ctx: "ExecutionContext"
    policy: StrKey
    route: StrKey | None
    methods: frozenset[str] | None

    # ....................... #

    def _should_wrap(self, name: str, attr: Any) -> bool:
        # Public methods only, narrowed to ``methods`` when the policy declares them.
        if name.startswith("_"):
            return False

        return self.methods is None or name in self.methods

    # ....................... #

    def _wrap_async(self, name: str, attr: Any) -> Any:
        del name

        @wraps(attr)
        async def guarded(*args: Any, **kwargs: Any) -> Any:
            return await self.ctx.resilience().run(
                lambda: attr(*args, **kwargs),
                policy=self.policy,
                route=self.route,
            )

        return guarded


# ....................... #


def wrap_port_policy[T](
    inner: T,
    *,
    ctx: "ExecutionContext",
    port_policy: PortPolicy,
    resolved_route: StrKey | None,
) -> T:
    """Return *inner* wrapped under *port_policy*.

    The state-keying route is the policy's explicit ``route`` when set,
    otherwise the route the port resolved under (typically ``spec.name``).
    """

    route = port_policy.route if port_policy.route is not None else resolved_route

    return cast(
        T,
        ResiliencePortProxy(
            inner=inner,
            ctx=ctx,
            policy=port_policy.policy,
            route=route,
            methods=(
                frozenset(port_policy.methods)
                if port_policy.methods is not None
                else None
            ),
        ),
    )
