"""Resilience executor dependency key, resolver, and port-level policy binding."""

from typing import Any, Iterable, Mapping, final

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from ..deps import ConvenientDeps, DepKey
from .ports import ResilienceExecutorPort

# ----------------------- #

ResilienceExecutorDepKey = DepKey[ResilienceExecutorPort]("resilience_executor")
"""Key for the process-wide resilience executor singleton."""


# ....................... #


def _methods_converter(value: Iterable[str] | None) -> tuple[str, ...] | None:
    return tuple(value) if value is not None else None


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PortPolicy:
    """Declarative binding of a named resilience policy to a resolved port.

    Registered via ``ResilienceDepsModule(port_policies=[...])``. At dependency
    resolution the resolved port instance for ``key`` is wrapped in a proxy so
    its public coroutine methods run through
    ``ctx.resilience().run(fn, policy=..., route=...)``. The wrapped instance is
    what lands in the per-scope port cache, so repeated resolutions reuse one
    proxy.

    Applies to **configurable** (spec-built) ports — the families resolved via
    contract accessors (``ctx.document.query(...)``, ``ctx.http.service(...)``,
    queue/pubsub ports, ...). Non-callables, private/dunder attributes, and
    **async-generator methods** (``consume``/``tail``/``subscribe``-style
    streams) are never wrapped: a stream cannot run inside a single
    ``run()`` call — guard the *consumption loop* with a policy instead.
    """

    key: DepKey[Any]
    """Dependency key whose resolved port instances get wrapped."""

    policy: StrKey
    """Named resilience policy applied to each wrapped method call."""

    route: StrKey | None = None
    """State-keying route passed to ``run()``; defaults to the route the
    port resolved under (e.g. ``spec.name``)."""

    methods: tuple[str, ...] | None = attrs.field(
        default=None,
        converter=_methods_converter,
    )
    """Explicit method names to wrap; ``None`` wraps all public coroutine
    methods."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not str(self.policy):
            raise exc.configuration("Port policy must name a resilience policy")

        if self.methods is not None:
            if not self.methods:
                raise exc.configuration(
                    f"Port policy for {self.key.name!r} declares an empty "
                    "methods tuple (use None to wrap all coroutine methods)",
                )

            if non_public := sorted(
                m for m in self.methods if not m or m.startswith("_")
            ):
                raise exc.configuration(
                    f"Port policy for {self.key.name!r} may only wrap public "
                    "methods: " + ", ".join(repr(m) for m in non_public),
                )


# ....................... #

PortPolicyTable = Mapping[DepKey[Any], PortPolicy]
"""Port policies keyed by the dependency key they wrap."""

ResiliencePortPoliciesDepKey = DepKey[PortPolicyTable]("resilience_port_policies")
"""Key for the declarative port-policy table consulted at port resolution."""


# ....................... #


class ResilienceDeps(ConvenientDeps):
    """Resolve the registered resilience executor."""

    def __call__(self) -> ResilienceExecutorPort:
        """Resolve the resilience executor (requires a registered module)."""

        return self._require_ctx().deps.provide(ResilienceExecutorDepKey)
