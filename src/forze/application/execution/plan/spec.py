"""Middleware and transaction specifications."""

from enum import StrEnum
from typing import Any, Iterable, Sequence, final

import attrs

from forze.application.execution.capability_keys import CapabilityKey

from ..context import ExecutionContext
from ..middleware import Effect
from .types import EffectFactory, MiddlewareFactory

# ----------------------- #


def frozenset_capability_keys(
    values: frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None,
) -> frozenset[str]:
    """Normalize ``requires`` / ``provides`` inputs to a ``frozenset[str]``.

    Accepts :class:`~forze.application.execution.capability_keys.CapabilityKey`
    values and other iterables of string-like keys used on plan builders and
    :class:`MiddlewareSpec`.
    """

    if values is None:
        return frozenset()

    if isinstance(values, frozenset):
        return frozenset(str(x) for x in values)

    if isinstance(values, set):
        return frozenset(str(x) for x in values)

    return frozenset(str(x) for x in values)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DispatchDeclaringEffectFactory:
    """Wraps an :class:`EffectFactory` and declares child op keys for dispatch graphs.

    Returned by :meth:`UsecaseDelegate.effect_factory` so :class:`UsecasePlan`
    builders can attach ``(source_op, target_op)`` edges to middleware specs.
    """

    inner: EffectFactory
    """Factory returning :class:`Effect`."""

    dispatch_targets: frozenset[str]
    """Logical child operation keys (same strings as registry registration)."""

    def __call__(self, ctx: ExecutionContext) -> Effect[Any, Any]:
        return self.inner(ctx)


# ....................... #


def dispatch_edges_for_delegate_effect(
    source_ops: Sequence[str],
    effect: EffectFactory,
) -> frozenset[tuple[str, str]]:
    """Build dispatch edge tuples when ``effect`` declares delegate targets."""

    if isinstance(effect, DispatchDeclaringEffectFactory):
        return frozenset(
            (src, t) for src in source_ops for t in effect.dispatch_targets
        )

    return frozenset()


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MiddlewareSpec:
    """Specification for a middleware attached to an operation plan.

    Middlewares are ordered by ``priority`` (descending) and created lazily from a
    :class:`ExecutionContext` when a plan is resolved.

    When :attr:`UsecasePlan.use_capability_engine` is enabled, guard and effect
    buckets additionally order steps by ``requires`` / ``provides`` capability
    keys (see the capability execution reference page).
    """

    priority: int = attrs.field(
        validator=[
            attrs.validators.gt(int(-1e5)),
            attrs.validators.lt(int(1e5)),
        ]
    )
    factory: MiddlewareFactory
    """Callable returning middleware; effect buckets may use :class:`DispatchDeclaringEffectFactory`."""

    dispatch_edges: frozenset[tuple[str, str]] = attrs.field(
        factory=frozenset,
        repr=False,
    )
    """Edges ``(source_op, target_op)`` derived for registry dispatch validation."""

    requires: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=frozenset_capability_keys,
    )
    """Capability keys that must be ready before this step runs (per-bucket graph)."""

    provides: frozenset[str] = attrs.field(
        factory=frozenset,
        converter=frozenset_capability_keys,
    )
    """Capability keys this step marks ready on success, or missing when skipped."""

    step_label: str | None = None
    """Optional stable label for logs and :meth:`UsecasePlan.explain`."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class TransactionSpec:
    """Specification for a transaction attached to an operation plan."""

    route: str | StrEnum
    """Routing key for the transaction."""
