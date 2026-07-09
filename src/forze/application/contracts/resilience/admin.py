"""Resilience admin / control plane: inspect live state, force-open (kill switch), hot-retune.

Kept **separate** from the operational :class:`~forze.application.contracts.resilience.ports.ResilienceExecutorPort`
(``run`` / ``run_hedged``) — the framework's management/data split — so an operator surface can read
and steer the live resilience state without being the request hot path. Both are backed by the same
in-process executor singleton.

The read side (:meth:`ResilienceAdminPort.inspect`) is a point-in-time snapshot of the executor-owned
adaptive state (dynamic-bulkhead concurrency limits, queue depth, hedge delays, and any force-open
kill-switch). The steering calls mutate that singleton: :meth:`force_open` trips a breaker open by
hand to shed a failing dependency without a redeploy, and :meth:`retune` hot-swaps a policy's
parameters so the adaptive controllers rebuild against the new limits on the next call.
"""

from __future__ import annotations

from typing import Awaitable, Protocol, Sequence, final, runtime_checkable

import attrs

from forze.base.primitives import StrKey

from .value_objects import ResiliencePolicy

# ----------------------- #


@final
@attrs.frozen(kw_only=True)
class ResilienceStateSnapshot:
    """A point-in-time view of one ``(policy, route)``'s live, executor-owned resilience state.

    Only state the executor itself holds is surfaced: the adaptive-bulkhead / Gradient2 concurrency
    limit and its live occupancy, the effective hedge delay, and whether an operator force-open
    kill-switch is in effect. Circuit-breaker *phase* lives in the pluggable (possibly Redis-shared)
    breaker store, not the executor, so it is observed via the breaker-state metric rather than here;
    :attr:`forced_open` is the executor's own kill-switch, which is reported.
    """

    policy: str
    """The resilience policy name."""

    route: str | None
    """The state-keying route (a distinct backend under the policy), or ``None``."""

    forced_open: bool
    """Whether a :meth:`ResilienceAdminPort.force_open` kill-switch is in effect for this key."""

    concurrency_limit: float | None
    """Current bulkhead admission limit — the adaptive (AIMD / Gradient2) controllers vary it; a
    fixed bulkhead reports its constant cap. ``None`` when the policy has no bulkhead state yet."""

    in_use: int | None
    """Calls currently admitted to the dynamic bulkhead, or ``None`` when there is no live state."""

    waiting: int | None
    """Calls queued for a bulkhead slot right now, or ``None`` when there is no live state."""

    hedge_delay: float | None
    """Effective adaptive hedge delay in seconds (P² quantile, clamped), or ``None`` when not hedged."""


# ....................... #


@runtime_checkable
class ResilienceAdminPort(Protocol):
    """Control-plane operations over the live resilience executor (an ops / SRE surface).

    Read the current adaptive limits and queue depths, trip a breaker open as a manual kill-switch
    (shed a known-bad dependency without shipping code), release it, and hot-retune a policy's
    parameters. The read side is a snapshot; the steering calls mutate the process singleton and take
    effect immediately for calls that start after them.
    """

    def inspect(
        self,
        *,
        policy: StrKey | None = None,
    ) -> Awaitable[Sequence[ResilienceStateSnapshot]]:
        """Snapshot the live, executor-owned resilience state.

        Returns one :class:`ResilienceStateSnapshot` per ``(policy, route)`` with live state,
        optionally filtered to a single *policy*. State appears lazily on first use of a policy, so a
        policy that has not run yet (and carries no force-open) yields nothing.
        """

        ...  # pragma: no cover

    # ....................... #

    def force_open(
        self,
        policy: StrKey,
        route: StrKey | None = None,
    ) -> Awaitable[None]:
        """Trip the ``(policy, route)`` breaker open by hand — a manual kill-switch.

        Every subsequent call under this policy/route is rejected (as if the circuit breaker were
        open) until :meth:`clear_forced_open`, regardless of the policy's configured strategies —
        the operator escape hatch to shed a failing dependency without a redeploy. Idempotent.
        """

        ...  # pragma: no cover

    # ....................... #

    def clear_forced_open(
        self,
        policy: StrKey,
        route: StrKey | None = None,
    ) -> Awaitable[None]:
        """Release a :meth:`force_open` kill-switch for ``(policy, route)``. Idempotent."""

        ...  # pragma: no cover

    # ....................... #

    def retune(self, policy: ResiliencePolicy) -> Awaitable[None]:
        """Hot-swap a policy's parameters by name.

        Replaces the named policy and drops the executor's cached adaptive state for it, so the
        AIMD / Gradient2 controllers, retry budgets, throttles, and hedge estimators rebuild against
        the new parameters on the next call. Calls already in flight drain safely on the state they
        captured at start (they are never stranded). Breaker/rate-limit *thresholds* live in the
        pluggable store and are passed per call, so they follow the store's own state lifecycle.
        """

        ...  # pragma: no cover
