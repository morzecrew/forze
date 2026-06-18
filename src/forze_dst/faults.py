"""Seed-driven fault injection — entirely over the core port-interception seam.

A dedicated fault RNG (separate from the application entropy seam and the scheduler RNG, so
faults vary independently and a fixed fault seed replays the exact failure sequence) drives
every fault at the port boundary, over *any* resolved port, with no hand-wiring of a specific
adapter — the app under test is never modified.

* **Declarative** — :class:`FaultPolicy` / :class:`FaultRule` (the blessed surface): per
  ``(surface, route, op)`` rates for every fault kind, compiled by the harness from a sub-seed
  derived from the run's master seed (so faults are seeded *by construction* — no caller RNG).
* **Primitives** — :class:`PortFaultInterceptor` (one transient error) and
  :class:`CrashInterceptor` (one :class:`SimulatedCrash`) for direct, single-kind use.

Fault kinds: ``error`` (retryable ``exc.infrastructure``), ``timeout`` (``exc.timeout``),
``crash`` (``SimulatedCrash``) — all raise-faults over any port; and the transport behaviours
``drop`` (silent loss — short-circuit, no real call), ``duplicate`` (the call runs twice — a
redelivery), and ``delay`` (advance virtual time before the call, reordering arrivals). The
seam never modifies the call's arguments — a delay is a virtual-time advance, not a rewritten
``delay`` parameter — so the injected environment stays faithful to the real app.
"""

from __future__ import annotations

import asyncio
import random
from datetime import timedelta
from typing import final

import attrs

from forze.application.execution.interception import PortCall, PortNext
from forze.base.exceptions import exc

# ----------------------- #


class SimulatedCrash(BaseException):
    """A simulated process crash (``kill -9``) at a port boundary.

    A :class:`BaseException`, not an :class:`Exception`, so it bypasses application
    ``except Exception`` handling — the operation gets no chance to compensate, modeling the
    process simply dying. The in-flight transaction is rolled back (the store's crash
    recovery — committed state stays consistent; uncommitted work is lost), and only a
    **restart** over the persisted store recovers; work an operation deferred to after a
    commit but had not yet performed is gone until a recovery pass re-drives it.
    """


# ....................... #


def _call_matches(
    call: PortCall,
    *,
    surface: str | None,
    route: str | None,
    op: str | None,
) -> bool:
    """Whether *call* matches the (surface, route, op) selector (``None`` matches anything)."""

    return (
        (surface is None or call.surface == surface)
        and (route is None or call.route == route)
        and (op is None or call.op == op)
    )


@final
@attrs.define(kw_only=True)
class PortFaultInterceptor:
    """Inject a transient downstream failure at a port boundary — over **any** port.

    A :class:`~forze.application.execution.interception.PortInterceptor` that, from a
    dedicated fault RNG, raises a retryable ``exc.infrastructure`` *before* the real call
    on the matched port operations — modeling a real adapter failing mid-operation
    (a dropped connection, a timeout). It plugs into the core port-interception seam, so it
    works against **real registries** without wrapping a specific port by hand. A single-kind
    primitive; for several kinds / rates / selectors use the declarative :class:`FaultPolicy`.

    Placed inside the resilience port-policy wrap, so the injected transient is retryable by
    a declared policy. The RNG is separate from the application entropy seam and the
    scheduler RNG, so faults vary independently and a fixed fault seed replays them.

    *surface* / *route* / *op* (any left ``None`` matches anything) select which calls are
    eligible; *probability* is the per-eligible-call chance of a fault (named to match
    :class:`CrashInterceptor`).
    """

    rng: random.Random
    probability: float = 1.0
    surface: str | None = None
    route: str | None = None
    op: str | None = None
    code: str = "dst.injected_port_fault"

    # ....................... #

    async def around(self, call: PortCall, nxt: PortNext) -> object:
        if (
            _call_matches(call, surface=self.surface, route=self.route, op=self.op)
            and self.probability > 0.0
            and self.rng.random() < self.probability
        ):
            raise exc.infrastructure(
                f"injected transient fault at {call.surface}[{call.route}].{call.op}",
                code=self.code,
                details={"surface": call.surface, "route": call.route, "op": call.op},
            )

        return await nxt(call)


# ....................... #


@final
@attrs.define(kw_only=True)
class CrashInterceptor:
    """Raise a :class:`SimulatedCrash` at a matched port boundary — the process dies mid-I/O.

    The seam-level crash primitive (WS4): unlike :class:`PortFaultInterceptor` (a retryable
    ``CoreException`` the application can catch and compensate), the crash is a
    :class:`BaseException`, so the operation gets no inline recovery — the in-flight
    transaction rolls back and the system is only made whole by a restart over the persisted
    store. The fault RNG is dedicated, so a fixed seed replays the exact crash point.

    *surface* / *route* / *op* (any ``None`` matches anything) select the eligible calls;
    *probability* is the per-eligible-call crash chance.
    """

    rng: random.Random
    probability: float = 1.0
    surface: str | None = None
    route: str | None = None
    op: str | None = None

    # ....................... #

    async def around(self, call: PortCall, nxt: PortNext) -> object:
        if (
            _call_matches(call, surface=self.surface, route=self.route, op=self.op)
            and self.probability > 0.0
            and self.rng.random() < self.probability
        ):
            raise SimulatedCrash(
                f"simulated crash at {call.surface}[{call.route}].{call.op}"
            )

        return await nxt(call)


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class FaultRule:
    """One fault rule: which calls it matches, and the per-eligible-call fault probabilities.

    *surface* / *route* / *op* (any ``None`` matches anything) select eligible calls. Each
    non-zero rate is rolled independently on a matched call; the first kind that fires wins
    (crash > error > timeout). All rolls draw from the policy's seeded fault RNG, so they are
    part of the reproducible fault stream.
    """

    surface: str | None = None
    route: str | None = None
    op: str | None = None
    error: float = 0.0
    """P(raise a retryable ``exc.infrastructure`` — a transient downstream failure)."""
    timeout: float = 0.0
    """P(raise ``exc.timeout`` — the call exceeded its budget)."""
    crash: float = 0.0
    """P(raise ``SimulatedCrash`` — the process dies; only a restart recovers)."""
    drop: float = 0.0
    """P(silently drop — the real call is skipped and a synthetic result returned; models
    broker loss. Target a fire-and-forget op, e.g. queue ``enqueue``)."""
    duplicate: float = 0.0
    """P(the call runs twice — a redelivered duplicate; exercises inbox idempotency)."""
    delay: float = 0.0
    """P(advance virtual time before the call by ``uniform(0, max_delay]`` — a slow/reordered
    delivery; the call's own arguments are never modified)."""
    max_delay: timedelta = timedelta(seconds=5)
    """Upper bound for an injected :attr:`delay` (drawn uniformly in ``(0, max_delay]``)."""

    def __attrs_post_init__(self) -> None:
        for name in ("error", "timeout", "crash", "drop", "duplicate", "delay"):
            value: float = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} probability must be in [0, 1], got {value}")


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class FaultPolicy:
    """A declarative, ordered set of :class:`FaultRule` s — the seeded, no-crutch fault surface.

    The harness compiles it (via :func:`compile_fault_policy`) with a fault RNG **derived from
    the run's master seed** (``derive_seed(seed, "fault")``), so faults are seeded by
    construction — the caller never supplies an RNG — and reproduce from one seed. The first
    matching rule applies (routing order). Works over any resolved port via the interception
    seam, no per-port wrapping.
    """

    rules: tuple[FaultRule, ...] = ()


# ....................... #


@final
@attrs.define(kw_only=True)
class _FaultPolicyInterceptor:
    """The compiled :class:`FaultPolicy` — rolls the first matching rule's faults per call."""

    rules: tuple[FaultRule, ...]
    rng: random.Random

    # ....................... #

    def _match(self, call: PortCall) -> FaultRule | None:
        for rule in self.rules:
            if _call_matches(call, surface=rule.surface, route=rule.route, op=rule.op):
                return rule

        return None

    # ....................... #

    def _dropped(self, call: PortCall) -> object:
        """A synthetic result for a silently-dropped call (so the caller still gets an id).

        Shaped for queue enqueue (returns a message id / list of ids); ``None`` otherwise.
        """

        token = f"dst-dropped-{self.rng.getrandbits(48):012x}"

        if call.op == "enqueue_many":
            payloads = call.args[1] if len(call.args) > 1 else ()
            return [token for _ in payloads]

        if call.op == "enqueue":
            return token

        return None

    # ....................... #

    async def around(self, call: PortCall, nxt: PortNext) -> object:
        rule = self._match(call)

        if rule is None:
            return await nxt(call)

        where = f"{call.surface}[{call.route}].{call.op}"

        # Raise-faults short-circuit the call entirely.
        if rule.crash > 0.0 and self.rng.random() < rule.crash:
            raise SimulatedCrash(f"simulated crash at {where}")

        if rule.error > 0.0 and self.rng.random() < rule.error:
            raise exc.infrastructure(
                f"injected fault at {where}", code="dst.injected_port_fault"
            )

        if rule.timeout > 0.0 and self.rng.random() < rule.timeout:
            raise exc.timeout(
                f"injected timeout at {where}", code="dst.injected_timeout"
            )

        # Transport faults. Drop skips the real call; delay advances virtual time before it
        # (never rewriting the call's args); duplicate re-runs it (a redelivery).
        if rule.drop > 0.0 and self.rng.random() < rule.drop:
            return self._dropped(call)

        if rule.delay > 0.0 and self.rng.random() < rule.delay:
            await asyncio.sleep(self.rng.uniform(0.0, rule.max_delay.total_seconds()))

        result = await nxt(call)

        if rule.duplicate > 0.0 and self.rng.random() < rule.duplicate:
            await nxt(call)

        return result


# ....................... #


def compile_fault_policy(
    policy: FaultPolicy, rng: random.Random
) -> _FaultPolicyInterceptor:
    """Compile *policy* into a seam interceptor that shares one seeded fault RNG."""

    return _FaultPolicyInterceptor(rules=policy.rules, rng=rng)
