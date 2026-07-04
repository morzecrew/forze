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
from typing import Any, AsyncIterator, final

import attrs

from forze.application.contracts.interception import (
    PortCall,
    PortNext,
    PortSelector,
    StreamPortNext,
)
from forze.base.exceptions import exc
from forze.base.primitives import monotonic
from forze_dst.oracle.recorder import record_event

# ----------------------- #

_TRANSPORT_OPS = frozenset(
    {"enqueue", "enqueue_many", "publish", "publish_many", "produce", "send"}
)
"""Broker-delivery operations. ``drop`` (loss) and ``duplicate`` (redelivery) model transport
behaviour, so they only apply to these — duplicating a document/outbox/idempotency write would
fabricate states that model no real redelivery."""

# ....................... #


def _record_fault(fault: str, call: PortCall, **extra: object) -> None:
    """Record an injected fault as a virtual-time-stamped ``fault`` event for the report.

    A no-op outside a recorded simulation (the recorder is unbound), so the fault interceptors
    stay usable standalone. The event feeds the report's injected-environment timeline, so a
    counterexample shows exactly which faults the seed produced and when (in virtual time).
    """

    record_event(
        "fault",
        at=monotonic(),
        fault=fault,
        surface=call.surface,
        route=call.route,
        op=call.op,
        **extra,
    )


# ....................... #


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


@final
@attrs.define(frozen=True, kw_only=True)
class PortFaultInterceptor(PortSelector):
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
    """The dedicated fault RNG."""

    probability: float = 1.0
    """The per-eligible-call chance of a fault."""

    code: str = "dst.injected_port_fault"
    """The error code to raise."""

    # ....................... #

    async def around(self, call: PortCall, nxt: PortNext) -> Any:
        if (
            self.matches(call)
            and self.probability > 0.0
            and self.rng.random() < self.probability
        ):
            await asyncio.sleep(
                0
            )  # yield so the failure interleaves at the port boundary
            _record_fault("error", call)
            raise exc.infrastructure(
                f"injected transient fault at {call.surface}[{call.route}].{call.op}",
                code=self.code,
                details={"surface": call.surface, "route": call.route, "op": call.op},
            )

        return await nxt(call)


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class CrashInterceptor(PortSelector):
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
    """The dedicated fault RNG."""

    probability: float = 1.0
    """The per-eligible-call crash chance."""

    # ....................... #

    async def around(self, call: PortCall, nxt: PortNext) -> Any:
        if (
            self.matches(call)
            and self.probability > 0.0
            and self.rng.random() < self.probability
        ):
            await asyncio.sleep(
                0
            )  # yield so the crash interleaves at the port boundary
            _record_fault("crash", call)
            raise SimulatedCrash(
                f"simulated crash at {call.surface}[{call.route}].{call.op}"
            )

        return await nxt(call)


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class CrashPolicy(PortSelector):
    """Declarative crash injection for the crash/restart scenario — seeded by construction.

    Selects the port boundary at which the process *dies* (a :class:`SimulatedCrash`); the
    harness compiles it (:func:`compile_crash`) with a crash sub-seed derived from the run's
    master seed (``derive_seed(seed, "crash")``), so the crash point varies independently yet
    reproduces from one seed. Set on :class:`~forze_dst.SimulationConfig.crash` to turn a run
    into a crash → restart → recovery scenario.

    *surface* / *route* / *op* (any ``None`` matches anything, inherited from
    :class:`~forze.application.contracts.interception.PortSelector`) select the eligible calls;
    *probability* is the per-eligible-call crash chance.
    """

    probability: float = 1.0
    """The per-eligible-call crash chance."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not 0.0 <= self.probability <= 1.0:
            raise ValueError(f"probability must be in [0, 1], got {self.probability}")


# ....................... #


def compile_crash(policy: CrashPolicy, rng: random.Random) -> CrashInterceptor:
    """Compile *policy* into a seam crash interceptor driven by the seeded crash RNG."""

    return CrashInterceptor(
        rng=rng,
        probability=policy.probability,
        surface=policy.surface,
        route=policy.route,
        op=policy.op,
    )


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class FaultRule(PortSelector):
    """One fault rule: which calls it matches, and the per-eligible-call fault probabilities.

    *surface* / *route* / *op* (any ``None`` matches anything, inherited from
    :class:`~forze.application.contracts.interception.PortSelector`) select eligible calls. Each
    non-zero rate is rolled independently on a matched call; the first kind that fires wins
    (crash > error > timeout). All rolls draw from the policy's seeded fault RNG, so they are
    part of the reproducible fault stream.
    """

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

    stream_faults: bool = False
    """Also roll ``error`` / ``timeout`` / ``crash`` **per item** of an async-generator port
    call (``find_cursor``, ``consume``, ``run_chunked``, …), modeling a downstream that fails
    partway through a stream. Off by default: a stream then behaves exactly as before — the
    same pre-open faults, then clean iteration — so enabling it does not perturb the fault RNG
    stream of runs that leave it off. Each item rolls the same rates, so a long stream is more
    fault-exposed (each item is one delivery opportunity)."""

    # ....................... #

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
    """The ordered set of fault rules."""


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class _FaultPolicyInterceptor:
    """The compiled :class:`FaultPolicy` — rolls the first matching rule's faults per call."""

    rules: tuple[FaultRule, ...]
    rng: random.Random

    # ....................... #

    def _match(self, call: PortCall) -> FaultRule | None:
        return next(
            (rule for rule in self.rules if rule.matches(call)),
            None,
        )

    # ....................... #

    def _dropped(self, call: PortCall) -> Any:
        """A synthetic result for a silently-dropped call (so the caller still gets an id).

        Shaped for queue enqueue (returns a message id / list of ids); ``None`` otherwise.
        """

        token = f"dst-dropped-{self.rng.getrandbits(48):012x}"

        if call.op == "enqueue_many":
            payloads = call.args[1] if len(call.args) > 1 else ()
            return [token for _ in payloads]

        return token if call.op == "enqueue" else None

    # ....................... #

    async def around(self, call: PortCall, nxt: PortNext) -> Any:
        rule = self._match(call)

        if rule is None:
            return await nxt(call)

        where = f"{call.surface}[{call.route}].{call.op}"
        transport = call.op in _TRANSPORT_OPS

        # Raise-faults short-circuit the call entirely. Yield first (``sleep(0)``) so the failure
        # still interleaves with concurrent operations at the port boundary — on a short-circuit
        # the inner cooperative interceptor never runs, so without this the failure path would be
        # atomic and hide exactly the interleavings DST explores.
        if rule.crash > 0.0 and self.rng.random() < rule.crash:
            await asyncio.sleep(0)
            _record_fault("crash", call)
            raise SimulatedCrash(f"simulated crash at {where}")

        if rule.error > 0.0 and self.rng.random() < rule.error:
            await asyncio.sleep(0)
            _record_fault("error", call)
            raise exc.infrastructure(
                f"injected fault at {where}", code="dst.injected_port_fault"
            )

        if rule.timeout > 0.0 and self.rng.random() < rule.timeout:
            await asyncio.sleep(0)
            _record_fault("timeout", call)
            raise exc.timeout(
                f"injected timeout at {where}", code="dst.injected_timeout"
            )

        # Transport behaviours (only on broker-delivery ops). Drop skips the real call; delay
        # advances virtual time before it (never rewriting the call's args); duplicate re-runs
        # it (a redelivery).
        if transport and rule.drop > 0.0 and self.rng.random() < rule.drop:
            await asyncio.sleep(0)
            _record_fault("drop", call)
            return self._dropped(call)

        if rule.delay > 0.0 and self.rng.random() < rule.delay:
            seconds = self.rng.uniform(0.0, rule.max_delay.total_seconds())
            _record_fault("delay", call, seconds=seconds)
            await asyncio.sleep(seconds)

        result = await nxt(call)

        if transport and rule.duplicate > 0.0 and self.rng.random() < rule.duplicate:
            _record_fault("duplicate", call)
            await nxt(call)

        return result

    # ....................... #

    def _mid_stream_fault(self, rule: FaultRule, call: PortCall, where: str) -> None:
        """Roll ``crash`` / ``error`` / ``timeout`` after a yielded item; raise the first that fires.

        Same rates and RNG as the pre-open rolls, so a mid-stream fault is part of the seeded,
        reproducible fault stream. Only reached when ``rule.stream_faults`` is set.
        """

        if rule.crash > 0.0 and self.rng.random() < rule.crash:
            _record_fault("crash", call, mid_stream=True)
            raise SimulatedCrash(f"simulated crash mid-stream at {where}")

        if rule.error > 0.0 and self.rng.random() < rule.error:
            _record_fault("error", call, mid_stream=True)
            raise exc.infrastructure(
                f"injected mid-stream fault at {where}", code="dst.injected_port_fault"
            )

        if rule.timeout > 0.0 and self.rng.random() < rule.timeout:
            _record_fault("timeout", call, mid_stream=True)
            raise exc.timeout(
                f"injected mid-stream timeout at {where}", code="dst.injected_timeout"
            )

    # ....................... #

    async def around_stream(
        self, call: PortCall, nxt: StreamPortNext
    ) -> AsyncIterator[Any]:
        """Fault an async-generator port call. The pre-open rolls (crash / error / timeout /
        delay) match :meth:`around` exactly — same rates, order, and RNG draws — so a stream
        with ``stream_faults`` off behaves identically to before (fail before opening, then
        clean iteration). With ``stream_faults`` on, a raise-fault may also fire *mid-stream*
        after any item, modeling a downstream that dies partway through delivery. Transport
        drop/duplicate never apply — no async-generator op is a transport op."""

        rule = self._match(call)

        if rule is None:
            async for item in nxt(call):
                yield item
            return

        where = f"{call.surface}[{call.route}].{call.op}"

        # Pre-open raise-faults (fail before the stream opens) — identical rolls to ``around``.
        if rule.crash > 0.0 and self.rng.random() < rule.crash:
            await asyncio.sleep(0)
            _record_fault("crash", call)
            raise SimulatedCrash(f"simulated crash at {where}")

        if rule.error > 0.0 and self.rng.random() < rule.error:
            await asyncio.sleep(0)
            _record_fault("error", call)
            raise exc.infrastructure(
                f"injected fault at {where}", code="dst.injected_port_fault"
            )

        if rule.timeout > 0.0 and self.rng.random() < rule.timeout:
            await asyncio.sleep(0)
            _record_fault("timeout", call)
            raise exc.timeout(
                f"injected timeout at {where}", code="dst.injected_timeout"
            )

        if rule.delay > 0.0 and self.rng.random() < rule.delay:
            seconds = self.rng.uniform(0.0, rule.max_delay.total_seconds())
            _record_fault("delay", call, seconds=seconds)
            await asyncio.sleep(seconds)

        async for item in nxt(call):
            yield item

            if rule.stream_faults:
                self._mid_stream_fault(rule, call, where)


# ....................... #


def compile_fault_policy(
    policy: FaultPolicy, rng: random.Random
) -> _FaultPolicyInterceptor:
    """Compile *policy* into a seam interceptor that shares one seeded fault RNG."""

    return _FaultPolicyInterceptor(rules=policy.rules, rng=rng)


# ....................... #

__all__ = [
    "FaultPolicy",
    "FaultRule",
    "CrashPolicy",
    "SimulatedCrash",
    "PortFaultInterceptor",
    "CrashInterceptor",
]
