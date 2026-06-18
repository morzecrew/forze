"""Seed-driven fault injection.

Two complementary surfaces, each driven by a dedicated fault RNG (separate from the
application entropy seam and the scheduler RNG, so faults vary independently and a fixed
fault seed replays the exact failure sequence):

* **Seam-based, over any port** (the modern path, via the core port-interception seam) —
  :class:`PortFaultInterceptor` injects a transient (retryable) failure, and
  :class:`CrashInterceptor` a process crash, at a matched port boundary on *any* resolved
  port, with no hand-wiring.
* **Transport-specific, queue-port wrapper** — :class:`TransportFaultPolicy` +
  :class:`FaultyQueueCommand` wrap a :class:`~forze.application.contracts.queue.QueueCommandPort`
  to inject the richer broker behaviours the seam interceptors do not yet model: duplicate
  delivery (exercise inbox idempotency), silent drops (broker loss), and delivery delays
  (which, varied per message, reorder arrivals in virtual time).

Division of labour: use the seam interceptors for transient/crash faults over real
registries; use the queue wrapper for duplicate/drop/delay until those are modelled at the
seam (a `TransportFaultInterceptor` is a planned follow-up that would retire the wrapper).
Both target the *port interfaces* (core contracts), not the mock, so the fault layer stays
free of any adapter dependency.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import Mapping, Sequence, final

import attrs

from forze.application.contracts.queue import QueueCommandPort
from forze.application.execution.interception import PortCall, PortNext
from forze.base.exceptions import exc

# ----------------------- #


class TransportFault(RuntimeError):
    """A simulated *retryable* transport failure (e.g. a transient enqueue error)."""


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
@attrs.define(frozen=True, kw_only=True)
class TransportFaultPolicy:
    """Per-operation fault probabilities for a simulated transport.

    Each is an independent probability in ``[0, 1]`` rolled per enqueue. ``delay`` adds
    a visibility delay drawn uniformly in ``(0, max_delay]`` — varying it per message
    reorders arrivals (the seam's stand-in for broker reordering).
    """

    transient: float = 0.0
    """P(enqueue raises a retryable :class:`TransportFault` before storing)."""

    duplicate: float = 0.0
    """P(an accepted message is stored an extra time — a redelivered duplicate)."""

    drop: float = 0.0
    """P(enqueue is silently lost — models broker loss; breaks at-least-once without
    an outbox/durable redelivery to recover it)."""

    delay: float = 0.0
    """P(a message's visibility is delayed)."""

    max_delay: timedelta = timedelta(seconds=5)
    """Upper bound for an injected delay (drawn uniformly in ``(0, max_delay]``)."""

    def __attrs_post_init__(self) -> None:
        for name in ("transient", "duplicate", "drop", "delay"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} probability must be in [0, 1], got {value}")


# ....................... #


@final
@attrs.define(kw_only=True)
class FaultyQueueCommand[M](QueueCommandPort[M]):
    """A :class:`QueueCommandPort` that injects :class:`TransportFaultPolicy` faults."""

    inner: QueueCommandPort[M]
    policy: TransportFaultPolicy
    rng: random.Random

    # ....................... #

    def _roll(self, probability: float) -> bool:
        return probability > 0.0 and self.rng.random() < probability

    def _extra_delay(self) -> timedelta | None:
        if not self._roll(self.policy.delay):
            return None
        seconds = self.rng.uniform(0.0, self.policy.max_delay.total_seconds())
        return timedelta(seconds=seconds)

    def _dropped_id(self) -> str:
        # An accepted-then-lost message: a plausible, deterministic synthetic id.
        return f"fault-dropped-{self.rng.getrandbits(48):012x}"

    @staticmethod
    def _combine(base: timedelta | None, extra: timedelta | None) -> timedelta | None:
        if base is None:
            return extra
        return base if extra is None else base + extra

    # ....................... #

    async def enqueue(
        self,
        queue: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        delay: timedelta | None = None,
        not_before: datetime | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> str:
        if self._roll(self.policy.transient):
            raise TransportFault("simulated transient enqueue failure")

        if self._roll(self.policy.drop):
            return self._dropped_id()

        message_id = await self.inner.enqueue(
            queue,
            payload,
            type=type,
            key=key,
            enqueued_at=enqueued_at,
            delay=self._combine(delay, self._extra_delay()),
            not_before=not_before,
            headers=headers,
        )

        if self._roll(self.policy.duplicate):
            await self.inner.enqueue(
                queue,
                payload,
                type=type,
                key=key,
                enqueued_at=enqueued_at,
                delay=self._combine(delay, self._extra_delay()),
                not_before=not_before,
                headers=headers,
            )

        return message_id

    # ....................... #

    async def enqueue_many(
        self,
        queue: str,
        payloads: Sequence[M],
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        delay: timedelta | None = None,
        not_before: datetime | None = None,
        headers: Mapping[str, str] | None = None,
        message_headers: Sequence[Mapping[str, str]] | None = None,
    ) -> list[str]:
        # Route each item through the single-message fault path so per-message
        # transient/drop/duplicate/delay all apply; preserves the batch's id list.
        ids: list[str] = []
        for index, payload in enumerate(payloads):
            per_headers = (
                message_headers[index]
                if message_headers is not None and index < len(message_headers)
                else headers
            )
            ids.append(
                await self.enqueue(
                    queue,
                    payload,
                    type=type,
                    key=key,
                    enqueued_at=enqueued_at,
                    delay=delay,
                    not_before=not_before,
                    headers=per_headers,
                )
            )

        return ids


# ....................... #


@final
@attrs.define(kw_only=True)
class PortFaultInterceptor:
    """Inject a transient downstream failure at a port boundary — over **any** port.

    A :class:`~forze.application.execution.interception.PortInterceptor` that, from a
    dedicated fault RNG, raises a retryable ``exc.infrastructure`` *before* the real call
    on the matched port operations — modeling a real adapter failing mid-operation
    (a dropped connection, a timeout). It plugs into the core port-interception seam, so it
    works against **real registries** without wrapping a specific port by hand. It covers
    *transient* faults over any port; the richer transport behaviours (duplicate / drop /
    delay) still live in :class:`FaultyQueueCommand` (see the module docstring's division of
    labour) until they too are modelled at the seam.

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

    async def around(self, call: PortCall, nxt: PortNext) -> object:
        rule = self._match(call)

        if rule is not None:
            where = f"{call.surface}[{call.route}].{call.op}"

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

        return await nxt(call)


# ....................... #


def compile_fault_policy(
    policy: FaultPolicy, rng: random.Random
) -> _FaultPolicyInterceptor:
    """Compile *policy* into a seam interceptor that shares one seeded fault RNG."""

    return _FaultPolicyInterceptor(rules=policy.rules, rng=rng)
