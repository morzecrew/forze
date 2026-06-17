"""Seed-driven fault injection for in-memory message transports.

Wraps a :class:`~forze.application.contracts.queue.QueueCommandPort` and, from a
dedicated fault RNG, injects the failures a real broker exhibits — transient enqueue
errors (force at-least-once retry), duplicate delivery (exercise inbox idempotency),
silent drops (model broker loss), and delivery delays (which, varied per message,
also reorder arrivals in virtual time). The RNG is separate from both the application
entropy seam and the scheduler RNG, so faults vary independently and a fixed fault
seed replays the exact failure sequence — any bug it surfaces reproduces.

These wrap the *port interfaces* (core contracts), not the mock, so the fault layer
stays free of any adapter dependency; a test supplies the in-memory adapter as the
wrapped inner.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import Mapping, Sequence, final

import attrs

from forze.application.contracts.queue import QueueCommandPort

# ----------------------- #


class TransportFault(RuntimeError):
    """A simulated *retryable* transport failure (e.g. a transient enqueue error)."""


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
