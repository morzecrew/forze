"""Pluggable ready-queue schedulers for the simulation loop.

The loop processes a *tick* at a time: a batch of ready callbacks (the continuations
concurrent tasks scheduled after their last ``await``). A scheduler decides the order they
run in — the interleaving. The order is where order-dependent races live, so exploring it
well is how DST finds concurrency bugs.

Two strategies ship:

* :class:`RandomScheduler` — shuffle each tick (the original perturbation). Cheap, but a
  uniform-random walk over interleavings has no bias toward the rare orderings bugs need.
* :class:`PCTScheduler` — *Probabilistic Concurrency Testing* (Burckhardt, Kothari, Musuvathi,
  Nagarakatte, ASPLOS 2010): give each task a random priority and run ready tasks
  highest-first; insert ``d-1`` random *priority-change points* over an estimated step
  budget, each demoting the running task below the rest. This provably finds any depth-``d``
  bug with probability ≥ ``1 / (n · k^(d-1))`` (``n`` tasks, ``k`` steps) — a far better
  per-run chance than uniform shuffling for the deep, specific interleavings that matter.

Both are seeded and deterministic: same RNG → same interleaving → reproducible. PCT here is
applied at the loop's *tick* granularity (priority-ordering each ready batch), matching the
loop's scheduling unit rather than classic per-instruction preemption.
"""

from __future__ import annotations

import asyncio
import random
from typing import Any, Protocol, Sequence, final, runtime_checkable

import attrs

# ----------------------- #


@runtime_checkable
class Scheduler(Protocol):
    """Reorders a tick's ready callbacks; *step* is the running count of scheduled ticks."""

    def reorder(self, ready: list[Any], step: int) -> list[Any]: ...


# ....................... #


@final
@attrs.define
class RandomScheduler(Scheduler):
    """Uniformly shuffle each tick's ready callbacks."""

    _rng: random.Random

    # ....................... #

    def reorder(self, ready: list[Any], step: int) -> list[Any]:
        del step
        self._rng.shuffle(ready)
        return ready


# ....................... #


def _task_of(handle: Any) -> asyncio.Task[Any] | None:
    """The asyncio Task a ready callback belongs to, or ``None`` (a plain ``call_soon``)."""

    owner = getattr(getattr(handle, "_callback", None), "__self__", None)

    return owner if isinstance(owner, asyncio.Task) else None  # pyright: ignore[reportUnknownVariableType]


# ....................... #


@final
@attrs.define
class PCTScheduler(Scheduler):
    """A PCT scheduler: priority-ordered tasks with ``d-1`` random priority-change points.

    *depth* is the bug depth the run targets (``d``); *steps* is an estimate of the number
    of scheduling ticks (the ``k`` the change points are spread over — an underestimate just
    means later change points may not fire, an overestimate spreads them thinner). The first
    time a task is seen it is assigned a random priority in a high band; ready tasks run
    highest-priority first. At each change-point tick the current highest task is demoted
    below the band, forcing the next-highest to take over — the controlled preemption that
    makes deep interleavings reachable with a useful probability.
    """

    _rng: random.Random
    depth: int = 3
    steps: int = 50
    _priorities: dict[asyncio.Task[Any], float] = attrs.field(factory=dict, init=False)
    _change_points: dict[int, float] = attrs.field(factory=dict, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        self.depth = max(1, self.depth)

        # d-1 change points at random ticks, sorted ascending; each demotes to a distinct
        # value below the high band, earlier change points staying above later ones.
        points = sorted(
            self._rng.randint(1, max(1, self.steps)) for _ in range(self.depth - 1)
        )
        self._change_points.update(
            {point: -(index + 1) for index, point in enumerate(points)}
        )

    # ....................... #

    def _priority(self, task: asyncio.Task[Any]) -> float:
        if task not in self._priorities:  # high band [depth, depth+1), distinct w.h.p.
            self._priorities[task] = self.depth + self._rng.random()

        return self._priorities[task]

    # ....................... #

    def reorder(self, ready: list[Any], step: int) -> list[Any]:
        tagged = [(_task_of(handle), handle) for handle in ready]

        change_to = self._change_points.get(step)
        if change_to is not None:
            present = [task for task, _ in tagged if task is not None]
            if present:
                top = max(present, key=self._priority)
                self._priorities[top] = change_to

        def rank(item: tuple[asyncio.Task[Any] | None, Any]) -> float:
            task, _handle = item
            return self._priority(task) if task is not None else float("-inf")

        # Highest priority first; plain call_soon callbacks (no task) keep FIFO at the end.
        ordered = sorted(tagged, key=rank, reverse=True)
        return [handle for _task, handle in ordered]


# ....................... #


@final
@attrs.define
class SystematicScheduler(Scheduler):
    """Deterministically pick which ready callback runs first each tick, per a choice vector.

    At tick *i* the callback at index ``choices[i] % n`` (of the ``n`` ready) is moved to the
    front; the rest keep their order. Beyond the prescribed prefix it defaults to FIFO
    (choice ``0``). It records the per-tick branching factor (``n``) so an explorer can
    enumerate the alternatives at each branch — the basis for systematic interleaving search.
    """

    _choices: Sequence[int]
    _index: int = attrs.field(default=0, init=False)
    branching: list[int] = attrs.field(factory=list, init=False)

    # ....................... #

    def reorder(self, ready: list[Any], step: int) -> list[Any]:
        del step
        size = len(ready)
        self.branching.append(size)

        choice = self._choices[self._index] if self._index < len(self._choices) else 0
        self._index += 1
        pick = choice % size if size else 0

        return ready if pick == 0 else [ready[pick], *ready[:pick], *ready[pick + 1 :]]


# ....................... #


def pct_scheduler_factory(*, depth: int = 3, steps: int = 50) -> Any:
    """A per-seed :class:`PCTScheduler` factory for the harness ``scheduler_factory`` knob."""

    def build(seed: int) -> PCTScheduler:
        return PCTScheduler(
            random.Random(seed),  # nosec B311 - deterministic sim schedule, not crypto
            depth=depth,
            steps=steps,
        )

    return build
