"""Generative scenario model — meaningful workloads instead of random noise.

Independent random operations on a real domain mostly bounce off validation (you cannot
``pay`` an order that was never created), so a naive workload explores a shallow slice. A
:class:`Scenario` fixes that with a two-phase, model-based design:

* **arrange** (serial) — fire rules that build valid state, capturing each operation's real
  return (e.g. a created id) into the :class:`ModelState`. Because the run is seeded, the
  same ids are produced on replay, so a minimized counterexample stays valid.
* **act** (concurrent) — sample rules that operate on the arranged state, building their
  inputs from the captured handles, then run them concurrently under scheduler
  perturbation. This is the raced phase where concurrency bugs surface, and the phase that
  gets minimized.

A :class:`Rule` maps a model decision to an operation: what it ``requires`` (the state
pools that gate it), how it builds its ``arg`` from the model, and (for arrange) what it
``produces`` back into the model. The author supplies thin domain hints; the operation
registry and deps stay exactly as in :class:`~forze_dst.harness.Simulation`. Drive a
scenario with :meth:`Simulation.explore_scenario`.
"""

from __future__ import annotations

import random
from typing import Any, Callable, final

import attrs

# ----------------------- #


@final
@attrs.define
class ModelState:
    """The author's model of the world: named pools of handles produced during arrange.

    A handle is whatever an arrange rule captures from an operation's result — typically an
    entity id. Act rules read pools to decide enablement and to build their inputs.
    """

    _pools: dict[str, list[Any]] = attrs.field(factory=dict, init=False)

    # ....................... #

    def add(self, kind: str, handle: Any) -> None:
        """Record a produced *handle* into the *kind* pool."""

        self._pools.setdefault(kind, []).append(handle)

    # ....................... #

    def pool(self, kind: str) -> tuple[Any, ...]:
        """All handles in the *kind* pool, in production order."""

        return tuple(self._pools.get(kind, ()))

    # ....................... #

    def count(self, kind: str) -> int:
        """How many handles the *kind* pool holds."""

        return len(self._pools.get(kind, ()))

    # ....................... #

    def has(self, *kinds: str) -> bool:
        """Whether every named pool is non-empty (the default precondition)."""

        return all(self._pools.get(kind) for kind in kinds)

    # ....................... #

    def pick(self, kind: str, rng: random.Random) -> Any:
        """Pick a handle from the *kind* pool (raises if empty — guard with :meth:`has`)."""

        if items := self._pools.get(kind):
            return rng.choice(items)

        else:
            raise KeyError(f"model pool {kind!r} is empty")


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class Rule:
    """A model decision bound to an operation: its gate, input source, and what it yields."""

    op: str
    """The operation key to invoke."""

    arg: Callable[[ModelState, random.Random], Any] = lambda _state, _rng: None
    """Build the operation input from the model + a seeded RNG. Default ``None`` input."""

    requires: tuple[str, ...] = ()
    """Pools that must be non-empty for this rule to be enabled (its inputs reference them)."""

    produces: str | None = None
    """For an arrange rule: the pool its captured result is added to. ``None`` → produces nothing."""

    capture: Callable[[Any], Any] = lambda result: result
    """Extract the handle to produce from the operation's result (default: the result itself)."""

    enabled: Callable[[ModelState], bool] | None = None
    """Extra precondition beyond ``requires`` (ANDed). ``None`` → no extra condition."""

    weight: float = 1.0
    """Selection weight when sampling act rules."""

    # ....................... #

    def is_enabled(self, state: ModelState) -> bool:
        """Whether this rule may fire against *state*."""

        if self.requires and not state.has(*self.requires):
            return False

        return self.enabled(state) if self.enabled is not None else True


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class Scenario:
    """A two-phase generative model: arrange (serial, builds state) then act (concurrent)."""

    state: Callable[[], ModelState]
    """Build a fresh model state per run (so each run starts clean)."""

    arrange: tuple[Rule, ...] = ()
    """Rules fired serially, in order, to build valid state. List a rule N times for N entities."""

    act: tuple[Rule, ...] = ()
    """Rules sampled into the concurrent, raced, minimized phase."""

    # ....................... #

    def enabled_act(self, state: ModelState) -> list[Rule]:
        """The act rules whose preconditions hold against *state*."""

        return [rule for rule in self.act if rule.is_enabled(state)]

    # ....................... #

    def generate_act(
        self,
        state: ModelState,
        count: int,
        rng: random.Random,
    ) -> list[tuple[str, Any]]:
        """Sample *count* enabled act calls — ``(op, arg)`` pairs built from the model."""

        rules = self.enabled_act(state)

        if not rules:
            return []

        chosen = rng.choices(rules, weights=[rule.weight for rule in rules], k=count)

        return [(rule.op, rule.arg(state, rng)) for rule in chosen]
