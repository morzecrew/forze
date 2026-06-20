"""Declarative, seeded simulated-I/O latency profiles.

A real downstream takes wall-clock time; under simulation that becomes virtual time advanced
at the port boundary (the cooperative interceptor). A :class:`LatencyProfile` declares, per
matched port call, a latency *distribution* — so a run explores realistic, varied delays
rather than a fixed number. The harness compiles it (:func:`compile_latency`) with a latency
RNG **derived from the run's master seed** (``derive_seed(seed, "latency")``), so the delays
are part of the reproducible, single-seed run — no artificial ``sleep`` in app handlers.
"""

from __future__ import annotations

import math
import random
from typing import final

import attrs

from forze.application.execution.interception import LatencyModel
from forze.base.primitives import monotonic
from forze_dst.oracle.recorder import record_event

# ----------------------- #


@final
@attrs.define(frozen=True)
class Constant:
    """A fixed latency in seconds."""

    seconds: float

    def sample(self, _rng: random.Random) -> float:
        return self.seconds


@final
@attrs.define(frozen=True)
class Uniform:
    """A latency drawn uniformly from ``[low, high]`` seconds."""

    low: float
    high: float

    def sample(self, rng: random.Random) -> float:
        return rng.uniform(self.low, self.high)


@final
@attrs.define(frozen=True)
class Exponential:
    """A latency drawn from an exponential distribution with the given *mean* (seconds)."""

    mean: float

    def sample(self, rng: random.Random) -> float:
        if self.mean <= 0.0:
            return 0.0

        return rng.expovariate(1.0 / self.mean)


@final
@attrs.define(frozen=True)
class LogNormal:
    """A heavy-tailed latency: log-normal with the given *median* (seconds) and log-space *sigma*.

    The canonical service-time model — a long right tail, so the p99 sits well above the median.
    *sigma* sets the tail heaviness (larger ⇒ fatter); the median anchors the typical case. This
    is what surfaces deadline / timeout bugs that a fixed or uniform latency never reaches.
    """

    median: float
    sigma: float = 1.0

    def sample(self, rng: random.Random) -> float:
        if self.median <= 0.0:
            return 0.0

        return rng.lognormvariate(math.log(self.median), self.sigma)


@final
@attrs.define(frozen=True)
class Pareto:
    """A power-law (Pareto) latency: minimum *scale* seconds, tail index *alpha*.

    The extreme-tail model — a small fraction of calls are dramatically slower. *alpha* governs the
    tail (smaller ⇒ heavier; ``alpha <= 1`` has an *infinite* mean, modelling pathological tails).
    Use it to stress retry/deadline logic against rare, very slow downstreams.
    """

    scale: float
    alpha: float = 1.5

    def sample(self, rng: random.Random) -> float:
        if self.scale <= 0.0 or self.alpha <= 0.0:
            return 0.0

        return self.scale * rng.paretovariate(self.alpha)


Distribution = Constant | Uniform | Exponential | LogNormal | Pareto
"""A latency distribution sampled per matched call from the seeded latency RNG. ``LogNormal`` and
``Pareto`` are heavy-tailed — realistic p99 blowups that expose timeout / deadline bugs."""


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class LatencyRule:
    """A latency distribution applied to calls matching ``surface`` / ``route`` / ``op``."""

    dist: Distribution
    surface: str | None = None
    route: str | None = None
    op: str | None = None


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class LatencyProfile:
    """An ordered set of :class:`LatencyRule` s; the first match supplies a call's latency."""

    rules: tuple[LatencyRule, ...] = ()


# ....................... #


def compile_latency(profile: LatencyProfile, rng: random.Random) -> LatencyModel:
    """Compile *profile* into a ``(surface, route, op) -> seconds`` model over one seeded RNG.

    Each call samples the first matching rule's distribution (0.0 if none match), so the
    per-call delays form a deterministic sequence for the given seed.
    """

    def model(surface: str | None, route: str | None, op: str) -> float:
        for rule in profile.rules:
            if (
                (rule.surface is None or surface == rule.surface)
                and (rule.route is None or route == rule.route)
                and (rule.op is None or op == rule.op)
            ):
                seconds = rule.dist.sample(rng)

                if seconds > 0.0:
                    # Surface the injected delay on the report's environment timeline (a no-op
                    # outside a recorded run). Stamped at the sample point, before time advances.
                    record_event(
                        "latency",
                        at=monotonic(),
                        surface=surface,
                        route=route,
                        op=op,
                        seconds=seconds,
                    )

                return seconds

        return 0.0

    return model
