"""Delay-based concurrency limiter (Gradient2, Netflix concurrency-limits).

The AIMD controller in :class:`~forze.application.execution.resilience.state.AdaptiveBulkheadState`
reacts *after* a completion crosses a configured ``latency_threshold``. A
delay-based limiter instead has **no threshold to tune**: it learns the no-load
latency baseline and tracks the point where latency starts inflating — the knee
of the load/latency curve — contracting concurrency before errors appear.

Gradient2 keeps a long-window EWMA of round-trip latency as the baseline (the
system is uncongested most of the time, so its long average approximates the
no-load RTT) and compares it to the current sample:

    gradient   = clamp(rtt_tolerance * long_rtt / rtt, 0.5, 1.0)
    new_limit  = limit * gradient + queue_size

``gradient`` is ``1.0`` while latency sits near baseline (``rtt_tolerance``
grants headroom — e.g. 1.5 tolerates a 50% rise) and falls toward its ``0.5``
floor as latency inflates, so the limit probes up by ``queue_size`` when healthy
and contracts when the gradient bends. Increases are smoothed (gentle ramp);
decreases apply directly (react fast to congestion) — "slow up, fast down".

This is the **pure controller** only — it owns the limit math, not the
admission machinery (counter, wait queue, CoDel). It reads no clock and no
context, so it is fully deterministic under test.
"""

from __future__ import annotations

import attrs

from forze.base.exceptions import exc

# ----------------------- #


@attrs.define(slots=True)
class Gradient2Limiter:
    """Self-tuning concurrency limit driven by the latency gradient.

    Feed each completed call's round-trip ``rtt`` and the observed ``inflight``
    concurrency to :meth:`observe`; read the current admission limit from
    :attr:`limit`. No ``latency_threshold`` is required — the baseline is
    learned.
    """

    initial_limit: int = attrs.field()
    """Starting (and typical resting) concurrency limit."""

    max_limit: int = attrs.field()
    """Ceiling the limit never exceeds."""

    min_limit: int = attrs.field(default=1)
    """Floor the limit never drops below."""

    rtt_tolerance: float = attrs.field(default=1.5)
    """Latency-rise headroom before contracting: ``1.5`` tolerates a 50% rise
    over baseline before the gradient drops below ``1.0``."""

    smoothing: float = attrs.field(default=0.2)
    """EWMA factor applied to limit *increases* (gentle ramp up)."""

    long_window: int = attrs.field(default=600)
    """Samples over which the no-load baseline RTT is averaged (the larger the
    window, the more the long average reflects uncongested latency)."""

    queue_size: float = attrs.field(default=4.0)
    """Standing headroom added each step: the limit probes up by this much (then
    smoothed) while latency is healthy."""

    # ....................... #

    _limit: float = attrs.field(default=0.0, init=False)
    _long_rtt: float = attrs.field(default=0.0, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.min_limit < 1:
            raise exc.configuration("Gradient2 min_limit must be >= 1")

        if self.max_limit < self.min_limit:
            raise exc.configuration("Gradient2 max_limit must be >= min_limit")

        if not self.min_limit <= self.initial_limit <= self.max_limit:
            raise exc.configuration(
                "Gradient2 initial_limit must be within [min_limit, max_limit]"
            )

        if self.rtt_tolerance < 1.0:
            raise exc.configuration("Gradient2 rtt_tolerance must be >= 1.0")

        if not 0.0 < self.smoothing <= 1.0:
            raise exc.configuration("Gradient2 smoothing must be in (0, 1]")

        if self.long_window < 1:
            raise exc.configuration("Gradient2 long_window must be >= 1")

        if self.queue_size < 0.0:
            raise exc.configuration("Gradient2 queue_size must be >= 0")

        self._limit = float(self.initial_limit)

    # ....................... #

    @property
    def limit(self) -> int:
        """The current admission limit (floored to an integer)."""

        return int(self._limit)

    # ....................... #

    @property
    def baseline_rtt(self) -> float:
        """The learned no-load baseline RTT (0.0 before the first sample)."""

        return self._long_rtt

    # ....................... #

    def observe(self, rtt: float, inflight: int) -> float:
        """Fold one completed call's ``rtt`` (with the ``inflight`` at completion).

        Returns the updated limit. The baseline always tracks ``rtt``; the limit
        is only adjusted when ``inflight`` is at least half the current limit —
        you cannot measure congestion you are not generating, so a lightly
        loaded limiter never ratchets its limit upward.
        """

        if rtt <= 0.0:
            raise exc.validation("Gradient2 rtt must be positive")

        if self._long_rtt <= 0.0:
            self._long_rtt = rtt

        else:
            weight = 1.0 / self.long_window
            self._long_rtt = self._long_rtt * (1.0 - weight) + rtt * weight

        # No-load guard: without enough concurrency to fill the limit, the
        # latency signal carries no congestion information — hold steady.
        if inflight * 2 < self._limit:
            return self._limit

        gradient = max(0.5, min(1.0, self.rtt_tolerance * self._long_rtt / rtt))
        new_limit = self._limit * gradient + self.queue_size

        if new_limit > self._limit:
            # Slow up: ease toward the probe target so the limit ramps gently.
            new_limit = self._limit + self.smoothing * (new_limit - self._limit)

        # Fast down: a contraction (gradient < 1) applies directly, bounded by
        # the 0.5 gradient floor so a single step can at most roughly halve.
        self._limit = min(
            float(self.max_limit),
            max(float(self.min_limit), new_limit),
        )

        return self._limit
