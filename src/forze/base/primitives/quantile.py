"""Streaming quantile estimation (P², Jain & Chlamtac 1985).

Five floats per quantile, no sample storage: the estimator maintains five
markers (min, p/2, p, (1+p)/2, max) whose heights are adjusted toward their
ideal positions with a piecewise-parabolic interpolation as observations
stream in. Accuracy is within a few percent of the true quantile for
realistic latency distributions — exactly the fidelity a hedge delay needs,
at O(1) memory and a handful of float ops per observation.

P² is cumulative: all history weighs equally, so a shifted distribution is
tracked sluggishly. :class:`WindowedP2Quantile` bounds that staleness with
two overlapping estimators rotated every ``window`` observations — the
serving estimator never holds more than two windows of history.
"""

import attrs

from forze.base.exceptions import exc

# ----------------------- #

_MARKERS = 5
"""P² marker count (min, p/2, p, (1+p)/2, max)."""


@attrs.define(slots=True)
class P2Quantile:
    """Single-quantile P² estimator.

    Feed observations with :meth:`observe`; :meth:`value` returns the current
    estimate, or ``None`` until the first five observations have seeded the
    markers (the algorithm is undefined before that — callers fall back).
    """

    p: float
    """Target quantile in ``(0, 1)``."""

    # ....................... #

    _heights: list[float] = attrs.field(factory=list, init=False)
    _positions: list[float] = attrs.field(factory=list, init=False)
    _desired: list[float] = attrs.field(factory=list, init=False)
    _increments: list[float] = attrs.field(factory=list, init=False)
    _count: int = attrs.field(default=0, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not 0.0 < self.p < 1.0:
            raise exc.configuration("P2 quantile p must be in (0, 1)")

    # ....................... #

    @property
    def count(self) -> int:
        """Number of observations consumed."""

        return self._count

    # ....................... #

    def observe(self, x: float) -> None:
        """Consume one observation."""

        self._count += 1

        if self._count <= _MARKERS:
            self._heights.append(x)

            if self._count == _MARKERS:
                self._heights.sort()
                self._positions = [1.0, 2.0, 3.0, 4.0, 5.0]
                p = self.p
                self._desired = [1.0, 1.0 + 2.0 * p, 1.0 + 4.0 * p, 3.0 + 2.0 * p, 5.0]
                self._increments = [0.0, p / 2.0, p, (1.0 + p) / 2.0, 1.0]

            return

        q, n, nd = self._heights, self._positions, self._desired

        # Locate the cell and update the extremes.
        if x < q[0]:
            q[0] = x
            k = 0

        elif x >= q[4]:
            q[4] = x
            k = 3

        else:
            k = 0

            while k < 3 and q[k + 1] <= x:
                k += 1

        for i in range(k + 1, _MARKERS):
            n[i] += 1.0

        for i in range(_MARKERS):
            nd[i] += self._increments[i]

        # Nudge the three interior markers toward their desired positions.
        for i in range(1, 4):
            d = nd[i] - n[i]

            if (d >= 1.0 and n[i + 1] - n[i] > 1.0) or (
                d <= -1.0 and n[i - 1] - n[i] < -1.0
            ):
                step = 1.0 if d > 0 else -1.0
                candidate = self._parabolic(i, step)

                q[i] = (
                    candidate
                    if q[i - 1] < candidate < q[i + 1]
                    else self._linear(i, step)
                )

                n[i] += step

    # ....................... #

    def _parabolic(self, i: int, d: float) -> float:
        q, n = self._heights, self._positions

        return q[i] + d / (n[i + 1] - n[i - 1]) * (
            (n[i] - n[i - 1] + d) * (q[i + 1] - q[i]) / (n[i + 1] - n[i])
            + (n[i + 1] - n[i] - d) * (q[i] - q[i - 1]) / (n[i] - n[i - 1])
        )

    # ....................... #

    def _linear(self, i: int, d: float) -> float:
        q, n = self._heights, self._positions
        j = i + int(d)

        return q[i] + d * (q[j] - q[i]) / (n[j] - n[i])

    # ....................... #

    def value(self) -> float | None:
        """The current quantile estimate, or ``None`` before five observations."""

        return None if self._count < _MARKERS else self._heights[2]


# ....................... #


@attrs.define(slots=True)
class WindowedP2Quantile:
    """P² with bounded staleness: two overlapping estimators, rotated.

    Every observation feeds both estimators; every ``window`` observations the
    older one is dropped and a fresh one starts warming. Reads serve from the
    older estimator (between one and two windows of history), so a shifted
    latency distribution is fully reflected within at most two windows instead
    of being averaged against all history.
    """

    p: float
    """Target quantile in ``(0, 1)``."""

    window: int = attrs.field(default=512)
    """Observations per rotation; staleness is bounded by ``2 * window``."""

    # ....................... #

    _old: P2Quantile = attrs.field(
        default=attrs.Factory(lambda self: P2Quantile(p=self.p), takes_self=True),
        init=False,
    )
    _young: P2Quantile = attrs.field(
        default=attrs.Factory(lambda self: P2Quantile(p=self.p), takes_self=True),
        init=False,
    )
    _since_rotation: int = attrs.field(default=0, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not 0.0 < self.p < 1.0:
            raise exc.configuration("Windowed P2 quantile p must be in (0, 1)")

        if self.window < 5:
            raise exc.configuration("Windowed P2 window must be >= 5")

    # ....................... #

    def observe(self, x: float) -> None:
        """Consume one observation; rotate estimators at the window boundary."""

        self._old.observe(x)
        self._young.observe(x)
        self._since_rotation += 1

        if self._since_rotation >= self.window:
            self._old = self._young
            self._young = P2Quantile(p=self.p)
            self._since_rotation = 0

    # ....................... #

    def value(self) -> float | None:
        """The current estimate, or ``None`` before five observations."""

        return self._old.value()
