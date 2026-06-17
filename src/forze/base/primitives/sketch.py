"""Mergeable, relative-error quantile sketch (DDSketch, Masson et al. VLDB 2019).

Where :class:`~forze.base.primitives.quantile.P2Quantile` tracks
a *single* quantile per estimator in five floats with no error guarantee, a
DDSketch tracks the *whole distribution* and answers any quantile with a bounded
**relative** error, and — crucially — two sketches over disjoint streams can be
**merged** into one sketch over the union. That makes it the right tool for
fleet-wide latency (combine per-replica sketches) and for reporting p50/p90/p99
from one structure, neither of which P² can do. It is not a P² replacement: for
a single-quantile in-process control loop (hedge delay, bulkhead congestion) P²
stays leaner.

The construction is a logarithmic bucketing: with relative accuracy ``alpha`` and
``gamma = (1 + alpha) / (1 - alpha)``, a positive value ``x`` lands in bucket
``key(x) = ceil(log_gamma(x))`` whose representative ``2 * gamma**key / (gamma + 1)``
is within ``alpha`` relative error of every value the bucket holds. Buckets are a
sparse ``{key: count}`` map plus a separate zero count; the map is capped at
``max_bins`` by **collapsing the lowest** keys (the smallest values), so the tail
that matters for latency keeps full accuracy under unbounded dynamic range.
"""

from __future__ import annotations

import math

import attrs

from forze.base.exceptions import exc

# ----------------------- #

_DEFAULT_RELATIVE_ACCURACY = 0.01
"""1% relative error: p99 estimate within 1% of the true p99."""

_DEFAULT_MAX_BINS = 2048
"""Bucket-count cap. ~1k buckets already span a 1e9 dynamic range at 1% accuracy,
so this is a safety bound that collapsing rarely reaches in latency workloads."""

# ....................... #


@attrs.define(slots=True)
class DDSketch:
    """Streaming relative-error quantile sketch over non-negative values.

    Feed observations with :meth:`observe`; :meth:`quantile` answers any quantile
    in ``[0, 1]``, or ``None`` while the sketch is empty (matching the warm-up
    contract of :class:`P2Quantile`). :meth:`merge` folds another sketch (same
    ``relative_accuracy``) into this one, order-independently.
    """

    relative_accuracy: float = attrs.field(default=_DEFAULT_RELATIVE_ACCURACY)
    """Quantile error bound ``alpha`` in ``(0, 1)``: every estimate is within
    ``alpha`` relative error of the true value."""

    max_bins: int = attrs.field(default=_DEFAULT_MAX_BINS)
    """Maximum distinct buckets; the lowest are collapsed past this."""

    # ....................... #

    _gamma: float = attrs.field(default=0.0, init=False)
    _multiplier: float = attrs.field(default=0.0, init=False)
    _bins: dict[int, int] = attrs.field(factory=dict, init=False)
    _zero_count: int = attrs.field(default=0, init=False)
    _count: int = attrs.field(default=0, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not 0.0 < self.relative_accuracy < 1.0:
            raise exc.configuration("DDSketch relative_accuracy must be in (0, 1)")

        if self.max_bins < 1:
            raise exc.configuration("DDSketch max_bins must be >= 1")

        self._gamma = (1.0 + self.relative_accuracy) / (1.0 - self.relative_accuracy)
        self._multiplier = 1.0 / math.log(self._gamma)

    # ....................... #

    @property
    def count(self) -> int:
        """Number of observations consumed."""

        return self._count

    # ....................... #

    def observe(self, x: float) -> None:
        """Consume one non-negative observation.

        ``x == 0`` is tracked in a dedicated zero bucket (its log is undefined);
        a negative ``x`` is a programming error for a latency/size sketch.
        """

        if x < 0.0:
            raise exc.validation("DDSketch observations must be non-negative")

        self._count += 1

        if x == 0.0:
            self._zero_count += 1

            return

        key = math.ceil(math.log(x) * self._multiplier)
        self._bins[key] = self._bins.get(key, 0) + 1

        if len(self._bins) > self.max_bins:
            self._collapse_lowest()

    # ....................... #

    def _collapse_lowest(self) -> None:
        """Fold the smallest-key buckets into the next-smallest until within cap.

        Latency cares about the tail, so accuracy is sacrificed at the low end.
        Rare in practice (only past ``max_bins`` distinct buckets), so the
        ``sorted`` pass is acceptable.
        """

        keys = sorted(self._bins)

        while len(self._bins) > self.max_bins:
            lowest = keys.pop(0)
            self._bins[keys[0]] += self._bins.pop(lowest)

    # ....................... #

    def _bin_value(self, key: int) -> float:
        """Representative value of a bucket: within ``relative_accuracy`` of its members.

        ``2 * gamma**key / (gamma + 1)``, computed via ``exp`` to stay stable
        across the full key range.
        """

        return 2.0 * math.exp(key / self._multiplier) / (self._gamma + 1.0)

    # ....................... #

    def quantile(self, q: float) -> float | None:
        """The estimated ``q``-quantile, or ``None`` while the sketch is empty."""

        if not 0.0 <= q <= 1.0:
            raise exc.validation("DDSketch quantile q must be in [0, 1]")

        if self._count == 0:
            return None

        rank = q * (self._count - 1)

        if rank < self._zero_count:
            return 0.0

        cumulative = self._zero_count

        for key in sorted(self._bins):
            cumulative += self._bins[key]

            if cumulative > rank:
                return self._bin_value(key)

        # Floating-point slack at q == 1.0: fall back to the top bucket.
        return self._bin_value(max(self._bins))

    # ....................... #

    def merge(self, other: DDSketch) -> None:
        """Fold ``other`` into this sketch (in place); both must share ``relative_accuracy``."""

        if other.relative_accuracy != self.relative_accuracy:
            raise exc.configuration(
                "cannot merge DDSketch instances with different relative_accuracy"
            )

        for key, cnt in other._bins.items():
            self._bins[key] = self._bins.get(key, 0) + cnt

        self._zero_count += other._zero_count
        self._count += other._count

        if len(self._bins) > self.max_bins:
            self._collapse_lowest()

    # ....................... #

    @classmethod
    def merged(cls, first: DDSketch, *rest: DDSketch) -> DDSketch:
        """A fresh sketch combining ``first`` and ``rest`` (order-independent)."""

        out = cls(
            relative_accuracy=first.relative_accuracy,
            max_bins=first.max_bins,
        )
        out.merge(first)

        for sketch in rest:
            out.merge(sketch)

        return out


# ....................... #


@attrs.define(slots=True)
class WindowedDDSketch:
    """DDSketch with bounded staleness: two overlapping sketches, rotated.

    The same two-estimator rotation as
    :class:`~forze.base.primitives.quantile.WindowedP2Quantile`:
    every observation feeds both sketches, and every ``window`` observations the
    older one is dropped and a fresh one warms. Reads serve from the older
    sketch (between one and two windows of history), so a shifted distribution
    is reflected within at most ``2 * window`` observations instead of being
    averaged against all history.
    """

    relative_accuracy: float = attrs.field(default=_DEFAULT_RELATIVE_ACCURACY)
    """Quantile error bound ``alpha`` in ``(0, 1)`` (see :class:`DDSketch`)."""

    window: int = attrs.field(default=512)
    """Observations per rotation; staleness is bounded by ``2 * window``."""

    max_bins: int = attrs.field(default=_DEFAULT_MAX_BINS)
    """Per-sketch bucket cap (see :class:`DDSketch`)."""

    # ....................... #

    _old: DDSketch = attrs.field(
        default=attrs.Factory(
            lambda self: DDSketch(
                relative_accuracy=self.relative_accuracy,
                max_bins=self.max_bins,
            ),
            takes_self=True,
        ),
        init=False,
    )
    _young: DDSketch = attrs.field(
        default=attrs.Factory(
            lambda self: DDSketch(
                relative_accuracy=self.relative_accuracy,
                max_bins=self.max_bins,
            ),
            takes_self=True,
        ),
        init=False,
    )
    _since_rotation: int = attrs.field(default=0, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.window < 1:
            raise exc.configuration("WindowedDDSketch window must be >= 1")

    # ....................... #

    def observe(self, x: float) -> None:
        """Consume one observation; rotate sketches at the window boundary."""

        self._old.observe(x)
        self._young.observe(x)
        self._since_rotation += 1

        if self._since_rotation >= self.window:
            self._old = self._young
            self._young = DDSketch(
                relative_accuracy=self.relative_accuracy,
                max_bins=self.max_bins,
            )
            self._since_rotation = 0

    # ....................... #

    def quantile(self, q: float) -> float | None:
        """The estimated ``q``-quantile over recent history, or ``None`` while empty."""

        return self._old.quantile(q)
