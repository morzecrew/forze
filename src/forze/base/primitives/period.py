"""A period between two endpoints, with its bounds convention carried in the value.

A period is four decisions, and every feature that declares one re-makes them: is the end in
force, is an open end expressible, what does overlap mean at a shared endpoint, and what is a
zero-length period. Left to prose, two modules answer them differently and the disagreement
surfaces as an off-by-one on the last day of a month.

:class:`Period` holds the answers instead. ``bounds`` travels with the endpoints, so
:meth:`Period.contains` and :meth:`Period.overlaps` are decided once rather than per caller, and
a signature that takes a period no longer needs a paragraph saying which end is included.

The default is **half-open** (``"[)"``), because consecutive periods must tile: ``[Jan 1, Feb 1)``
and ``[Feb 1, Mar 1)`` cover a timeline with no gap and no overlap, which a closed convention
cannot do without subtracting a grain somewhere. ``"[]"`` is for periods humans wrote — "valid
through 31 March" is inclusive — and saying so in the value beats subtracting a day on the way in.

Endpoints are compared as an **interval over the value space**, never normalized to the grain:
``("2026-01-01", "2026-01-02")`` with both ends excluded is not rewritten to the empty period the
way a database's discrete range type would rewrite it, so two such periods are reported as
overlapping although no ``date`` lies in either. The two conventions the framework's own
declarations use — ``"[)"`` and ``"[]"`` — are unaffected; an exclusive *start* on a ``date`` grain
is the shape to avoid when a backend constraint has to agree with this predicate.

What this is not: interval arithmetic. There is no union, intersection, difference or gap
analysis, and a consumer that needs a coverage report reads the periods and computes. What a
*day* or a *month* is, when the answer depends on a zone, belongs to the civil-time helpers that
build periods rather than to the type itself.
"""

from datetime import date, datetime
from typing import Literal, final

import attrs

from forze.base.exceptions import exc

# ----------------------- #

Bounds = Literal["[)", "[]", "(]", "()"]
"""Which endpoints are in force: ``[`` and ``]`` include, ``(`` and ``)`` exclude.

Four members rather than the two the framework's own declarations use: the predicates are
symmetric in the endpoints, so an exclusive start costs a row in a table instead of a branch in
the code."""

_START_CLOSED: frozenset[str] = frozenset({"[)", "[]"})
"""Bounds whose first endpoint is in force."""

_END_CLOSED: frozenset[str] = frozenset({"[]", "(]"})
"""Bounds whose last endpoint is in force."""

# ....................... #


@final
@attrs.define(frozen=True, slots=True)
class Period[T: (date, datetime)]:
    """A span from :attr:`start` to :attr:`end` under a :data:`Bounds` convention.

    :raises CoreException: ``validation`` when the endpoints are of different grains (a ``date``
        against a ``datetime``) or when ``end`` precedes ``start``.
    """

    start: T
    """First endpoint, in force when :attr:`bounds` opens with ``[``."""

    end: T | None = None
    """Last endpoint, in force when :attr:`bounds` closes with ``]``; ``None`` is open-ended —
    still in force, with no last moment. Never a sentinel date, which would lie in every export,
    comparison and report that met it."""

    bounds: Bounds = "[)"
    """Which endpoints are in force (:data:`Bounds`)."""

    def __attrs_post_init__(self) -> None:
        # `datetime` subclasses `date`, so a constrained type variable resolves a mixed pair to
        # `date` and a type checker passes it (verified against mypy --strict). The mismatch then
        # survives to the first comparison, where the stdlib raises `TypeError` from inside
        # whichever predicate happened to touch it. Refused here instead, naming both endpoints.
        if self.end is not None and type(self.start) is not type(self.end):
            raise exc.validation(
                f"Period endpoints are of different grains: start is "
                f"{type(self.start).__name__}, end is {type(self.end).__name__}. A period spans "
                "one grain; convert the endpoint, or build two periods."
            )

        if self.end is not None and self.end < self.start:
            raise exc.validation(f"Period ends before it starts: {self.start!r} to {self.end!r}.")

    # ....................... #

    @property
    def is_empty(self) -> bool:
        """Whether no point falls inside the period.

        True only for a zero-length period under a convention that excludes an endpoint; under
        ``"[]"`` that period is the single point ``start``. A zero-length period is **not**
        refused at construction: a booking cancelled in the instant it was made is a real row, and
        a type that rejected it would push the case back into every caller.
        """

        return self.end is not None and self.start == self.end and self.bounds != "[]"

    # ....................... #

    def contains(self, at: T) -> bool:
        """Whether *at* falls inside the period, under :attr:`bounds`.

        :raises CoreException: ``validation`` when *at* is of a different grain.
        """

        self._require_same_grain(at)

        return self._after_start(at) and self._before_end(at)

    def overlaps(self, other: "Period[T]") -> bool:
        """Whether this period and *other* share any point.

        Symmetric by construction: each period's start is tested against the other's end, and a
        shared endpoint counts only when **both** sides have it in force — so two periods whose
        conventions differ are compared under the stricter reading of the point they touch at, and
        a mixed comparison never claims more than both conventions agree on. An open end reaches
        every point its own convention admits and no earlier one — so an open-ended period whose
        start is excluded still does not meet a point at that start.

        :raises CoreException: ``validation`` when the two periods are of different grains.
        """

        self._require_same_grain(other.start)

        if self.is_empty or other.is_empty:
            return False

        return self._starts_before_end_of(other) and other._starts_before_end_of(self)

    def intersects(self, start: T, end: T | None) -> bool:
        """Whether the period overlaps the window *start* to *end*, read half-open.

        The window is a pair rather than a period because a caller asking "what falls in March"
        has two values and no convention of its own, and the half-open reading is the one that
        tiles.

        :raises CoreException: ``validation`` when the window is of a different grain, or when its
            end precedes its start.
        """

        return self.overlaps(Period(start=start, end=end, bounds="[)"))

    # ....................... #

    def _after_start(self, at: T) -> bool:
        """Whether *at* is at or past the start, counting the start only when it is in force."""

        if at == self.start:
            return self.bounds in _START_CLOSED

        return at > self.start

    def _before_end(self, at: T) -> bool:
        """Whether *at* is before the end, counting the end only when it is in force."""

        # `self.end is None` rather than a predicate: a property does not narrow the optional
        # for a type checker, and the comparisons below need it narrowed.
        if self.end is None:
            return True

        if at == self.end:
            return self.bounds in _END_CLOSED

        return at < self.end

    def _starts_before_end_of(self, other: "Period[T]") -> bool:
        """Whether this period's start falls before *other* ends.

        Half of :meth:`overlaps`, applied in both directions: at a shared point the answer needs
        this period's start *and* the other's end to be in force, which is what makes the shared
        endpoint case symmetric.
        """

        if other.end is None:
            return True

        if self.start == other.end:
            return self.bounds in _START_CLOSED and other.bounds in _END_CLOSED

        return self.start < other.end

    def _require_same_grain(self, value: T) -> None:
        if type(value) is not type(self.start):
            raise exc.validation(
                f"Period comparison mixes grains: this period holds "
                f"{type(self.start).__name__}, the other value is {type(value).__name__}."
            )
