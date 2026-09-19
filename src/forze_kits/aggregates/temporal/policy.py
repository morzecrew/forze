"""What an author declares to make an aggregate effective-dated."""

from typing import Any, final

import attrs

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.guarantees import NonOverlapping
from forze.base.exceptions import exc
from forze.base.primitives import Bounds
from forze_kits.domain.temporal.constants import VALIDITY_PERIOD

# ----------------------- #


@final
@attrs.frozen(kw_only=True)
class TemporalPolicy:
    """The key a period is scoped by, and which endpoints are in force.

    Both are required. The key because "no two periods overlap" is meaningless without saying
    *whose* periods — every relation would be one timeline. The convention because the kit reads
    it in three places that must agree: the effective-on predicate, the timeline's window, and
    the guarantee the store enforces. An aggregate that left it to a default would have those
    three agreeing by luck.

    :raises CoreException: ``configuration`` when no key field is named.
    """

    key: tuple[str, ...]
    """The fields a period is scoped by. Rows differing here never conflict."""

    bounds: Bounds = "[]"
    """Which endpoints are in force.

    ``"[]"`` by default because the origin case is a human-entered contract, where "valid
    through 31 March" includes that day. A day-aligned or instant-grained period wants
    ``"[)"``, which tiles."""

    def __attrs_post_init__(self) -> None:
        if not self.key:
            raise exc.configuration(
                "TemporalPolicy names no key field. Without one the aggregate asserts that no "
                "two rows in the whole relation overlap, which is one timeline for every "
                "record it holds and not a property any consumer has asked for.",
            )

    # ....................... #

    @property
    def guarantee(self) -> NonOverlapping:
        """The property the store must keep for this aggregate to mean anything.

        Built from the policy rather than written out by the author, so the convention the reads
        use and the convention the store enforces are the same value and cannot drift.
        """

        return NonOverlapping(key=self.key, period=VALIDITY_PERIOD, bounds=self.bounds)


# ....................... #


def assert_guarantee(spec: DocumentSpec[Any, Any, Any, Any], policy: TemporalPolicy) -> None:
    """Refuse a temporal aggregate whose spec does not declare the non-overlap guarantee.

    The kit's correctness rests on the store, not on its own write path: a read-then-insert
    check cannot be correct under concurrency, which is the accepted race in every hand-rolled
    version of this. So the declaration is not optional, and an aggregate missing it is refused
    at build rather than serving reads whose single answer is an accident.

    Compared by value rather than through a set, because ``NonOverlapping`` holds tuples and a
    ``where``-carrying sibling in the same collection makes it unhashable.

    :raises CoreException: ``configuration`` when the guarantee is absent or differs.
    """

    wanted = policy.guarantee

    if any(declared == wanted for declared in spec.guarantees):
        return

    raise exc.configuration(
        f"Document {spec.name!r} is declared temporal over {list(policy.key)} with bounds "
        f"{policy.bounds!r}, so its store must guarantee that no two of its rows under one key "
        "hold overlapping periods — and the spec does not declare it. Add it to the spec:\n"
        f"  guarantees = (NonOverlapping(key={policy.key!r}, period={VALIDITY_PERIOD!r}, "
        f"bounds={policy.bounds!r}),)\n"
        "Without it the aggregate's reads are asking for the one row in force on a day, from a "
        "store that permits several.",
        details={"document": str(spec.name), "key": list(policy.key)},
    )


# ....................... #

__all__ = ["TemporalPolicy", "assert_guarantee"]
