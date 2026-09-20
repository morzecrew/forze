"""The two effective-dated reads, and the filters that must agree with `Period`."""

from collections.abc import Sequence
from datetime import date
from typing import Any

import attrs

from forze.application.contracts.document import DocumentQueryPort
from forze.application.contracts.execution import Handler
from forze.application.contracts.querying import (
    QueryFilterExpression,
    QueryValueOpConjunction,
)
from forze.base.exceptions import exc
from forze.base.primitives import Bounds, Period
from forze.domain.models import ReadDocument
from forze_kits.domain.temporal.constants import VALID_FROM_FIELD, VALID_TO_FIELD
from forze_kits.dto.paginated import Paginated, Pagination

from .dto import EffectiveOnDTO, TimelineDTO
from .policy import TemporalPolicy

# ----------------------- #


def _at_or_before(value: date, *, inclusive: bool) -> QueryValueOpConjunction:
    """``<= value`` when the endpoint is in force, ``< value`` when it is not."""

    return {"$lte": value} if inclusive else {"$lt": value}


def _at_or_after(value: date, *, inclusive: bool) -> QueryValueOpConjunction:
    """``>= value`` when the endpoint is in force, ``> value`` when it is not."""

    return {"$gte": value} if inclusive else {"$gt": value}


# ....................... #


def _still_open(cmp: QueryValueOpConjunction) -> QueryFilterExpression:
    """A row whose end is null, or satisfies *cmp*.

    Null is open-ended rather than missing, so it is on the "still in force" side of every
    comparison — the reason the vocabulary has ``$null`` and the schema has no sentinel date.
    """

    return {
        "$or": [
            {"$values": {VALID_TO_FIELD: {"$null": True}}},
            {"$values": {VALID_TO_FIELD: cmp}},
        ]
    }


# ....................... #


def _key_filter(key: tuple[str, ...], values: dict[str, Any]) -> QueryFilterExpression:
    """The key's fields pinned to *values*, refusing a caller that named others.

    An unnamed key field would widen the read to every row in the relation and an extra one
    would narrow it to none; both answer a different question than the caller asked, and
    silently. So the match is exact.

    :raises CoreException: ``validation`` when the supplied fields are not the key's.
    """

    supplied = set(values)
    declared = set(key)

    if supplied != declared:
        raise exc.validation(
            f"This aggregate's periods are scoped by {sorted(declared)}, and the read supplied "
            f"{sorted(supplied)}. A key field left out reads every row in the relation as one "
            "timeline; one added reads none.",
            details={"expected": sorted(declared), "received": sorted(supplied)},
        )

    return {"$values": {field: values[field] for field in key}}


# ....................... #


def effective_on_filter(
    key: tuple[str, ...],
    values: dict[str, Any],
    on: date,
    bounds: Bounds,
    restrict: Sequence[QueryFilterExpression] = (),
) -> QueryFilterExpression:
    """Rows under *values* whose period is in force on *on*.

    This is :meth:`~forze.base.primitives.Period.contains` expressed in the filter language, and
    the two are pinned against each other: the store answering one way while the value object
    answers the other is a row a caller believed in force that no read returns.

    *restrict* carries what the aggregate's **other** arms exclude — soft-deleted rows, rows
    that are not the current version. These reads build their own filter rather than passing
    through the mapper the generated reads share, so an arm's restriction reaches them only by
    being conjoined here; without it a composed aggregate answers "what is in force" with a row
    it hides from every other read.
    """

    return {
        "$and": [
            _key_filter(key, values),
            {"$values": {VALID_FROM_FIELD: _at_or_before(on, inclusive=bounds[0] == "[")}},
            _still_open(_at_or_after(on, inclusive=bounds[1] == "]")),
            *restrict,
        ]
    }


# ....................... #


def timeline_filter(
    key: tuple[str, ...],
    values: dict[str, Any],
    start: date,
    end: date | None,
    bounds: Bounds,
    restrict: Sequence[QueryFilterExpression] = (),
) -> QueryFilterExpression:
    """Rows under *values* whose period meets the window ``start``–``end``.

    :meth:`~forze.base.primitives.Period.overlaps` in the filter language, over two periods
    sharing one convention: they meet when each one starts before the other ends, and "before"
    admits equality only when both of those endpoints are in force — which is ``"[]"`` and
    nothing else, since any other convention excludes one side of that touch.

    An open window end, like an open period end, is on the "still in force" side.

    Exact over the rows this kit stores, which are never empty — an empty period overlaps
    nothing at all, and that is a fact about the row rather than about the window, so the filter
    language cannot express it: it compares a field to a value, never to another field. The kit
    refuses to write one instead, which is why no row reaching here can be one.

    :raises CoreException: ``validation`` when the window itself is empty.
    """

    window = Period(start, end, bounds)

    if window.is_empty:
        raise exc.validation(
            f"The window {start.isoformat()}–{end.isoformat() if end else '∞'} under bounds "
            f"{bounds!r} is in force on no day, so nothing can meet it. Widen it, or declare "
            "the convention that puts its endpoints in force.",
            details={"start": start.isoformat(), "bounds": bounds},
        )

    # Two periods sharing one convention meet when each starts before the other ends, and
    # "before" admits equality only where both of those endpoints are in force — which is `[]`
    # and nothing else, since every other convention excludes one side of that touch.
    touching = bounds == "[]"
    clauses: list[QueryFilterExpression] = [
        _key_filter(key, values),
        _still_open(_at_or_after(start, inclusive=touching)),
        *restrict,
    ]

    if end is not None:
        clauses.append({"$values": {VALID_FROM_FIELD: _at_or_before(end, inclusive=touching)}})

    return {"$and": clauses}


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class EffectiveOn[Out: ReadDocument](Handler[EffectiveOnDTO, Out]):
    """The row in force for one key on a given day.

    One row or nothing: the declared guarantee is what makes "the row" a well-formed request,
    since a store permitting two overlapping periods would make this an arbitrary choice
    between them. A single-row query rather than a filtered chain, for the reason every read
    here is bounded — an unbounded one meets the store's implicit cap and answers from a slice
    without knowing it did.
    """

    query: DocumentQueryPort[Out]
    """Query port for the temporal aggregate."""

    policy: TemporalPolicy
    """The key and the convention this aggregate declared."""

    restrict: tuple[QueryFilterExpression, ...] = ()
    """What the aggregate's other arms exclude from every read."""

    # ....................... #

    async def __call__(self, args: EffectiveOnDTO) -> Out:
        """Return the row in force on *args.on*.

        :param args: The key's values and the day.
        :returns: The row in force then.
        :raises CoreException: ``not_found`` when no row covers that day.
        """

        page = await self.query.find_page(
            filters=effective_on_filter(
                self.policy.key, dict(args.key), args.on, self.policy.bounds, self.restrict
            ),
            sorts={VALID_FROM_FIELD: "desc"},
            pagination=Pagination(page=1, size=1).to_offset_expression(),
        )

        for row in page.hits:
            return row

        raise exc.not_found(
            f"Nothing is in force for this key on {args.on.isoformat()}.",
            details={"key": dict(args.key), "on": args.on.isoformat()},
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class Timeline[Out: ReadDocument](Handler[TimelineDTO, Paginated[Out]]):
    """Every row for one key whose period meets a window, earliest first.

    The operation the hand-rolled version of this aggregate did not have: without it, "which
    version applied on each day of March" is asked once per day, which is thirty round trips to
    answer one question about a month.
    """

    query: DocumentQueryPort[Out]
    """Query port for the temporal aggregate."""

    policy: TemporalPolicy
    """The key and the convention this aggregate declared."""

    restrict: tuple[QueryFilterExpression, ...] = ()
    """What the aggregate's other arms exclude from every read."""

    # ....................... #

    async def __call__(self, args: TimelineDTO) -> Paginated[Out]:
        """Return the rows meeting the window, in period order.

        :param args: The key's values, the window and the page.
        :returns: The matching rows, earliest first.
        """

        page = await self.query.find_page(
            filters=timeline_filter(
                self.policy.key,
                dict(args.key),
                args.start,
                args.end,
                self.policy.bounds,
                self.restrict,
            ),
            sorts={VALID_FROM_FIELD: "asc"},
            pagination=args.to_offset_expression(),
        )

        return Paginated.from_page(page)


# ....................... #

__all__ = ["EffectiveOn", "Timeline", "effective_on_filter", "timeline_filter"]
