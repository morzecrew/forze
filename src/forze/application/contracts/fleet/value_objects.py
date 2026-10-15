"""A fleet's members and the view a beat returns, with the policies apps compute over it.

Every policy is a pure function of one view, so two replicas holding the same view always
reach the same answer. Replicas reading at different moments can hold different views, so
the answers agree per view, not across views.
"""

import hashlib
import math
from collections.abc import Collection, Mapping
from datetime import datetime, timedelta
from typing import Literal, final

import attrs

from forze.base.primitives import MappingConverter

# ----------------------- #

MemberState = Literal["starting", "serving", "draining"]
"""Where a member is in its life: joining, taking work, or on its way out."""

Report = Mapping[str, int | float | str]
"""A small flat map a member publishes with its entry, such as its load."""

_SERVING: tuple[MemberState, ...] = ("serving",)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class Member:
    """One live member of a fleet, as the backend last saw it."""

    id: str
    """The member id the replica beats with."""

    state: MemberState
    """The state the member last beat with."""

    since: datetime
    """When the member entered :attr:`state`, on the backend's clock.

    Compare it with :attr:`FleetView.at`, never with a local clock.
    """

    report: Report = attrs.field(converter=MappingConverter.frozen)  # type: ignore[misc]
    """What the member reported on its last beat."""


# ....................... #


def _sorted_by_id(members: Collection[Member]) -> tuple[Member, ...]:
    return tuple(sorted(members, key=lambda member: member.id))


def _number(value: object) -> float | None:
    # ``bool`` is an ``int`` subclass; a report flag is not a quantity.
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None

    try:
        return float(value)

    except OverflowError:  # an integer past any float: not a load to average
        return None


def _weight(key: str, member_id: str) -> int:
    # A process-independent hash: Python's ``hash()`` is salted per process, so two
    # replicas would rank the same key differently.
    digest = hashlib.blake2b(f"{key}\0{member_id}".encode(), digest_size=8).digest()

    return int.from_bytes(digest, "big")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class FleetView:
    """The live members of a fleet at one instant of the backend's clock."""

    members: tuple[Member, ...] = attrs.field(converter=_sorted_by_id)
    """Every member whose entry has not expired, sorted by id."""

    at: datetime
    """The backend's clock when the view was read."""

    # ....................... #

    def _serving(self) -> list[Member]:
        return [member for member in self.members if member.state == "serving"]

    # ....................... #

    def settled(self, for_: timedelta) -> bool:
        """Whether the fleet has stopped changing.

        True when the view has at least one member, every member is serving, and each has
        been serving for at least *for_* by the view's own clock.

        :param for_: How long every member must have been serving.
        :returns: ``True`` when the fleet is settled.
        """

        return bool(self.members) and all(
            member.state == "serving" and self.at - member.since >= for_ for member in self.members
        )

    # ....................... #

    def share(self, total: int, me: str) -> int:
        """This member's part of *total* units split evenly over the serving members.

        Members are taken in id order, and the first ``total % n`` get one extra, so the
        shares sum to *total* and differ by at most one.

        :param total: The number of units to split.
        :param me: This member's id.
        :returns: The number of units that are *me*'s; ``0`` when *me* is not serving.
        """

        ids = [member.id for member in self._serving()]

        if me not in ids:
            return 0

        base, extra = divmod(total, len(ids))

        return base + (1 if ids.index(me) < extra else 0)

    # ....................... #

    def rank(
        self,
        key: str,
        *,
        states: Collection[MemberState] = _SERVING,
    ) -> tuple[str, ...]:
        """The members in *states*, in rendezvous order for *key*.

        The first id owns the unit *key*, and the rest are its successors in order. Removing
        a member moves only the keys it owned. The order is the same in every process.

        :param key: The unit being placed, such as a tenant id.
        :param states: The member states that are candidates.
        :returns: Candidate member ids, the owner first.
        """

        allowed = frozenset(states)
        candidates = [member.id for member in self.members if member.state in allowed]

        return tuple(
            sorted(
                candidates,
                key=lambda member_id: (_weight(key, member_id), member_id),
                reverse=True,
            )
        )

    # ....................... #

    def excess(
        self,
        me: str,
        *,
        load: str,
        movable: str | None = None,
        tolerance: float = 0.15,
        settle: timedelta,
        smallest: int = 1,
    ) -> int:
        """How many units this member holds above the fleet's mean load.

        The mean is over the serving members whose *load* report is a number. The result is
        ``0`` unless the fleet is :meth:`settled` for *settle*, *me* is serving and reports
        *load*, and that load exceeds ``mean * (1 + tolerance)``. Then it is
        ``ceil(load - mean)``, capped by the floor of *me*'s *movable* report when that is a
        number. A result below *smallest* is ``0``.

        :param me: This member's id.
        :param load: The report key holding each member's load.
        :param movable: The report key holding how many units *me* could move, if any.
        :param tolerance: How far above the mean, relative to it, is still fine.
        :param settle: How long the fleet must have been settled.
        :param smallest: The smallest excess worth acting on.
        :returns: A whole number of units, ``0`` or more.
        """

        if not self.settled(settle):
            return 0

        serving = {member.id: member for member in self._serving()}
        loads = {
            member_id: value
            for member_id, member in serving.items()
            if (value := _number(member.report.get(load))) is not None
        }

        mine = loads.get(me)

        if mine is None:
            return 0

        # Divided before summing: a sum of large finite loads can overflow to infinity.
        mean = sum(value / len(loads) for value in loads.values())

        if mine <= mean * (1 + tolerance):
            return 0

        result = math.ceil(mine - mean)

        if movable is not None:
            cap = _number(serving[me].report.get(movable))

            if cap is not None:
                result = min(result, math.floor(cap))

        return result if result >= max(smallest, 1) else 0
