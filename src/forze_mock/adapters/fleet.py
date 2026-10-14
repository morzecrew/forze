"""In-memory fleet membership on an injectable monotonic clock."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Final, final

import attrs

from forze.application.contracts.fleet import (
    FleetPort,
    FleetSpec,
    FleetView,
    Member,
    MemberState,
    Report,
    decode_report,
    prepare_beat,
    validate_member_id,
)
from forze.base.primitives import monotonic
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin

# ----------------------- #

_EPOCH: Final[datetime] = datetime(1970, 1, 1, tzinfo=UTC)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockFleetAdapter(MockTenancyMixin, FleetPort):
    """The Redis fleet's model in memory: per-member expiries on one clock, read whole.

    The clock stands in for the server's: it alone decides expiry, and a member is alive
    while its expiry is still ahead of it. The view's datetimes count from the clock's own
    epoch, so compare a member's ``since`` with the view's ``at``, never with a wall clock.
    """

    spec: FleetSpec
    state: MockState
    namespace: str

    clock: Callable[[], float] = monotonic
    """Seconds on a monotonic clock; inject one to move time in a test."""

    # ....................... #

    def _members(self) -> dict[str, tuple[int, MemberState, int, str]]:
        route = self._partitioned_namespace(self.namespace)

        return self.state.fleets.setdefault(route, {})

    def _now_ms(self) -> int:
        return int(self.clock() * 1000)

    @staticmethod
    def _view(members: dict[str, tuple[int, MemberState, int, str]], now_ms: int) -> FleetView:
        return FleetView(
            members=[
                Member(
                    id=member_id,
                    state=state,
                    since=_EPOCH + timedelta(milliseconds=since_ms),
                    report=decode_report(report),
                )
                for member_id, (expiry_ms, state, since_ms, report) in members.items()
                if expiry_ms > now_ms
            ],
            at=_EPOCH + timedelta(milliseconds=now_ms),
        )

    # ....................... #

    async def beat(
        self,
        member_id: str,
        state: MemberState,
        report: Report,
        *,
        ttl: timedelta | None = None,
    ) -> FleetView:
        encoded, ttl_ms = prepare_beat(self.spec, member_id, state, report, ttl)
        now_ms = self._now_ms()

        with self.state.lock:
            members = self._members()

            for expired in [m for m, entry in members.items() if entry[0] <= now_ms]:
                del members[expired]

            previous = members.get(member_id)
            since_ms = previous[2] if previous is not None and previous[1] == state else now_ms
            members[member_id] = (now_ms + ttl_ms, state, since_ms, encoded)

            return self._view(members, now_ms)

    # ....................... #

    async def view(self) -> FleetView:
        now_ms = self._now_ms()

        with self.state.lock:
            return self._view(self._members(), now_ms)

    # ....................... #

    async def leave(self, member_id: str) -> None:
        validate_member_id(member_id)

        with self.state.lock:
            self._members().pop(member_id, None)
