"""Redis-backed fleet membership: one script per call, on the server's clock.

Two keys under one hash tag hold a fleet: a ZSET of member ids scored by their expiry in
milliseconds, and a HASH of member id to ``state|since_ms|report_json``. Nothing here reads a
local clock — expiry, ``since`` and the view's ``at`` all come from the server's ``TIME``, so
replicas with skewed clocks see the same fleet.
"""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

from datetime import UTC, datetime, timedelta
from typing import Final, cast, final, get_args

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
from forze.base.exceptions import exc

from ..kernel.scripts import FLEET_BEAT, FLEET_LEAVE, FLEET_VIEW
from ._logger import logger
from .base import RedisBaseAdapter

# ----------------------- #

_FLEET_SCOPE: Final[str] = "fleet"

_EPOCH: Final[datetime] = datetime(1970, 1, 1, tzinfo=UTC)

_STATES: Final[frozenset[str]] = frozenset(get_args(MemberState))

# ....................... #


def _instant(ms: int) -> datetime:
    return _EPOCH + timedelta(milliseconds=ms)


def _member(line: str) -> Member | None:
    try:
        member_id, _expiry_ms, entry = line.split("\t", 2)
        state, since_ms, report = entry.split("|", 2)

        if state not in _STATES:
            raise ValueError(f"unknown state {state!r}")

        return Member(
            id=member_id,
            state=cast(MemberState, state),
            since=_instant(int(since_ms)),
            report=decode_report(report),
        )

    except (ValueError, OverflowError, RecursionError) as error:
        # Ids are checked before they are written, so a line that does not parse came from
        # another writer — a newer release with a state this one does not know, say. It
        # reads as absent rather than failing every replica's beat.
        # The member id only: the rest of the line is the member's report.
        logger.warning(
            "Skipping fleet entry %r, which this release cannot read: %s",
            line.split("\t", 1)[0],
            error,
        )

        return None


def parse_fleet_reply(raw: str) -> FleetView:
    """Parse a fleet script's reply: the server time, then one line per live member.

    :param raw: The script's reply.
    :returns: The view.
    :raises CoreException: ``infrastructure`` when the first line is not the server time.
    """

    # ``split``, not ``splitlines``: a report may hold other line separators, such as U+2028,
    # which JSON leaves unescaped.
    head, *lines = raw.split("\n")

    try:
        now_ms = int(head)

    except ValueError as error:
        raise exc.infrastructure(
            "Fleet script reply does not start with the server time",
            code="fleet.reply_malformed",
        ) from error

    members = [member for line in lines if (member := _member(line)) is not None]

    return FleetView(members=members, at=_instant(now_ms))


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisFleetAdapter(FleetPort, RedisBaseAdapter):
    """Fleet membership in a Redis ZSET of expiries and a HASH of entries."""

    spec: FleetSpec
    """The fleet."""

    # ....................... #

    def __keys(self) -> list[str]:
        # One hash tag over the whole prefix keeps both keys in one cluster slot.
        tag = f"{{{self.construct_key(_FLEET_SCOPE, self.spec.name)}}}"

        return [f"{tag}{self.key_sep}z", f"{tag}{self.key_sep}h"]

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
        await self._prepare_keys()

        raw = await self.client.run_script(
            FLEET_BEAT,
            self.__keys(),
            [member_id, state, encoded, ttl_ms],
        )

        return parse_fleet_reply(raw)

    # ....................... #

    async def view(self) -> FleetView:
        await self._prepare_keys()

        return parse_fleet_reply(await self.client.run_script(FLEET_VIEW, self.__keys(), []))

    # ....................... #

    async def leave(self, member_id: str) -> None:
        validate_member_id(member_id)
        await self._prepare_keys()

        await self.client.run_script(FLEET_LEAVE, self.__keys(), [member_id])
