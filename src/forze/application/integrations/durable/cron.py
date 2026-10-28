"""Cron next-fire computation for the self-hosted durable scheduler.

Deterministic: the next fire is a pure function of the expression, the base instant, and
the timezone — so it is safe under deterministic simulation (no wall clock read here; the
caller passes the instant from the time source).
"""

from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter

from forze.base.exceptions import exc

# ----------------------- #


def validate_cron(expression: str, *, tz: str | None = None) -> None:
    """Reject an invalid cron *expression* (and *tz*) at wiring time, loudly."""

    if not croniter.is_valid(expression):
        raise exc.validation(f"Invalid cron expression: {expression!r}")

    if tz is not None:
        try:
            ZoneInfo(tz)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise exc.validation(f"Unknown timezone: {tz!r}") from error


# ....................... #


def next_cron_fire(
    expression: str,
    *,
    after: datetime,
    tz: str | None = None,
) -> datetime:
    """The first cron occurrence strictly after *after*, as a tz-aware UTC datetime.

    Fire-once / skip-missed: the next fire is computed relative to the given instant, so a
    scheduler that wakes late jumps straight to the first future occurrence — the
    intermediate missed occurrences are skipped, not backfilled. When *tz* is set the
    schedule is evaluated in that timezone (e.g. ``"0 3 * * *"`` = 03:00 local) and the
    result is normalised back to UTC for storage.

    Across a daylight-saving change, as cron does: a wall time the clocks skip fires once,
    when the gap ends (``"30 2 * * *"`` at 03:00 on the spring-forward day), and a wall time
    the clocks repeat fires once, at its first occurrence — unless the expression runs every
    hour, which fires in each real hour, both passes included.

    *after* must be timezone-aware: a naive datetime would be read in the host timezone,
    making the schedule environment-dependent.
    """

    if after.tzinfo is None:
        raise exc.validation(
            "next_cron_fire requires a timezone-aware 'after' datetime "
            "(a naive value would be read in the host timezone)."
        )

    if tz is None:
        return croniter(expression, after).get_next(datetime).astimezone(UTC)

    zone = ZoneInfo(tz)
    occurrences = croniter(expression, after.astimezone(zone))
    every_hour = occurrences.expanded[1] == ["*"]

    while True:
        fire = occurrences.get_next(datetime).astimezone(UTC)

        # The second pass through a repeated wall time: an expression naming its hours fired
        # at the first one already, so firing again would run a daily job twice.
        if every_hour or not fire.astimezone(zone).fold:
            return fire
