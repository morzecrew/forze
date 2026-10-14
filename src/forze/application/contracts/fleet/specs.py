from datetime import timedelta
from typing import final

import attrs

from forze.base.exceptions import exc

from ..base import BaseSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class FleetSpec(BaseSpec):
    """Specification for one service's fleet of replicas.

    ``name`` names the fleet: every replica of the service beats into the same one.
    """

    ttl: timedelta = timedelta(seconds=10)
    """How long a member's entry lives without another beat.

    The failure-detection latency: a replica that stops beating stays a member for up to
    this long. Beat well within it — a member that beats every ``ttl / 3`` survives one
    missed beat.
    """

    report_max_bytes: int = 1024
    """Largest encoded report a beat may carry; a larger one is refused before sending."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.ttl < timedelta(milliseconds=1):
            raise exc.configuration("Fleet ttl must be at least 1 millisecond")

        if self.report_max_bytes < 2:
            # The smallest report, ``{}``, encodes to two bytes.
            raise exc.configuration("Fleet report_max_bytes must be at least 2")
