"""Wake an outbox relay when rows commit, so it need not wait out its poll."""

from typing import Final, final

import attrs
from pydantic import BaseModel, ConfigDict

from forze.base.primitives import StrKey
from forze.base.serialization import PydanticModelCodec

from ..pubsub import PubSubSpec

# ----------------------- #

DEFAULT_OUTBOX_WAKE_TOPIC: Final[str] = "forze.outbox.wake"
"""Pub/sub topic every outbox route wakes its relay on, unless told otherwise."""


@final
class OutboxWakeSignal(BaseModel):
    """What a wake says: which outbox route has committed rows. Nothing else.

    A wake carries no rows, so losing one costs a relay one poll interval and never a
    row, and a stale or duplicate one costs one claim query that finds nothing.
    """

    model_config = ConfigDict(frozen=True)

    route: str
    """Name of the outbox route whose rows committed."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OutboxWake:
    """Publish a wake after a flush's rows commit, and have the relay wait on it.

    A relay polls; a row committed just after a pass waits out the rest of the interval.
    With a wake, the flush publishes :class:`OutboxWakeSignal` once its transaction commits
    (never on rollback, and never before the rows are visible), and the background relay
    runs a pass as soon as it hears one. The poll stays as the fallback, so a lost wake
    delays a row by one interval at most.

    The wake travels over the pub/sub contract on deps route :attr:`route`, so every
    process that flushes this outbox route, and the one that relays it, needs that route
    wired. Keep it tenant-global: the relay listens outside any tenant.
    """

    route: StrKey
    """Deps route of the pub/sub backend that carries the wake."""

    topic: str = DEFAULT_OUTBOX_WAKE_TOPIC
    """Topic the wake is published on; outbox routes may share it."""

    # ....................... #

    @property
    def spec(self) -> PubSubSpec[OutboxWakeSignal]:
        """The pub/sub spec both sides resolve their port with."""

        return PubSubSpec(name=self.route, codec=PydanticModelCodec(OutboxWakeSignal))
