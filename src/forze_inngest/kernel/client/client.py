from forze_inngest._compat import require_inngest

require_inngest()

# ....................... #

from typing import final

import attrs
import inngest

from .._logger import logger
from .config import InngestConfig
from .port import InngestClientPort

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class InngestClient(InngestClientPort):
    """Thin wrapper around the Inngest Python SDK client."""

    app_id: str
    """Inngest application id."""

    config: InngestConfig = attrs.field(factory=InngestConfig)
    """Optional client configuration."""

    _sdk: inngest.Inngest = attrs.field(
        default=attrs.Factory(
            lambda self: inngest.Inngest(
                app_id=self.app_id,
                is_production=self.config.is_production,
                event_key=(
                    self.config.event_key.get_secret_value()
                    if self.config.event_key is not None
                    else None
                ),
                signing_key=(
                    self.config.signing_key.get_secret_value()
                    if self.config.signing_key is not None
                    else None
                ),
                request_timeout=self.config.request_timeout,
            ),
            takes_self=True,
        ),
        init=False,
        repr=False,
    )

    # ....................... #

    @property
    def native(self) -> inngest.Inngest:
        return self._sdk

    # ....................... #

    async def close(self) -> None:
        """Release client resources.

        No-op: the Inngest SDK holds no persistent connection (events are sent
        over per-call HTTP), matching the previous routed-pool disposal.
        """

    # ....................... #

    async def send(
        self,
        events: inngest.Event | list[inngest.Event],
    ) -> list[str]:
        ids = await self._sdk.send(events)
        logger.debug("Inngest events sent", count=len(ids))
        return ids
