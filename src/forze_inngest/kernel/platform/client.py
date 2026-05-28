from forze_inngest._compat import require_inngest

require_inngest()

# ....................... #

from datetime import timedelta
from typing import cast, final

import attrs
import inngest

from .config import InngestConfig
from .port import InngestClientPort

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class InngestClient(InngestClientPort):
    """Thin wrapper around the Inngest Python SDK client."""

    app_id: str
    """Inngest application id."""

    config: InngestConfig = attrs.field(
        factory=lambda: cast(InngestConfig, {}),
    )
    """Optional client configuration."""

    _sdk: inngest.Inngest = attrs.field(init=False, repr=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        timeout: timedelta | None = None

        if timeout_ms := self.config.get("request_timeout_ms"):
            timeout = timedelta(milliseconds=timeout_ms)

        object.__setattr__(
            self,
            "_sdk",
            inngest.Inngest(
                app_id=self.app_id,
                is_production=self.config.get("is_production"),
                event_key=self.config.get("event_key"),
                signing_key=self.config.get("signing_key"),
                request_timeout=timeout,
            ),
        )

    # ....................... #

    @property
    def native(self) -> inngest.Inngest:
        return self._sdk

    # ....................... #

    async def send(
        self,
        events: inngest.Event | list[inngest.Event],
    ) -> list[str]:
        return await self._sdk.send(events)
