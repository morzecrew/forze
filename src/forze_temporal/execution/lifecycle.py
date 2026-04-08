from typing import final

import attrs

from forze.application.execution import ExecutionContext, LifecycleHook, LifecycleStep

from ..kernel.platform import TemporalConfig
from .deps import TemporalClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TemporalStartupHook(LifecycleHook):
    """Startup hook that initializes the Temporal client from the deps container."""

    host: str
    """Connection host for the Temporal server."""

    config: TemporalConfig = TemporalConfig()
    """Configuration for the Temporal client."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        temporal_client = ctx.dep(TemporalClientDepKey)
        await temporal_client.initialize(self.host, config=self.config)


# ....................... #


def temporal_lifecycle_step(
    name: str = "temporal_lifecycle",
    *,
    host: str,
    config: TemporalConfig = TemporalConfig(),
) -> LifecycleStep:
    """Build a lifecycle step for Temporal client init and shutdown."""

    startup_hook = TemporalStartupHook(host=host, config=config)

    return LifecycleStep(name=name, startup=startup_hook)
