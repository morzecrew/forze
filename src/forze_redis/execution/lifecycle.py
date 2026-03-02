from typing import final

import attrs

from forze.application.execution import ExecutionContext, LifecycleHook, LifecycleStep

from ..kernel.platform import RedisConfig
from .deps import RedisClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RedisStartupHook(LifecycleHook):
    dsn: str
    config: RedisConfig = RedisConfig()

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        redis_client = ctx.dep(RedisClientDepKey)
        await redis_client.initialize(self.dsn, config=self.config)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RedisShutdownHook(LifecycleHook):
    async def __call__(self, ctx: ExecutionContext) -> None:
        redis_client = ctx.dep(RedisClientDepKey)
        await redis_client.close()


# ....................... #


def redis_lifecycle_step(
    name: str = "redis_lifecycle",
    *,
    dsn: str,
    config: RedisConfig = RedisConfig(),
) -> LifecycleStep:
    startup_hook = RedisStartupHook(dsn=dsn, config=config)
    shutdown_hook = RedisShutdownHook()

    return LifecycleStep(name=name, startup=startup_hook, shutdown=shutdown_hook)
