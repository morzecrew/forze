from typing import final

import attrs

from forze.application.execution import ExecutionContext, LifecycleHook, LifecycleStep

from ..kernel.platform import PostgresConfig
from .deps import PostgresClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresStartupHook(LifecycleHook):
    dsn: str
    config: PostgresConfig = PostgresConfig()

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        postgres_client = ctx.dep(PostgresClientDepKey)
        await postgres_client.initialize(self.dsn, config=self.config)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresShutdownHook(LifecycleHook):
    async def __call__(self, ctx: ExecutionContext) -> None:
        postgres_client = ctx.dep(PostgresClientDepKey)
        await postgres_client.close()


# ....................... #


def postgres_lifecycle_step(
    dsn: str,
    config: PostgresConfig = PostgresConfig(),
    name: str = "postgres_lifecycle",
) -> LifecycleStep:
    startup_hook = PostgresStartupHook(dsn=dsn, config=config)
    shutdown_hook = PostgresShutdownHook()

    return LifecycleStep(name=name, startup=startup_hook, shutdown=shutdown_hook)
