from typing import Optional, final

import attrs
from botocore.config import Config as AioConfig

from forze.application.execution import ExecutionContext, LifecycleHook, LifecycleStep

from .deps import S3ClientDepKey

# ----------------------- #
#! typed dicts maybe ?


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class S3StartupHook(LifecycleHook):
    endpoint: str
    access_key_id: str
    secret_access_key: str
    config: Optional[AioConfig] = None

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        s3_client = ctx.dep(S3ClientDepKey)
        await s3_client.initialize(
            self.endpoint,
            self.access_key_id,
            self.secret_access_key,
            config=self.config,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class S3ShutdownHook(LifecycleHook):
    async def __call__(self, ctx: ExecutionContext) -> None:
        s3_client = ctx.dep(S3ClientDepKey)
        s3_client.close()


# ....................... #


def s3_lifecycle_step(
    name: str = "s3_lifecycle",
    *,
    endpoint: str,
    access_key_id: str,
    secret_access_key: str,
    config: Optional[AioConfig] = None,
) -> LifecycleStep:
    startup_hook = S3StartupHook(
        endpoint=endpoint,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        config=config,
    )
    shutdown_hook = S3ShutdownHook()
    return LifecycleStep(name=name, startup=startup_hook, shutdown=shutdown_hook)
