"""Lifecycle hooks for Firestore client initialization and shutdown."""

from typing import cast, final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution import ExecutionContext

from ..kernel.platform import FirestoreClient
from .deps import FirestoreClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class FirestoreStartupHook(LifecycleHook):
    project_id: str
    database: str = "(default)"

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        client = cast(FirestoreClient, ctx.deps.provide(FirestoreClientDepKey))

        await client.initialize(
            project_id=self.project_id,
            database=self.database,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class FirestoreShutdownHook(LifecycleHook):
    async def __call__(self, ctx: ExecutionContext) -> None:
        client = ctx.deps.provide(FirestoreClientDepKey)
        await client.close()


# ....................... #


def firestore_lifecycle_step(
    name: str = "firestore_lifecycle",
    *,
    project_id: str,
    database: str = "(default)",
) -> LifecycleStep:
    return LifecycleStep(
        id=name,
        startup=FirestoreStartupHook(
            project_id=project_id,
            database=database,
        ),
        shutdown=FirestoreShutdownHook(),
    )
