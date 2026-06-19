"""Firestore client pool lifecycle hooks and step factories."""

from typing import Any, cast, final

import attrs

from forze.application.contracts.deps import DepKey
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution import ExecutionContext
from forze.application.execution.lifecycle.builtin import (
    ClientShutdownHook,
    routed_client_lifecycle_step,
)

from ...kernel.client import FirestoreClient, RoutedFirestoreClient
from ..deps import FirestoreClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class FirestoreStartupHook(LifecycleHook):
    """Startup hook that initializes the Firestore client from the deps container."""

    project_id: str
    """GCP project id."""

    database: str = "(default)"
    """Firestore database id."""

    lazy_transaction: bool = True
    """Defer ``_begin`` to the first operation inside a transaction scope."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        client = cast(FirestoreClient, ctx.deps.provide(FirestoreClientDepKey))

        await client.initialize(
            project_id=self.project_id,
            database=self.database,
            lazy_transaction=self.lazy_transaction,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class FirestoreShutdownHook(ClientShutdownHook):
    """Shutdown hook that closes the Firestore client."""

    dep_key: DepKey[Any] = attrs.field(default=FirestoreClientDepKey, init=False)


# ....................... #


def firestore_lifecycle_step(
    name: str = "firestore_lifecycle",
    *,
    project_id: str,
    database: str = "(default)",
) -> LifecycleStep:
    """Build a lifecycle step for Firestore client init and shutdown."""

    return LifecycleStep(
        id=name,
        startup=FirestoreStartupHook(
            project_id=project_id,
            database=database,
        ),
        shutdown=FirestoreShutdownHook(),
    )


def routed_firestore_lifecycle_step(
    name: str = "routed_firestore_lifecycle",
    *,
    client: RoutedFirestoreClient,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedFirestoreClient` registered as :data:`FirestoreClientDepKey`."""

    return routed_client_lifecycle_step(name, client=client)
