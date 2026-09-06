"""Mongo HLC checkpoint dep factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import attrs

from ....adapters.hlc_checkpoint import MongoHlcCheckpointStore
from ..configs.hlc_checkpoint import MongoHlcCheckpointConfig
from ..keys import MongoClientDepKey

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMongoHlcCheckpoint:
    """Build a :class:`MongoHlcCheckpointStore` — a node-global ``SimpleDepPort``.

    No per-route spec: there is one clock per runtime, so the store is a singleton resolved
    once per scope from ``ctx``."""

    config: MongoHlcCheckpointConfig
    """Mongo-specific configuration (collection + node key)."""

    def __call__(self, ctx: ExecutionContext) -> MongoHlcCheckpointStore:
        client = ctx.deps.provide(MongoClientDepKey)

        return MongoHlcCheckpointStore(client=client, config=self.config)
