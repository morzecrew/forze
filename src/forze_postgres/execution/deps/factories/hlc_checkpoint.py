"""Postgres HLC checkpoint dep factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import attrs

from ....adapters.hlc_checkpoint import PostgresHlcCheckpointStore
from ..configs.hlc_checkpoint import PostgresHlcCheckpointConfig
from ..keys import PostgresClientDepKey

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePostgresHlcCheckpoint:
    """Build a :class:`PostgresHlcCheckpointStore` — a node-global ``SimpleDepPort``.

    No per-route spec: there is one clock per runtime, so the store is a singleton resolved
    once per scope from ``ctx``."""

    config: PostgresHlcCheckpointConfig
    """Postgres-specific configuration (relation + node key)."""

    def __call__(self, ctx: ExecutionContext) -> PostgresHlcCheckpointStore:
        client = ctx.deps.provide(PostgresClientDepKey)
        return PostgresHlcCheckpointStore(client=client, config=self.config)
