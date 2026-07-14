"""Redis counter dep factories (allocation port + admin enumeration port)."""

from typing import final

import attrs

from forze.application.contracts.counter import (
    CounterAdminDepPort,
    CounterDepPort,
    CounterSpec,
)
from forze.application.execution import ExecutionContext

from ....adapters import RedisCounterAdapter, RedisCounterAdminAdapter
from ..configs import RedisCounterConfig, RedisUniversalConfig
from ..keys import RedisClientDepKey

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _ConfigurableRedisCounterBase:
    """Shared config + client resolution for the counter data and admin factories."""

    config: RedisCounterConfig | RedisUniversalConfig = attrs.field(
        validator=attrs.validators.instance_of(RedisUniversalConfig),
    )
    """Configuration for the counter."""

    # ....................... #

    def _kwargs(self, ctx: ExecutionContext) -> dict[str, object]:
        return {
            "client": ctx.deps.provide(RedisClientDepKey),
            "namespace": self.config.namespace,
            "tenant_aware": self.config.tenant_aware,
            "tenant_provider": ctx.inv_ctx.get_tenant,
        }


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRedisCounter(_ConfigurableRedisCounterBase, CounterDepPort):
    """Configurable Redis counter adapter."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: CounterSpec,
    ) -> RedisCounterAdapter:
        return RedisCounterAdapter(**self._kwargs(ctx))  # type: ignore[arg-type]


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRedisCounterAdmin(_ConfigurableRedisCounterBase, CounterAdminDepPort):
    """Configurable Redis counter admin (enumeration) adapter.

    Built from the **same** route config as the allocation port, so a wired counter is always
    enumerable: an admin port behind its own opt-in flag would be missing exactly when an
    export needed it, and the export would then have to choose between failing on a route the
    application uses perfectly well and skipping its sequence numbers in silence.
    """

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: CounterSpec,
    ) -> RedisCounterAdminAdapter:
        return RedisCounterAdminAdapter(**self._kwargs(ctx))  # type: ignore[arg-type]
