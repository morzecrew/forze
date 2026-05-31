"""Redis counter dep factory."""

from typing import final

import attrs

from forze.application.contracts.counter import CounterDepPort, CounterSpec
from forze.application.execution import ExecutionContext

from ....adapters import RedisCounterAdapter
from ..configs import RedisCounterConfig, RedisUniversalConfig
from ..keys import RedisClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRedisCounter(CounterDepPort):
    """Configurable Redis counter adapter."""

    config: RedisCounterConfig | RedisUniversalConfig = attrs.field(
        validator=attrs.validators.instance_of(RedisUniversalConfig),
    )
    """Configuration for the counter."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: CounterSpec,
    ) -> RedisCounterAdapter:
        client = ctx.deps.provide(RedisClientDepKey)

        return RedisCounterAdapter(
            client=client,
            namespace=self.config.namespace,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
