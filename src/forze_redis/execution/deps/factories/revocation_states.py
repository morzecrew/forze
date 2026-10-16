"""Redis revocation states dep factory."""

from typing import final

import attrs

from forze.application.contracts.authn import RevocationStatesSpec
from forze.application.execution import ExecutionContext

from ....adapters.revocation_states import RedisRevocationStatesAdapter
from ..configs import RedisRevocationStatesConfig
from ..keys import RedisClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRedisRevocationStates:
    """Build :class:`RedisRevocationStatesAdapter` for a revocation states route."""

    config: RedisRevocationStatesConfig
    """Configuration (namespace)."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: RevocationStatesSpec,
    ) -> RedisRevocationStatesAdapter:
        _ = spec

        return RedisRevocationStatesAdapter(
            client=ctx.deps.provide(RedisClientDepKey),
            namespace=self.config.namespace,
        )
