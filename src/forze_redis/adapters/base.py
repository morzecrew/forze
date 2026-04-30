from forze_redis._compat import require_redis

require_redis()

# ....................... #

from typing import Any

import attrs

from forze_contrib.tenancy import MultiTenancyMixin

from ..kernel.platform import RedisClientPort
from .codecs import RedisKeyCodec

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisBaseAdapter(MultiTenancyMixin):
    """Base adapter class for Redis integration."""

    client: RedisClientPort
    """Redis client instance."""

    key_codec: RedisKeyCodec
    """Redis key codec instance - used for key construction."""

    # ....................... #

    def __tenant_prefix(self) -> tuple[str, ...] | None:
        """Construct a tenant prefix from attached tenant ID if any."""

        tenant_id = self.require_tenant_if_aware()

        if tenant_id is not None:
            return ("tenant", str(tenant_id))

        return None

    # ....................... #

    def construct_key(self, scope: str | tuple[str, ...], *parts: Any) -> str:
        """Construct a key for the given scope and parts."""

        tenant_prefix = self.__tenant_prefix()

        return self.key_codec.join(tenant_prefix, scope, *parts)
