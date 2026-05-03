from datetime import timedelta
from typing import final

import attrs

# ----------------------- #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class RedisConfig:
    """Redis configuration."""

    max_size: int = 20
    socket_timeout: timedelta | None = None
    connect_timeout: timedelta | None = None
