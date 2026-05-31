from forze_temporal._compat import require_temporal

require_temporal()

# ....................... #

from typing import final

import attrs
from temporalio.client import Interceptor

# ----------------------- #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class TemporalConfig:
    """Temporal configuration."""

    namespace: str = "default"
    """Namespace to use for the client."""

    lazy: bool = False
    """Whether to lazy initialize the client."""

    interceptors: list[Interceptor] | None = attrs.field(default=None)
    """Interceptors to apply to the client."""
