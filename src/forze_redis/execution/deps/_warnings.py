"""Private tenancy warning descriptors for Redis deps module."""

from forze.application.contracts.tenancy import IntegrationRouteWarning

from .configs import RedisUniversalConfig

# ----------------------- #


def _namespace_warning(*, kind: str) -> IntegrationRouteWarning[RedisUniversalConfig]:
    return IntegrationRouteWarning(
        kind=kind,
        tenant_aware=lambda config: config.tenant_aware,
        named_fields=lambda config: [("namespace", config.namespace)],
    )


REDIS_CACHE_WARNING = _namespace_warning(kind="cache")
REDIS_COUNTER_WARNING = _namespace_warning(kind="counter")
REDIS_IDEMPOTENCY_WARNING = _namespace_warning(kind="idempotency")
REDIS_SEARCH_SNAPSHOT_WARNING = _namespace_warning(kind="search_snapshot")
REDIS_DLOCK_WARNING = _namespace_warning(kind="dlock")
