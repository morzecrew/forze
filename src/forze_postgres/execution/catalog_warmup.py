"""Prefetch Postgres catalog metadata used by search adapters (introspection cache)."""

from collections.abc import Mapping
from typing import Any, cast, final

import attrs

from forze.application._logger import logger
from forze.application.execution import ExecutionContext, LifecycleHook, LifecycleStep
from forze.base.errors import CoreError

from ..kernel.introspect import PostgresIntrospector
from .deps.configs import (
    PostgresFederatedSearchConfig,
    PostgresHubSearchConfig,
    PostgresSearchConfig,
    is_postgres_federated_embedded_hub_config,
)
from .deps.keys import PostgresIntrospectorDepKey

# ----------------------- #


async def _warm_postgres_search_config(
    introspector: PostgresIntrospector,
    cfg: PostgresSearchConfig,
) -> None:
    read = cfg["read"]
    await introspector.get_column_types(schema=read[0], relation=read[1])

    heap = cfg.get("heap", read)
    if heap[0] != read[0] or heap[1] != read[1]:
        await introspector.get_column_types(schema=heap[0], relation=heap[1])

    if cfg["engine"] in ("fts", "pgroonga"):
        idx = cfg["index"]
        await introspector.get_index_info(index=idx[1], schema=idx[0])


# ....................... #


async def _warm_postgres_hub_search_config(
    introspector: PostgresIntrospector,
    cfg: PostgresHubSearchConfig,
) -> None:
    hub = cfg["hub"]
    await introspector.get_column_types(schema=hub[0], relation=hub[1])

    for member_cfg in cfg["members"].values():
        await _warm_postgres_search_config(introspector, member_cfg)


# ....................... #


async def warm_postgres_catalog(
    ctx: ExecutionContext,
    *,
    searches: Mapping[Any, PostgresSearchConfig] | None = None,
    hub_searches: Mapping[Any, PostgresHubSearchConfig] | None = None,
    federated_searches: Mapping[Any, PostgresFederatedSearchConfig] | None = None,
) -> None:
    """Populate :class:`~forze_postgres.kernel.introspect.PostgresIntrospector` caches for search wiring.

    Safe to skip when using a partitioned introspector without a tenant in scope
    (logs at trace and returns). Idempotent with respect to cache contents.
    """

    introspector = ctx.dep(PostgresIntrospectorDepKey)

    try:
        if searches:
            for search_cfg in searches.values():
                await _warm_postgres_search_config(introspector, search_cfg)

        if hub_searches:
            for hub_cfg in hub_searches.values():
                await _warm_postgres_hub_search_config(introspector, hub_cfg)

        if federated_searches:
            for fed in federated_searches.values():
                for member_cfg in fed["members"].values():
                    if is_postgres_federated_embedded_hub_config(member_cfg):
                        await _warm_postgres_hub_search_config(
                            introspector,
                            cast(PostgresHubSearchConfig, member_cfg),
                        )

                    else:
                        await _warm_postgres_search_config(
                            introspector,
                            cast(PostgresSearchConfig, member_cfg),
                        )

    except CoreError as e:
        if getattr(e, "code", None) == "introspection_partition_required":
            logger.trace(
                "Postgres catalog warmup skipped (introspector partition unavailable)",
            )
            return

        raise


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresCatalogWarmupHook(LifecycleHook):
    """Startup hook that warms introspection caches for configured searches."""

    searches: Mapping[Any, PostgresSearchConfig] | None = None
    hub_searches: Mapping[Any, PostgresHubSearchConfig] | None = None
    federated_searches: Mapping[Any, PostgresFederatedSearchConfig] | None = None

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        await warm_postgres_catalog(
            ctx,
            searches=self.searches,
            hub_searches=self.hub_searches,
            federated_searches=self.federated_searches,
        )


# ....................... #


def postgres_catalog_warmup_lifecycle_step(
    name: str = "postgres_catalog_warmup",
    *,
    searches: Mapping[Any, PostgresSearchConfig] | None = None,
    hub_searches: Mapping[Any, PostgresHubSearchConfig] | None = None,
    federated_searches: Mapping[Any, PostgresFederatedSearchConfig] | None = None,
) -> LifecycleStep:
    """Build a lifecycle step that prefetches search-related catalog metadata.

    Run after :func:`~forze_postgres.execution.lifecycle.postgres_lifecycle_step`
    (or :func:`~forze_postgres.execution.lifecycle.routed_postgres_lifecycle_step`)
    so the pool is initialized. With ``introspector_cache_partition_key`` set and
    no tenant during startup, warmup is a no-op (trace log only).

    :param name: Unique step name.
    :param searches: Same mapping as :attr:`~forze_postgres.execution.deps.PostgresDepsModule.searches`.
    :param hub_searches: Same mapping as :attr:`~forze_postgres.execution.deps.PostgresDepsModule.hub_searches`.
    :param federated_searches: Same mapping as
        :attr:`~forze_postgres.execution.deps.PostgresDepsModule.federated_searches`.
    :returns: Lifecycle step with startup hook only.
    """

    return LifecycleStep(
        name=name,
        startup=PostgresCatalogWarmupHook(
            searches=searches,
            hub_searches=hub_searches,
            federated_searches=federated_searches,
        ),
    )
