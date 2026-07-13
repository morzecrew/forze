"""Prefetch Postgres catalog metadata used by search adapters (introspection cache)."""

from collections.abc import Mapping
from typing import Any, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc
from forze_postgres.kernel.relation import RelationSpec, is_static_relation

from ...kernel.catalog.introspect import PostgresIntrospector
from ..deps.configs import (
    PostgresFederatedSearchConfig,
    PostgresFederatedSearchLegHub,
    PostgresHubSearchConfig,
    PostgresSearchConfig,
)
from ..deps.keys import PostgresIntrospectorDepKey
from .capabilities import POSTGRES_CLIENT_CAPABILITY

# ----------------------- #


async def _warm_column_types(
    introspector: PostgresIntrospector,
    spec: RelationSpec,
    *,
    label: str,
) -> None:
    if not is_static_relation(spec):
        logger.trace(
            "Postgres catalog warmup skipped dynamic relation %s",
            label,
        )
        return

    schema, relation = spec
    await introspector.get_column_types(schema=schema, relation=relation)


# ....................... #


async def _warm_postgres_search_config(
    introspector: PostgresIntrospector,
    cfg: PostgresSearchConfig,
) -> None:
    read = cfg.read
    await _warm_column_types(introspector, read, label="search.read")

    heap = cfg.heap_relation
    if is_static_relation(read) and is_static_relation(heap):
        if heap[0] != read[0] or heap[1] != read[1]:
            await _warm_column_types(introspector, heap, label="search.heap")
    else:
        await _warm_column_types(introspector, heap, label="search.heap")

    if cfg.engine in ("fts", "pgroonga"):
        await _warm_index_info(introspector, cfg.index, label="search.index")


# ....................... #


async def _warm_index_info(
    introspector: PostgresIntrospector,
    spec: RelationSpec,
    *,
    label: str,
) -> None:
    if not is_static_relation(spec):
        logger.trace(
            "Postgres catalog warmup skipped dynamic index relation %s",
            label,
        )
        return

    schema, index = spec
    await introspector.get_index_info(index=index, schema=schema)


# ....................... #


async def _warm_postgres_hub_search_config(
    introspector: PostgresIntrospector,
    cfg: PostgresHubSearchConfig,
) -> None:
    await _warm_column_types(introspector, cfg.hub, label="hub.hub")

    for member_cfg in cfg.members.values():
        await _warm_postgres_search_config(introspector, member_cfg)


# ....................... #


async def warm_postgres_catalog(
    ctx: ExecutionContext,
    *,
    searches: Mapping[Any, PostgresSearchConfig] | None = None,
    hub_searches: Mapping[Any, PostgresHubSearchConfig] | None = None,
    federated_searches: Mapping[Any, PostgresFederatedSearchConfig] | None = None,
) -> None:
    """Populate :class:`~forze_postgres.kernel.catalog.introspect.PostgresIntrospector` caches for search wiring.

    Safe to skip when using a partitioned introspector without a tenant in scope
    (logs at trace and returns). Idempotent with respect to cache contents.

    With ``cache_partition_key`` on the introspector, startup often has no tenant
    context; in that case this hook intentionally no-ops and you should rely on
    per-request catalog access (single-flight coalesces concurrent cold loads) or
    run a tenant-scoped warmup job after authentication.

    Dynamic :class:`~forze_postgres.kernel.relation.RelationSpec` resolvers are skipped
    at warmup (trace log per relation); catalog loads on first tenant-scoped query.
    """

    introspector = ctx.deps.provide(PostgresIntrospectorDepKey)

    try:
        if searches:
            for search_cfg in searches.values():
                await _warm_postgres_search_config(introspector, search_cfg)

        if hub_searches:
            for hub_cfg in hub_searches.values():
                await _warm_postgres_hub_search_config(introspector, hub_cfg)

        if federated_searches:
            for fed in federated_searches.values():
                for member_cfg in fed.members.values():
                    if isinstance(member_cfg, PostgresFederatedSearchLegHub):
                        await _warm_postgres_hub_search_config(
                            introspector,
                            member_cfg.hub,
                        )

                    else:
                        await _warm_postgres_search_config(
                            introspector,
                            member_cfg.search,
                        )

    except exc as e:
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

    Requires :data:`~forze_postgres.execution.lifecycle.capabilities.POSTGRES_CLIENT_CAPABILITY`.
    With ``introspector_cache_partition_key`` set and no tenant during startup, warmup
    is a no-op (trace log only).

    :param name: Unique step name.
    :param searches: Same mapping as :attr:`~forze_postgres.execution.deps.PostgresDepsModule.searches`.
    :param hub_searches: Same mapping as :attr:`~forze_postgres.execution.deps.PostgresDepsModule.hub_searches`.
    :param federated_searches: Same mapping as
        :attr:`~forze_postgres.execution.deps.PostgresDepsModule.federated_searches`.
    :returns: Lifecycle step with startup hook only.
    """

    return LifecycleStep(
        id=name,
        startup=PostgresCatalogWarmupHook(
            searches=searches,
            hub_searches=hub_searches,
            federated_searches=federated_searches,
        ),
        requires=(POSTGRES_CLIENT_CAPABILITY,),
    )
