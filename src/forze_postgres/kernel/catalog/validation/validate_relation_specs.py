"""Warnings for Postgres RelationSpec wiring on integration configs."""

from typing import Sequence

from forze.application.contracts.resolution import RelationSpec
from forze.application.contracts.tenancy import (
    warn_dynamic_relation_with_tenant_aware as _warn_dynamic_relation,
)
from forze_postgres.kernel._logger import logger

# ----------------------- #


def warn_dynamic_relation_with_tenant_aware(
    *,
    route_name: str,
    kind: str,
    tenant_aware: bool,
    fields: Sequence[tuple[str, RelationSpec | None]],
) -> None:
    """Log when a Postgres route combines row filters with per-tenant relation resolvers."""

    _warn_dynamic_relation(
        integration="Postgres",
        route_name=route_name,
        kind=kind,
        tenant_aware=tenant_aware,
        relation_fields=fields,
        log_warning=logger.warning,
    )
