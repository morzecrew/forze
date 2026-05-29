"""Postgres analytics query and ingest adapter."""

from __future__ import annotations

from typing import final

import attrs
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsIngestPort,
    AnalyticsQueryPort,
    AnalyticsSpec,
)
from forze.application.contracts.tenancy import TenantProviderPort
from forze_postgres.execution.deps.configs import PostgresAnalyticsConfig
from forze_postgres.kernel.client import PostgresClientPort
from forze_postgres.kernel.gateways import PostgresQualifiedName

from ._chunked import PostgresAnalyticsChunkedMixin
from ._cursor import PostgresAnalyticsCursorMixin
from ._ingest import PostgresAnalyticsIngestMixin
from ._port import PostgresAnalyticsPortMixin
from ._query import PostgresAnalyticsQueryMixin

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresAnalyticsAdapter[R: BaseModel, Ing: BaseModel](
    PostgresAnalyticsPortMixin[R, Ing],
    PostgresAnalyticsCursorMixin[R, Ing],
    PostgresAnalyticsChunkedMixin[R, Ing],
    PostgresAnalyticsIngestMixin[R, Ing],
    PostgresAnalyticsQueryMixin[R, Ing],
    AnalyticsQueryPort[R],
    AnalyticsIngestPort[Ing],
):
    """Analytics ports backed by PostgreSQL via :class:`~forze_postgres.kernel.client.PostgresClient`."""

    client: PostgresClientPort
    spec: AnalyticsSpec[R, Ing]
    config: PostgresAnalyticsConfig
    tenant_provider: TenantProviderPort | None = None
    """Tenant context for dynamic ingest :class:`~forze_postgres.kernel.relation.RelationSpec` resolvers."""

    _ingest_qname_resolved: PostgresQualifiedName | None = attrs.field(
        default=None,
        init=False,
        eq=False,
        repr=False,
    )
