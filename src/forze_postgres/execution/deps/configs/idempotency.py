"""Postgres idempotency integration configuration."""

from typing import final

import attrs

from forze.application.contracts.resolution import RelationSpec, coerce_relation_spec
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresIdempotencyConfig(TenantAwareIntegrationConfig):
    """Postgres configuration for :class:`~forze_postgres.adapters.idempotency.PostgresIdempotencyStore`."""

    relation: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Schema-qualified idempotency table (see the store for the expected columns)."""
