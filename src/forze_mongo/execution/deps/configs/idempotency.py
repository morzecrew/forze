"""Mongo idempotency integration configuration."""

from typing import final

import attrs

from forze.application.contracts.resolution import RelationSpec, coerce_relation_spec
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoIdempotencyConfig(TenantAwareIntegrationConfig):
    """Mongo configuration for :class:`~forze_mongo.adapters.idempotency.MongoIdempotencyStore`."""

    collection: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Database and collection for idempotency claims (see the store for the document shape)."""
