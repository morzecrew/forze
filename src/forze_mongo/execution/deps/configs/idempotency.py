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
    """Database and collection for idempotency claims (see the store for the document shape).

    **A static collection shared by tenants needs ``tenant_aware=True``.** An idempotency
    key is supplied by the caller (an ``Idempotency-Key`` header), so unlike the inbox —
    whose keys are globally unique event ids — two tenants routinely send the same one.
    Without the tenant in the claim key they share it, and one tenant's request replays or
    blocks another's. ``tenant_aware=False`` is for a per-tenant resolver, where the
    collection itself is the boundary.
    """
