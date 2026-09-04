"""Mongo inbox integration configuration."""

from typing import final

import attrs

from forze.application.contracts.resolution import RelationSpec, coerce_relation_spec
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoInboxConfig(TenantAwareIntegrationConfig):
    """Mongo configuration for :class:`~forze_mongo.adapters.inbox.MongoInboxStore`."""

    collection: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Database and collection for inbox dedup marks
    (``inbox_route``, ``message_id``, ``tenant_id``, ``processed_at``)."""
