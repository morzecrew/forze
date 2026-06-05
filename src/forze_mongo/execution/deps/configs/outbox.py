"""Mongo outbox integration configuration."""

from datetime import timedelta

import attrs

from forze.application.contracts.resolution import RelationSpec, coerce_relation_spec
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoOutboxConfig(TenantAwareIntegrationConfig):
    """Mongo configuration for :class:`~forze_mongo.adapters.outbox.MongoOutboxStore`."""

    collection: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Database and collection for outbox documents."""

    max_flush_rows: int = 500
    """Maximum rows per :meth:`~forze.application.contracts.outbox.OutboxCommandPort.flush`."""

    max_claim_rows: int = 100
    """Default batch size for :meth:`~forze.application.contracts.outbox.OutboxQueryPort.claim_pending`."""

    default_processing_lease: timedelta = attrs.field(
        factory=lambda: timedelta(minutes=5)
    )
    """Suggested lease for :func:`~forze_kits.integrations.outbox.relay_outbox_to_queue` ``reclaim_stale_after`` (documentation default)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.default_processing_lease.total_seconds() <= 0:
            raise exc.configuration("Default processing lease must be positive")
