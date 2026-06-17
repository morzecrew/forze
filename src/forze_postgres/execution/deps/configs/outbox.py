"""Postgres outbox integration configuration."""

from datetime import timedelta

import attrs

from forze.application.contracts.resolution import RelationSpec, coerce_relation_spec
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresOutboxConfig(TenantAwareIntegrationConfig):
    """Postgres configuration for :class:`~forze_postgres.adapters.outbox.PostgresOutboxStore`."""

    relation: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Schema-qualified outbox table."""

    max_flush_rows: int = 500
    """Maximum rows per :meth:`~forze.application.contracts.outbox.OutboxCommandPort.flush`."""

    max_claim_rows: int = 100
    """Default batch size for :meth:`~forze.application.contracts.outbox.OutboxQueryPort.claim_pending`."""

    hlc_ordering: bool = False
    """Persist each event's Hybrid Logical Clock and claim in causal order
    (``ORDER BY hlc NULLS LAST, created_at, id``) instead of ``created_at`` only.
    Requires an ``hlc BIGINT`` column on the outbox table (legacy null rows fall
    back to ``created_at``). Off by default — opt in **after** migrating the table."""

    default_processing_lease: timedelta = attrs.field(
        factory=lambda: timedelta(minutes=5)
    )
    """Suggested lease for :meth:`~forze_kits.integrations.outbox.OutboxRelay.to_queue` ``reclaim_stale_after`` (documentation default)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.default_processing_lease.total_seconds() <= 0:
            raise exc.configuration("Default processing lease must be positive")
