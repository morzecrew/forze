"""Shared outbox integration-config base for relational/document backends."""

from datetime import timedelta

import attrs

from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OutboxIntegrationConfig(TenantAwareIntegrationConfig):
    """Backend-agnostic outbox tuning shared by every outbox store config.

    A concrete backend config subclasses this and adds only its own relation field
    (a table, a collection, ...). The flush/claim batch sizes, HLC-ordering toggle, and
    processing-lease default — identical across backends — live here once.
    """

    max_flush_rows: int = 500
    """Maximum rows per :meth:`~forze.application.contracts.outbox.OutboxCommandPort.flush`."""

    max_claim_rows: int = 100
    """Default batch size for :meth:`~forze.application.contracts.outbox.OutboxQueryPort.claim_pending`."""

    hlc_ordering: bool = False
    """Persist each event's Hybrid Logical Clock and claim in causal order instead of
    ``created_at`` only. Off by default — opt in **after** migrating the store; the exact
    ordering/legacy-row semantics are documented on the backend config. """

    default_processing_lease: timedelta = attrs.field(
        factory=lambda: timedelta(minutes=5)
    )
    """Suggested lease for :meth:`~forze_kits.integrations.outbox.OutboxRelay.to_queue`
    ``reclaim_stale_after`` (documentation default)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.default_processing_lease.total_seconds() <= 0:
            raise exc.configuration("Default processing lease must be positive")
