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

    propagate_trace: bool = False
    """Persist each event's W3C ``traceparent`` so the relay forwards it as ``HEADER_TRACEPARENT``
    and the consume side links its span to the publish span (one distributed trace across the async
    hop). Off by default — opt in **after** adding a nullable ``traceparent`` text column to the store
    (a relational backend; legacy/null rows simply carry no parent). Independent of ``hlc_ordering``."""

    default_processing_lease: timedelta = attrs.field(factory=lambda: timedelta(minutes=5))
    """Suggested lease for :meth:`~forze_kits.integrations.outbox.OutboxRelay.to_queue`
    ``reclaim_stale_after`` (documentation default)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_flush_rows <= 0:
            raise exc.configuration("max_flush_rows must be positive")

        if self.max_claim_rows <= 0:
            raise exc.configuration("max_claim_rows must be positive")

        if self.default_processing_lease.total_seconds() <= 0:
            raise exc.configuration("Default processing lease must be positive")
