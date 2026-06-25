"""Mongo outbox integration configuration."""

import attrs

from forze.application.contracts.outbox import OutboxIntegrationConfig
from forze.application.contracts.resolution import RelationSpec, coerce_relation_spec

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoOutboxConfig(OutboxIntegrationConfig):
    """Mongo configuration for :class:`~forze_mongo.adapters.outbox.MongoOutboxStore`.

    With ``hlc_ordering`` enabled, claims sort ``[(hlc, 1), (created_at, 1), (id, 1)]``
    (``hlc`` packed int64). During migration Mongo sorts legacy missing-``hlc`` rows
    *first* (oldest drain first) — the inverse of Postgres ``NULLS LAST``; both are
    best-effort. Shared tuning (flush/claim batch sizes, lease) is inherited.
    """

    collection: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Database and collection for outbox documents."""
