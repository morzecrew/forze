"""Postgres outbox integration configuration."""

import attrs

from forze.application.contracts.outbox import OutboxIntegrationConfig
from forze.application.contracts.resolution import RelationSpec, coerce_relation_spec

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresOutboxConfig(OutboxIntegrationConfig):
    """Postgres configuration for :class:`~forze_postgres.adapters.outbox.PostgresOutboxStore`.

    With ``hlc_ordering`` enabled, claims order ``hlc NULLS LAST, created_at, id`` and
    require an ``hlc BIGINT`` column on the outbox table (legacy null rows fall back to
    ``created_at``). Shared tuning (flush/claim batch sizes, lease) is inherited.
    """

    relation: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Schema-qualified outbox table."""
