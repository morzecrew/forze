"""Postgres outbox integration configuration."""

from datetime import timedelta
from typing import Literal

import attrs

from forze.application.contracts.outbox import OutboxIntegrationConfig
from forze.application.contracts.resolution import RelationSpec, coerce_relation_spec
from forze.base.exceptions import exc

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

    storage: Literal["retain", "consume"] = "retain"
    """``retain`` claims a row by moving it to ``processing`` and marks it ``published``.
    ``consume`` runs each relay pass in one transaction: the claim locks its rows without
    updating them, and a published row is deleted, so a row that publishes first time is
    written twice, staged and deleted, and nothing published is left to prune."""

    pass_deadline: timedelta = attrs.field(factory=lambda: timedelta(seconds=5))
    """``consume`` only: how long a pass keeps publishing before it commits what it did. The
    pass holds a connection and its batch's row locks until then."""

    pass_statement_timeout: timedelta = attrs.field(factory=lambda: timedelta(seconds=1))
    """``consume`` only: the ``statement_timeout`` and ``lock_timeout`` of each statement in a
    pass that owns its transaction. The session is also cut off when it sits idle in the pass
    for longer than ``pass_deadline`` plus this."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()

        if self.storage not in ("retain", "consume"):
            raise exc.configuration(
                f"Outbox storage must be 'retain' or 'consume', got {self.storage!r}"
            )

        # Postgres reads a zero timeout as no limit at all, so a non-positive one would
        # silently remove the bound rather than tighten it.
        if self.pass_deadline.total_seconds() <= 0:
            raise exc.configuration("Outbox pass_deadline must be positive")

        if self.pass_statement_timeout.total_seconds() <= 0:
            raise exc.configuration("Outbox pass_statement_timeout must be positive")
