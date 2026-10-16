"""Postgres principal epoch integration configuration."""

from typing import final

import attrs

from forze.application.contracts.resolution import RelationSpec, coerce_relation_spec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresPrincipalEpochConfig:
    """Postgres configuration for
    :class:`~forze_postgres.adapters.principal_epoch.PostgresPrincipalEpochStore`.

    A principal spans tenants, so its epoch is **not** tenant-partitioned: one relation holds
    every principal's, in the database that holds the sessions."""

    relation: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Schema-qualified epochs table (see the store for the expected columns)."""
