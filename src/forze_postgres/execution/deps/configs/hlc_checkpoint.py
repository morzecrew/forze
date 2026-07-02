"""Postgres HLC checkpoint integration configuration."""

from typing import final

import attrs

from forze.application.contracts.resolution import RelationSpec, coerce_relation_spec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresHlcCheckpointConfig:
    """Postgres configuration for
    :class:`~forze_postgres.adapters.hlc_checkpoint.PostgresHlcCheckpointStore`.

    The HLC is node-global (one clock per runtime, spanning every tenant), so the mark is
    **not** tenant-partitioned — a single relation holds it for the deployment."""

    relation: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Schema-qualified high-water-mark table (see the store for the expected columns)."""

    node_key: str = "default"
    """Row key this runtime writes. A single shared key (the default) records one
    deployment-wide mark; distinct per-replica keys avoid write contention on one row, and
    :meth:`~...PostgresHlcCheckpointStore.load` reads the max across all keys either way."""
