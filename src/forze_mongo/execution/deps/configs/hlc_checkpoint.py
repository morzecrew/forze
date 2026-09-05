"""Mongo HLC checkpoint integration configuration."""

from typing import final

import attrs

from forze.application.contracts.resolution import RelationSpec, coerce_relation_spec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoHlcCheckpointConfig:
    """Mongo configuration for
    :class:`~forze_mongo.adapters.hlc_checkpoint.MongoHlcCheckpointStore`.

    The HLC is node-global (one clock per runtime, spanning every tenant), so the mark is
    **not** tenant-partitioned — a single collection holds it for the deployment."""

    collection: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """``(database, collection)`` holding the high-water mark (see the store for the shape)."""

    node_key: str = "default"
    """Document key this runtime writes. A single shared key (the default) records one
    deployment-wide mark; distinct per-replica keys avoid write contention on one document,
    and :meth:`~...MongoHlcCheckpointStore.load` reads the max across all keys either way."""
