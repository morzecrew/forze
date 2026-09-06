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
    """Document key this runtime writes; :meth:`~...MongoHlcCheckpointStore.load` reads the
    max across all keys either way.

    **Give each replica its own key when more than one flushes concurrently.** The default
    records one deployment-wide mark, and on Mongo two replicas advancing it inside their
    business transactions contend on a single document: the server aborts one with a
    write conflict, which surfaces as a transient transaction error the flush must retry.
    Postgres serialises the same contention on a row lock instead, so this is a difference
    worth wiring around rather than inheriting.

    Keep the set of keys small and **stable** — one per replica, not one per boot. Recovery
    reads every key's document, so keys minted per process turn a fixed handful into an
    unbounded scan."""
