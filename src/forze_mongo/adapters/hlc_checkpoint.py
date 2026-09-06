"""Mongo HLC high-water-mark store — co-located, in-transaction advance."""

from __future__ import annotations

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from typing import final

import attrs
from pymongo.asynchronous.collection import AsyncCollection

from forze.application.contracts.hlc import HlcCheckpointPort
from forze.base.exceptions import exc
from forze.base.primitives import HlcTimestamp, JsonDict
from forze_mongo.execution.deps.configs.hlc_checkpoint import MongoHlcCheckpointConfig
from forze_mongo.kernel.client import MongoClientPort
from forze_mongo.kernel.relation import resolve_mongo_collection

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoHlcCheckpointStore(HlcCheckpointPort):
    """Mongo store for a node's HLC high-water mark.

    :meth:`advance` is a single ``$max`` upsert keyed on the node's ``_id``. The client
    attaches the ambient session automatically, so inside a transaction the mark commits —
    or rolls back — atomically with the HLC-stamped writes it guards: a committed stamp is
    never durable without a mark covering it. :meth:`load` reads the highest mark across
    every node document at startup, so a restart resumes above the whole deployment's
    emissions.

    ``$max`` is the whole monotonicity story, and it is the operator Postgres spells
    ``GREATEST``: the server compares and keeps the larger value in one atomic update, so
    concurrent or out-of-order writers never lower the mark and no read-modify-write window
    exists to lose one. On a missing document it inserts with the field set, which is why
    the upsert needs no separate seed.

    Documents look like ``{_id, hlc}``, where ``_id`` is the ``node_key`` and ``hlc`` is the
    packed timestamp (``physical_ms << 16 | logical``, an int64 for any wall clock this
    side of the year 6000). **The application owns the collection and needs no index for
    it**: the write is keyed on ``_id``, and :meth:`load` reads one document per node — a
    handful — so the descending scan it sorts has nothing to grow into. That is one
    migration fewer than the Postgres table, which needs its primary key declared.
    """

    client: MongoClientPort
    config: MongoHlcCheckpointConfig

    # ....................... #

    async def _collection(self) -> AsyncCollection[JsonDict]:
        # Node-global (not tenant-partitioned): resolve without a tenant.
        db_name, coll_name = await resolve_mongo_collection(self.config.collection, None)

        return await self.client.collection(coll_name, db_name=db_name)

    # ....................... #

    async def load(self) -> HlcTimestamp | None:
        coll = await self._collection()

        # The max across every node's document, as one sorted read rather than an
        # aggregation: there is one document per node key, so "sort descending, take one" is
        # the whole scan and needs no index and no pipeline.
        rows = await self.client.find_many(
            coll,
            {},
            projection={"hlc": 1},
            sort=[("hlc", -1)],
            limit=1,
        )

        # Emptiness is "no documents at all", never "the front document has no usable
        # mark". Mongo sorts a null field exactly where a missing one sorts, so a mark
        # overwritten with ``null`` looks identical to a collection nothing ever wrote —
        # and reading either as "no mark" is the failure this store exists to prevent: the
        # clock resumes at ``(0, 0)`` and re-issues beneath stamps already relayed, in
        # silence. Every document here is written by ``advance``, which always sets an
        # integer, so anything else present is corruption and says so.
        if not rows:
            return None

        packed = rows[0].get("hlc")

        if not isinstance(packed, int) or isinstance(packed, bool):
            raise exc.configuration(
                f"HLC checkpoint document {rows[0].get('_id')!r} holds no usable mark "
                f"({packed!r}); the collection must hold only marks this store wrote.",
            )

        return HlcTimestamp.unpack(packed)

    # ....................... #

    async def advance(self, mark: HlcTimestamp) -> None:
        coll = await self._collection()

        # One ``$max`` upsert on this node's document, inside the business transaction so
        # the mark commits with the rows it stamps. The server does the comparison, so a
        # mark at or below the stored value writes nothing and cannot lower it.
        await self.client.update_one_upsert(
            coll,
            {"_id": self.config.node_key},
            {"$max": {"hlc": mark.pack()}},
        )
