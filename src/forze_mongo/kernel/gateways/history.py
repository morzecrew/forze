"""Mongo gateway for document revision history storage and retrieval."""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from collections.abc import Sequence
from typing import Any, final
from uuid import UUID

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import OnceCell
from forze.base.serialization import ModelCodec
from forze.domain.constants import (
    HISTORY_DATA_FIELD,
    HISTORY_SOURCE_FIELD,
    ID_FIELD,
    REV_FIELD,
)
from forze.domain.models import Document, DocumentHistory

from ..relation import RelationSpec, is_static_relation, resolve_mongo_collection
from .base import MongoGateway

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoHistoryGateway[D: Document](MongoGateway[D]):
    """Gateway for persisting and querying document revision history in Mongo.

    Each history record wraps a full document snapshot keyed by the document's
    ID and revision number, scoped to a :attr:`target_source` collection. Used
    by :class:`MongoWriteGateway` to enable historical consistency checks.
    """

    target_relation: RelationSpec
    """Write collection ``(database, collection)`` this history tracks."""

    history_codec: ModelCodec[Any, Any] = attrs.field(kw_only=True, eq=False, repr=False)
    """Codec for :class:`~forze.domain.models.DocumentHistory` persistence rows."""

    _target_cell: OnceCell[tuple[str, str]] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()

    # ....................... #

    async def _history_source_key(self) -> str:
        async def _factory() -> tuple[str, str]:
            return await resolve_mongo_collection(
                self.target_relation,
                self._tenant_id_for_resolve(),
            )

        # Only memoize tenant-independent (static) relations; a dynamic resolver
        # depends on the bound tenant and the adapter may be shared across tenants.
        database, collection = await self._target_cell.resolve(
            _factory,
            cache=is_static_relation(self.target_relation),
        )

        return f"{database}.{collection}"

    # ....................... #

    async def read(self, pk: UUID, rev: int) -> D:
        """Retrieve a single historical snapshot by primary key and revision.

        :param pk: Document primary key.
        :param rev: Revision number.
        :raises NotFoundError: If the history record or its payload is missing.
        """

        raw = await self.client.find_one(
            await self.coll(),
            {
                HISTORY_SOURCE_FIELD: await self._history_source_key(),
                ID_FIELD: self._storage_pk(pk),
                REV_FIELD: rev,
            },
        )

        if raw is None:
            raise exc.not_found(f"History not found: {pk}, {rev}")

        payload = raw.get(HISTORY_DATA_FIELD)
        if payload is None:
            raise exc.not_found(f"History payload not found: {pk}, {rev}")

        return self._decode_row(payload)

    # ....................... #

    async def read_many(self, pks: Sequence[UUID], revs: Sequence[int]) -> Sequence[D]:
        """Retrieve multiple historical snapshots by primary key and revision pairs.

        Results are returned in the same order as the inputs. Pairs that
        cannot be found are silently omitted.

        :param pks: Document primary keys.
        :param revs: Corresponding revision numbers (same length as *pks*).
        :raises ValidationError: If the lengths of *pks* and *revs* differ.
        """

        if len(pks) != len(revs):
            raise exc.precondition("Length of pks and revs must be the same")

        if not pks:
            return []

        lookup = [
            {ID_FIELD: self._storage_pk(pk), REV_FIELD: rev}
            for pk, rev in zip(pks, revs, strict=True)
        ]
        rows = await self.client.find_many(
            await self.coll(),
            {
                HISTORY_SOURCE_FIELD: await self._history_source_key(),
                "$or": lookup,
            },
        )
        keyed = {
            (
                str(row.get(ID_FIELD)),
                int(row.get(REV_FIELD)),  # type: ignore[arg-type]
            ): row
            for row in rows
        }
        ordered_raw: list[Any] = []

        for pk, rev in zip(pks, revs, strict=True):
            row = keyed.get((self._storage_pk(pk), rev))

            if row is None:
                continue

            payload = row.get(HISTORY_DATA_FIELD)

            if payload is None:
                continue

            ordered_raw.append(payload)

        return self._decode_rows(ordered_raw)

    # ....................... #

    async def _from_data(self, data: D) -> DocumentHistory[D]:
        return DocumentHistory(
            source=await self._history_source_key(),
            id=data.id,
            rev=data.rev,
            data=data,
        )

    # ....................... #

    async def write(self, data: D) -> None:
        """Persist a single document snapshot as a history record.

        :param data: Document to snapshot.
        """

        record = await self._from_data(data)
        raw_payload = self.history_codec.encode_persistence_mapping(record)
        raw_payload = self.adapt_payload_for_write(raw_payload)

        payload = self._coerce_query_value(raw_payload)

        await self.client.insert_one(await self.coll(), payload)

    # ....................... #

    async def write_many(self, data: Sequence[D]) -> None:
        """Persist multiple document snapshots as history records in bulk.

        :param data: Documents to snapshot. No-ops when empty.
        """

        if not data:
            return

        source_key = await self._history_source_key()
        records = [
            DocumentHistory(
                source=source_key,
                id=item.id,
                rev=item.rev,
                data=item,
            )
            for item in data
        ]
        raw_payloads = self.history_codec.encode_persistence_mapping_many(records)
        raw_payloads = list(map(self.adapt_payload_for_write, raw_payloads))

        payloads = list(map(self._coerce_query_value, raw_payloads))

        await self.client.insert_many(await self.coll(), payloads)
