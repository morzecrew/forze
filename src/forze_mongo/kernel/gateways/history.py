"""Mongo gateway for document revision history storage and retrieval."""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from functools import cached_property
from typing import Any, Sequence, final
from uuid import UUID

import attrs

from forze.base.errors import NotFoundError, ValidationError
from forze.base.serialization import (
    pydantic_dump,
    pydantic_dump_many,
    pydantic_validate,
    pydantic_validate_many,
)
from forze.domain.constants import (
    HISTORY_DATA_FIELD,
    HISTORY_SOURCE_FIELD,
    ID_FIELD,
    REV_FIELD,
)
from forze.domain.models import Document, DocumentHistory

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

    target_database: str
    """Name of the database where the target collection resides."""

    target_collection: str
    """Name of the primary collection this history tracks."""

    # ....................... #

    @cached_property
    def _full_target(self) -> str:
        return f"{self.target_database}.{self.target_collection}"

    # ....................... #

    async def read(self, pk: UUID, rev: int) -> D:
        """Retrieve a single historical snapshot by primary key and revision.

        :param pk: Document primary key.
        :param rev: Revision number.
        :raises NotFoundError: If the history record or its payload is missing.
        """

        raw = await self.client.find_one(
            self.coll(),
            {
                HISTORY_SOURCE_FIELD: self._full_target,
                ID_FIELD: self._storage_pk(pk),
                REV_FIELD: rev,
            },
        )

        if raw is None:
            raise NotFoundError(f"History not found: {pk}, {rev}")

        payload = raw.get(HISTORY_DATA_FIELD)
        if payload is None:
            raise NotFoundError(f"History payload not found: {pk}, {rev}")

        return pydantic_validate(self.model_type, payload)

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
            raise ValidationError("Length of pks and revs must be the same")

        if not pks:
            return []

        lookup = [
            {ID_FIELD: self._storage_pk(pk), REV_FIELD: rev}
            for pk, rev in zip(pks, revs, strict=True)
        ]
        rows = await self.client.find_many(
            self.coll(),
            {
                HISTORY_SOURCE_FIELD: self._full_target,
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

        return pydantic_validate_many(self.model_type, ordered_raw)

    # ....................... #

    def _from_data(self, data: D) -> DocumentHistory[D]:
        return DocumentHistory(
            source=self._full_target,
            id=data.id,
            rev=data.rev,
            data=data,
        )

    # ....................... #

    async def write(self, data: D) -> None:
        """Persist a single document snapshot as a history record.

        :param data: Document to snapshot.
        """

        record = self._from_data(data)
        payload = pydantic_dump(record)
        await self.client.insert_one(self.coll(), self._coerce_query_value(payload))

    # ....................... #

    async def write_many(self, data: Sequence[D]) -> None:
        """Persist multiple document snapshots as history records in bulk.

        :param data: Documents to snapshot. No-ops when empty.
        """

        if not data:
            return

        records = list(map(self._from_data, data))
        raw_payloads = pydantic_dump_many(records)
        payloads = list(map(self._coerce_query_value, raw_payloads))

        await self.client.insert_many(self.coll(), payloads)
