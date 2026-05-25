"""Firestore gateway for document revision history."""

from forze_firestore._compat import require_firestore

require_firestore()

# ....................... #

from functools import cached_property
from typing import Sequence, final
from uuid import UUID

import attrs
from google.cloud.firestore_v1.base_query import And, FieldFilter

from forze.base.errors import NotFoundError, ValidationError
from forze.base.primitives import JsonDict
from forze.base.serialization import (
    pydantic_dump,
    pydantic_validate,
)
from forze.domain.constants import (
    HISTORY_DATA_FIELD,
    HISTORY_SOURCE_FIELD,
    ID_FIELD,
    REV_FIELD,
)
from forze.domain.models import Document, DocumentHistory

from .base import FirestoreGateway

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class FirestoreHistoryGateway[D: Document](FirestoreGateway[D]):
    """Gateway for document revision history in Firestore."""

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
        flt = And(
            filters=[
                FieldFilter(HISTORY_SOURCE_FIELD, "==", self._full_target),
                FieldFilter(ID_FIELD, "==", self._storage_pk(pk)),
                FieldFilter(REV_FIELD, "==", rev),
            ]
        )
        flt = self._add_tenant_filter(flt)  # type: ignore[assignment]
        rows = await self.client.query_stream(await self.coll(), filters=flt, limit=1)

        if not rows:
            raise NotFoundError(f"History not found: {pk}, {rev}")

        payload = rows[0].get(HISTORY_DATA_FIELD)

        if payload is None:
            raise NotFoundError(f"History payload not found: {pk}, {rev}")

        return pydantic_validate(self.model_type, payload)

    # ....................... #

    async def read_many(self, pks: Sequence[UUID], revs: Sequence[int]) -> Sequence[D]:
        if len(pks) != len(revs):
            raise ValidationError("Length of pks and revs must be the same")

        if not pks:
            return []

        # Firestore compound OR queries are brittle on the emulator; read sequentially.
        ordered: list[D] = []

        for pk, rev in zip(pks, revs, strict=True):
            try:
                ordered.append(await self.read(pk, rev))
            except NotFoundError:
                continue

        return ordered

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
        record = self._from_data(data)
        raw_payload = pydantic_dump(record)
        raw_payload = self.adapt_payload_for_write(raw_payload)

        await self.client.set_document(
            await self.coll(),
            f"{self._storage_pk(data.id)}_{data.rev}",
            raw_payload,
        )

    # ....................... #

    async def write_many(self, data: Sequence[D]) -> None:
        if not data:
            return

        documents: list[tuple[str, JsonDict]] = []

        for item in data:
            record = self._from_data(item)
            raw_payload = pydantic_dump(record)
            raw_payload = self.adapt_payload_for_write(raw_payload)
            documents.append((f"{self._storage_pk(item.id)}_{item.rev}", raw_payload))

        await self.client.insert_many(await self.coll(), documents)
