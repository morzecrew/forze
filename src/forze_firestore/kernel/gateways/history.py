"""Firestore gateway for document revision history."""

from forze_firestore._compat import require_firestore

require_firestore()

# ....................... #

from typing import Sequence, final
from uuid import UUID

import attrs
from google.cloud.firestore_v1.base_query import And, FieldFilter

from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import JsonDict
from forze.base.serialization import PydanticRecordMappingCodec
from forze.domain.constants import (
    HISTORY_DATA_FIELD,
    HISTORY_SOURCE_FIELD,
    ID_FIELD,
    REV_FIELD,
)
from forze.domain.models import Document, DocumentHistory

from ..relation import RelationSpec, resolve_firestore_collection
from .base import FirestoreGateway

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class FirestoreHistoryGateway[D: Document](FirestoreGateway[D]):
    """Gateway for document revision history in Firestore."""

    target_relation: RelationSpec
    """Write collection ``(database, collection)`` this history tracks."""

    _target_resolved: tuple[str, str] | None = attrs.field(
        default=None,
        init=False,
        eq=False,
        repr=False,
    )

    # ....................... #

    async def _history_source_key(self) -> str:
        if self._target_resolved is not None:
            database, collection = self._target_resolved

        else:
            database, collection = await resolve_firestore_collection(
                self.target_relation,
                self._tenant_id_for_resolve(),
            )
            object.__setattr__(self, "_target_resolved", (database, collection))

        return f"{database}.{collection}"

    # ....................... #

    async def read(self, pk: UUID, rev: int) -> D:
        flt = And(
            filters=[
                FieldFilter(
                    HISTORY_SOURCE_FIELD,
                    "==",
                    await self._history_source_key(),
                ),
                FieldFilter(ID_FIELD, "==", self._storage_pk(pk)),
                FieldFilter(REV_FIELD, "==", rev),
            ]
        )
        flt = self._add_tenant_filter(flt)  # type: ignore[assignment]
        rows = await self.client.query_stream(await self.coll(), filters=flt, limit=1)

        if not rows:
            raise exc.not_found(f"History not found: {pk}, {rev}")

        payload = rows[0].get(HISTORY_DATA_FIELD)

        if payload is None:
            raise exc.not_found(f"History payload not found: {pk}, {rev}")

        return self._decode_row(payload)

    # ....................... #

    async def read_many(self, pks: Sequence[UUID], revs: Sequence[int]) -> Sequence[D]:
        if len(pks) != len(revs):
            raise exc.validation("Length of pks and revs must be the same")

        if not pks:
            return []

        # Firestore compound OR queries are brittle on the emulator; read sequentially.
        ordered: list[D] = []

        for pk, rev in zip(pks, revs, strict=True):
            try:
                ordered.append(await self.read(pk, rev))

            except CoreException as err:
                if err.kind is ExceptionKind.NOT_FOUND:
                    continue

                raise

        return ordered

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
        record = await self._from_data(data)
        raw_payload = PydanticRecordMappingCodec(
            DocumentHistory[D]
        ).encode_persistence_mapping(
            record,
        )
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

        source_key = await self._history_source_key()
        documents: list[tuple[str, JsonDict]] = []

        for item in data:
            record = DocumentHistory(
                source=source_key,
                id=item.id,
                rev=item.rev,
                data=item,
            )
            raw_payload = PydanticRecordMappingCodec(
                DocumentHistory[D]
            ).encode_persistence_mapping(
                record,
            )

            raw_payload = self.adapt_payload_for_write(raw_payload)
            documents.append((f"{self._storage_pk(item.id)}_{item.rev}", raw_payload))

        await self.client.insert_many(await self.coll(), documents)
