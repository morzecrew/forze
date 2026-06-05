"""Firestore gateway for document revision history."""

from forze_firestore._compat import require_firestore

require_firestore()

# ....................... #

from typing import Any, Sequence, final
from uuid import UUID

import attrs
from google.cloud.firestore_v1.base_query import And, FieldFilter

from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import JsonDict, OnceCell
from forze.base.serialization import ModelCodec
from forze.domain.constants import (
    HISTORY_DATA_FIELD,
    HISTORY_SOURCE_FIELD,
    ID_FIELD,
    REV_FIELD,
)
from forze.domain.models import Document, DocumentHistory

from ..relation import RelationSpec, is_static_relation, resolve_firestore_collection
from .base import FirestoreGateway

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class FirestoreHistoryGateway[D: Document](FirestoreGateway[D]):
    """Gateway for document revision history in Firestore."""

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
            return await resolve_firestore_collection(
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
        raw_payload = self.history_codec.encode_persistence_mapping(record)
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
            raw_payload = self.history_codec.encode_persistence_mapping(record)

            raw_payload = self.adapt_payload_for_write(raw_payload)
            documents.append((f"{self._storage_pk(item.id)}_{item.rev}", raw_payload))

        await self.client.insert_many(await self.coll(), documents)
