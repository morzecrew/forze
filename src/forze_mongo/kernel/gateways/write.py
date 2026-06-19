"""Mongo gateway for document write operations (create, update, delete, restore)."""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from datetime import UTC, datetime
from typing import Any, Mapping, Sequence, cast, final
from uuid import UUID

import attrs
from pymongo import UpdateOne

from forze.application.contracts.querying import QueryFilterExpression
from forze.application.contracts.resilience import ResilienceExecutorPort
from forze.application.execution.resilience import (
    default_resilience_executor,
    occ_retry,
)
from forze.application.integrations.persistence import (
    DocumentWriteCodecMixin,
    HistoryOccMixin,
)
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import JsonDict
from forze.base.serialization import ModelCodec
from forze.domain.constants import ID_FIELD, REV_FIELD
from forze.domain.models import BaseDTO, Document

from ..relation import relations_match
from .base import MongoGateway
from .history import MongoHistoryGateway
from .read import MongoReadGateway

# ----------------------- #


def _bson_normalize_value(value: Any) -> Any:
    """Normalize a value the way a BSON write/read round trip would.

    BSON stores datetimes as UTC milliseconds since the epoch, and the client
    is not ``tz_aware``, so reads yield naive UTC datetimes truncated to
    millisecond precision. Applying the same normalization to an insert
    payload lets us decode it in memory and return a model identical to what
    a subsequent read would produce.
    """

    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(UTC).replace(tzinfo=None)

        return value.replace(microsecond=(value.microsecond // 1000) * 1000)

    if isinstance(value, list):
        return [
            _bson_normalize_value(item)
            for item in value  # pyright: ignore[reportUnknownVariableType]
        ]

    if isinstance(value, dict):
        return {
            key: _bson_normalize_value(item)
            for key, item in cast(JsonDict, value).items()
        }

    return value


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoWriteGateway[D: Document, C: BaseDTO, U: BaseDTO](
    DocumentWriteCodecMixin[D],
    HistoryOccMixin[D],
    MongoGateway[D],
):
    """Write gateway for Mongo documents with optimistic concurrency and optional history.

    Uses a :class:`MongoReadGateway` for read-before-write patterns and
    delegates history snapshots to an optional :class:`MongoHistoryGateway`.
    Revision bumps are controlled by :attr:`rev_bump_strategy`; concurrent
    writes to the same revision are detected and raise
    :exc:`~forze.base.errors.ConcurrencyError`.
    """

    read_gw: MongoReadGateway[D]
    """Companion read gateway; must share the same client, source, and database."""

    resilience: ResilienceExecutorPort = attrs.field(
        factory=default_resilience_executor,
        eq=False,
        repr=False,
    )
    """Resilience executor backing optimistic-concurrency retries."""

    create_cmd_type: type[C]
    """Pydantic model for creation payloads."""

    update_cmd_type: type[U] | None = attrs.field(default=None)
    """Pydantic model for update payloads."""

    create_codec: ModelCodec[D, Any] = attrs.field(kw_only=True, eq=False, repr=False)

    update_codec: ModelCodec[U, Any] | None = attrs.field(
        kw_only=True, eq=False, repr=False
    )

    history_gw: MongoHistoryGateway[D] | None = attrs.field(default=None)  # type: ignore[override]
    """Optional history gateway for revision snapshots."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()

        if not relations_match(self.relation, self.read_gw.relation):
            raise exc.configuration(
                "Relation mismatch. Write gateway and nested read gateway must use the same relation."
            )

        if self.client is not self.read_gw.client:
            raise exc.configuration(
                "Client mismatch. Write gateway and nested read gateway must use the same client."
            )

        if self.tenant_aware != self.read_gw.tenant_aware:
            raise exc.configuration(
                "Tenant awareness mismatch. Write gateway and nested read gateway must have the same tenant awareness."
            )

        if self.history_gw is not None:
            if self.client is not self.history_gw.client:
                raise exc.configuration(
                    "Client mismatch. Write gateway and nested history gateway must use the same client."
                )

            if not relations_match(self.relation, self.history_gw.target_relation):
                raise exc.configuration(
                    "Relation mismatch. Write gateway and nested history gateway must point to the same write relation."
                )

            if self.tenant_aware != self.history_gw.tenant_aware:
                raise exc.configuration(
                    "Tenant awareness mismatch. Write gateway and nested history gateway must have the same tenant awareness."
                )

    # ....................... #

    # ....................... #

    def _require_update_cmd(self) -> None:
        if self.update_cmd_type is None:
            raise exc.configuration(
                "Update command type is not supported for this model"
            )

    # ....................... #

    def _upsert_filter_for_id(self, pk: UUID) -> JsonDict:
        """Build the upsert filter for a document primary key."""

        return self._add_tenant_filter({"_id": self._storage_pk(pk)})

    def _bulk_upsert_set_on_insert_ops(
        self,
        models: Sequence[D],
        payloads: Sequence[JsonDict],
    ) -> list[UpdateOne]:
        """Build ``UpdateOne`` upserts with ``$setOnInsert`` for a batch."""

        return [
            UpdateOne(
                self._upsert_filter_for_id(m.id),
                {"$setOnInsert": self._storage_doc(p)},
                upsert=True,
            )
            for m, p in zip(models, payloads, strict=True)
        ]

    async def _run_bulk_upsert_set_on_insert(
        self,
        ops: list[UpdateOne],
        *,
        offset: int,
    ) -> set[int]:
        """Run bulk upserts; return global indices of newly inserted documents."""

        bres: Any = await self.client.bulk_write(
            await self.coll(),
            ops,
            ordered=False,
        )
        umap = cast(Mapping[int, Any], bres.upserted_ids or {})

        return {offset + int(idx) for idx in umap}

    async def _load_existing_by_ids(self, ids: Sequence[UUID]) -> dict[UUID, D]:
        """Load documents by primary key after a bulk upsert did not insert them."""

        if not ids:
            return {}

        try:
            fetched = await self.read_gw.get_many(ids)

        except CoreException as err:
            if err.kind is ExceptionKind.NOT_FOUND:
                raise exc.conflict(
                    "Document not inserted and not found by primary key; "
                    "possible unique index violation on insert.",
                    code="mongo_ensure_bulk_miss",
                ) from err

            raise

        return {d.id: d for d in fetched}

    # ....................... #

    def _from_cdto(self, payload: C, id: UUID | None = None) -> D:
        model = self.create_codec.transform(payload)

        if id is not None:
            model = model.model_copy(update={ID_FIELD: id}, deep=True)

        return model

    # ....................... #

    def _from_cdto_many(
        self,
        payloads: Sequence[C],
        ids: Sequence[UUID] | None = None,
    ) -> Sequence[D]:
        models = list(self.create_codec.transform_many(payloads))

        if ids is not None:
            models = [
                m.model_copy(update={ID_FIELD: i}, deep=True)
                for m, i in zip(models, ids, strict=True)
            ]

        return models

    # ....................... #

    def _patch_codec(self) -> ModelCodec[Any, Any]:
        if self.update_codec is not None:
            return self.update_codec

        if self.update_cmd_type is not None:
            raise exc.configuration(
                "Update codec is required when update commands are supported"
            )

        return self.read_codec

    # ....................... #

    def _decode_inserted(self, storage_doc: JsonDict) -> D:
        """Decode the exact inserted storage document back into a domain model.

        Mongo applies no server-side defaults or transforms, so the insert
        payload *is* the stored document; decoding it in memory replaces the
        post-insert read-back round trip. The document is first normalized via
        :func:`_bson_normalize_value` (millisecond truncation, naive UTC) so
        the returned model matches what a subsequent read returns. Unlike the
        raw ``_from_cdto`` model, the decoded model has every field explicitly
        set, which the adapter's ``hydrate_from_write`` transform
        (``exclude={"unset": True}``) relies on.
        """

        normalized = cast(JsonDict, _bson_normalize_value(storage_doc))

        return self._decode_row(self._from_storage_doc(normalized))

    # ....................... #

    @occ_retry
    async def create(self, payload: C, *, id: UUID | None = None) -> D:
        """Insert a new document from a creation payload and record its history.

        :param payload: Creation payload (domain fields only).
        :param id: Optional caller-chosen primary key; server-generated when omitted.
        :returns: The persisted domain document.
        """

        model = self._from_cdto(payload, id)
        data = await self._encode_domain_one(model)
        data = self.adapt_payload_for_write(data, create=True)
        storage = self._storage_doc(data)

        await self.client.insert_one(await self.coll(), storage)

        created = self._decode_inserted(storage)
        await self._write_history(created)

        return created

    # ....................... #

    @occ_retry
    async def create_many(
        self,
        payloads: Sequence[C],
        *,
        batch_size: int = 200,
    ) -> Sequence[D]:
        """Bulk-insert documents from creation payloads and record their history.

        :param payloads: Creation payloads (server-assigned ids). No-ops when empty.
        """

        if not payloads:
            return []

        models = self._from_cdto_many(payloads)
        raw_payloads = await self._encode_domain_many(models)
        write_payloads = self.adapt_many_payload_for_write(raw_payloads, create=True)
        docs = list(map(self._storage_doc, write_payloads))

        await self.client.insert_many(await self.coll(), docs, batch_size=batch_size)

        created = [self._decode_inserted(doc) for doc in docs]
        await self._write_history(*created)

        return created

    # ....................... #

    @occ_retry
    async def ensure(self, id: UUID, payload: C) -> D:
        """Insert a document at *id* when missing using ``$setOnInsert``; no updates on match."""

        model = self._from_cdto(payload, id)
        data = await self._encode_domain_one(model)
        data = self.adapt_payload_for_write(data, create=True)
        storage = self._storage_doc(data)
        res: Any = await self.client.update_one_upsert(
            await self.coll(),
            self._upsert_filter_for_id(model.id),
            {"$setOnInsert": storage},
        )

        if res.upserted_id is not None:
            created = self._decode_inserted(storage)
            await self._write_history(created)
            return created

        return await self.read_gw.get(model.id)

    # ....................... #

    @occ_retry
    async def ensure_many(
        self,
        ids: Sequence[UUID],
        payloads: Sequence[C],
        *,
        batch_size: int = 200,
    ) -> Sequence[D]:
        """Bulk insert-when-missing with ``$setOnInsert`` upserts; order preserved for reads."""

        if not payloads:
            return []

        models = self._from_cdto_many(payloads, ids)
        raw_payloads = await self._encode_domain_many(models)
        write_payloads = self.adapt_many_payload_for_write(raw_payloads, create=True)

        inserted_idx: set[int] = set()

        for offset in range(0, len(write_payloads), batch_size):
            chunk_payloads = write_payloads[offset : offset + batch_size]
            chunk_models = models[offset : offset + batch_size]
            ops = self._bulk_upsert_set_on_insert_ops(chunk_models, chunk_payloads)
            inserted_idx |= await self._run_bulk_upsert_set_on_insert(
                ops,
                offset=offset,
            )

        by_inserted: dict[int, D] = {
            i: self._decode_inserted(self._storage_doc(write_payloads[i]))
            for i in inserted_idx
        }

        if by_inserted:
            await self._write_history(*by_inserted.values())

        conflict_ids = [m.id for i, m in enumerate(models) if i not in inserted_idx]
        by_existing = await self._load_existing_by_ids(conflict_ids)

        return [
            by_inserted[i] if i in inserted_idx else by_existing[models[i].id]
            for i in range(len(models))
        ]

    # ....................... #

    @occ_retry
    async def upsert(self, id: UUID, create: C, update: U) -> D:
        """Insert *create* at *id* with ``$setOnInsert`` when missing; else delegate to :meth:`update`."""

        self._require_update_cmd()

        model = self._from_cdto(create, id)
        data = await self._encode_domain_one(model)
        data = self.adapt_payload_for_write(data, create=True)
        storage = self._storage_doc(data)

        res: Any = await self.client.update_one_upsert(
            await self.coll(),
            self._upsert_filter_for_id(model.id),
            {"$setOnInsert": storage},
        )
        if res.upserted_id is not None:
            created = self._decode_inserted(storage)
            await self._write_history(created)
            return created

        current = await self.read_gw.get(model.id)
        u_res, _ = await self.update(model.id, update, rev=current.rev)
        return u_res

    # ....................... #

    @occ_retry
    async def upsert_many(
        self,
        ids: Sequence[UUID],
        creates: Sequence[C],
        updates: Sequence[U],
        *,
        batch_size: int = 200,
    ) -> Sequence[D]:
        """Bulk :meth:`upsert`: ``$setOnInsert`` batch, then :meth:`update_many` for existing."""

        self._require_update_cmd()

        if not creates:
            return []

        models = self._from_cdto_many(creates, ids)
        raw_payloads = await self._encode_domain_many(models)
        payloads = self.adapt_many_payload_for_write(raw_payloads, create=True)
        u_all = list(updates)

        inserted_idx: set[int] = set()

        for offset in range(0, len(creates), batch_size):
            chunk_payloads = payloads[offset : offset + batch_size]
            chunk_models = models[offset : offset + batch_size]
            ops = self._bulk_upsert_set_on_insert_ops(chunk_models, chunk_payloads)
            inserted_idx |= await self._run_bulk_upsert_set_on_insert(
                ops,
                offset=offset,
            )

        by_inserted: dict[int, D] = {
            i: self._decode_inserted(self._storage_doc(payloads[i]))
            for i in inserted_idx
        }

        if by_inserted:
            await self._write_history(*by_inserted.values())

        to_update: list[tuple[UUID, U]] = []
        by_updated: dict[UUID, D] = {}

        for i, m in enumerate(models):
            if i in inserted_idx:
                continue

            to_update.append((m.id, u_all[i]))

        if to_update:
            pks = [a[0] for a in to_update]
            u_dtos = [a[1] for a in to_update]
            by_c = await self._load_existing_by_ids(pks)
            revs = [by_c[pk].rev for pk in pks]
            updated, _ = await self.update_many(
                pks, u_dtos, revs=revs, batch_size=batch_size
            )
            by_updated = {d.id: d for d in updated}

        return [
            by_inserted[i] if i in inserted_idx else by_updated[models[i].id]
            for i in range(len(models))
        ]

    # ....................... #

    def _bump_rev(self, current: D, diff: JsonDict) -> JsonDict:
        diff[REV_FIELD] = current.rev + 1

        return diff

    # ....................... #

    @occ_retry
    async def _patch(
        self,
        pk: UUID,
        update: JsonDict | None = None,
        *,
        rev: int | None = None,
    ) -> tuple[D, JsonDict]:
        current = await self.read_gw.get(pk)

        if update is not None:
            if rev is not None:
                await self._validate_history((current, rev, update))

            _, diff = current.update(update, materialized=self.read_codec.materialized)

        else:
            _, diff = current.touch()

        if not diff:
            return current, diff

        diff = self._bump_rev(current, diff)
        diff = self.adapt_payload_for_write(diff, create=False)

        flt = self._add_tenant_filter(
            {"_id": self._storage_pk(current.id), REV_FIELD: current.rev}
        )
        # Atomic update-and-return: one round trip instead of update + re-get,
        # and the returned document cannot contain a concurrent writer's fields
        # (no read-back race between the update and the snapshot).
        raw = await self.client.find_one_and_update(
            await self.coll(),
            flt,
            {"$set": self._coerce_query_value(diff)},
        )

        if raw is None:
            # Not-found vs stale-rev are indistinguishable here; the preceding
            # ``read_gw.get`` already raised NOT_FOUND for missing documents.
            raise exc.concurrency("Failed to update record")

        updated = self._decode_row(self._from_storage_doc(raw))
        await self._write_history(updated)

        return updated, diff

    # ....................... #

    @occ_retry
    async def _patch_many(
        self,
        pks: Sequence[UUID],
        updates: Sequence[JsonDict] | None = None,
        *,
        revs: Sequence[int] | None = None,
        batch_size: int = 200,
    ) -> tuple[Sequence[D], Sequence[JsonDict]]:
        if not pks or (not updates and updates is not None):
            return [], []

        currents = await self.read_gw.get_many(pks)

        # 1. Validation and preparation
        to_patch: list[tuple[int, D, JsonDict]] = []

        if updates is not None:
            if revs is not None:
                await self._validate_history(
                    *[
                        (c, r, u)
                        for c, r, u in zip(currents, revs, updates, strict=True)
                    ]
                )

            for i, (current, update) in enumerate(zip(currents, updates, strict=True)):
                _, diff = current.update(update, materialized=self.read_codec.materialized)
                if diff:
                    to_patch.append((i, current, diff))
        else:
            for i, current in enumerate(currents):
                _, diff = current.touch()
                if diff:
                    to_patch.append((i, current, diff))

        if not to_patch:
            return currents, [{} for _ in currents]

        # 2. Execution (Bulk)
        id_to_written: dict[UUID, JsonDict] = {}
        operations: list[tuple[JsonDict, JsonDict]] = []
        for _, current, diff in to_patch:
            bumped = self._bump_rev(current, diff)
            bumped = self.adapt_payload_for_write(bumped, create=False)
            id_to_written[current.id] = bumped
            flt = self._add_tenant_filter(
                {"_id": self._storage_pk(current.id), REV_FIELD: current.rev}
            )
            operations.append(
                (
                    flt,
                    {"$set": self._coerce_query_value(bumped)},
                )
            )

        matched = await self.client.bulk_update(
            await self.coll(), operations, batch_size=batch_size
        )
        if matched != len(to_patch):
            raise exc.concurrency("Failed to update one or more records")

        updated = await self.read_gw.get_many(pks)
        await self._write_history(*updated)

        res_diffs = [id_to_written.get(c.id, {}) for c in currents]

        return updated, res_diffs

    # ....................... #

    async def update(
        self, pk: UUID, dto: U, *, rev: int | None = None
    ) -> tuple[D, JsonDict]:
        """Apply an update DTO to an existing document.

        :param pk: Document primary key.
        :param dto: Update payload.
        :param rev: Expected revision for historical consistency validation.
        :returns: The updated domain document and the adapted write payload (diff).
        """

        self._require_update_cmd()

        update_data = await self._encode_patch_one(dto, record_id=pk)
        return await self._patch(pk, update_data, rev=rev)

    # ....................... #

    async def update_many(
        self,
        pks: Sequence[UUID],
        dtos: Sequence[U],
        *,
        revs: Sequence[int] | None = None,
        batch_size: int = 200,
    ) -> tuple[Sequence[D], Sequence[JsonDict]]:
        """Bulk-update documents with corresponding DTOs.

        :param pks: Document primary keys (must be unique).
        :param dtos: Update payloads matching *pks* by position.
        :param revs: Optional expected revisions for history validation.
        :returns: Updated documents and per-document adapted write payloads (diffs).
        :raises exc.internal: If lengths of *pks* and *dtos* (or *revs*) differ.
        :raises ValidationError: If *pks* contains duplicates.
        """

        self._require_update_cmd()

        if len(pks) != len(dtos):
            raise exc.precondition("Length mismatch between primary keys and updates")

        if len(pks) != len(set(pks)):
            raise exc.precondition("Primary keys must be unique")

        if revs is not None and len(revs) != len(pks):
            raise exc.precondition("Length mismatch between primary keys and revisions")

        updates = await self._encode_patch_many(dtos, record_ids=pks)
        return await self._patch_many(pks, updates, revs=revs, batch_size=batch_size)

    # ....................... #

    @occ_retry
    async def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        batch_size: int = 200,
    ) -> tuple[int, Sequence[D]]:
        """Bulk-update documents matching *filters* using batched ``update_many``.

        Each batch targets primary keys from a keyset page (``id`` ascending)
        so cache and history can use stable identities. Revisions are bumped with
        ``$inc`` (no per-row expected ``rev`` in the filter).
        """

        self._require_update_cmd()
        self._reject_matching_update_with_materialized()

        update_data = await self._encode_patch_one(dto)

        if not update_data:
            return 0, []

        adapted = dict(self.adapt_payload_for_write(update_data, create=False))
        adapted.pop(REV_FIELD, None)

        if not adapted:
            return 0, []

        to_set = self._coerce_query_value(adapted)
        update_doc = {"$set": to_set, "$inc": {REV_FIELD: 1}}

        total = 0
        out_domains: list[D] = []
        last_id: UUID | None = None

        while True:
            chunk_filter: QueryFilterExpression = (  # type: ignore[valid-type]
                filters
                if last_id is None
                else {
                    "$and": [
                        filters,
                        {"$values": {ID_FIELD: {"$gt": last_id}}},
                    ]
                }
            )
            id_rows = await self.read_gw.find_many(
                filters=chunk_filter,
                limit=batch_size,
                sorts={ID_FIELD: "asc"},
                return_fields=[ID_FIELD],
            )
            if not id_rows:
                break

            pks = [UUID(str(row[ID_FIELD])) for row in id_rows]
            flt = self._add_tenant_filter(
                {"_id": {"$in": [self._storage_pk(pk) for pk in pks]}},
            )

            matched = await self.client.update_many(
                await self.coll(),
                flt,
                update_doc,
            )
            total += matched

            chunk_domains = await self.read_gw.get_many(pks)
            await self._write_history(*chunk_domains)
            out_domains.extend(chunk_domains)

            last_id = pks[-1]

            if len(pks) < batch_size:
                break

        return total, out_domains

    # ....................... #

    async def touch(self, pk: UUID) -> D:
        """Bump a document's revision without changing its data.

        :param pk: Document primary key.
        """

        res, _ = await self._patch(pk)

        return res

    # ....................... #

    async def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        batch_size: int = 200,
    ) -> Sequence[D]:
        """Bump revisions for multiple documents without changing their data.

        :param pks: Document primary keys (must be unique).
        :param batch_size: Batch size for the bulk operation.
        :raises ValidationError: If *pks* contains duplicates.
        """

        if len(pks) != len(set(pks)):
            raise exc.precondition("Primary keys must be unique")

        res, _ = await self._patch_many(pks, batch_size=batch_size)

        return res

    # ....................... #

    async def kill(self, pk: UUID) -> None:
        """Hard-delete a document from the collection.

        :param pk: Document primary key.
        :raises NotFoundError: If the document does not exist (or is not
            accessible in the current tenant scope).
        """

        n = await self.client.delete_one(
            await self.coll(),
            self._add_tenant_filter({"_id": self._storage_pk(pk)}),
        )

        if n == 0:
            raise exc.not_found(f"Record not found: {pk}")

    # ....................... #

    async def kill_many(self, pks: Sequence[UUID], *, batch_size: int = 200) -> None:
        """Hard-delete multiple documents from the collection.

        :param pks: Document primary keys (must be unique). No-ops when empty.
        :raises ValidationError: If *pks* contains duplicates.
        :raises NotFoundError: If any document does not exist (or is not
            accessible in the current tenant scope).
        """

        if not pks:
            return

        if len(pks) != len(set(pks)):
            raise exc.precondition("Primary keys must be unique")

        n = await self.client.delete_many(
            await self.coll(),
            self._add_tenant_filter(
                {"_id": {"$in": [self._storage_pk(pk) for pk in pks]}}
            ),
        )

        if n != len(pks):
            if self.tenant_aware:
                raise exc.not_found(
                    "Some records not found or not accessible in this tenant scope"
                )

            raise exc.not_found("Some records not found")
