"""Firestore gateway for document write operations."""

from forze_firestore._compat import require_firestore

require_firestore()

# ....................... #

from typing import Any, Sequence, cast, final
from uuid import UUID

import attrs

from forze.application.contracts.querying import QueryFilterExpression
from forze.application.contracts.resilience import ResilienceExecutorPort
from forze.application.execution.resilience import (
    default_resilience_executor,
    occ_retry,
)
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import JsonDict
from forze.base.serialization import ModelCodec
from forze.domain.constants import ID_FIELD, REV_FIELD
from forze.domain.models import BaseDTO, Document

from ..relation import relations_match
from .base import FirestoreGateway
from .history import FirestoreHistoryGateway
from .read import FirestoreReadGateway

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class FirestoreWriteGateway[D: Document, C: BaseDTO, U: BaseDTO](
    FirestoreGateway[D]
):
    """Write gateway for Firestore documents with optimistic concurrency."""

    read_gw: FirestoreReadGateway[D]
    resilience: ResilienceExecutorPort = attrs.field(
        factory=default_resilience_executor,
        eq=False,
        repr=False,
    )
    create_cmd_type: type[C]
    update_cmd_type: type[U] | None = attrs.field(default=None)
    create_codec: ModelCodec[D, Any] = attrs.field(kw_only=True, eq=False, repr=False)
    update_codec: ModelCodec[U, Any] | None = attrs.field(
        kw_only=True, eq=False, repr=False
    )
    history_gw: FirestoreHistoryGateway[D] | None = attrs.field(default=None)

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
            raise exc.internal("Update command type is not supported for this model")

    # ....................... #

    async def _write_history(self, *data: D) -> None:
        if self.history_gw is not None:
            await self.history_gw.write_many(data)

    # ....................... #

    def _materialize_after_write(self, payload: JsonDict) -> D:
        """Build a domain model from written storage without a post-write read."""

        return self._decode_row(self._from_storage_doc(payload))

    # ....................... #

    async def _load_after_write(self, pk: UUID, *, merged: JsonDict | None = None) -> D:
        """Return a document after a write, avoiding read-after-write inside transactions."""

        if merged is not None and self.client.is_in_transaction():
            return self._decode_row(merged)

        return await self.read_gw.get(pk)

    # ....................... #

    async def _validate_history(self, *data: tuple[D, int, JsonDict]) -> None:
        if self.history_gw is None:
            for current, rev, _ in data:
                if rev != current.rev:
                    raise exc.precondition(
                        "Revision mismatch", code="revision_mismatch"
                    )

            return

        to_check = [
            (current, rev, update)
            for current, rev, update in data
            if rev != current.rev
        ]
        bad_records = [rev for current, rev, _ in to_check if rev > current.rev]

        if bad_records:
            raise exc.precondition("Invalid revision number")

        if to_check:
            pks_to_check = [current.id for current, _, _ in to_check]
            revs_to_check = [rev for _, rev, _ in to_check]
            hist_records = await self.history_gw.read_many(pks_to_check, revs_to_check)

            if len(hist_records) != len(to_check):
                raise exc.not_found(
                    "History records not found. Please retry with actual revision number."
                )

            for (current, _, update), historical in zip(
                to_check, hist_records, strict=True
            ):
                if not current.validate_historical_consistency(historical, update):
                    raise exc.conflict(
                        "Historical consistency violation during update",
                        code="historical_consistency_violation",
                    )

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
            raise exc.internal(
                "Update codec is required when update commands are supported"
            )

        return self.read_codec

    # ....................... #

    @occ_retry
    async def create(self, payload: C, *, id: UUID | None = None) -> D:
        model = self._from_cdto(payload, id)
        data = self.read_codec.encode_persistence_mapping(model)
        data = self.adapt_payload_for_write(data, create=True)
        coll = await self.coll()
        await self.client.set_document(coll, self._storage_pk(model.id), data)
        if self.client.is_in_transaction():
            created = self._materialize_after_write(data)
        else:
            created = await self.read_gw.get(model.id)
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
        if not payloads:
            return []

        models = self._from_cdto_many(payloads)
        raw_payloads = self.read_codec.encode_persistence_mapping_many(models)
        write_payloads = self.adapt_many_payload_for_write(raw_payloads, create=True)
        documents = [
            (self._storage_pk(m.id), dict(p))
            for m, p in zip(models, write_payloads, strict=True)
        ]
        await self.client.insert_many(
            await self.coll(), documents, batch_size=batch_size
        )
        if self.client.is_in_transaction():
            created = [self._materialize_after_write(dict(p)) for p in write_payloads]
        else:
            created = await self.read_gw.get_many([model.id for model in models])
        await self._write_history(*created)
        return created

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

            _, diff = current.update(update)

        else:
            _, diff = current.touch()

        if not diff:
            return current, diff

        diff = self._bump_rev(current, diff)
        merged = self.read_codec.encode_persistence_mapping(current)
        merged.update(self.adapt_payload_for_write(diff, create=False))

        coll = await self.coll()
        storage = self.adapt_payload_for_write(merged, create=False)
        await self.client.set_document(coll, self._storage_pk(pk), storage)
        updated = await self._load_after_write(pk, merged=merged)
        await self._write_history(updated)

        return updated, diff

    # ....................... #

    async def update(
        self,
        pk: UUID,
        dto: U,
        *,
        rev: int | None = None,
    ) -> tuple[D, JsonDict]:
        self._require_update_cmd()
        update_data = self._patch_codec().encode_persistence_mapping(
            cast(Any, dto),
            exclude={"unset": True},
        )

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
        _ = batch_size
        self._require_update_cmd()

        if len(pks) != len(dtos):
            raise exc.internal("Length mismatch between primary keys and updates")

        if len(pks) != len(set(pks)):
            raise exc.precondition("Primary keys must be unique")

        if revs is not None and len(revs) != len(pks):
            raise exc.internal("Length mismatch between primary keys and revisions")

        results: list[D] = []
        diffs: list[JsonDict] = []

        for pk, dto, rev in zip(
            pks,
            dtos,
            revs if revs is not None else [None] * len(pks),
            strict=True,
        ):
            updated, diff = await self.update(pk, dto, rev=rev)
            results.append(updated)
            diffs.append(diff)

        return results, diffs

    # ....................... #

    @occ_retry
    async def ensure(self, id: UUID, payload: C) -> D:
        try:
            return await self.read_gw.get(id)

        except CoreException as err:
            if err.kind is ExceptionKind.NOT_FOUND:
                return await self.create(payload, id=id)

            raise

    # ....................... #

    @occ_retry
    async def ensure_many(
        self,
        ids: Sequence[UUID],
        payloads: Sequence[C],
        *,
        batch_size: int = 200,
    ) -> Sequence[D]:
        if not payloads:
            return []

        out: list[D] = []

        for id_, payload in zip(ids, payloads, strict=True):
            out.append(await self.ensure(id_, payload))

        _ = batch_size
        return out

    # ....................... #

    @occ_retry
    async def upsert(self, id: UUID, create: C, update: U) -> D:
        self._require_update_cmd()

        try:
            current = await self.read_gw.get(id)

        except CoreException as err:
            if err.kind is ExceptionKind.NOT_FOUND:
                return await self.create(create, id=id)

            raise

        updated, _ = await self.update(current.id, update, rev=current.rev)
        return updated

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
        _ = batch_size

        if not creates:
            return []

        return [
            await self.upsert(i, c, u)
            for i, c, u in zip(ids, creates, updates, strict=True)
        ]

    # ....................... #

    async def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        batch_size: int = 200,
    ) -> tuple[int, Sequence[D]]:
        _ = filters, dto, batch_size
        raise exc.internal("Firestore adapter does not support update_matching in MVP")

    # ....................... #

    async def touch(self, pk: UUID) -> D:
        res, _ = await self._patch(pk)
        return res

    # ....................... #

    async def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        batch_size: int = 200,
    ) -> Sequence[D]:
        _ = batch_size

        if len(pks) != len(set(pks)):
            raise exc.precondition("Primary keys must be unique")

        out: list[D] = []

        for pk in pks:
            doc, _ = await self._patch(pk)
            out.append(doc)

        return out

    # ....................... #

    async def kill(self, pk: UUID) -> None:
        await self.client.delete_document(await self.coll(), self._storage_pk(pk))

    # ....................... #

    async def kill_many(self, pks: Sequence[UUID], *, batch_size: int = 200) -> None:
        _ = batch_size

        if not pks:
            return

        if len(pks) != len(set(pks)):
            raise exc.precondition("Primary keys must be unique")

        for pk in pks:
            await self.kill(pk)
