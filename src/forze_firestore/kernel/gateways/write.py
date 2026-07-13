"""Firestore gateway for document write operations."""

from forze_firestore._compat import require_firestore

require_firestore()

# ....................... #

from collections.abc import Sequence
from typing import Any, final
from uuid import UUID

import attrs

from forze.application.contracts.querying import QueryFilterExpression
from forze.application.contracts.resilience import ResilienceExecutorPort
from forze.application.execution.resilience import (
    default_resilience_executor,
    occ_retry,
)
from forze.application.integrations.persistence import DocumentWriteCodecMixin
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
    DocumentWriteCodecMixin[D],
    FirestoreGateway[D],
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
    update_codec: ModelCodec[U, Any] | None = attrs.field(kw_only=True, eq=False, repr=False)
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
                    raise exc.precondition("Revision mismatch", code="revision_mismatch")

            return

        to_check = [(current, rev, update) for current, rev, update in data if rev != current.rev]
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

            for (current, _, update), historical in zip(to_check, hist_records, strict=True):
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
            raise exc.internal("Update codec is required when update commands are supported")

        return self.read_codec

    # ....................... #

    @occ_retry
    async def create(self, payload: C, *, id: UUID | None = None) -> D:
        model = self._from_cdto(payload, id)
        data = await self._encode_domain_one(model)
        data = self.adapt_payload_for_write(data)
        coll = await self.coll()
        # Fail closed on an existing id (``conflict``) rather than overwriting it,
        # matching the Postgres/Mongo ``create`` contract. Callers wanting
        # insert-or-replace use ``ensure``/``upsert``.
        await self.client.create_document(coll, self._storage_pk(model.id), data)
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
        raw_payloads = await self._encode_domain_many(models)
        write_payloads = self.adapt_many_payload_for_write(raw_payloads)
        documents = [
            (self._storage_pk(m.id), dict(p)) for m, p in zip(models, write_payloads, strict=True)
        ]
        # Create-only so a colliding id fails closed (``conflict``) instead of
        # silently overwriting, matching single-document ``create``.
        await self.client.insert_many(
            await self.coll(), documents, batch_size=batch_size, create_only=True
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
        # The read-check-write runs inside a Firestore transaction so the write is
        # conditional on the document not having changed since the read: Firestore's
        # optimistic concurrency aborts the commit (CONCURRENCY) if a competing
        # writer touched the document, and @occ_retry re-runs with a fresh read.
        # Without this, the unconditional full-document ``set`` would let two writers
        # that both read the same revision clobber each other (lost update). The
        # scope is re-entrant: when a caller already opened a transaction it is
        # reused (no independent nested commit).
        async with self.client.transaction():
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
            merged = await self._encode_domain_one(current)
            merged.update(self.adapt_payload_for_write(diff))

            coll = await self.coll()
            # The merged image is rebuilt from the domain model, which cannot
            # carry infrastructure-plane fields; ``adapt_payload_for_write``
            # re-stamps the tenant so the full-document ``set`` below does not
            # strip it from the stored row.
            storage = self.adapt_payload_for_write(merged)
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
            if err.kind is not ExceptionKind.NOT_FOUND:
                raise

        try:
            return await self.create(payload, id=id)

        except CoreException as race_err:
            # Lost the create race: another writer inserted this id between the
            # read above and this (now fail-closed) create. Ensure's contract is
            # to return the existing row, so re-read it rather than surfacing the
            # conflict.
            if race_err.kind is not ExceptionKind.CONFLICT:
                raise

            return await self.read_gw.get(id)

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
            if err.kind is not ExceptionKind.NOT_FOUND:
                raise

            try:
                return await self.create(create, id=id)

            except CoreException as create_err:
                # Lost the create race: another writer inserted this id between
                # the read above and this (now fail-closed) create. Fall through
                # to update the now-existing row, preserving upsert semantics.
                if create_err.kind is not ExceptionKind.CONFLICT:
                    raise

            current = await self.read_gw.get(id)

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

        return [await self.upsert(i, c, u) for i, c, u in zip(ids, creates, updates, strict=True)]

    # ....................... #

    async def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        batch_size: int = 200,
    ) -> tuple[int, Sequence[D]]:
        _ = filters, dto, batch_size
        raise exc.precondition(
            "Firestore adapter does not support update_matching in MVP; "
            "update records individually with update/update_many.",
            code="core.document.update_matching_unsupported",
        )

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
        """Hard-delete a document, scoped to the current tenant.

        Firestore deletes by document id and cannot combine a delete with a query
        filter, so the delete is guarded by a read-verify-delete inside a
        transaction: the document is fetched, its tenant is checked, and only then
        deleted (atomically, so no competing writer slips in between). A missing
        document — or one owned by another tenant under tagged tenancy — raises
        ``not_found`` and never touches another tenant's data.

        :param pk: Document primary key.
        :raises NotFoundError: If the document does not exist or is not accessible
            in the current tenant scope.
        """

        async with self.client.transaction():
            coll = await self.coll()
            storage_pk = self._storage_pk(pk)
            raw = await self.client.get_document(coll, storage_pk)

            if raw is None or not self._row_matches_tenant(raw):
                raise exc.not_found(f"Record not found: {pk}")

            await self.client.delete_document(coll, storage_pk)

    # ....................... #

    async def kill_many(self, pks: Sequence[UUID], *, batch_size: int = 200) -> None:
        """Hard-delete multiple documents, each scoped to the current tenant.

        Deletes are looped (Firestore has no atomic tenant-filtered bulk delete);
        each document is tenant-verified via :meth:`kill`, so a missing or
        cross-tenant id raises ``not_found``.

        **Not atomic — partial success is possible.** The ids are deleted one at a
        time in order and each ``kill`` commits immediately, so a ``not_found`` on a
        later id leaves the earlier deletes already applied (they are not rolled
        back). Callers needing all-or-nothing semantics must pre-validate existence,
        or delete within their own transaction rather than relying on this helper.

        :param pks: Document primary keys (must be unique). No-ops when empty.
        :raises NotFoundError: If any document does not exist or is not accessible
            in the current tenant scope (earlier deletes in the batch still commit).
        """

        _ = batch_size

        if not pks:
            return

        if len(pks) != len(set(pks)):
            raise exc.precondition("Primary keys must be unique")

        for pk in pks:
            await self.kill(pk)
