"""Mongo gateway for document write operations (create, update, delete, restore)."""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from typing import Sequence, final
from uuid import UUID

import attrs
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from forze.base.errors import (
    ConcurrencyError,
    ConflictError,
    CoreError,
    NotFoundError,
    ValidationError,
)
from forze.base.primitives import JsonDict
from forze.base.serialization import (
    pydantic_dump,
    pydantic_dump_many,
    pydantic_validate,
    pydantic_validate_many,
)
from forze.domain.constants import REV_FIELD, SOFT_DELETE_FIELD
from forze.domain.mixins import SoftDeletionMixin
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from .base import MongoGateway
from .history import MongoHistoryGateway
from .read import MongoReadGateway

# ----------------------- #


def optimistic_retry(*, attempts: int = 3):  # type: ignore[no-untyped-def]
    """Return a tenacity retry decorator for :exc:`~forze.base.errors.ConcurrencyError`.

    Mirrors the Postgres retry strategy: exponential back-off with re-raise
    after *attempts* failures.

    :param attempts: Maximum number of attempts before re-raising.
    """

    return retry(
        retry=retry_if_exception_type(ConcurrencyError),
        stop=stop_after_attempt(attempts),
        wait=wait_exponential(multiplier=0.01, min=0.01, max=0.2),
        reraise=True,
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoWriteGateway[D: Document, C: CreateDocumentCmd, U: BaseDTO](MongoGateway[D]):
    """Write gateway for Mongo documents with optimistic concurrency and optional history.

    Uses a :class:`MongoReadGateway` for read-before-write patterns and
    delegates history snapshots to an optional :class:`MongoHistoryGateway`.
    Revision bumps are controlled by :attr:`rev_bump_strategy`; concurrent
    writes to the same revision are detected and raise
    :exc:`~forze.base.errors.ConcurrencyError`.
    """

    read_gw: MongoReadGateway[D]
    """Companion read gateway; must share the same client, source, and database."""

    create_cmd_type: type[C]
    """Pydantic model for creation payloads."""

    update_cmd_type: type[U]
    """Pydantic model for update payloads."""

    history_gw: MongoHistoryGateway[D] | None = None
    """Optional history gateway for revision snapshots."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.collection != self.read_gw.collection:
            raise CoreError(
                "Collection mismatch. Write gateway and nested read gateway must have the same collection."
            )

        if self.client is not self.read_gw.client:
            raise CoreError(
                "Client mismatch. Write gateway and nested read gateway must use the same client."
            )

        if self.database != self.read_gw.database:
            raise CoreError(
                "Database mismatch. Write gateway and nested read gateway must use the same database."
            )

        if self.tenant_aware != self.read_gw.tenant_aware:
            raise CoreError(
                "Tenant awareness mismatch. Write gateway and nested read gateway must have the same tenant awareness."
            )

        if self.history_gw is not None:
            if self.client is not self.history_gw.client:
                raise CoreError(
                    "Client mismatch. Write gateway and nested history gateway must use the same client."
                )

            if self.collection != self.history_gw.target_collection:
                raise CoreError(
                    "Collection mismatch. Write gateway and nested history gateway must point to the same collection."
                )

            if self.database != self.history_gw.target_database:
                raise CoreError(
                    "Database mismatch. Write gateway and nested history gateway must point to the same database."
                )

            if self.tenant_aware != self.history_gw.tenant_aware:
                raise CoreError(
                    "Tenant awareness mismatch. Write gateway and nested history gateway must have the same tenant awareness."
                )

    # ....................... #

    async def _write_history(self, *data: D) -> None:
        if self.history_gw is not None:
            await self.history_gw.write_many(data)

    # ....................... #

    async def _validate_history(self, *data: tuple[D, int, JsonDict]) -> None:
        if self.history_gw is None:
            for current, rev, _ in data:
                if rev != current.rev:
                    raise ConflictError("Revision mismatch", code="revision_mismatch")

            return

        to_check = [
            (current, rev, update)
            for current, rev, update in data
            if rev != current.rev
        ]
        bad_records = [rev for current, rev, _ in to_check if rev > current.rev]

        if bad_records:
            raise ValidationError("Invalid revision number")

        if to_check:
            pks_to_check = [current.id for current, _, _ in to_check]
            revs_to_check = [rev for _, rev, _ in to_check]
            hist_records = await self.history_gw.read_many(pks_to_check, revs_to_check)

            if len(hist_records) != len(to_check):
                raise NotFoundError(
                    "History records not found. Please retry with actual revision number."
                )

            for (current, _, update), historical in zip(
                to_check, hist_records, strict=True
            ):
                if not current.validate_historical_consistency(historical, update):
                    raise ConflictError(
                        "Historical consistency violation during update",
                        code="historical_consistency_violation",
                    )

    # ....................... #

    def supports_soft_delete(self) -> bool:
        """Return whether the underlying model declares a soft-delete field."""

        return issubclass(self.model_type, SoftDeletionMixin)

    # ....................... #

    #! TODO: canonical mapper from there
    def _from_cdto(self, dto: C) -> D:
        data = pydantic_dump(dto, exclude={"unset": True})
        return pydantic_validate(self.model_type, data)

    # ....................... #

    #! TODO: canonical batch mapper from there
    def _from_cdto_many(self, dtos: Sequence[C]) -> Sequence[D]:
        data = pydantic_dump_many(dtos, exclude={"unset": True})
        return pydantic_validate_many(self.model_type, data)

    # ....................... #

    @optimistic_retry()  # type: ignore[untyped-decorator]
    async def create(self, dto: C) -> D:
        """Insert a new document from a creation DTO and record its history.

        :param dto: Creation payload.
        :returns: The persisted domain document.
        """

        model = self._from_cdto(dto)
        data = pydantic_dump(model)
        data = self.adapt_payload_for_write(data, create=True)

        await self.client.insert_one(self.coll(), self._storage_doc(data))

        created = await self.read_gw.get(model.id)
        await self._write_history(created)

        return created

    # ....................... #

    @optimistic_retry()  # type: ignore[untyped-decorator]
    async def create_many(
        self,
        dtos: Sequence[C],
        *,
        batch_size: int = 200,
    ) -> Sequence[D]:
        """Bulk-insert documents from creation DTOs and record their history.

        :param dtos: Creation payloads. No-ops when empty.
        """

        if not dtos:
            return []

        models = self._from_cdto_many(dtos)
        raw_payloads = pydantic_dump_many(models)
        payloads = self.adapt_many_payload_for_write(raw_payloads, create=True)
        payloads = list(map(self._storage_doc, payloads))

        await self.client.insert_many(self.coll(), payloads, batch_size=batch_size)

        created = await self.read_gw.get_many([model.id for model in models])
        await self._write_history(*created)

        return created

    # ....................... #

    def _bump_rev(self, current: D, diff: JsonDict) -> JsonDict:
        diff[REV_FIELD] = current.rev + 1

        return diff

    # ....................... #

    @optimistic_retry()  # type: ignore[untyped-decorator]
    async def _patch(
        self,
        pk: UUID,
        update: JsonDict | None = None,
        *,
        rev: int | None = None,
    ) -> D:
        current = await self.read_gw.get(pk)

        if update is not None:
            if rev is not None:
                await self._validate_history((current, rev, update))

            _, diff = current.update(update)

        else:
            _, diff = current.touch()

        if not diff:
            return current

        diff = self._bump_rev(current, diff)
        diff = self.adapt_payload_for_write(diff, create=False)

        flt = self._add_tenant_filter(
            {"_id": self._storage_pk(current.id), REV_FIELD: current.rev}
        )
        matched = await self.client.update_one(
            self.coll(),
            flt,
            {"$set": self._coerce_query_value(diff)},
        )

        if matched != 1:
            raise ConcurrencyError("Failed to update record")

        updated = await self.read_gw.get(pk)
        await self._write_history(updated)

        return updated

    # ....................... #

    @optimistic_retry()  # type: ignore[untyped-decorator]
    async def _patch_many(
        self,
        pks: Sequence[UUID],
        updates: Sequence[JsonDict] | None = None,
        *,
        revs: Sequence[int] | None = None,
        batch_size: int = 200,
    ) -> Sequence[D]:
        if not pks:
            return []

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
                _, diff = current.update(update)
                if diff:
                    to_patch.append((i, current, diff))
        else:
            for i, current in enumerate(currents):
                _, diff = current.touch()
                if diff:
                    to_patch.append((i, current, diff))

        if not to_patch:
            return currents

        # 2. Execution (Bulk)
        operations: list[tuple[JsonDict, JsonDict]] = []
        for _, current, diff in to_patch:
            bumped = self._bump_rev(current, diff)
            bumped = self.adapt_payload_for_write(bumped, create=False)
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
            self.coll(), operations, batch_size=batch_size
        )
        if matched != len(to_patch):
            raise ConcurrencyError("Failed to update one or more records")

        updated = await self.read_gw.get_many(pks)
        await self._write_history(*updated)

        return updated

    # ....................... #

    async def update(self, pk: UUID, dto: U, *, rev: int | None = None) -> D:
        """Apply an update DTO to an existing document.

        :param pk: Document primary key.
        :param dto: Update payload.
        :param rev: Expected revision for historical consistency validation.
        :returns: The updated domain document.
        """

        update_data = pydantic_dump(dto, exclude={"unset": True})
        return await self._patch(pk, update_data, rev=rev)

    # ....................... #

    async def update_many(
        self,
        pks: Sequence[UUID],
        dtos: Sequence[U],
        *,
        revs: Sequence[int] | None = None,
        batch_size: int = 200,
    ) -> Sequence[D]:
        """Bulk-update documents with corresponding DTOs.

        :param pks: Document primary keys (must be unique).
        :param dtos: Update payloads matching *pks* by position.
        :param revs: Optional expected revisions for history validation.
        :raises CoreError: If lengths of *pks* and *dtos* (or *revs*) differ.
        :raises ValidationError: If *pks* contains duplicates.
        """

        if len(pks) != len(dtos):
            raise CoreError("Length mismatch between primary keys and updates")

        if len(pks) != len(set(pks)):
            raise ValidationError("Primary keys must be unique")

        if revs is not None and len(revs) != len(pks):
            raise CoreError("Length mismatch between primary keys and revisions")

        updates = pydantic_dump_many(dtos, exclude={"unset": True})
        return await self._patch_many(pks, updates, revs=revs, batch_size=batch_size)

    # ....................... #

    async def touch(self, pk: UUID) -> D:
        """Bump a document's revision without changing its data.

        :param pk: Document primary key.
        """

        return await self._patch(pk)

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
            raise ValidationError("Primary keys must be unique")

        return await self._patch_many(pks, batch_size=batch_size)

    # ....................... #

    async def kill(self, pk: UUID) -> None:
        """Hard-delete a document from the collection.

        :param pk: Document primary key.
        """

        await self.client.delete_one(
            self.coll(),
            self._add_tenant_filter({"_id": self._storage_pk(pk)}),
        )

    # ....................... #

    async def kill_many(self, pks: Sequence[UUID]) -> None:
        """Hard-delete multiple documents from the collection.

        :param pks: Document primary keys (must be unique). No-ops when empty.
        :raises ValidationError: If *pks* contains duplicates.
        """

        if not pks:
            return

        if len(pks) != len(set(pks)):
            raise ValidationError("Primary keys must be unique")

        await self.client.delete_many(
            self.coll(),
            self._add_tenant_filter(
                {"_id": {"$in": [self._storage_pk(pk) for pk in pks]}}
            ),
        )

    # ....................... #

    async def delete(self, pk: UUID, *, rev: int | None = None) -> D:
        """Soft-delete a document by setting the deleted flag.

        :param pk: Document primary key.
        :param rev: Expected revision for historical consistency validation.
        :raises CoreError: If the model does not support soft deletion.
        """

        if not self.supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        return await self._patch(pk, {SOFT_DELETE_FIELD: True}, rev=rev)

    # ....................... #

    async def delete_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Sequence[int] | None = None,
        batch_size: int = 200,
    ) -> Sequence[D]:
        """Soft-delete multiple documents.

        :param pks: Document primary keys (must be unique).
        :param revs: Optional expected revisions for history validation.
        :param batch_size: Batch size for the bulk operation.
        :raises CoreError: If the model does not support soft deletion.
        :raises ValidationError: If *pks* contains duplicates.
        """

        if not self.supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        if len(pks) != len(set(pks)):
            raise ValidationError("Primary keys must be unique")

        if revs is not None and len(revs) != len(pks):
            raise CoreError("Length mismatch between primary keys and revisions")

        updates = [{SOFT_DELETE_FIELD: True} for _ in pks]
        return await self._patch_many(pks, updates, revs=revs, batch_size=batch_size)

    # ....................... #

    async def restore(self, pk: UUID, *, rev: int | None = None) -> D:
        """Restore a soft-deleted document by clearing the deleted flag.

        :param pk: Document primary key.
        :param rev: Expected revision for historical consistency validation.
        :raises CoreError: If the model does not support soft deletion.
        """

        if not self.supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        return await self._patch(pk, {SOFT_DELETE_FIELD: False}, rev=rev)

    # ....................... #

    async def restore_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Sequence[int] | None = None,
        batch_size: int = 200,
    ) -> Sequence[D]:
        """Restore multiple soft-deleted documents.

        :param pks: Document primary keys (must be unique).
        :param revs: Optional expected revisions for history validation.
        :param batch_size: Batch size for the bulk operation.
        :raises CoreError: If the model does not support soft deletion.
        :raises ValidationError: If *pks* contains duplicates.
        """

        if not self.supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        if len(pks) != len(set(pks)):
            raise ValidationError("Primary keys must be unique")

        if revs is not None and len(revs) != len(pks):
            raise CoreError("Length mismatch between primary keys and revisions")

        updates = [{SOFT_DELETE_FIELD: False} for _ in pks]

        return await self._patch_many(pks, updates, revs=revs, batch_size=batch_size)
