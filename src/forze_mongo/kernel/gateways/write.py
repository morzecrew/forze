"""Mongo gateway for document write operations (create, update, delete, restore)."""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from typing import Literal, Optional, Sequence, final, get_args
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
from forze.base.serialization import pydantic_dump, pydantic_field_names, pydantic_validate
from forze.domain.constants import REV_FIELD, SOFT_DELETE_FIELD
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from .base import MongoGateway
from .history import MongoHistoryGateway
from .read import MongoReadGateway

# ----------------------- #

MongoRevBumpStrategy = Literal["application"]
"""Supported revision bump strategies for Mongo document writes."""


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

    read: MongoReadGateway[D]
    """Companion read gateway; must share the same client, source, and database."""

    create_dto: type[C]
    """Pydantic model for creation payloads."""

    update_dto: type[U]
    """Pydantic model for update payloads."""

    history: Optional[MongoHistoryGateway[D]] = None
    """Optional history gateway for revision snapshots."""

    rev_bump_strategy: MongoRevBumpStrategy = "application"
    """Strategy used to increment the document revision on writes."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.source != self.read.source:
            raise CoreError(
                "Source mismatch. Write gateway and nested read gateway must have the same source."
            )

        if self.client is not self.read.client:
            raise CoreError(
                "Client mismatch. Write gateway and nested read gateway must use the same client."
            )

        if self.db_name != self.read.db_name:
            raise CoreError(
                "Database mismatch. Write gateway and nested read gateway must use the same database."
            )

        if self.history is not None:
            if self.client is not self.history.client:
                raise CoreError(
                    "Client mismatch. Write gateway and nested history gateway must use the same client."
                )

            if self.source != self.history.target_source:
                raise CoreError(
                    "Source mismatch. Write gateway and nested history gateway must point to the same source."
                )

            if self.db_name != self.history.db_name:
                raise CoreError(
                    "Database mismatch. Write gateway and nested history gateway must use the same database."
                )

        if self.rev_bump_strategy not in get_args(MongoRevBumpStrategy):
            raise CoreError(f"Invalid revision bump strategy: {self.rev_bump_strategy}")

    # ....................... #

    async def _write_history(self, *data: D) -> None:
        if self.history is not None:
            await self.history.write_many(data)

    # ....................... #

    async def _validate_history(self, *data: tuple[D, int, JsonDict]) -> None:
        if self.history is None:
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
            hist_records = await self.history.read_many(pks_to_check, revs_to_check)

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

        return SOFT_DELETE_FIELD in pydantic_field_names(self.read.model)

    # ....................... #

    def _from_cdto(self, dto: C) -> D:
        data = pydantic_dump(dto, exclude={"unset": True})
        return pydantic_validate(self.model, data)

    # ....................... #

    @optimistic_retry()  # type: ignore[untyped-decorator]
    async def create(self, dto: C) -> D:
        """Insert a new document from a creation DTO and record its history.

        :param dto: Creation payload.
        :returns: The persisted domain document.
        """

        model = self._from_cdto(dto)
        data = pydantic_dump(model)

        await self.client.insert_one(self.coll(), self._storage_doc(data))
        await self._write_history(model)

        return model

    # ....................... #

    @optimistic_retry()  # type: ignore[untyped-decorator]
    async def create_many(self, dtos: Sequence[C]) -> Sequence[D]:
        """Bulk-insert documents from creation DTOs and record their history.

        :param dtos: Creation payloads. No-ops when empty.
        """

        if not dtos:
            return []

        models = [self._from_cdto(d) for d in dtos]
        payloads = [self._storage_doc(pydantic_dump(m)) for m in models]

        await self.client.insert_many(self.coll(), payloads)
        await self._write_history(*models)

        return models

    # ....................... #

    def _bump_rev(self, current: D, diff: JsonDict) -> JsonDict:
        if self.rev_bump_strategy == "application":
            diff[REV_FIELD] = current.rev + 1

        return diff

    # ....................... #

    @optimistic_retry()  # type: ignore[untyped-decorator]
    async def _patch(
        self,
        pk: UUID,
        update: Optional[JsonDict] = None,
        *,
        rev: Optional[int] = None,
    ) -> D:
        current = await self.read.get(pk)

        if update is not None:
            if rev is not None:
                await self._validate_history((current, rev, update))

            _, diff = current.update(update)

        else:
            _, diff = current.touch()

        if not diff:
            return current

        diff = self._bump_rev(current, diff)

        matched = await self.client.update_one(
            self.coll(),
            {"_id": self._storage_pk(current.id), REV_FIELD: current.rev},
            {"$set": self._coerce_query_value(diff)},
        )

        if matched != 1:
            raise ConcurrencyError("Failed to update record")

        updated = current.model_copy(update=diff, deep=True)
        await self._write_history(updated)

        return updated

    # ....................... #

    @optimistic_retry()  # type: ignore[untyped-decorator]
    async def _patch_many(
        self,
        pks: Sequence[UUID],
        updates: Optional[Sequence[JsonDict]] = None,
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[D]:
        if not pks:
            return []

        currents = await self.read.get_many(pks)

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
            operations.append(
                (
                    {"_id": self._storage_pk(current.id), REV_FIELD: current.rev},
                    {"$set": self._coerce_query_value(bumped)},
                )
            )

        matched = await self.client.bulk_update(self.coll(), operations)
        if matched != len(to_patch):
            raise ConcurrencyError("Failed to update one or more records")

        # 3. Finalization
        updated_models: list[D] = []
        updated_map: dict[int, D] = {}

        for i, current, diff in to_patch:
            bumped = self._bump_rev(current, diff)
            model = current.model_copy(update=bumped, deep=True)
            updated_models.append(model)
            updated_map[i] = model

        out = [updated_map.get(idx, currents[idx]) for idx in range(len(currents))]
        await self._write_history(*updated_models)

        return out

    # ....................... #

    async def update(self, pk: UUID, dto: U, *, rev: Optional[int] = None) -> D:
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
        revs: Optional[Sequence[int]] = None,
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

        updates = [pydantic_dump(d, exclude={"unset": True}) for d in dtos]
        return await self._patch_many(pks, updates, revs=revs)

    # ....................... #

    async def touch(self, pk: UUID) -> D:
        """Bump a document's revision without changing its data.

        :param pk: Document primary key.
        """

        return await self._patch(pk)

    # ....................... #

    async def touch_many(self, pks: Sequence[UUID]) -> Sequence[D]:
        """Bump revisions for multiple documents without changing their data.

        :param pks: Document primary keys (must be unique).
        :raises ValidationError: If *pks* contains duplicates.
        """

        if len(pks) != len(set(pks)):
            raise ValidationError("Primary keys must be unique")

        return await self._patch_many(pks)

    # ....................... #

    async def kill(self, pk: UUID) -> None:
        """Hard-delete a document from the collection.

        :param pk: Document primary key.
        """

        await self.client.delete_one(self.coll(), {"_id": self._storage_pk(pk)})

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
            {"_id": {"$in": [self._storage_pk(pk) for pk in pks]}},
        )

    # ....................... #

    async def delete(self, pk: UUID, *, rev: Optional[int] = None) -> D:
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
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[D]:
        """Soft-delete multiple documents.

        :param pks: Document primary keys (must be unique).
        :param revs: Optional expected revisions for history validation.
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
        return await self._patch_many(pks, updates, revs=revs)

    # ....................... #

    async def restore(self, pk: UUID, *, rev: Optional[int] = None) -> D:
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
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[D]:
        """Restore multiple soft-deleted documents.

        :param pks: Document primary keys (must be unique).
        :param revs: Optional expected revisions for history validation.
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
        return await self._patch_many(pks, updates, revs=revs)
