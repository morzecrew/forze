from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from typing import Literal, Optional, Sequence, final, get_args
from uuid import UUID

import attrs

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


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoWriteGateway[D: Document, C: CreateDocumentCmd, U: BaseDTO](MongoGateway[D]):
    read: MongoReadGateway[D]
    create_dto: type[C]
    update_dto: type[U]
    history: Optional[MongoHistoryGateway[D]] = None
    rev_bump_strategy: MongoRevBumpStrategy = "application"

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
        return SOFT_DELETE_FIELD in pydantic_field_names(self.read.model)

    # ....................... #

    def _from_cdto(self, dto: C) -> D:
        data = pydantic_dump(dto, exclude={"unset": True})
        return pydantic_validate(self.model, data)

    # ....................... #

    async def create(self, dto: C) -> D:
        model = self._from_cdto(dto)
        data = pydantic_dump(model)

        await self.client.insert_one(self.coll(), self._storage_doc(data))
        await self._write_history(model)

        return model

    # ....................... #

    async def create_many(self, dtos: Sequence[C]) -> Sequence[D]:
        if not dtos:
            return []

        models = [self._from_cdto(d) for d in dtos]
        payloads = [self._storage_doc(pydantic_dump(m)) for m in models]

        await self.client.insert_many(self.coll(), payloads)
        await self._write_history(*models)

        return models

    # ....................... #

    def __bump_rev(self, current: D, diff: JsonDict) -> JsonDict:
        if self.rev_bump_strategy == "application":
            diff[REV_FIELD] = current.rev + 1

        return diff

    # ....................... #

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

        diff = self.__bump_rev(current, diff)

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

    async def update(self, pk: UUID, dto: U, *, rev: Optional[int] = None) -> D:
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
        if not pks:
            return []

        if len(pks) != len(dtos):
            raise CoreError("Length mismatch between primary keys and updates")

        if len(pks) != len(set(pks)):
            raise ValidationError("Primary keys must be unique")

        if revs is not None and len(revs) != len(pks):
            raise CoreError("Length mismatch between primary keys and revisions")

        out: list[D] = []
        for i, (pk, dto) in enumerate(zip(pks, dtos, strict=True)):
            out.append(await self.update(pk, dto, rev=None if revs is None else revs[i]))

        return out

    # ....................... #

    async def touch(self, pk: UUID) -> D:
        return await self._patch(pk)

    # ....................... #

    async def touch_many(self, pks: Sequence[UUID]) -> Sequence[D]:
        if not pks:
            return []

        if len(pks) != len(set(pks)):
            raise ValidationError("Primary keys must be unique")

        out: list[D] = []
        for pk in pks:
            out.append(await self.touch(pk))

        return out

    # ....................... #

    async def kill(self, pk: UUID) -> None:
        await self.client.delete_one(self.coll(), {"_id": self._storage_pk(pk)})

    # ....................... #

    async def kill_many(self, pks: Sequence[UUID]) -> None:
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
        if not self.supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        if not pks:
            return []

        if len(pks) != len(set(pks)):
            raise ValidationError("Primary keys must be unique")

        if revs is not None and len(revs) != len(pks):
            raise CoreError("Length mismatch between primary keys and revisions")

        out: list[D] = []
        for i, pk in enumerate(pks):
            out.append(await self.delete(pk, rev=None if revs is None else revs[i]))

        return out

    # ....................... #

    async def restore(self, pk: UUID, *, rev: Optional[int] = None) -> D:
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
        if not self.supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        if not pks:
            return []

        if len(pks) != len(set(pks)):
            raise ValidationError("Primary keys must be unique")

        if revs is not None and len(revs) != len(pks):
            raise CoreError("Length mismatch between primary keys and revisions")

        out: list[D] = []
        for i, pk in enumerate(pks):
            out.append(await self.restore(pk, rev=None if revs is None else revs[i]))

        return out
