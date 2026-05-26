"""Validation helpers for document ports."""

from typing import Any, Sequence
from uuid import UUID

from forze.base.exceptions import exc
from forze.domain.models import CreateDocumentCmd

# ----------------------- #


def require_create_id[X: CreateDocumentCmd](dto: X | tuple[X, Any]) -> UUID:
    """Return ``dto.id`` or raise if it is unset.

    ``ensure`` / ``ensure_many`` / ``upsert`` / ``upsert_many`` require
    a client-provided primary key so the store can insert only when
    that id is absent.
    """

    if isinstance(dto, tuple):
        dto_ = dto[0]

    else:
        dto_ = dto

    if dto_.id is None:
        raise exc.internal(
            "ensure, ensure_many, upsert, and upsert_many require cmd DTO id to be set",
        )

    return dto_.id


# ....................... #


def require_create_id_for_many[X: CreateDocumentCmd](
    dtos: Sequence[X] | Sequence[tuple[X, Any]],
) -> None:
    """Require each DTO to have an id and ids to be unique within the batch."""

    seen = set[UUID]()

    for d in dtos:
        uid = require_create_id(d)

        if uid in seen:
            raise exc.internal(
                "ensure, ensure_many, upsert, and upsert_many require distinct id values in the batch"
            )

        seen.add(uid)
