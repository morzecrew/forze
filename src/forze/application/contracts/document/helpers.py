"""Validation helpers for document ports."""

from typing import Any, Sequence
from uuid import UUID

from forze.base.errors import ValidationError
from forze.domain.models import CreateDocumentCmd

# ----------------------- #


def require_create_id(dto: CreateDocumentCmd | tuple[CreateDocumentCmd, Any]) -> UUID:
    """Return ``dto.id`` or raise if it is unset.

    ``ensure`` / ``ensure_many`` / ``upsert`` / ``upsert_many`` require
    a client-provided primary key so the store can insert only when
    that id is absent.
    """

    if isinstance(dto, tuple):
        dto = dto[0]

    if dto.id is None:
        raise ValidationError(
            "ensure, ensure_many, upsert, and upsert_many require cmd DTO id to be set",
            code="missing_id",
        )

    return dto.id


# ....................... #


def require_create_id_for_many(
    dtos: Sequence[CreateDocumentCmd] | Sequence[tuple[CreateDocumentCmd, Any]],
) -> None:
    """Require each DTO to have an id and ids to be unique within the batch."""

    seen = set[UUID]()

    for d in dtos:
        uid = require_create_id(d)

        if uid in seen:
            raise ValidationError(
                "ensure, ensure_many, upsert, and upsert_many require distinct id values in the batch",
                code="duplicate_id",
            )
        seen.add(uid)
