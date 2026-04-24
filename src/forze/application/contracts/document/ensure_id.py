"""Validation helpers for :func:`DocumentCommandPort.ensure` / ``ensure_many``."""

from typing import Sequence
from uuid import UUID

from forze.base.errors import ValidationError
from forze.domain.models import CreateDocumentCmd

# ----------------------- #


def require_create_id_for_ensure(dto: CreateDocumentCmd) -> UUID:
    """Return ``dto.id`` or raise if it is unset.

    ``ensure`` / ``ensure_many`` require a client-provided primary key so the
    store can insert only when that id is absent.
    """

    if dto.id is None:
        raise ValidationError(
            "ensure and ensure_many require CreateDocumentCmd.id to be set",
            code="ensure_missing_id",
        )
    return dto.id


def assert_unique_ensure_ids(dtos: Sequence[CreateDocumentCmd]) -> None:
    """Require every DTO to have an id and ids to be unique within the batch."""

    seen: set[UUID] = set()
    for d in dtos:
        uid = require_create_id_for_ensure(d)
        if uid in seen:
            raise ValidationError(
                "ensure_many requires distinct id values in the batch",
                code="ensure_duplicate_id",
            )
        seen.add(uid)
