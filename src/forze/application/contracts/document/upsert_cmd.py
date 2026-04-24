"""Validation helpers for :func:`DocumentCommandPort.upsert` / ``upsert_many``."""

from typing import Any, Sequence
from uuid import UUID

from forze.base.errors import ValidationError
from forze.domain.models import CreateDocumentCmd

# ----------------------- #


def require_create_id_for_upsert(dto: CreateDocumentCmd) -> UUID:
    """Return ``dto.id`` or raise if unset (same id rule as :func:`~.ensure_id.require_create_id_for_ensure`)."""

    if dto.id is None:
        raise ValidationError(
            "upsert and upsert_many require CreateDocumentCmd.id to be set",
            code="upsert_missing_id",
        )
    return dto.id


def assert_unique_upsert_pairs(pairs: Sequence[tuple[CreateDocumentCmd, Any]]) -> None:
    """Require each create command to have an id and ids to be unique in the batch."""

    seen: set[UUID] = set()
    for c, _ in pairs:
        uid = require_create_id_for_upsert(c)
        if uid in seen:
            raise ValidationError(
                "upsert_many requires distinct id values in the create commands",
                code="upsert_duplicate_id",
            )
        seen.add(uid)
