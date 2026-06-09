"""Shared document models and expectations for nested field filter/sort scenarios."""

from __future__ import annotations

from pydantic import BaseModel

from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

# ----------------------- #


class NestedFilterMeta(BaseModel):
    """Nested payload on document rows (JSONB / BSON)."""

    score: int
    tag: str = ""


class NestedFilterRowDoc(Document):
    title: str
    meta: NestedFilterMeta


class NestedFilterRowCreate(CreateDocumentCmd):
    title: str
    meta: NestedFilterMeta


class NestedFilterRowUpdate(BaseDTO):
    title: str | None = None
    meta: NestedFilterMeta | None = None


class NestedFilterRowRead(ReadDocument):
    title: str
    meta: NestedFilterMeta


# Expected outcomes reused by Postgres and Mongo integration tests.


def expected_scores_ascending() -> list[int]:
    return [10, 20, 30]
