"""Shared document models and expectations for nested field filter/sort scenarios."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel

from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

# ----------------------- #


class NestedFilterMeta(BaseModel):
    """Nested payload on document rows (JSONB / BSON)."""

    score: int
    tag: str = ""
    price: Decimal = Decimal("0")


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


# Array-element projection scenario: a row carries a list of sub-objects so a dotted
# projection path (``items.sku``) can map over the list across backends.


class NestedArrayItem(BaseModel):
    sku: str
    qty: int = 0


class NestedArrayRowDoc(Document):
    ref: str
    items: list[NestedArrayItem]


class NestedArrayRowCreate(CreateDocumentCmd):
    ref: str
    items: list[NestedArrayItem]


class NestedArrayRowUpdate(BaseDTO):
    ref: str | None = None


class NestedArrayRowRead(ReadDocument):
    ref: str
    items: list[NestedArrayItem]
