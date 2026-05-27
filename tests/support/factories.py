"""Polyfactory helpers for integration tests."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from polyfactory import Use
from polyfactory.factories.pydantic_factory import ModelFactory
from pydantic import BaseModel, Field

from forze.domain.models import BaseDTO, CreateDocumentCmd, Document


class IntegrationDocument(Document):
    """Minimal document model for gateway integration tests."""

    name: str = Field(default="integration-doc")


class IntegrationCreateCmd(CreateDocumentCmd):
    """Create command paired with :class:`IntegrationDocument`."""

    name: str = "integration-doc"


class IntegrationUpdateCmd(BaseDTO):
    """Update command for integration document tests."""

    name: str | None = None


class IntegrationSearchHit(BaseModel):
    """Search hit row used in Postgres search integration tests."""

    id: UUID
    title: str
    content: str = ""


class IntegrationDocumentFactory(ModelFactory[IntegrationDocument]):
    __model__ = IntegrationDocument
    __set_as_default_factory_for_type__ = False

    id = Use(uuid4)
    name = Use(lambda: f"doc-{uuid4().hex[:8]}")


class IntegrationSearchHitFactory(ModelFactory[IntegrationSearchHit]):
    __model__ = IntegrationSearchHit
    __set_as_default_factory_for_type__ = False

    id = Use(uuid4)
    title = Use(lambda: f"title-{uuid4().hex[:6]}")
    content = Use(lambda: f"content-{uuid4().hex[:6]}")


def make_document(
    *,
    doc_id: UUID | None = None,
    name: str = "integration-doc",
    **extra: Any,
) -> IntegrationDocument:
    """Build a document with an explicit id when tests assert on PKs."""

    return IntegrationDocument(id=doc_id or uuid4(), name=name, **extra)


def make_create_cmd(
    *,
    name: str = "integration-doc",
    doc_id: UUID | None = None,
    **extra: Any,
) -> IntegrationCreateCmd:
    """Build a create command for write-gateway tests."""

    if doc_id is not None:
        extra.setdefault("id", doc_id)
    return IntegrationCreateCmd(name=name, **extra)
