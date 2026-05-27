"""Shared document models with Pydantic computed fields for integration tests."""

from pydantic import computed_field

from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument


class ComputedStoredDoc(Document):
    """Domain document with a derived ``doubled`` field (not stored)."""

    value: int

    @computed_field
    @property
    def doubled(self) -> int:
        return self.value * 2


class ComputedReadDoc(ReadDocument):
    """Read projection with the same derived field."""

    value: int

    @computed_field
    @property
    def doubled(self) -> int:
        return self.value * 2


class ComputedCreate(CreateDocumentCmd):
    value: int


class ComputedUpdate(BaseDTO):
    value: int | None = None
