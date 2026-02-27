"""Unit tests for forze.application.composition.document."""

import pytest

from forze.application.composition.document import (
    DTOSpec,
    DocumentUsecasesFacadeProvider,
    build_document_registry,
)
from forze.application.contracts.document import DocumentSpec
from forze.application.execution import UsecaseRegistry
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument

# ----------------------- #


def _minimal_spec(supports_update: bool = False, supports_soft_delete: bool = False) -> DocumentSpec:
    """Build a minimal DocumentSpec for testing."""
    from forze.domain.models import BaseDTO

    class UpdateCmd(BaseDTO):
        """Minimal update DTO with one field for supports_update."""

        title: str | None = None

    models = {
        "read": ReadDocument,
        "domain": Document,
        "create_cmd": CreateDocumentCmd,
        "update_cmd": UpdateCmd if supports_update else type("EmptyUpdate", (BaseDTO,), {}),
    }
    return DocumentSpec(
        namespace="test",
        relations={"read": "test_read", "write": "test_write"},
        models=models,
    )


class TestBuildDocumentRegistry:
    """Tests for build_document_registry."""

    def test_returns_registry(self) -> None:
        spec = _minimal_spec()
        reg = build_document_registry(spec)
        assert isinstance(reg, UsecaseRegistry)

    def test_has_core_operations(self) -> None:
        spec = _minimal_spec()
        reg = build_document_registry(spec)
        assert reg.exists("get")
        assert reg.exists("search")
        assert reg.exists("raw_search")
        assert reg.exists("create")
        assert reg.exists("kill")

    def test_update_registered_when_supports_update(self) -> None:
        spec = _minimal_spec(supports_update=True)
        reg = build_document_registry(spec)
        assert reg.exists("update")

    def test_update_not_registered_when_no_supports_update(self) -> None:
        spec = _minimal_spec(supports_update=False)
        reg = build_document_registry(spec)
        assert not reg.exists("update")

    def test_resolve_get_returns_usecase(
        self,
        composition_ctx,
    ) -> None:
        spec = _minimal_spec()
        reg = build_document_registry(spec)
        uc = reg.resolve("get", composition_ctx)
        assert uc is not None


class TestDocumentUsecasesFacadeProvider:
    """Tests for DocumentUsecasesFacadeProvider."""

    def test_call_returns_facade(
        self,
        composition_ctx,
    ) -> None:
        spec = _minimal_spec()
        dtos: DTOSpec = {"read": ReadDocument}
        provider = DocumentUsecasesFacadeProvider(spec=spec, dtos=dtos)
        facade = provider(composition_ctx)
        assert facade is not None
        assert facade.ctx is composition_ctx

    def test_facade_get_resolves(
        self,
        composition_ctx,
    ) -> None:
        spec = _minimal_spec()
        dtos: DTOSpec = {"read": ReadDocument}
        provider = DocumentUsecasesFacadeProvider(spec=spec, dtos=dtos)
        facade = provider(composition_ctx)
        uc = facade.get()
        assert uc is not None
