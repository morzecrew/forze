"""Unit tests for forze.application.composition.document."""

from forze.application.composition.document import (
    DocumentDTOSpec,
    DocumentOperation,
    DocumentUsecasesFacadeProvider,
    build_document_plan,
    build_document_registry,
)
from forze.application.contracts.document import DocumentSpec
from forze.application.execution import UsecaseRegistry
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument

# ----------------------- #


def _minimal_spec(
    supports_update: bool = False, supports_soft_delete: bool = False
) -> DocumentSpec:
    """Build a minimal DocumentSpec for testing."""
    from forze.domain.models import BaseDTO

    class UpdateCmd(BaseDTO):
        """Minimal update DTO with one field for supports_update."""

        title: str | None = None

    update_cmd = (
        UpdateCmd if supports_update else type("EmptyUpdate", (BaseDTO,), {})
    )
    return DocumentSpec(
        namespace="test",
        read={"source": "test_read", "model": ReadDocument},
        write={
            "source": "test_write",
            "models": {
                "domain": Document,
                "create_cmd": CreateDocumentCmd,
                "update_cmd": update_cmd,
            },
        },
    )


def _minimal_dto_spec(supports_update: bool = False) -> DocumentDTOSpec:
    """Build a minimal DocumentDTOSpec for testing."""
    from forze.domain.models import BaseDTO

    class UpdateCmd(BaseDTO):
        title: str | None = None

    empty_update = type("EmptyUpdate", (BaseDTO,), {})
    dto: DocumentDTOSpec = {
        "read": ReadDocument,
        "create": CreateDocumentCmd,
        "update": UpdateCmd if supports_update else empty_update,
    }
    return dto


class TestBuildDocumentRegistry:
    """Tests for build_document_registry."""

    def test_returns_registry(self) -> None:
        spec = _minimal_spec()
        dto_spec = _minimal_dto_spec()
        reg = build_document_registry(spec, dto_spec)
        assert isinstance(reg, UsecaseRegistry)

    def test_has_core_operations(self) -> None:
        spec = _minimal_spec()
        dto_spec = _minimal_dto_spec()
        reg = build_document_registry(spec, dto_spec)
        assert reg.exists(DocumentOperation.GET)
        assert reg.exists(DocumentOperation.CREATE)
        assert reg.exists(DocumentOperation.KILL)

    def test_update_registered_when_supports_update(self) -> None:
        spec = _minimal_spec(supports_update=True)
        dto_spec = _minimal_dto_spec(supports_update=True)
        reg = build_document_registry(spec, dto_spec)
        assert reg.exists(DocumentOperation.UPDATE)

    def test_update_not_registered_when_no_supports_update(self) -> None:
        spec = _minimal_spec(supports_update=False)
        dto_spec = _minimal_dto_spec()
        reg = build_document_registry(spec, dto_spec)
        assert not reg.exists(DocumentOperation.UPDATE)

    def test_resolve_get_returns_usecase(
        self,
        composition_ctx,
    ) -> None:
        spec = _minimal_spec()
        dto_spec = _minimal_dto_spec()
        reg = build_document_registry(spec, dto_spec)
        uc = reg.resolve(DocumentOperation.GET, composition_ctx)
        assert uc is not None


class TestDocumentUsecasesFacadeProvider:
    """Tests for DocumentUsecasesFacadeProvider."""

    def test_call_returns_facade(
        self,
        composition_ctx,
    ) -> None:
        spec = _minimal_spec()
        dto_spec = _minimal_dto_spec()
        reg = build_document_registry(spec, dto_spec)
        plan = build_document_plan()
        dtos: DocumentDTOSpec = {"read": ReadDocument, "create": CreateDocumentCmd}
        provider = DocumentUsecasesFacadeProvider(
            spec=spec,
            reg=reg,
            plan=plan,
            dtos=dtos,
        )
        facade = provider(composition_ctx)
        assert facade is not None
        assert facade.ctx is composition_ctx

    def test_facade_get_resolves(
        self,
        composition_ctx,
    ) -> None:
        spec = _minimal_spec()
        dto_spec = _minimal_dto_spec()
        reg = build_document_registry(spec, dto_spec)
        plan = build_document_plan()
        dtos: DocumentDTOSpec = {"read": ReadDocument, "create": CreateDocumentCmd}
        provider = DocumentUsecasesFacadeProvider(
            spec=spec,
            reg=reg,
            plan=plan,
            dtos=dtos,
        )
        facade = provider(composition_ctx)
        uc = facade.get()
        assert uc is not None
