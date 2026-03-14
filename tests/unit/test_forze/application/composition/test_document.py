"""Unit tests for forze.application.composition.document."""

import pytest

from forze.application.composition.base import BaseUsecasesFacadeProvider
from forze.application.composition.document import (
    DocumentDTOSpec,
    DocumentOperation,
    DocumentUsecasesFacade,
    DocumentUsecasesModule,
    build_document_create_mapper,
    build_document_registry,
    tx_document_plan,
)
from forze.application.composition.document.factories import (
    build_document_list_mapper,
    build_document_raw_list_mapper,
    build_document_update_mapper,
)
from forze.application.contracts.document import DocumentSpec
from forze.application.execution import UsecaseRegistry
from forze.application.mapping import DTOMapper
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

    update_cmd = UpdateCmd if supports_update else type("EmptyUpdate", (BaseDTO,), {})
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


class TestBuildDocumentCreateMapper:
    """Tests for build_document_create_mapper."""

    def test_returns_mapper_when_spec_supports_create(self) -> None:
        spec = _minimal_spec()
        dto_spec = _minimal_dto_spec()
        mapper = build_document_create_mapper(spec, dto_spec)
        assert isinstance(mapper, DTOMapper)
        assert mapper.in_ == CreateDocumentCmd
        assert mapper.out == CreateDocumentCmd

    def test_raises_when_spec_has_no_write(self) -> None:
        from forze.base.errors import CoreError

        spec = DocumentSpec(
            namespace="test",
            read={"source": "test_read", "model": ReadDocument},
            write=None,
        )
        dto_spec = _minimal_dto_spec()
        with pytest.raises(CoreError, match="does not support write operations"):
            build_document_create_mapper(spec, dto_spec)

    def test_raises_when_dto_spec_has_no_create(self) -> None:
        from forze.base.errors import CoreError

        spec = _minimal_spec()
        dto_spec: DocumentDTOSpec = {"read": ReadDocument}
        with pytest.raises(CoreError, match="does not support create operations"):
            build_document_create_mapper(spec, dto_spec)


class TestBuildDocumentUpdateMapper:
    """Tests for build_document_update_mapper."""

    def test_returns_mapper_when_spec_supports_update(self) -> None:
        spec = _minimal_spec(supports_update=True)
        dto_spec = _minimal_dto_spec(supports_update=True)
        mapper = build_document_update_mapper(spec, dto_spec)
        assert isinstance(mapper, DTOMapper)

    def test_raises_when_spec_has_no_write(self) -> None:
        from forze.base.errors import CoreError

        spec = DocumentSpec(
            namespace="test",
            read={"source": "test_read", "model": ReadDocument},
            write=None,
        )
        dto_spec = _minimal_dto_spec(supports_update=True)
        with pytest.raises(CoreError, match="does not support write operations"):
            build_document_update_mapper(spec, dto_spec)

    def test_raises_when_dto_spec_has_no_update(self) -> None:
        from forze.base.errors import CoreError

        spec = _minimal_spec(supports_update=True)
        dto_spec: DocumentDTOSpec = {"read": ReadDocument, "create": CreateDocumentCmd}
        with pytest.raises(CoreError, match="does not support update operations"):
            build_document_update_mapper(spec, dto_spec)


class TestBuildDocumentListMapper:
    """Tests for build_document_list_mapper."""

    def test_returns_mapper(self) -> None:
        from forze.application.dto import ListRequestDTO

        spec = _minimal_spec()
        dto_spec = _minimal_dto_spec()
        mapper = build_document_list_mapper(spec, dto_spec)
        assert isinstance(mapper, DTOMapper)
        assert mapper.out == ListRequestDTO


class TestBuildDocumentRawListMapper:
    """Tests for build_document_raw_list_mapper."""

    def test_returns_mapper(self) -> None:
        from forze.application.dto import RawListRequestDTO

        spec = _minimal_spec()
        dto_spec = _minimal_dto_spec()
        mapper = build_document_raw_list_mapper(spec, dto_spec)
        assert isinstance(mapper, DTOMapper)
        assert mapper.out == RawListRequestDTO


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


class TestDocumentUsecasesModule:
    """Tests for DocumentUsecasesModule."""

    def test_provider_call_returns_facade(
        self,
        composition_ctx,
    ) -> None:
        spec = _minimal_spec()
        dto_spec = _minimal_dto_spec()
        reg = build_document_registry(spec, dto_spec)
        plan = tx_document_plan
        dtos: DocumentDTOSpec = {"read": ReadDocument, "create": CreateDocumentCmd}
        provider = BaseUsecasesFacadeProvider(
            reg=reg,
            plan=plan,
            facade=DocumentUsecasesFacade,
        )
        module = DocumentUsecasesModule(spec=spec, dtos=dtos, provider=provider)
        facade = module.provider(composition_ctx)
        assert facade is not None
        assert facade.ctx is composition_ctx

    def test_facade_get_resolves(
        self,
        composition_ctx,
    ) -> None:
        spec = _minimal_spec()
        dto_spec = _minimal_dto_spec()
        reg = build_document_registry(spec, dto_spec)
        plan = tx_document_plan
        dtos: DocumentDTOSpec = {"read": ReadDocument, "create": CreateDocumentCmd}
        provider = BaseUsecasesFacadeProvider(
            reg=reg,
            plan=plan,
            facade=DocumentUsecasesFacade,
        )
        module = DocumentUsecasesModule(spec=spec, dtos=dtos, provider=provider)
        facade = module.provider(composition_ctx)
        uc = facade.get()
        assert uc is not None
