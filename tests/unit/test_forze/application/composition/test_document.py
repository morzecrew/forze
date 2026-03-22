"""Unit tests for forze.application.composition.document."""

import pytest

from forze.application.composition.document import (
    DocumentDTOs,
    DocumentOperation,
    build_document_create_mapper,
    build_document_registry,
)
from forze.application.composition.document.factories import (
    build_document_list_mapper,
    build_document_raw_list_mapper,
    build_document_update_mapper,
)
from forze.application.contracts.document import DocumentSpec
from forze.application.execution import UsecasePlan, UsecaseRegistry
from forze.application.mapping import DTOMapper
from forze.domain.mixins import SoftDeletionMixin
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument

# ----------------------- #


class _SoftDoc(Document, SoftDeletionMixin):
    """Document with soft-delete for facade validation tests."""

    pass


def _minimal_spec(
    supports_update: bool = False, supports_soft_delete: bool = False
) -> DocumentSpec:
    """Build a minimal DocumentSpec for testing."""
    from forze.domain.models import BaseDTO

    class UpdateCmd(BaseDTO):
        """Minimal update DTO with one field for supports_update."""

        title: str | None = None

    update_cmd = UpdateCmd if supports_update else type("EmptyUpdate", (BaseDTO,), {})
    domain = _SoftDoc if supports_soft_delete else Document
    return DocumentSpec(
        namespace="test",
        read={"source": "test_read", "model": ReadDocument},
        write={
            "source": "test_write",
            "models": {
                "domain": domain,
                "create_cmd": CreateDocumentCmd,
                "update_cmd": update_cmd,
            },
        },
    )


def _minimal_dtos(supports_update: bool = False) -> DocumentDTOs:
    """Build minimal DocumentDTOs for testing."""
    from forze.domain.models import BaseDTO

    class UpdateCmd(BaseDTO):
        title: str | None = None

    empty_update = type("EmptyUpdate", (BaseDTO,), {})
    return DocumentDTOs(
        read=ReadDocument,
        create=CreateDocumentCmd,
        update=UpdateCmd if supports_update else empty_update,
    )


class TestBuildDocumentCreateMapper:
    """Tests for build_document_create_mapper."""

    def test_returns_mapper_when_spec_supports_create(self) -> None:
        spec = _minimal_spec()
        dtos = _minimal_dtos()
        mapper = build_document_create_mapper(spec, dtos)
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
        dtos = _minimal_dtos()
        with pytest.raises(CoreError, match="does not support write operations"):
            build_document_create_mapper(spec, dtos)

    def test_raises_when_dtos_has_no_create(self) -> None:
        from forze.base.errors import CoreError

        spec = _minimal_spec()
        dtos = DocumentDTOs(read=ReadDocument)
        with pytest.raises(CoreError, match="does not support create operations"):
            build_document_create_mapper(spec, dtos)


class TestBuildDocumentUpdateMapper:
    """Tests for build_document_update_mapper."""

    def test_returns_mapper_when_spec_supports_update(self) -> None:
        spec = _minimal_spec(supports_update=True)
        dtos = _minimal_dtos(supports_update=True)
        mapper = build_document_update_mapper(spec, dtos)
        assert isinstance(mapper, DTOMapper)

    def test_raises_when_spec_has_no_write(self) -> None:
        from forze.base.errors import CoreError

        spec = DocumentSpec(
            namespace="test",
            read={"source": "test_read", "model": ReadDocument},
            write=None,
        )
        dtos = _minimal_dtos(supports_update=True)
        with pytest.raises(CoreError, match="does not support write operations"):
            build_document_update_mapper(spec, dtos)

    def test_raises_when_dtos_has_no_update(self) -> None:
        from forze.base.errors import CoreError

        spec = _minimal_spec(supports_update=True)
        dtos = DocumentDTOs(read=ReadDocument, create=CreateDocumentCmd)
        with pytest.raises(CoreError, match="does not support update operations"):
            build_document_update_mapper(spec, dtos)


class TestBuildDocumentListMapper:
    """Tests for build_document_list_mapper."""

    def test_returns_mapper(self) -> None:
        from forze.application.dto import ListRequestDTO

        mapper = build_document_list_mapper()
        assert isinstance(mapper, DTOMapper)
        assert mapper.in_ == ListRequestDTO
        assert mapper.out == ListRequestDTO


class TestBuildDocumentRawListMapper:
    """Tests for build_document_raw_list_mapper."""

    def test_returns_mapper(self) -> None:
        from forze.application.dto import RawListRequestDTO

        mapper = build_document_raw_list_mapper()
        assert isinstance(mapper, DTOMapper)
        assert mapper.in_ == RawListRequestDTO
        assert mapper.out == RawListRequestDTO


class TestBuildDocumentRegistry:
    """Tests for build_document_registry."""

    def test_returns_registry(self) -> None:
        spec = _minimal_spec()
        dtos = _minimal_dtos()
        reg = build_document_registry(spec, dtos)
        assert isinstance(reg, UsecaseRegistry)

    def test_has_core_operations(self) -> None:
        spec = _minimal_spec()
        dtos = _minimal_dtos()
        reg = build_document_registry(spec, dtos)
        assert reg.exists(DocumentOperation.GET)
        assert reg.exists(DocumentOperation.CREATE)
        assert reg.exists(DocumentOperation.KILL)

    def test_update_registered_when_supports_update(self) -> None:
        spec = _minimal_spec(supports_update=True)
        dtos = _minimal_dtos(supports_update=True)
        reg = build_document_registry(spec, dtos)
        assert reg.exists(DocumentOperation.UPDATE)

    def test_update_not_registered_when_no_supports_update(self) -> None:
        spec = _minimal_spec(supports_update=False)
        dtos = _minimal_dtos()
        reg = build_document_registry(spec, dtos)
        assert not reg.exists(DocumentOperation.UPDATE)

    def test_resolve_get_returns_usecase(
        self,
        composition_ctx,
    ) -> None:
        spec = _minimal_spec()
        dtos = _minimal_dtos()
        reg = build_document_registry(spec, dtos)
        reg.finalize("document", inplace=True)
        uc = reg.resolve(DocumentOperation.GET, composition_ctx)
        assert uc is not None


class TestDocumentFacadeWithRegistry:
    """Tests for DocumentUsecasesFacade with build_document_registry."""

    def test_facade_resolves_get_usecase(
        self,
        composition_ctx,
    ) -> None:
        """Facade built from registry resolves get usecase."""
        from forze.application.composition.document import DocumentUsecasesFacade

        spec = _minimal_spec(supports_update=True, supports_soft_delete=True)
        dtos = _minimal_dtos(supports_update=True)
        reg = build_document_registry(spec, dtos).extend_plan(UsecasePlan().tx("*"))
        reg.finalize("document", inplace=True)
        facade = DocumentUsecasesFacade(ctx=composition_ctx, reg=reg)
        uc = facade.get
        assert uc is not None
