"""Tests for generated document routes (forze_fastapi.routes.attach_document_routes)."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from uuid import UUID

from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel, field_validator

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution.operations import OperationDescriptor
from forze.application.execution.operations.registry import FrozenOperationRegistry
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, ReadDocument
from forze_fastapi.exceptions import ERROR_CODE_HEADER, register_exception_handlers
from forze_fastapi.routes import attach_document_routes
from forze_kits.aggregates.document import (
    DocumentDTOs,
    DocumentKernelOp,
    build_document_registry,
)
from forze_kits.aggregates.soft_deletion import (
    SoftDeletionKernelOp,
    build_soft_deletion_registry,
)
from forze_kits.domain.soft_deletion.models import (
    DocWithSoftDeletion,
    UpdateCmdWithSoftDeletion,
)
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_modules

# ----------------------- #


class _NoteRead(ReadDocument):
    title: str
    is_deleted: bool = False


class _NoteCreate(BaseDTO):
    title: str = ""


class _NoteUpdate(UpdateCmdWithSoftDeletion):
    title: str | None = None


class _Note(DocWithSoftDeletion):
    title: str = ""


# ....................... #


def _spec(*, writable: bool = True) -> DocumentSpec[_NoteRead, _Note, _NoteCreate, _NoteUpdate]:
    return DocumentSpec(
        name="notes",
        read=_NoteRead,
        write=(
            DocumentWriteTypes(
                domain=_Note, create_cmd=_NoteCreate, update_cmd=_NoteUpdate
            )
            if writable
            else None
        ),
    )


def _registry(spec: DocumentSpec) -> FrozenOperationRegistry:
    dtos = DocumentDTOs(
        read=_NoteRead,
        create=_NoteCreate if spec.write else None,
        update=_NoteUpdate if spec.write else None,
    )
    reg = build_document_registry(spec, dtos)
    reg = reg.merge(build_soft_deletion_registry(spec))
    return reg.freeze()


def _build_app(
    style,
    *,
    writable: bool = True,
    include=None,
    resource=None,
    path_overrides=None,
) -> FastAPI:
    spec = _spec(writable=writable)
    state = MockState()

    router = APIRouter(prefix="/notes")
    attach_document_routes(
        router,
        registry=_registry(spec),
        # Use the resource-prefix convenience when requested, else the explicit ns.
        ns=None if resource is not None else spec.default_namespace,
        resource=resource,
        ctx_dep=lambda: context_from_modules(MockDepsModule(state=state)),
        style=style,
        include=include,
        path_overrides=path_overrides,
    )

    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)

    return app


def _operation_ids(app: FastAPI) -> set[str]:
    return {
        operation["operationId"]
        for methods in app.openapi()["paths"].values()
        for operation in methods.values()
    }


_ALL_OPS = (*DocumentKernelOp, *SoftDeletionKernelOp)
_ALL_OP_IDS = {f"notes.{op.value}" for op in _ALL_OPS}
_READ_OP_IDS = {
    f"notes.{op.value}"
    for op in (
        DocumentKernelOp.GET,
        DocumentKernelOp.LIST,
        DocumentKernelOp.RAW_LIST,
        DocumentKernelOp.LIST_CURSOR,
        DocumentKernelOp.RAW_LIST_CURSOR,
        DocumentKernelOp.AGG_LIST,
    )
}


# ....................... #


class TestRestStyle:
    def test_crud_round_trip(self) -> None:
        client = TestClient(_build_app("rest"))

        created = client.post("/notes", json={"title": "hello"})
        assert created.status_code == 201
        note = created.json()
        assert note["title"] == "hello"

        fetched = client.get(f"/notes/{note['id']}")
        assert fetched.status_code == 200
        assert fetched.json()["title"] == "hello"

        updated = client.patch(
            f"/notes/{note['id']}",
            params={"rev": note["rev"]},
            json={"title": "world"},
        )
        assert updated.status_code == 200
        assert updated.json()["data"]["title"] == "world"

        listed = client.post("/notes/list", json={})
        assert listed.status_code == 200
        assert [hit["title"] for hit in listed.json()["hits"]] == ["world"]

        killed = client.delete(f"/notes/{note['id']}")
        assert killed.status_code == 204

        assert client.get(f"/notes/{note['id']}").status_code == 404

    def test_paths_and_methods(self) -> None:
        paths = _build_app("rest").openapi()["paths"]

        assert set(paths["/notes"]) == {"post"}
        assert set(paths["/notes/{id}"]) == {"get", "patch", "delete"}
        assert set(paths["/notes/list"]) == {"post"}
        assert set(paths["/notes/agg_list"]) == {"post"}
        assert set(paths["/notes/{id}/delete"]) == {"post"}
        assert set(paths["/notes/{id}/restore"]) == {"post"}

    def test_soft_delete_and_restore(self) -> None:
        client = TestClient(_build_app("rest"))

        note = client.post("/notes", json={"title": "x"}).json()

        deleted = client.post(
            f"/notes/{note['id']}/delete", params={"rev": note["rev"]}
        )
        assert deleted.status_code == 200
        assert deleted.json()["is_deleted"] is True

        restored = client.post(
            f"/notes/{note['id']}/restore",
            params={"rev": deleted.json()["rev"]},
        )
        assert restored.status_code == 200
        assert restored.json()["is_deleted"] is False

    def test_update_rev_is_a_required_query_param(self) -> None:
        client = TestClient(_build_app("rest"))

        created = client.post("/notes", json={"title": "x"}).json()
        response = client.patch(f"/notes/{created['id']}", json={"title": "y"})

        assert response.status_code == 422


class TestRpcStyle:
    def test_crud_round_trip(self) -> None:
        client = TestClient(_build_app("rpc"))

        created = client.post("/notes/create", json={"title": "hello"})
        assert created.status_code == 201
        note = created.json()

        fetched = client.get("/notes/get", params={"id": note["id"]})
        assert fetched.status_code == 200
        assert fetched.json()["title"] == "hello"

        updated = client.patch(
            "/notes/update",
            params={"id": note["id"], "rev": note["rev"]},
            json={"title": "world"},
        )
        assert updated.status_code == 200
        assert updated.json()["data"]["title"] == "world"

        listed = client.post("/notes/list", json={})
        assert listed.status_code == 200
        assert listed.json()["count"] == 1

        killed = client.delete("/notes/kill", params={"id": note["id"]})
        assert killed.status_code == 204

    def test_soft_delete_and_restore(self) -> None:
        client = TestClient(_build_app("rpc"))

        note = client.post("/notes/create", json={"title": "x"}).json()

        deleted = client.patch(
            "/notes/delete", params={"id": note["id"], "rev": note["rev"]}
        )
        assert deleted.status_code == 200
        assert deleted.json()["is_deleted"] is True

        restored = client.patch(
            "/notes/restore",
            params={"id": note["id"], "rev": deleted.json()["rev"]},
        )
        assert restored.status_code == 200
        assert restored.json()["is_deleted"] is False

    def test_paths_and_methods(self) -> None:
        paths = _build_app("rpc").openapi()["paths"]

        assert set(paths) == {f"/notes/{op.value}" for op in _ALL_OPS}
        assert set(paths["/notes/get"]) == {"get"}
        assert set(paths["/notes/create"]) == {"post"}
        assert set(paths["/notes/update"]) == {"patch"}
        assert set(paths["/notes/kill"]) == {"delete"}
        assert set(paths["/notes/delete"]) == {"patch"}
        assert set(paths["/notes/restore"]) == {"patch"}
        assert set(paths["/notes/list"]) == {"post"}
        assert set(paths["/notes/agg_list"]) == {"post"}

    def test_update_rev_is_a_required_query_param(self) -> None:
        client = TestClient(_build_app("rpc"))

        created = client.post("/notes/create", json={"title": "x"}).json()
        # Supply a valid id so the 422 isolates the missing rev, not the missing id.
        response = client.patch(f"/notes/update?id={created['id']}", json={"title": "y"})

        assert response.status_code == 422


class TestCatalogProjection:
    @pytest.mark.parametrize("style", ["rest", "rpc"])
    def test_operation_ids_are_registry_keys_verbatim(self, style: str) -> None:
        assert _operation_ids(_build_app(style)) == _ALL_OP_IDS

    def test_read_only_spec_attaches_only_reads(self) -> None:
        assert _operation_ids(_build_app("rest", writable=False)) == _READ_OP_IDS

    def test_include_narrows_to_subset(self) -> None:
        app = _build_app("rest", include={DocumentKernelOp.GET, "list"})

        assert _operation_ids(app) == {"notes.get", "notes.list"}

    def test_include_of_unregistered_operation_raises(self) -> None:
        with pytest.raises(CoreException, match="not registered"):
            _build_app("rest", writable=False, include={DocumentKernelOp.CREATE})

    def test_include_of_unknown_operation_raises(self) -> None:
        with pytest.raises(CoreException, match="Unknown operations"):
            _build_app("rest", include={"nope"})

    def test_empty_namespace_raises(self) -> None:
        from forze.base.primitives import StrKeyNamespace

        spec = _spec()

        with pytest.raises(CoreException, match="No matching operations"):
            attach_document_routes(
                APIRouter(),
                registry=_registry(spec),
                ns=StrKeyNamespace(prefix="other"),
                ctx_dep=lambda: context_from_modules(MockDepsModule()),
                style="rest",
            )


class TestResourcePrefix:
    def test_resource_prefix_matches_explicit_namespace(self) -> None:
        # resource="notes" builds StrKeyNamespace(prefix="notes"), the same prefix
        # the registry was built under (spec.name == "notes").
        assert _operation_ids(_build_app("rest", resource="notes")) == _ALL_OP_IDS

    def test_resource_prefix_round_trips(self) -> None:
        client = TestClient(_build_app("rest", resource="notes"))

        created = client.post("/notes", json={"title": "hi"})
        assert created.status_code == 201
        assert client.get(f"/notes/{created.json()['id']}").json()["title"] == "hi"

    def test_resource_mismatch_finds_no_operations(self) -> None:
        with pytest.raises(CoreException, match="No matching operations"):
            _build_app("rest", resource="other")

    def test_both_ns_and_resource_raises(self) -> None:
        spec = _spec()

        with pytest.raises(CoreException, match="exactly one"):
            attach_document_routes(
                APIRouter(),
                registry=_registry(spec),
                ns=spec.default_namespace,
                resource="notes",
                ctx_dep=lambda: context_from_modules(MockDepsModule()),
                style="rest",
            )

    def test_neither_ns_nor_resource_raises(self) -> None:
        spec = _spec()

        with pytest.raises(CoreException, match="exactly one"):
            attach_document_routes(
                APIRouter(),
                registry=_registry(spec),
                ctx_dep=lambda: context_from_modules(MockDepsModule()),
                style="rest",
            )


class TestPathOverrides:
    def test_override_keeps_operation_id_and_moves_path(self) -> None:
        app = _build_app(
            "rest", path_overrides={DocumentKernelOp.GET: "/by-id/{id}"}
        )
        paths = app.openapi()["paths"]

        assert "/notes/by-id/{id}" in paths
        assert "get" not in paths.get("/notes/{id}", {})
        # operationId stays the verbatim catalog key regardless of the new path.
        assert _operation_ids(app) == _ALL_OP_IDS

    def test_overridden_route_dispatches(self) -> None:
        client = TestClient(
            _build_app("rest", path_overrides={DocumentKernelOp.GET: "/by-id/{id}"})
        )

        created = client.post("/notes", json={"title": "moved"})
        note = created.json()

        # GET no longer serves the old path (PATCH/DELETE still live there → 405).
        assert client.get(f"/notes/{note['id']}").status_code == 405
        fetched = client.get(f"/notes/by-id/{note['id']}")
        assert fetched.status_code == 200
        assert fetched.json()["title"] == "moved"

    def test_override_of_paramless_path(self) -> None:
        app = _build_app("rest", path_overrides={DocumentKernelOp.LIST: "/search"})
        paths = app.openapi()["paths"]

        assert "/notes/search" in paths
        assert "/notes/list" not in paths

    def test_override_accepts_str_key(self) -> None:
        app = _build_app("rest", path_overrides={"get": "/by-id/{id}"})

        assert "/notes/by-id/{id}" in app.openapi()["paths"]

    def test_override_dropping_path_param_raises(self) -> None:
        with pytest.raises(CoreException, match="drops path parameter"):
            _build_app("rest", path_overrides={DocumentKernelOp.GET: "/by-id"})

    def test_override_adding_path_param_raises(self) -> None:
        with pytest.raises(CoreException, match="adds path parameter"):
            _build_app(
                "rest", path_overrides={DocumentKernelOp.GET: "/{tenant}/by-id/{id}"}
            )

    def test_override_of_unknown_operation_raises(self) -> None:
        with pytest.raises(CoreException, match="Unknown path override"):
            _build_app("rest", path_overrides={"nope": "/whatever"})

    def test_override_of_excluded_operation_raises(self) -> None:
        # GET is a real op but excluded by `include`, so the override is dead
        # config — caught instead of silently ignored.
        with pytest.raises(CoreException, match="Unknown path override"):
            _build_app(
                "rest",
                include=[DocumentKernelOp.LIST],
                path_overrides={DocumentKernelOp.GET: "/by-id/{id}"},
            )


# ....................... #

_RESERVED_ID = UUID("00000000-0000-0000-0000-0000000000ff")


class _BadGetDTO(BaseDTO):
    """Get DTO with a required field the ``{id}`` route cannot supply."""

    id: UUID
    tenant: str


class _OptionalExtraGetDTO(BaseDTO):
    """Get DTO whose extra field has a default — satisfiable from ``{id}`` alone."""

    id: UUID
    verbose: bool = False


class _PickyGetDTO(BaseDTO):
    """Get DTO whose model-level validation can still fail at request time."""

    id: UUID

    @field_validator("id")
    @classmethod
    def _reject_reserved(cls, value: UUID) -> UUID:
        if value == _RESERVED_ID:
            raise ValueError("id is reserved")
        return value


def _build_app_with_get_input(input_type: type[BaseModel]) -> FastAPI:
    """Build the app with the ``get`` descriptor's input DTO swapped out."""

    spec = _spec()
    dtos = DocumentDTOs(read=_NoteRead, create=_NoteCreate, update=_NoteUpdate)
    reg = build_document_registry(spec, dtos)
    reg = reg.merge(build_soft_deletion_registry(spec))
    reg = reg.set_descriptors(
        {
            DocumentKernelOp.GET: OperationDescriptor(
                input_type=input_type, output_type=_NoteRead
            )
        },
        override=True,
        namespace=spec.default_namespace,
    )

    state = MockState()
    router = APIRouter(prefix="/notes")
    attach_document_routes(
        router,
        registry=reg.freeze(),
        ns=spec.default_namespace,
        ctx_dep=lambda: context_from_modules(MockDepsModule(state=state)),
        style="rest",
    )

    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)

    return app


class TestDtoShapeValidation:
    def test_unsatisfiable_required_field_fails_at_attach_time(self) -> None:
        # Regression: this used to attach fine and answer 500 on every request.
        with pytest.raises(CoreException, match=r"required fields \['tenant'\]") as e:
            _build_app_with_get_input(_BadGetDTO)

        assert e.value.kind.value == "configuration"

    def test_optional_extra_field_attaches_and_serves(self) -> None:
        client = TestClient(_build_app_with_get_input(_OptionalExtraGetDTO))

        note = client.post("/notes", json={"title": "hello"}).json()
        fetched = client.get(f"/notes/{note['id']}")

        assert fetched.status_code == 200
        assert fetched.json()["title"] == "hello"

    def test_runtime_validation_error_answers_standard_422(self) -> None:
        client = TestClient(_build_app_with_get_input(_PickyGetDTO))

        response = client.get(f"/notes/{_RESERVED_ID}")

        assert response.status_code == 422
        assert "detail" in response.json()
        assert response.headers[ERROR_CODE_HEADER] == "core.validation"


class TestSensitiveSpecRefusal:
    def _sensitive_registry(self) -> tuple[DocumentSpec, FrozenOperationRegistry]:
        spec = DocumentSpec(
            name="notes",
            read=_NoteRead,
            write=DocumentWriteTypes(
                domain=_Note, create_cmd=_NoteCreate, update_cmd=_NoteUpdate
            ),
            sensitive=True,
        )
        dtos = DocumentDTOs(read=_NoteRead, create=_NoteCreate, update=_NoteUpdate)
        return spec, build_document_registry(spec, dtos).freeze()

    def test_sensitive_registry_fails_at_attach_time(self) -> None:
        # The refusal must happen at attach time (startup), never as a 500 at
        # request time.
        spec, registry = self._sensitive_registry()
        router = APIRouter(prefix="/notes")

        with pytest.raises(CoreException, match="sensitive") as e:
            attach_document_routes(
                router,
                registry=registry,
                ns=spec.default_namespace,
                ctx_dep=lambda: context_from_modules(MockDepsModule()),
                style="rest",
            )

        assert e.value.kind.value == "configuration"
        # Nothing was served: no app exists, so there is no request-time surface.
        assert all(route.path != "/notes/{id}" for route in router.routes)

    def test_non_sensitive_registry_attaches_unchanged(self) -> None:
        app = _build_app("rest")

        assert _operation_ids(app)


class TestDescriptorTags:
    def _build_app_with_get_tags(self, tags: tuple[str, ...]) -> FastAPI:
        """Build the app with the ``get`` descriptor carrying OpenAPI tags."""

        from forze_kits.aggregates.document import DocumentIdDTO

        spec = _spec()
        dtos = DocumentDTOs(read=_NoteRead, create=_NoteCreate, update=_NoteUpdate)
        reg = build_document_registry(spec, dtos)
        reg = reg.merge(build_soft_deletion_registry(spec))
        reg = reg.set_descriptors(
            {
                DocumentKernelOp.GET: OperationDescriptor(
                    input_type=DocumentIdDTO,
                    output_type=_NoteRead,
                    tags=tags,
                )
            },
            override=True,
            namespace=spec.default_namespace,
        )

        router = APIRouter(prefix="/notes")
        attach_document_routes(
            router,
            registry=reg.freeze(),
            ns=spec.default_namespace,
            ctx_dep=lambda: context_from_modules(MockDepsModule(state=MockState())),
            style="rest",
        )

        app = FastAPI()
        app.include_router(router)

        return app

    def test_tagged_descriptor_projects_openapi_tags(self) -> None:
        app = self._build_app_with_get_tags(("notes", "read-side"))

        paths = app.openapi()["paths"]

        assert paths["/notes/{id}"]["get"]["tags"] == ["notes", "read-side"]
        # Sibling operations without descriptor tags stay untagged.
        assert "tags" not in paths["/notes"]["post"]

    def test_untagged_descriptors_unchanged(self) -> None:
        paths = _build_app("rest").openapi()["paths"]

        for methods in paths.values():
            for operation in methods.values():
                assert "tags" not in operation
