"""Catalog-derived OpenAPI projections (idempotency header + declared permissions).

End-to-end through the real attach machinery: a document registry built by the kit,
app-attached ``IdempotencyWrap`` + ``AuthzBeforeAuthorize`` hooks on the create
operation, frozen, and projected via ``attach_document_routes`` with a
``MockDepsModule``-backed execution context.
"""

from __future__ import annotations

import json
from datetime import timedelta

import pytest

pytest.importorskip("fastapi")

from typing import Any

from fastapi import APIRouter, FastAPI

from forze.application.contracts.authz import AuthzSpec
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.idempotency import IdempotencySpec
from forze.application.execution.operations.registry import FrozenOperationRegistry
from forze.application.hooks.authz import AuthzBeforeAuthorize
from forze.application.hooks.idempotency import IdempotencyWrap
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_fastapi.middlewares import IDEMPOTENCY_KEY_HEADER
from forze_fastapi.routes import attach_document_routes
from forze_kits.aggregates.document import (
    DocumentDTOs,
    DocumentKernelOp,
    build_document_registry,
)
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_modules

# ----------------------- #


class _NoteRead(ReadDocument):
    title: str


class _NoteCreate(BaseDTO):
    title: str = ""


class _Note(Document):
    title: str = ""


_SPEC = DocumentSpec(
    name="notes",
    read=_NoteRead,
    write=DocumentWriteTypes(domain=_Note, create_cmd=_NoteCreate),
)
_CREATE_OP = str(_SPEC.default_namespace.key(DocumentKernelOp.CREATE))
_GET_OP = str(_SPEC.default_namespace.key(DocumentKernelOp.GET))


def _registry(*, with_hooks: bool) -> FrozenOperationRegistry:
    reg = build_document_registry(
        _SPEC, DocumentDTOs(read=_NoteRead, create=_NoteCreate)
    )

    if with_hooks:
        reg = (
            reg.bind(_CREATE_OP)
            .with_deadline(timedelta(seconds=5))
            .bind_outer()
            .before(
                AuthzBeforeAuthorize(
                    spec=AuthzSpec(name="z"), action="notes.write"
                ).to_step(step_id="authz", requires=())
            )
            .wrap(
                IdempotencyWrap(
                    op=_CREATE_OP,
                    spec=IdempotencySpec(name="s"),
                    result_type=_NoteRead,
                ).to_step()
            )
            .finish(deep=True)
        )

    return reg.freeze()


def _openapi(*, with_hooks: bool) -> dict[str, Any]:
    router = APIRouter(prefix="/notes")
    attach_document_routes(
        router,
        registry=_registry(with_hooks=with_hooks),
        ns=_SPEC.default_namespace,
        ctx_dep=lambda: context_from_modules(MockDepsModule(state=MockState())),
        style="rest",
    )

    app = FastAPI()
    app.include_router(router)

    return app.openapi()


def _operations(doc: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        operation["operationId"]: operation
        for methods in doc["paths"].values()
        for operation in methods.values()
    }


# ....................... #


class TestEndToEndProjection:
    def test_catalog_entry_shows_all_derived_facts(self) -> None:
        catalog = _registry(with_hooks=True).catalog()

        assert catalog[_CREATE_OP].supports_idempotency_key is True
        assert catalog[_CREATE_OP].required_permissions == ("notes.write",)
        assert catalog[_CREATE_OP].requires_authn is True
        assert catalog[_CREATE_OP].deadline == timedelta(seconds=5)

        assert catalog[_GET_OP].supports_idempotency_key is False
        assert catalog[_GET_OP].required_permissions == ()
        assert catalog[_GET_OP].requires_authn is False
        assert catalog[_GET_OP].deadline is None

    def test_flagged_route_carries_requires_authn_extension(self) -> None:
        ops = _operations(_openapi(with_hooks=True))

        assert ops[_CREATE_OP]["x-requires-authn"] is True
        assert "x-requires-authn" not in ops[_GET_OP]

    def test_flagged_route_documents_optional_idempotency_header(self) -> None:
        create = _operations(_openapi(with_hooks=True))[_CREATE_OP]
        headers = [
            param
            for param in create.get("parameters", [])
            if param.get("in") == "header"
        ]

        assert len(headers) == 1
        header = headers[0]
        assert header["name"] == IDEMPOTENCY_KEY_HEADER
        assert header["required"] is False
        assert header["schema"]["type"] == "string"

    def test_flagged_route_carries_permissions_extension_and_description(self) -> None:
        create = _operations(_openapi(with_hooks=True))[_CREATE_OP]
        descriptor = _registry(with_hooks=True).descriptors[_CREATE_OP]

        assert create["x-required-permissions"] == ["notes.write"]
        # Descriptor text preserved, declared-permissions line appended.
        assert descriptor.description is not None
        assert create["description"].startswith(descriptor.description)
        assert "Requires permissions: `notes.write`" in create["description"]
        assert "declared by attached authorization hooks" in create["description"]

    def test_flagged_route_carries_deadline_extension_and_description(self) -> None:
        create = _operations(_openapi(with_hooks=True))[_CREATE_OP]

        assert create["x-deadline-seconds"] == 5.0
        assert "Time budget: 5s" in create["description"]
        assert "deadline_exceeded" in create["description"]

    def test_unflagged_route_is_untouched(self) -> None:
        get = _operations(_openapi(with_hooks=True))[_GET_OP]
        descriptor = _registry(with_hooks=True).descriptors[_GET_OP]

        assert "x-required-permissions" not in get
        assert "x-requires-authn" not in get
        assert "x-deadline-seconds" not in get
        assert not any(
            param.get("name") == IDEMPOTENCY_KEY_HEADER
            for param in get.get("parameters", [])
        )
        assert get["description"] == descriptor.description

    def test_unflagged_registry_openapi_identical_outside_flagged_op(self) -> None:
        # Attaching hooks to one operation must not perturb any other route's OpenAPI.
        flagged = _openapi(with_hooks=True)
        plain = _openapi(with_hooks=False)

        def _without_create(doc: dict[str, Any]) -> str:
            doc = json.loads(json.dumps(doc))
            for methods in doc["paths"].values():
                for method in list(methods):
                    if methods[method]["operationId"] == _CREATE_OP:
                        del methods[method]
            return json.dumps(doc, sort_keys=True)

        assert _without_create(flagged) == _without_create(plain)

    def test_plain_registry_has_no_catalog_derived_additions_anywhere(self) -> None:
        raw = json.dumps(_openapi(with_hooks=False))

        assert IDEMPOTENCY_KEY_HEADER not in raw
        assert "x-required-permissions" not in raw
        assert "Requires permissions" not in raw
        assert "x-requires-authn" not in raw
        assert "x-deadline-seconds" not in raw
        assert "Time budget" not in raw
