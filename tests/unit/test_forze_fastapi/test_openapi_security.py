"""``apply_openapi_security``: project a configured ``AuthnRequirement`` onto OpenAPI.

End-to-end through the real attach machinery — a document registry whose ``create``
operation carries an authz hook (so the catalog flags it ``requires_authn``) projected
via ``attach_document_routes``, then enriched with the same ``AuthnRequirement`` an app
would hand the security middleware.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

pytest.importorskip("fastapi")

from fastapi import APIRouter, FastAPI

from forze.application.contracts.authn import AuthnSpec
from forze.application.contracts.authz import AuthzSpec
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution.operations.registry import FrozenOperationRegistry
from forze.application.hooks.authz import AuthzBeforeAuthorize
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_fastapi.routes import attach_document_routes
from forze_fastapi.security import (
    AuthnRequirement,
    HeaderApiKeyAuthn,
    HeaderTokenAuthn,
    apply_openapi_security,
)
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

_AUTHN = AuthnSpec(name="api")
_BEARER = HeaderTokenAuthn(
    authn_spec=_AUTHN, header_name="Authorization", description="JWT access token"
)
_API_KEY = HeaderApiKeyAuthn(authn_spec=_AUTHN, header_name="X-API-Key")


def _registry() -> FrozenOperationRegistry:
    reg = build_document_registry(
        _SPEC, DocumentDTOs(read=_NoteRead, create=_NoteCreate)
    )
    reg = (
        reg.bind(_CREATE_OP)
        .with_deadline(timedelta(seconds=5))
        .bind_outer()
        .before(
            AuthzBeforeAuthorize(spec=AuthzSpec(name="z"), action="notes.write").to_step(
                step_id="authz", requires=()
            )
        )
        .finish(deep=True)
    )

    return reg.freeze()


def _app() -> FastAPI:
    router = APIRouter(prefix="/notes")
    attach_document_routes(
        router,
        registry=_registry(),
        ns=_SPEC.default_namespace,
        ctx_dep=lambda: context_from_modules(MockDepsModule(state=MockState())),
        style="rest",
    )

    app = FastAPI()
    app.include_router(router)

    return app


def _operations(doc: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        operation["operationId"]: operation
        for methods in doc["paths"].values()
        for operation in methods.values()
    }


# ....................... #


class TestApplyOpenApiSecurity:
    def test_registers_a_scheme_per_ingress(self) -> None:
        app = _app()
        apply_openapi_security(app, AuthnRequirement(ingress=(_BEARER, _API_KEY)))

        schemes = app.openapi()["components"]["securitySchemes"]

        assert schemes["bearerAuth"] == {
            "type": "http",
            "scheme": "bearer",
            "description": "JWT access token",
        }
        assert schemes["apiKey_X-API-Key"] == {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key",
        }

    def test_security_attaches_only_to_protected_operations(self) -> None:
        app = _app()
        apply_openapi_security(app, AuthnRequirement(ingress=(_BEARER, _API_KEY)))

        ops = _operations(app.openapi())

        # OR alternatives: one single-key object per ingress.
        assert ops[_CREATE_OP]["security"] == [{"bearerAuth": []}, {"apiKey_X-API-Key": []}]
        # The unguarded read stays open.
        assert "security" not in ops[_GET_OP]

    def test_exclude_leaves_a_flagged_operation_open(self) -> None:
        app = _app()
        apply_openapi_security(
            app, AuthnRequirement(ingress=(_BEARER,)), exclude={_CREATE_OP}
        )

        ops = _operations(app.openapi())

        assert "security" not in ops[_CREATE_OP]

    def test_is_idempotent_across_repeated_schema_builds(self) -> None:
        app = _app()
        apply_openapi_security(app, AuthnRequirement(ingress=(_BEARER,)))

        first = app.openapi()
        second = app.openapi()

        assert first == second
        assert _operations(second)[_CREATE_OP]["security"] == [{"bearerAuth": []}]

    def test_does_not_touch_schema_without_protected_ops(self) -> None:
        # A requirement whose ingress is documented, but no op is flagged → schemes
        # are still registered (discoverable) yet no operation gains `security`.
        router = APIRouter(prefix="/notes")
        attach_document_routes(
            router,
            registry=build_document_registry(
                _SPEC, DocumentDTOs(read=_NoteRead, create=_NoteCreate)
            ).freeze(),
            ns=_SPEC.default_namespace,
            ctx_dep=lambda: context_from_modules(MockDepsModule(state=MockState())),
            style="rest",
        )
        app = FastAPI()
        app.include_router(router)
        apply_openapi_security(app, AuthnRequirement(ingress=(_BEARER,)))

        doc = app.openapi()

        assert "bearerAuth" in doc["components"]["securitySchemes"]
        assert all("security" not in op for op in _operations(doc).values())
