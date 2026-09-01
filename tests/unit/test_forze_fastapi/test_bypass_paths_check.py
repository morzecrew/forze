"""``check_bypass_paths`` proves a configured bypass is both real and safe."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from fastapi import APIRouter, FastAPI

from forze.application.contracts.authn import AuthnSpec
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import build_runtime
from forze.base.exceptions import CoreException
from forze.base.logging import DEFAULT_HEALTH_PATHS
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_fastapi import runtime_lifespan
from forze_fastapi.middlewares import (
    CustomHeadersMiddleware,
    InvocationMetadataMiddleware,
    SecurityContextMiddleware,
    check_bypass_paths,
)
from forze_fastapi.routes import attach_document_routes, attach_liveness_route
from forze_fastapi.security import AuthnRequirement, HeaderTokenAuthn
from forze_kits.aggregates.document import DocumentDTOs, build_document_registry
from forze_mock import MockDepsModule

# ----------------------- #

_TOKEN_SPEC = AuthnSpec(name="auth", enabled_methods=frozenset({"token"}))


class _Note(Document):
    title: str = ""


class _NoteCreate(BaseDTO):
    title: str = ""


class _NoteRead(ReadDocument):
    title: str


# ....................... #


def _app(
    *,
    bypass: frozenset[str] | set[str],
    security_bypass: frozenset[str] | set[str] | None = None,
    prefix: str = "",
    notes: bool = True,
    probes: bool = True,
) -> FastAPI:
    """An app whose probe and generated routes sit under *prefix*."""

    runtime = build_runtime(MockDepsModule())

    app = FastAPI(lifespan=runtime_lifespan(runtime))

    if probes:
        router = APIRouter(prefix=prefix)
        attach_liveness_route(router)
        app.include_router(router)

    if notes:
        spec = DocumentSpec(
            name="notes",
            read=_NoteRead,
            write=DocumentWriteTypes(domain=_Note, create_cmd=_NoteCreate),
        )
        registry = build_document_registry(
            spec, DocumentDTOs(read=_NoteRead, create=_NoteCreate)
        ).freeze()
        notes_router = APIRouter(prefix=f"{prefix}/notes")
        attach_document_routes(
            notes_router,
            registry=registry,
            ns=spec.default_namespace,
            ctx_dep=runtime.get_context,
            style="rest",
        )
        app.include_router(notes_router)

    # An unrelated middleware in the stack: the gates are found by the field they
    # declare, so one that declares nothing must simply be skipped.
    app.add_middleware(CustomHeadersMiddleware, static_headers={"X-App": "test"})
    app.add_middleware(
        InvocationMetadataMiddleware,
        ctx_dep=runtime.get_context,
        bypass_paths=frozenset(bypass),
    )
    app.add_middleware(
        SecurityContextMiddleware,
        ctx_dep=runtime.get_context,
        authn=AuthnRequirement(
            ingress=(HeaderTokenAuthn(authn_spec=_TOKEN_SPEC, header_name="Authorization"),)
        ),
        when_multiple_credentials="first_in_order",
        bypass_paths=frozenset(bypass if security_bypass is None else security_bypass),
    )

    return app


def _refused(app: FastAPI) -> str:
    with pytest.raises(CoreException) as error:
        check_bypass_paths(app)

    return str(error.value)


class TestCheckBypassPaths:
    def test_a_matching_bypass_passes(self) -> None:
        # DEFAULT_HEALTH_PATHS is deliberately a superset — ten paths, of which this
        # app serves one. A superset is fine; a set matching nothing is not.
        check_bypass_paths(_app(bypass=DEFAULT_HEALTH_PATHS))

    def test_no_bypass_is_a_no_op(self) -> None:
        check_bypass_paths(_app(bypass=frozenset()))

    def test_a_generated_operation_route_may_not_be_bypassed(self) -> None:
        # The security case: those routes read and write tenant data through the
        # registry, and the middleware they would skip is what binds the tenant.
        message = _refused(_app(bypass={"/notes"}))

        assert "/notes" in message
        assert "unauthenticated" in message

    def test_the_gates_must_agree(self) -> None:
        # Both middlewares resolve the context, so a path only one of them skips is
        # still resolved by the other — the bypass reads as configured and does nothing.
        message = _refused(_app(bypass={"/livez"}, security_bypass=set()))

        assert "/livez" in message
        assert "SecurityContextMiddleware" in message

    def test_a_set_matching_no_route_is_refused(self) -> None:
        # Finding this at boot is the whole point: the middlewares run before routing,
        # so a router mounted under a prefix makes every route-local path inert and
        # the probe goes on failing exactly as it did.
        message = _refused(_app(bypass=DEFAULT_HEALTH_PATHS, prefix="/api"))

        assert "not one of those paths is routed" in message
        assert "/api/livez" in message or "full mounted path" in message

    def test_the_mounted_path_passes(self) -> None:
        check_bypass_paths(_app(bypass={"/api/livez"}, prefix="/api"))

    def test_the_check_runs_at_boot(self) -> None:
        # Wired into runtime_lifespan, so a misconfigured app fails to start rather
        # than serving 500s at its probe path.
        from fastapi.testclient import TestClient

        with pytest.raises(CoreException), TestClient(_app(bypass={"/notes"})):
            pass  # pragma: no cover - startup is what raises

    def test_a_mounted_sub_application_is_out_of_reach(self) -> None:
        # Documented limitation, pinned: routes inside a mounted app are invisible to a
        # check that runs on the outer one, so bypassing them there reads as inert.
        sub = FastAPI()
        sub_router = APIRouter()
        attach_liveness_route(sub_router)
        sub.include_router(sub_router)

        app = _app(bypass={"/sub/livez"}, notes=False)
        app.mount("/sub", sub)

        assert "not one of those paths is routed" in _refused(app)
        assert "mounted sub-application" in _refused(app)

    def test_a_nested_router_resolves_to_its_effective_path(self) -> None:
        # Two levels of include: the route's own `path` keeps only the innermost
        # prefix chain, so reading it would see `/v1/livez` and miss `/api`. The
        # bypass would then read as inert and fail a correct boot.
        inner = APIRouter(prefix="/v1")
        attach_liveness_route(inner)
        outer = APIRouter(prefix="/api")
        outer.include_router(inner)

        app = _app(bypass={"/api/v1/livez"}, notes=False, probes=False)
        app.include_router(outer)

        check_bypass_paths(app)

    def test_a_nested_generated_route_is_still_caught(self) -> None:
        # The same resolution, on the check that matters: a governed operation route
        # under two prefixes must still be refused, not missed.
        runtime = build_runtime(MockDepsModule())
        spec = DocumentSpec(
            name="notes",
            read=_NoteRead,
            write=DocumentWriteTypes(domain=_Note, create_cmd=_NoteCreate),
        )
        registry = build_document_registry(
            spec, DocumentDTOs(read=_NoteRead, create=_NoteCreate)
        ).freeze()

        inner = APIRouter(prefix="/notes")
        attach_document_routes(
            inner,
            registry=registry,
            ns=spec.default_namespace,
            ctx_dep=runtime.get_context,
            style="rest",
        )
        outer = APIRouter(prefix="/api")
        outer.include_router(inner)

        app = _app(bypass={"/api/notes"}, notes=False, probes=False)
        app.include_router(outer)

        message = _refused(app)

        assert "/api/notes" in message
        assert "unauthenticated" in message
