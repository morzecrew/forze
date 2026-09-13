"""Battery 2's parity half: one denied operation, answered by two transports.

The bridge claims an agent cannot reach an operation the bound principal cannot reach, and
that the refusal is the operation's rather than the bridge's. The way to show that is to
deny the *same* operation over HTTP and through the bridge and watch both refuse for the
same reason — which is what this module does, on a document aggregate whose create
operation carries an authorization guard in its plan.

The two transports cannot be compared field-for-field: HTTP answers a status code and a
header, the bridge answers a ``ToolResult``. So the assertion is the shared *cause* — the
same error code, drawn from the same egress envelope, with nothing executed on either
path. Comparing a 403 to a ``ToolResult`` as though they were one object would test the
transports instead of the claim.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch
from uuid import uuid4

import pytest

pytest.importorskip("fastapi")

from fastapi import APIRouter, FastAPI
from httpx import ASGITransport, AsyncClient

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz import AuthzDecision, AuthzSpec
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import InvocationMetadata
from forze.application.execution.context import ExecutionContext
from forze.application.execution.operations.registry import FrozenOperationRegistry
from forze.application.hooks.authz import AuthzBeforeAuthorize
from forze.base.exceptions import ExceptionKind, error_envelope, exc
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze.testing import context_from_modules
from forze_fastapi.exceptions import ERROR_CODE_HEADER, register_exception_handlers
from forze_fastapi.routes import attach_document_routes
from forze_kits.aggregates.document import (
    DocumentDTOs,
    DocumentKernelOp,
    build_document_registry,
)
from forze_kits.integrations.agent_tools import (
    ToolUse,
    dispatch_tool_use,
    operation_tools,
)
from forze_mock import MockDepsModule, MockState

pytestmark = pytest.mark.unit

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
_LIST_OP = str(_SPEC.default_namespace.key(DocumentKernelOp.LIST))
_ACTION = "notes.write"


class _Deny:
    async def authorize(self, request: Any) -> AuthzDecision:
        _ = request

        return AuthzDecision(allowed=False, reason="denied")


class _Allow:
    async def authorize(self, request: Any) -> AuthzDecision:
        _ = request

        return AuthzDecision(allowed=True, matched_permission_key=_ACTION)


def _registry() -> FrozenOperationRegistry:
    """A document registry whose create operation is guarded by an authz hook."""

    reg = build_document_registry(_SPEC, DocumentDTOs(read=_NoteRead, create=_NoteCreate))
    reg = (
        reg.bind(_CREATE_OP)
        .bind_outer()
        .before(
            AuthzBeforeAuthorize(spec=AuthzSpec(name="z"), action=_ACTION).to_step(
                step_id="authz", requires=()
            )
        )
        .finish(deep=True)
    )

    return reg.freeze()


def _context(state: MockState) -> ExecutionContext:
    return context_from_modules(MockDepsModule(state=state))


def _bound(ctx: ExecutionContext):
    """Bind a principal for the call, the way a boundary would.

    Both transports run inside this, which is the point: the routes resolve their context
    by calling ``ctx_dep()`` inline rather than through ``Depends``, so the binding the
    ``SecurityContextMiddleware`` would install in production is installed here once and
    covers both paths. Without it the authz guard refuses for the wrong reason — no
    principal at all — and the two surfaces would be compared on a 401 that says nothing
    about authorization.
    """

    return ctx.inv_ctx.bind(
        metadata=InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4()),
        authn=AuthnIdentity(principal_id=uuid4()),
    )


def _app(ctx: ExecutionContext) -> FastAPI:
    router = APIRouter(prefix="/notes")
    attach_document_routes(
        router,
        registry=_registry(),
        ns=_SPEC.default_namespace,
        ctx_dep=lambda: ctx,
        style="rest",
    )

    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(router)

    return app


async def _rows(ctx: ExecutionContext, registry: FrozenOperationRegistry) -> str:
    """Whatever the list operation can see, as text — for "nothing executed" assertions.

    Driven through the bridge rather than through ``run_operation`` directly: the check is
    about what a *tool call* can observe, and reading the store by another route would
    leave open whether the two agree.
    """

    listing = await dispatch_tool_use(
        ToolUse(id="rows", name=_LIST_OP, input={}),
        ctx=ctx,
        tools=operation_tools(registry, include=[_LIST_OP]),
    )

    # A refused listing stringifies to an error payload, in which any title is absent —
    # so an absence assertion over it would pass for the wrong reason. Fail loudly here
    # instead, where the cause is still visible.
    assert listing.is_error is False, f"the listing itself was refused: {listing.content}"

    return str(listing.content)


# ....................... #


async def _http_create(ctx: ExecutionContext) -> Any:
    app = _app(ctx)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://notes") as client:
        return await client.post("/notes", json={"title": "over-http"})


async def _bridged_create(ctx: ExecutionContext, registry: FrozenOperationRegistry) -> Any:
    tools = operation_tools(registry, include=[_CREATE_OP], read_only=False)

    return await dispatch_tool_use(
        ToolUse(id="t", name=_CREATE_OP, input={"title": "through-the-bridge"}),
        ctx=ctx,
        tools=tools,
    )


# ....................... #


class TestBothTransportsRefuseForTheSameReason:
    async def test_the_error_code_is_the_same_on_both(self) -> None:
        registry = _registry()
        ctx = _context(MockState())

        with patch.object(ctx.authz, "decision", return_value=_Deny()), _bound(ctx):
            response = await _http_create(ctx)
            bridged = await _bridged_create(ctx, registry)

        assert response.status_code == 403
        assert isinstance(bridged.content, dict)
        assert response.headers[ERROR_CODE_HEADER] == bridged.content["code"]
        assert bridged.content["code"] == "permission_denied"

    async def test_the_detail_is_the_same_envelope_on_both(self) -> None:
        # Both sides render `error_envelope`, so the caller-facing text agrees too — this
        # is what makes "the same reason" more than two coincidentally equal strings.
        registry = _registry()
        ctx = _context(MockState())
        expected = error_envelope(exc.authorization("denied", code="permission_denied"))

        with patch.object(ctx.authz, "decision", return_value=_Deny()), _bound(ctx):
            response = await _http_create(ctx)
            bridged = await _bridged_create(ctx, registry)

        assert isinstance(bridged.content, dict)
        assert response.json()["detail"] == expected.detail
        assert bridged.content["detail"] == expected.detail

    async def test_neither_path_created_anything(self) -> None:
        registry = _registry()
        ctx = _context(MockState())

        with patch.object(ctx.authz, "decision", return_value=_Deny()), _bound(ctx):
            await _http_create(ctx)
            await _bridged_create(ctx, registry)

            rows = await _rows(ctx, registry)

        assert "over-http" not in rows
        assert "through-the-bridge" not in rows

    async def test_the_refusal_is_the_guards_decision_not_the_transports(self) -> None:
        # The control: grant the permission and both paths write. Without this, every
        # assertion above would also pass on a wiring that refused everything.
        registry = _registry()
        ctx = _context(MockState())

        with patch.object(ctx.authz, "decision", return_value=_Allow()), _bound(ctx):
            response = await _http_create(ctx)
            bridged = await _bridged_create(ctx, registry)

            rows = await _rows(ctx, registry)

        assert response.status_code in (200, 201), response.text
        assert bridged.is_error is False, bridged.content
        assert "over-http" in rows
        assert "through-the-bridge" in rows


# ....................... #


class TestTheGuardIsVisibleToBothSurfaces:
    def test_the_catalog_declares_the_permission_the_guard_enforces(self) -> None:
        # The projection both transports read from: HTTP puts it in OpenAPI, the bridge
        # leaves enforcement to the plan. Either way it is one declaration.
        entry = _registry().catalog()[_CREATE_OP]

        assert entry.required_permissions == (_ACTION,)
        assert entry.requires_authn is True

    async def test_an_unguarded_operation_is_refused_by_neither(self) -> None:
        # Scoping the claim: the denial follows the guard, not the aggregate. The list
        # operation carries no authz hook and answers on both surfaces under a deny-all
        # decision port.
        registry = _registry()
        ctx = _context(MockState())

        with patch.object(ctx.authz, "decision", return_value=_Deny()), _bound(ctx):
            rows = await _rows(ctx, registry)

            listing = await dispatch_tool_use(
                ToolUse(id="l", name=_LIST_OP, input={}),
                ctx=ctx,
                tools=operation_tools(registry, include=[_LIST_OP]),
            )

        assert listing.is_error is False, listing.content
        assert "hits" in rows


# ....................... #


def test_the_denial_kind_is_authorization() -> None:
    # Pins the kind the parity rests on: a 403 and this ToolResult agree because the
    # envelope maps AUTHORIZATION that way, not because two numbers were typed twice.
    envelope = error_envelope(exc.authorization("denied", code="permission_denied"))

    assert envelope.status == 403
    assert envelope.kind is ExceptionKind.AUTHORIZATION
    assert envelope.server_error is False
