"""Tenancy through the bridge: a tool call is scoped to the tenant bound on the context.

The bridge holds no tenancy logic of its own — it dispatches through ``run_operation``,
whose tenancy every other caller already depends on. What is worth proving here is that
the bridge does not *bypass* it: a tool call under one tenant must not see another
tenant's rows, and the dispatch path adds no place for that to go wrong.
"""

from uuid import uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionRuntime, InvocationMetadata
from forze.application.execution.context import ExecutionContext
from forze.application.execution.deps import DepsRegistry
from forze.base.primitives import StrKeyNamespace
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_kits.aggregates.document import build_document_registry
from forze_kits.integrations.agent_tools import (
    ToolUse,
    dispatch_tool_use,
    operation_tools,
)
from forze_mock import MockDepsModule, MockRouteConfig

pytestmark = pytest.mark.unit

# ----------------------- #

_NS = StrKeyNamespace(prefix="notes")

TENANT_A = TenantIdentity(tenant_id=uuid4())
TENANT_B = TenantIdentity(tenant_id=uuid4())


class _Note(Document):
    title: str


class _NoteRead(ReadDocument):
    title: str


class _CreateNote(CreateDocumentCmd):
    title: str


def _spec() -> DocumentSpec:
    return DocumentSpec(
        name="notes",
        read=_NoteRead,
        write={"domain": _Note, "create_cmd": _CreateNote},
    )


def _registry():
    return build_document_registry(_spec(), ns=_NS).freeze()


def _bound(ctx: ExecutionContext, tenant: TenantIdentity):
    return ctx.inv_ctx.bind(
        metadata=InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4()),
        authn=AuthnIdentity(principal_id=uuid4()),
        tenant=tenant,
    )


# ....................... #


class TestATenantAwareToolCall:
    async def test_a_tool_call_cannot_read_another_tenants_rows(self) -> None:
        # Battery 6: written under A, listed under B, and the palette is identical for both
        # — the isolation is the operation's, and the bridge keeps it.
        registry = _registry()
        module = MockDepsModule(routes={"notes": MockRouteConfig(tenant_aware=True)})
        runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())

        create = operation_tools(
            registry, include=[_NS.key("create")], read_only=False
        )
        listing = operation_tools(registry, include=[_NS.key("list")])

        async with runtime.scope():
            ctx = runtime.get_context()

            with _bound(ctx, TENANT_A):
                written = await dispatch_tool_use(
                    ToolUse(id="w", name=str(_NS.key("create")), input={"title": "a-only"}),
                    ctx=ctx,
                    tools=create,
                )

            assert written.is_error is False, written.content

            with _bound(ctx, TENANT_A):
                own = await dispatch_tool_use(
                    ToolUse(id="r1", name=str(_NS.key("list")), input={}),
                    ctx=ctx,
                    tools=listing,
                )

            with _bound(ctx, TENANT_B):
                other = await dispatch_tool_use(
                    ToolUse(id="r2", name=str(_NS.key("list")), input={}),
                    ctx=ctx,
                    tools=listing,
                )

        assert own.is_error is False, own.content
        assert other.is_error is False, other.content
        assert "a-only" in str(own.content)
        assert "a-only" not in str(other.content)

    async def test_an_unbound_tenant_fails_closed(self) -> None:
        # The mock partitions storage and refuses when tenant_aware without a bound tenant;
        # the bridge surfaces that as a governed error rather than an empty answer.
        registry = _registry()
        module = MockDepsModule(routes={"notes": MockRouteConfig(tenant_aware=True)})
        runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())

        listing = operation_tools(registry, include=[_NS.key("list")])

        async with runtime.scope():
            ctx = runtime.get_context()

            result = await dispatch_tool_use(
                ToolUse(id="r", name=str(_NS.key("list")), input={}),
                ctx=ctx,
                tools=listing,
            )

        assert result.is_error is True
        assert "a-only" not in str(result.content)
