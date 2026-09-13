"""Shared scenario: a tool call is scoped to the tenant bound on its context.

The agent-tool bridge holds no tenancy logic — it dispatches through ``run_operation``,
whose tenancy every other caller already depends on. So the claim worth proving is not
that the bridge *implements* isolation but that it does not *bypass* it, and that claim is
only interesting if it holds on a backend that enforces tenancy with a real predicate as
well as on the in-memory one that partitions a dict.

Two tests that happen to agree would not establish that. One body driven twice does, which
is why these checks live here rather than in either leg: a divergence between the engines
becomes a failure instead of a slow drift nobody re-reads.

Seeding is the harness's job. The engines create their storage in incompatible ways — the
mock partitions a namespace, Postgres wants a table with a real ``tenant_id`` column — and
that difference is architectural rather than something a shared seam could hide. What the
harness hands over is a context, a frozen registry over the same document spec, and two
tenants to be isolated from each other.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import uuid4

import attrs

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import InvocationMetadata
from forze.application.execution.context import ExecutionContext
from forze.application.execution.operations.registry import FrozenOperationRegistry
from forze.base.primitives import StrKeyNamespace
from forze_kits.integrations.agent_tools import (
    ToolResult,
    ToolUse,
    dispatch_tool_use,
    operation_tools,
)

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AgentToolsTenancyHarness:
    """One backend's seam for the tenancy scenario."""

    ctx: ExecutionContext
    """A context whose document deps are wired tenant-aware."""

    registry: FrozenOperationRegistry
    """A frozen registry built over the document spec the harness provisioned."""

    ns: StrKeyNamespace
    """The namespace that registry's operation keys were built under."""

    backend: str
    """Label used in assertion messages, so a failure names the leg that disagreed."""

    tenant_a: TenantIdentity = attrs.field(factory=lambda: TenantIdentity(tenant_id=uuid4()))
    """The tenant that writes."""

    tenant_b: TenantIdentity = attrs.field(factory=lambda: TenantIdentity(tenant_id=uuid4()))
    """The tenant that must not read what the first one wrote."""

    # ....................... #

    def bound(self, tenant: TenantIdentity | None):
        """Bind an invocation as a principal of *tenant* (or of no tenant at all)."""

        return self.ctx.inv_ctx.bind(
            metadata=InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4()),
            authn=AuthnIdentity(principal_id=uuid4()),
            tenant=tenant,
        )

    # ....................... #

    async def create(self, tenant: TenantIdentity, title: str) -> ToolResult:
        """Dispatch a create tool call as *tenant*."""

        tools = operation_tools(self.registry, include=[self.ns.key("create")], read_only=False)

        with self.bound(tenant):
            return await dispatch_tool_use(
                ToolUse(id=f"w-{title}", name=str(self.ns.key("create")), input={"title": title}),
                ctx=self.ctx,
                tools=tools,
            )

    # ....................... #

    async def listing(self, tenant: TenantIdentity | None) -> ToolResult:
        """Dispatch a list tool call as *tenant*."""

        tools = operation_tools(self.registry, include=[self.ns.key("list")])

        with self.bound(tenant):
            return await dispatch_tool_use(
                ToolUse(id="r", name=str(self.ns.key("list")), input={}),
                ctx=self.ctx,
                tools=tools,
            )


Check = Callable[[AgentToolsTenancyHarness], Any]
"""One scenario check. Async, but typed loosely so the tuple stays homogeneous."""


# ....................... #


async def check_a_tenants_own_write_is_readable_through_a_tool_call(
    h: AgentToolsTenancyHarness,
) -> None:
    """Without this, the isolation check below could pass on a write that never landed."""

    title = f"own-{uuid4().hex[:8]}"
    written = await h.create(h.tenant_a, title)

    assert written.is_error is False, (h.backend, written.content)

    own = await h.listing(h.tenant_a)

    assert own.is_error is False, (h.backend, own.content)
    assert title in str(own.content), f"{h.backend}: a tenant cannot read its own write"


# ....................... #


async def check_a_tool_call_cannot_read_another_tenants_rows(
    h: AgentToolsTenancyHarness,
) -> None:
    """The claim: the palette is identical for both tenants, the visible rows are not."""

    title = f"a-only-{uuid4().hex[:8]}"
    written = await h.create(h.tenant_a, title)

    assert written.is_error is False, (h.backend, written.content)

    other = await h.listing(h.tenant_b)

    assert other.is_error is False, (h.backend, other.content)
    assert title not in str(other.content), f"{h.backend}: cross-tenant read through a tool"


# ....................... #


async def check_an_unbound_tenant_fails_closed(h: AgentToolsTenancyHarness) -> None:
    """A tenant-aware read with no tenant bound refuses; it does not answer emptily.

    An empty answer is the dangerous outcome here: an agent told "no rows" will report that
    as fact, where a refusal makes the missing binding the operator's problem instead.
    """

    result = await h.listing(None)

    assert result.is_error is True, f"{h.backend}: an unbound tenant answered instead of refusing"
    assert isinstance(result.content, dict)


# ....................... #


AGENT_TOOLS_TENANCY_BATTERY: tuple[Check, ...] = (
    check_a_tenants_own_write_is_readable_through_a_tool_call,
    check_a_tool_call_cannot_read_another_tenants_rows,
    check_an_unbound_tenant_fails_closed,
)
"""The scenario, in the order a reader should meet it: the write lands, it stays put, and
an unbound read refuses.

Both legs drive this by ``parametrize``, which is silent about an empty argument list — a
battery emptied by a bad edit would collect zero tests and report green on both engines.
:func:`battery_is_populated` is the guard against that, asserted by each leg."""


# ....................... #


def battery_is_populated() -> None:
    """Refuse a battery that has lost its checks.

    Named rather than inlined so each leg asserts the same floor, and stated as a count
    plus the names: a reordering is free, a deletion is not.
    """

    names = {check.__name__ for check in AGENT_TOOLS_TENANCY_BATTERY}

    if names != {
        "check_a_tenants_own_write_is_readable_through_a_tool_call",
        "check_a_tool_call_cannot_read_another_tenants_rows",
        "check_an_unbound_tenant_fails_closed",
    }:
        raise AssertionError(f"the tenancy battery changed shape: {sorted(names)}")
