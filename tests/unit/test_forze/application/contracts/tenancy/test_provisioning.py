"""Unit tests for tenant infrastructure provisioning."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.tenancy import (
    CompositeTenantProvisioner,
    FunctionTenantProvisioner,
    NoopTenantProvisioner,
    TenantIdentity,
    TenantProvisionerPort,
)

# ----------------------- #


def _identity() -> TenantIdentity:
    return TenantIdentity(tenant_id=uuid4(), tenant_key="acme")


@pytest.mark.asyncio
async def test_noop_provisioner_does_nothing() -> None:
    p = NoopTenantProvisioner()
    assert isinstance(p, TenantProvisionerPort)
    await p.provision(_identity())
    await p.deprovision(_identity())


@pytest.mark.asyncio
async def test_function_provisioner_runs_callables() -> None:
    seen: list[str] = []

    p = FunctionTenantProvisioner(
        on_provision=lambda t: _record(seen, f"prov:{t.tenant_key}"),
        on_deprovision=lambda t: _record(seen, f"deprov:{t.tenant_key}"),
    )

    ident = _identity()
    await p.provision(ident)
    await p.deprovision(ident)

    assert seen == ["prov:acme", "deprov:acme"]


@pytest.mark.asyncio
async def test_function_provisioner_deprovision_defaults_to_noop() -> None:
    seen: list[str] = []
    p = FunctionTenantProvisioner(on_provision=lambda t: _record(seen, "prov"))

    await p.deprovision(_identity())  # no on_deprovision → no-op, no error
    assert seen == []


@pytest.mark.asyncio
async def test_composite_runs_in_order_and_tears_down_in_reverse() -> None:
    order: list[str] = []

    def step(name: str) -> FunctionTenantProvisioner:
        return FunctionTenantProvisioner(
            on_provision=lambda _t: _record(order, f"+{name}"),
            on_deprovision=lambda _t: _record(order, f"-{name}"),
        )

    composite = CompositeTenantProvisioner(provisioners=[step("a"), step("b"), step("c")])
    ident = _identity()

    await composite.provision(ident)
    await composite.deprovision(ident)

    assert order == ["+a", "+b", "+c", "-c", "-b", "-a"]


# ....................... #


async def _record(sink: list[str], value: str) -> None:
    sink.append(value)
