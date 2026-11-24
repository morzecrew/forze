"""A role bound in one tenant never grants outside it, with the grants cache on (RFC 0082).

One seeded workload: a member holds a role in tenant T only, and writes to a ledger the role
permits, from inside T, from inside U and with no tenant bound, interleaved under the cache.
The invariant holds; seeding the role in every tenant instead must make it fire, which also
proves the workload reaches the writes outside T. A revoke inside T closes the permission
there, with and without the cross-process broadcast.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import Any
from uuid import UUID

import attrs
import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz import AuthzScope, AuthzSpec
from forze.application.contracts.cache import CacheSpec
from forze.application.contracts.execution import Handler
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.application.execution.lifecycle import LifecyclePlan
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.application.hooks.authz import authorize_action
from forze_dst import ModelState, Rule, Scenario, Simulation, SimulationConfig, Strategy
from forze_dst.invariants import (
    no_permission_after_deactivation,
    no_permission_outside_its_tenant,
    operation_succeeds,
)
from forze_identity.authz import (
    AuthzKernelConfig,
    GrantsCache,
    GrantsInvalidation,
    grants_invalidation_lifecycle_step,
)
from forze_identity.authz.application.specs import (
    permission_definition_spec,
    role_definition_spec,
    role_permission_binding_spec,
)
from forze_identity.authz.domain.models.bindings import CreateRolePermissionBindingCmd
from forze_identity.authz.domain.models.permission_definition import (
    CreatePermissionDefinitionCmd,
)
from forze_identity.authz.domain.models.role_definition import CreateRoleDefinitionCmd
from forze_identity.authz.execution import AuthzDepsModule
from forze_mock import MockDepsModule, MockRouteConfig

pytestmark = pytest.mark.unit

# ----------------------- #

MEMBER = UUID(int=1)
T, U = UUID(int=10), UUID(int=11)
WRITE = "ledger.write"
REAL = AuthzSpec(name="real")
"""Routed apart from the mock's own ``main`` authz ports."""
MARKERS = CacheSpec(name="grants_markers")


@attrs.define(slots=True, kw_only=True)
class _Seed(Handler[None, None]):
    ctx: ExecutionContext
    held_in: UUID | None

    async def __call__(self, _args: None) -> None:
        cmd = self.ctx.doc.command
        permission = await cmd(permission_definition_spec).create(
            CreatePermissionDefinitionCmd(permission_key=WRITE)
        )
        role = await cmd(role_definition_spec).create(CreateRoleDefinitionCmd(role_key="writer"))
        await cmd(role_permission_binding_spec).create(
            CreateRolePermissionBindingCmd(role_id=role.id, permission_id=permission.id)
        )
        await self.ctx.authz.principal_registry(REAL).ensure_principal(MEMBER, kind="user")
        # Nothing is bound here, so the scope picks the tenant the role is written in.
        scope = AuthzScope(tenant_id=self.held_in) if self.held_in is not None else None
        await self.ctx.authz.role_assignment(REAL).assign_role(MEMBER, "writer", scope=scope)


@attrs.define(slots=True, kw_only=True)
class _Write(Handler[None, None]):
    ctx: ExecutionContext
    tenant_id: UUID | None

    async def __call__(self, _args: None) -> None:
        tenant = TenantIdentity(tenant_id=self.tenant_id) if self.tenant_id is not None else None

        with self.ctx.inv_ctx.bind_identity(
            authn=AuthnIdentity(principal_id=MEMBER), tenant=tenant
        ):
            await authorize_action(
                self.ctx, self.ctx.authz.decision(REAL), WRITE, delegation_port=None
            )


@attrs.define(slots=True, kw_only=True)
class _RevokeInT(Handler[None, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        with self.ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=T)):
            await self.ctx.authz.role_assignment(REAL).revoke_role(MEMBER, "writer")


def _registry(held_in: UUID | None) -> FrozenOperationRegistry:
    def described() -> OperationDescriptor:
        return OperationDescriptor(input_type=None, output_type=None, description="x")

    handlers: dict[str, Callable[[ExecutionContext], Any]] = {
        "seed": lambda ctx: _Seed(ctx=ctx, held_in=held_in),
        "write_in_t": lambda ctx: _Write(ctx=ctx, tenant_id=T),
        "write_in_u": lambda ctx: _Write(ctx=ctx, tenant_id=U),
        "write_untenanted": lambda ctx: _Write(ctx=ctx, tenant_id=None),
        "revoke_in_t": lambda ctx: _RevokeInT(ctx=ctx),
    }

    return OperationRegistry(
        handlers=handlers, descriptors={op: described() for op in handlers}
    ).freeze()


class _NeverForgets(GrantsCache):
    def forget(self, principal_id: UUID) -> None:
        return None


def _run(
    held_in: UUID | None,
    *,
    concurrency: int,
    revoke: bool = False,
    broadcast: bool = False,
    cache: type[GrantsCache] = GrantsCache,
) -> Any:
    # One kernel for every run, as the lifecycle step needs it: each run's subscription starts
    # with the flush a connect sends, so no run is served another's entries.
    kernel = AuthzKernelConfig(
        grants_cache=cache(ttl=timedelta(hours=1)),
        grants_invalidation=GrantsInvalidation(cache=MARKERS) if broadcast else None,
    )

    def deps() -> list[Any]:
        return [
            MockDepsModule(routes={MARKERS.name: MockRouteConfig(invalidation_push=True)}),
            AuthzDepsModule(
                kernel=kernel,
                principal_registry={"real"},
                role_assignment={"real"},
                decision={"real"},
            ),
        ]

    outside = no_permission_outside_its_tenant(["write_in_u", "write_untenanted"])
    act = [Rule(op="write_in_t", weight=2.0), Rule(op="write_in_u"), Rule(op="write_untenanted")]

    if revoke:
        act.append(Rule(op="revoke_in_t"))
        invariants = [
            outside,
            no_permission_after_deactivation("revoke_in_t", ["write_in_t"]),
            # A revoke that failed would make the line above hold for nothing.
            operation_succeeds("revoke_in_t"),
        ]

    else:
        # Inside T the role grants: the writes outside it fail on the tenant alone.
        invariants = [outside, operation_succeeds("write_in_t")]

    steps = (grants_invalidation_lifecycle_step(kernel),) if broadcast else ()

    return Simulation(
        operations=_registry(held_in),
        deps=deps,
        invariants=invariants,
        lifecycle=LifecyclePlan.from_steps(*steps).freeze(),
    ).run(
        SimulationConfig(
            strategy=Strategy.SCENARIO,
            act_count=12,
            concurrency=concurrency,
            seeds=range(5),
            runtime=broadcast,
        ),
        scenario=Scenario(state=ModelState, arrange=(Rule(op="seed"),), act=tuple(act)),
    )


@pytest.mark.parametrize("concurrency", [1, 3])
class TestARoleBoundInATenant:
    def test_never_grants_in_another_tenant_or_none(self, concurrency: int) -> None:
        assert _run(T, concurrency=concurrency) is None

    def test_a_role_held_everywhere_is_caught(self, concurrency: int) -> None:
        report = _run(None, concurrency=concurrency)

        assert report is not None
        assert "no_permission_outside_its_tenant" in str(report)

    @pytest.mark.parametrize("broadcast", [False, True])
    def test_a_revoke_in_the_tenant_closes_it_there(
        self, concurrency: int, broadcast: bool
    ) -> None:
        assert _run(T, concurrency=concurrency, revoke=True, broadcast=broadcast) is None

    def test_a_cache_that_never_forgets_the_revoke_is_caught(self, concurrency: int) -> None:
        report = _run(T, concurrency=concurrency, revoke=True, cache=_NeverForgets)

        assert report is not None
        assert "no_permission_after_deactivation" in str(report)
