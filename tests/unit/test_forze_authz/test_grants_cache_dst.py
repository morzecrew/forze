"""With the grants cache on, a revocation or a deactivation still closes the permission at once.

One seeded workload: a member holding a role writes to a ledger the role permits, and a role
revocation, or a deactivation, lands somewhere in the run. With the cache forgetting on the
revocation the invariant holds; with a cache that never forgets it must fire, which also proves
the workload reaches writes after the revocation at all.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import Any
from uuid import UUID

import attrs
import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz import AuthzSpec
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.application.hooks.authz import authorize_action
from forze_dst import ModelState, Rule, Scenario, Simulation, SimulationConfig, Strategy
from forze_dst.invariants import no_permission_after_deactivation
from forze_identity.authz import AuthzKernelConfig, GrantsCache
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
from forze_mock import MockDepsModule

pytestmark = pytest.mark.unit

# ----------------------- #

MEMBER = UUID(int=1)
WRITE = "ledger.write"
REAL = AuthzSpec(name="real")
"""Routed apart from the mock's own ``main`` authz ports."""


class _NeverForgets(GrantsCache):
    def forget(self, principal_id: UUID) -> None:
        return None


@attrs.define(slots=True, kw_only=True)
class _Seed(Handler[None, None]):
    ctx: ExecutionContext

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
        await self.ctx.authz.role_assignment(REAL).assign_role(MEMBER, "writer")


@attrs.define(slots=True, kw_only=True)
class _Write(Handler[None, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        with self.ctx.inv_ctx.bind_identity(authn=AuthnIdentity(principal_id=MEMBER)):
            await authorize_action(
                self.ctx, self.ctx.authz.decision(REAL), WRITE, delegation_port=None
            )


@attrs.define(slots=True, kw_only=True)
class _Revoke(Handler[None, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        await self.ctx.authz.role_assignment(REAL).revoke_role(MEMBER, "writer")


@attrs.define(slots=True, kw_only=True)
class _Deactivate(Handler[None, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        await self.ctx.authz.principal_registry(REAL).deactivate_principal(MEMBER)


def _registry() -> FrozenOperationRegistry:
    def described() -> OperationDescriptor:
        return OperationDescriptor(input_type=None, output_type=None, description="x")

    handlers: dict[str, Callable[[ExecutionContext], Any]] = {
        "seed": lambda ctx: _Seed(ctx=ctx),
        "write": lambda ctx: _Write(ctx=ctx),
        "revoke": lambda ctx: _Revoke(ctx=ctx),
        "deactivate": lambda ctx: _Deactivate(ctx=ctx),
    }

    return OperationRegistry(
        handlers=handlers, descriptors={op: described() for op in handlers}
    ).freeze()


def _run(closing: str, cache: Callable[[], GrantsCache], *, concurrency: int) -> Any:
    def deps() -> list[Any]:
        return [
            MockDepsModule(),
            AuthzDepsModule(
                kernel=AuthzKernelConfig(grants_cache=cache()),
                principal_registry={"real"},
                role_assignment={"real"},
                decision={"real"},
            ),
        ]

    scenario = Scenario(
        state=ModelState,
        arrange=(Rule(op="seed"),),
        act=(Rule(op="write", weight=4.0), Rule(op=closing)),
    )

    return Simulation(
        operations=_registry(),
        deps=deps,
        invariants=[no_permission_after_deactivation(closing, ["write"])],
    ).run(
        SimulationConfig(
            strategy=Strategy.SCENARIO, act_count=12, concurrency=concurrency, seeds=range(5)
        ),
        scenario=scenario,
    )


def _cache() -> GrantsCache:
    return GrantsCache(ttl=timedelta(hours=1))


def _never_forgets() -> GrantsCache:
    return _NeverForgets(ttl=timedelta(hours=1))


@pytest.mark.parametrize("concurrency", [1, 3])
class TestWithTheCacheOn:
    def test_a_revocation_closes_the_permission(self, concurrency: int) -> None:
        assert _run("revoke", _cache, concurrency=concurrency) is None

    def test_a_deactivation_closes_the_permission(self, concurrency: int) -> None:
        assert _run("deactivate", _cache, concurrency=concurrency) is None

    def test_a_cache_that_never_forgets_is_caught(self, concurrency: int) -> None:
        report = _run("revoke", _never_forgets, concurrency=concurrency)

        assert report is not None
        assert "no_permission_after_deactivation" in str(report)
