"""Two processes, one store: a revocation in one closes the permission in the other.

A two-node :class:`~forze_dst.Cluster` over one ``MockState``, each node with its own kernel,
grants cache and subscription, as two processes would have. Node 0 revokes a member's role
once the member has cached it; node 1 runs the member's writes, two at a time, before and after.
Each node records its calls as ``operation`` events in execution order, so the single-process
invariant ``no_permission_after_deactivation`` judges the cluster: no write that began after
the revoke returned may be allowed. With the broadcast dropped it must fire, which also proves
node 1 serves cached grants after the revoke at all.
"""

from __future__ import annotations

import asyncio
import itertools
from collections.abc import Awaitable, Callable
from datetime import timedelta
from uuid import UUID

import attrs
import pytest

from forze.application.contracts.authz import AuthzRequest, AuthzSpec, AuthzSubject
from forze.application.contracts.cache import CacheSpec
from forze.application.execution import ExecutionContext
from forze_dst import Cluster, SimulationConfig
from forze_dst.config import ClusterConfig
from forze_dst.invariants import expect, named, no_permission_after_deactivation
from forze_dst.markers import record_event
from forze_identity.authz import (
    AuthzKernelConfig,
    ConfigurableAuthzDecision,
    ConfigurablePrincipalRegistry,
    ConfigurableRoleAssignment,
    GrantsCache,
    GrantsInvalidation,
    build_authz_shared_services,
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
from forze_mock import MockDepsModule, MockRouteConfig
from forze_mock.state import MockState

pytestmark = pytest.mark.unit

# ----------------------- #

MEMBER = UUID(int=1)
WRITE = "ledger.write"
SPEC = AuthzSpec(name="main")
MARKERS = CacheSpec(name="grants_markers")


@attrs.define(slots=True, kw_only=True)
class _Run:
    """What one run's nodes share besides the store: an order for their calls, and a kernel each."""

    dropping: bool
    state: MockState = attrs.field(factory=MockState)
    order: itertools.count[int] = attrs.field(factory=itertools.count)
    kernels: dict[int, AuthzKernelConfig] = attrs.field(factory=dict)

    def kernel(self, node_id: int) -> AuthzKernelConfig:
        return self.kernels.setdefault(
            node_id,
            AuthzKernelConfig(
                grants_cache=GrantsCache(ttl=timedelta(hours=1)),
                grants_invalidation=GrantsInvalidation(cache=MARKERS),
            ),
        )

    async def call(self, op: str, run: Callable[[], Awaitable[bool]]) -> None:
        start = next(self.order)
        allowed = await run()
        record_event(
            "operation",
            op=op,
            outcome="ok" if allowed else "denied",
            start_seq=start,
            end_seq=next(self.order),
        )


def _cluster(*, dropping: bool) -> Cluster:
    runs: list[_Run] = []

    def fresh() -> MockState:
        runs.append(_Run(dropping=dropping))
        return runs[-1].state

    def deps(state: MockState) -> MockDepsModule:
        return MockDepsModule(
            state=state, routes={MARKERS.name: MockRouteConfig(invalidation_push=True)}
        )

    async def seed(ctx: ExecutionContext) -> None:
        cmd = ctx.doc.command
        permission = await cmd(permission_definition_spec).create(
            CreatePermissionDefinitionCmd(permission_key=WRITE)
        )
        role = await cmd(role_definition_spec).create(CreateRoleDefinitionCmd(role_key="writer"))
        await cmd(role_permission_binding_spec).create(
            CreateRolePermissionBindingCmd(role_id=role.id, permission_id=permission.id)
        )
        await ConfigurablePrincipalRegistry()(ctx, SPEC).ensure_principal(MEMBER, kind="user")
        shared = build_authz_shared_services(AuthzKernelConfig())
        await ConfigurableRoleAssignment(shared=shared)(ctx, SPEC).assign_role(MEMBER, "writer")
        runs[-1].state.cache_push.dropping = runs[-1].dropping

    async def node(node_id: int, ctx: ExecutionContext) -> None:
        run = runs[-1]
        kernel = run.kernel(node_id)
        shared = build_authz_shared_services(kernel)
        step = grants_invalidation_lifecycle_step(kernel)
        await step.startup(ctx)

        async def revoke() -> bool:
            await ConfigurableRoleAssignment(shared=shared)(ctx, SPEC).revoke_role(MEMBER, "writer")
            return True

        async def write() -> bool:
            decision = await ConfigurableAuthzDecision(shared=shared)(ctx, SPEC).authorize(
                AuthzRequest(subject=AuthzSubject(principal_id=MEMBER), action=WRITE)
            )
            return decision.allowed

        async def writes() -> None:
            for _ in range(6):
                await run.call("write", write)
                await asyncio.sleep(0.1)

        try:
            if node_id == 0:
                await asyncio.sleep(0.25)  # node 1 has cached the grant by now
                await run.call("revoke", revoke)

            else:
                await asyncio.gather(writes(), writes())

        finally:
            await step.shutdown(ctx)

    return Cluster(
        deps=deps,
        state_factory=fresh,
        setup=seed,
        node=node,
        invariants=[
            no_permission_after_deactivation("revoke", ["write"]),
            # A node that stopped on an error would leave the first invariant nothing to judge.
            named(
                "no_node_error",
                expect("node_error", lambda _event: False, message="a node stopped on an error"),
            ),
        ],
    )


def _config() -> SimulationConfig:
    return SimulationConfig(seeds=range(8), cluster=ClusterConfig(nodes=2))


class TestAcrossProcesses:
    def test_a_revocation_in_one_closes_the_permission_in_the_other(self) -> None:
        assert _cluster(dropping=False).run(_config()) is None

    def test_a_lost_broadcast_is_caught(self) -> None:
        report = _cluster(dropping=True).run(_config())

        assert report is not None
        assert "no_permission_after_deactivation" in str(report)
        assert {node for _kind, node in report.workload} == {0, 1}
