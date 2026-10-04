"""The opt-in grants cache: what it remembers, for whom, and when it forgets.

A principal's catalog grants — what role, group and permission bindings give it — are remembered
per principal, tenant and scope for the cache's TTL. Its active flag and provider-derived
permissions are read on every decision. A role assignment forgets the principal when its write
commits, and an entry read before a forget is never stored after it.
"""

from __future__ import annotations

import asyncio
import contextvars
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import attrs
import pytest

from forze.application.contracts.authz import (
    AuthzRequest,
    AuthzScope,
    AuthzSpec,
    AuthzSubject,
    DerivedPermissions,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException, ExceptionKind
from forze.testing import context_from_modules
from forze_identity.authz import (
    AuthzKernelConfig,
    ConfigurableAuthzDecision,
    ConfigurablePrincipalRegistry,
    ConfigurableRoleAssignment,
    GrantsCache,
    build_authz_shared_services,
)
from forze_identity.authz.application.specs import (
    permission_definition_spec,
    principal_role_binding_spec,
    role_definition_spec,
    role_permission_binding_spec,
)
from forze_identity.authz.domain.models.bindings import (
    CreatePrincipalRoleBindingCmd,
    CreateRolePermissionBindingCmd,
)
from forze_identity.authz.domain.models.permission_definition import (
    CreatePermissionDefinitionCmd,
)
from forze_identity.authz.domain.models.role_definition import CreateRoleDefinitionCmd
from forze_identity.authz.services.grants import AuthzGrantResolver
from forze_mock import MockDepsModule

pytestmark = pytest.mark.unit

# ----------------------- #

SPEC = AuthzSpec(name="main")
WRITE = "ledger.write"
READ = "ledger.read"


class _Clock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now


@attrs.define(slots=True, kw_only=True)
class _Provider:
    """Grants :data:`READ` while :attr:`grant` holds, and denies it otherwise."""

    name: str = "flags"
    keys: frozenset[str] = frozenset({READ})
    grant: bool = True

    async def derive(self, principal_id: UUID, ctx: Any) -> DerivedPermissions:
        return DerivedPermissions(granted=self.keys) if self.grant else DerivedPermissions()


@attrs.define(slots=True, kw_only=True)
class _World:
    ctx: Any
    cache: GrantsCache | None
    principal: UUID
    registry: Any
    roles: Any
    decision: Any
    resolver: AuthzGrantResolver

    async def held(self, *, scope: AuthzScope | None = None) -> set[str]:
        grants = await self.resolver.resolve_effective_grants(self.principal, scope=scope)

        return {ref.role_key for ref in grants.roles}

    async def allowed(self, action: str) -> bool:
        decision = await self.decision.authorize(
            AuthzRequest(subject=AuthzSubject(principal_id=self.principal), action=action)
        )

        return decision.allowed

    async def drop_binding(self) -> None:
        """Remove the role binding by a plain document command, which no cache hears about."""

        rows = await self.ctx.doc.query(principal_role_binding_spec).find_many(
            filters={"$values": {"principal_id": self.principal}}
        )

        for row in rows.hits:
            await self.ctx.doc.command(principal_role_binding_spec).kill(row.id)


async def _world(
    cache: GrantsCache | None,
    *,
    providers: tuple[Any, ...] = (),
    strict_tx: bool = False,
    assign: bool = True,
) -> _World:
    ctx = context_from_modules(MockDepsModule(strict_tx=strict_tx))
    shared = build_authz_shared_services(
        AuthzKernelConfig(grants_cache=cache, permission_providers=providers)
    )
    cmd = ctx.doc.command
    permission = await cmd(permission_definition_spec).create(
        CreatePermissionDefinitionCmd(permission_key=WRITE)
    )
    role = await cmd(role_definition_spec).create(CreateRoleDefinitionCmd(role_key="writer"))
    await cmd(role_permission_binding_spec).create(
        CreateRolePermissionBindingCmd(role_id=role.id, permission_id=permission.id)
    )

    if providers:
        await cmd(permission_definition_spec).create(
            CreatePermissionDefinitionCmd(permission_key=READ)
        )

    registry = ConfigurablePrincipalRegistry()(ctx, SPEC)
    roles = ConfigurableRoleAssignment(shared=shared)(ctx, SPEC)
    decision = ConfigurableAuthzDecision(shared=shared)(ctx, SPEC)
    principal = (await registry.create_principal("user")).principal_id

    if assign:
        await roles.assign_role(principal, "writer")

    return _World(
        ctx=ctx,
        cache=cache,
        principal=principal,
        registry=registry,
        roles=roles,
        decision=decision,
        resolver=decision.resolver,
    )


def _cache(**kwargs: Any) -> GrantsCache:
    return GrantsCache(ttl=timedelta(minutes=5), **kwargs)


# ----------------------- #


class TestTheSetting:
    async def test_without_one_every_decision_reads_the_bindings(self) -> None:
        world = await _world(None)

        assert world.resolver.cache is None
        assert await world.held() == {"writer"}

        await world.drop_binding()

        assert await world.held() == set()

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"ttl": timedelta(0)},
            {"ttl": timedelta(seconds=-1)},
            {"ttl": 30},
            {"ttl": timedelta(minutes=1), "max_entries": 0},
            {"ttl": timedelta(minutes=1), "max_entries": True},
            {"ttl": timedelta(minutes=1), "max_entries": 1.5},
            {"ttl": timedelta(minutes=1), "clock": 0.0},
        ],
    )
    def test_a_cache_that_would_hold_nothing_is_refused(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(CoreException) as refused:
            GrantsCache(**kwargs)

        assert refused.value.kind is ExceptionKind.CONFIGURATION

    def test_the_kernel_refuses_anything_but_a_grants_cache(self) -> None:
        with pytest.raises(CoreException) as refused:
            AuthzKernelConfig(grants_cache=timedelta(minutes=1))  # type: ignore[arg-type]

        assert refused.value.kind is ExceptionKind.CONFIGURATION

    async def test_a_resolver_without_a_context_takes_no_cache(self) -> None:
        # Without a context its key's tenant would be a fixed one while its reads follow the
        # request's tenant, so one tenant's entry would answer for another.
        world = await _world(_cache())

        with pytest.raises(CoreException) as refused:
            attrs.evolve(world.resolver, ctx=None)

        assert refused.value.kind is ExceptionKind.CONFIGURATION

    async def test_every_port_built_from_one_kernel_shares_the_cache(self) -> None:
        cache = _cache()
        world = await _world(cache)

        assert world.roles.resolver.cache is cache
        assert world.decision.resolver.cache is cache


class TestWhatItRemembers:
    async def test_a_grant_is_served_from_the_cache_until_it_expires(self) -> None:
        clock = _Clock()
        world = await _world(_cache(clock=clock))

        assert await world.held() == {"writer"}

        await world.drop_binding()

        assert await world.held() == {"writer"}  # remembered: the TTL is the revocation delay

        clock.now += timedelta(minutes=5).total_seconds()

        assert await world.held() == set()

    async def test_an_entry_expires_a_ttl_after_its_read_began_not_after_it_ended(self) -> None:
        # A slow read may have seen grants revoked while it ran; the TTL bounds how long they
        # are served from the moment the read began.
        clock = _Clock()
        cache = _cache(clock=clock)
        world = await _world(cache)
        slow = _Slow(world.resolver, clock, seconds=timedelta(minutes=4).total_seconds())

        assert await slow.held(world.principal) == {"writer"}

        await world.drop_binding()
        clock.now += timedelta(minutes=1).total_seconds()  # 5 minutes after the read began

        assert await world.held() == set()

    async def test_a_read_slower_than_the_ttl_is_never_served(self) -> None:
        clock = _Clock()
        world = await _world(_cache(clock=clock))
        slow = _Slow(world.resolver, clock, seconds=timedelta(minutes=6).total_seconds())

        assert await slow.held(world.principal) == {"writer"}

        await world.drop_binding()

        assert await world.held() == set()

    async def test_the_least_recently_used_entry_goes_first(self) -> None:
        cache = _cache(max_entries=2)
        world = await _world(cache)
        others = [(await world.registry.create_principal("user")).principal_id for _ in range(2)]

        await world.held()
        await world.resolver.resolve_effective_grants(others[0])
        await world.held()  # used again: the first of the others is now the oldest
        await world.resolver.resolve_effective_grants(others[1])

        assert {key[0] for key in cache._entries} == {world.principal, others[1]}  # pyright: ignore[reportPrivateUsage]

    async def test_each_tenant_has_its_own_entry(self) -> None:
        world = await _world(_cache())
        first, second = TenantIdentity(tenant_id=uuid4()), TenantIdentity(tenant_id=uuid4())

        with world.ctx.inv_ctx.bind_identity(tenant=first):
            assert await world.held() == {"writer"}

        await world.drop_binding()

        with world.ctx.inv_ctx.bind_identity(tenant=second):
            assert await world.held() == set()  # another tenant: read, never served the first's

        with world.ctx.inv_ctx.bind_identity(tenant=first):
            assert await world.held() == {"writer"}

    async def test_each_scope_has_its_own_entry(self) -> None:
        world = await _world(_cache())

        assert await world.held() == {"writer"}

        await world.drop_binding()

        assert await world.held(scope=AuthzScope()) == set()
        assert await world.held() == {"writer"}


class TestWhatStaysLive:
    async def test_a_deactivated_principal_is_denied_at_once(self) -> None:
        world = await _world(_cache())

        assert await world.allowed(WRITE)

        await world.registry.deactivate_principal(world.principal)

        assert not await world.allowed(WRITE)

    async def test_provider_permissions_are_asked_for_every_decision(self) -> None:
        provider = _Provider(grant=False)
        world = await _world(_cache(), providers=(provider,))

        assert not await world.allowed(READ)

        provider.grant = True

        assert await world.allowed(READ)  # a hit for the catalog, a new answer from the provider

        provider.grant = False
        await world.drop_binding()

        assert not await world.allowed(READ)
        assert await world.allowed(WRITE)  # the catalog part still comes from the cache


class TestForgetting:
    async def test_revoking_a_role_forgets_the_principal(self) -> None:
        world = await _world(_cache())

        assert await world.allowed(WRITE)

        await world.roles.revoke_role(world.principal, "writer")

        assert not await world.allowed(WRITE)

    async def test_revoking_a_role_someone_else_removed_still_forgets_the_principal(self) -> None:
        # Another process removed the binding: this revoke finds nothing to remove, and its own
        # cached entry must go all the same.
        world = await _world(_cache())

        assert await world.allowed(WRITE)

        await world.drop_binding()
        await world.roles.revoke_role(world.principal, "writer")

        assert not await world.allowed(WRITE)

    async def test_assigning_a_role_someone_else_bound_still_forgets_the_principal(self) -> None:
        world = await _world(_cache(), assign=False)
        role = await world.ctx.doc.query(role_definition_spec).find(
            {"$values": {"role_key": "writer"}}
        )

        assert await world.held() == set()

        await world.ctx.doc.command(principal_role_binding_spec).create(
            CreatePrincipalRoleBindingCmd(principal_id=world.principal, role_id=role.id)
        )
        await world.roles.assign_role(world.principal, "writer")

        assert await world.held() == {"writer"}

    async def test_assigning_a_role_forgets_the_principal(self) -> None:
        world = await _world(_cache(), assign=False)

        assert await world.held() == set()

        await world.roles.assign_role(world.principal, "writer")

        assert await world.held() == {"writer"}

    async def test_forget_drops_one_principal_in_every_tenant_and_scope(self) -> None:
        cache = _cache()
        world = await _world(cache)
        other = (await world.registry.create_principal("user")).principal_id

        with world.ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=uuid4())):
            await world.held()

        await world.held(scope=AuthzScope())
        await world.resolver.resolve_effective_grants(other)

        cache.forget(world.principal)

        assert {key[0] for key in cache._entries} == {other}  # pyright: ignore[reportPrivateUsage]

    async def test_clear_drops_everyone(self) -> None:
        cache = _cache()
        world = await _world(cache)

        await world.held()
        await world.drop_binding()
        cache.clear()

        assert await world.held() == set()


class TestInATransaction:
    async def test_a_decision_inside_a_transaction_reads_the_bindings(self) -> None:
        cache = _cache()
        world = await _world(cache, strict_tx=True)

        assert await world.held() == {"writer"}

        await world.drop_binding()

        async with world.ctx.tx_ctx.scope("mock"):
            assert await world.held() == set()  # read, not served the entry made outside

        assert await world.held() == {"writer"}  # outside, the entry is still served

    async def test_a_savepoint_rolled_back_leaves_no_grant_behind(self) -> None:
        world = await _world(_cache(), strict_tx=True, assign=False)
        role = await world.ctx.doc.query(role_definition_spec).find(
            {"$values": {"role_key": "writer"}}
        )

        assert await world.held() == set()
        assert world.cache is not None
        world.cache.clear()

        async with world.ctx.tx_ctx.scope("mock"):
            with pytest.raises(RuntimeError):
                async with world.ctx.tx_ctx.scope("mock"):  # a savepoint
                    await world.ctx.doc.command(principal_role_binding_spec).create(
                        CreatePrincipalRoleBindingCmd(principal_id=world.principal, role_id=role.id)
                    )

                    assert await world.held() == {"writer"}

                    raise RuntimeError("roll the savepoint back")

        assert await world.held() == set()

    async def test_a_revoking_transaction_is_not_served_the_role_it_revoked(self) -> None:
        # A reader outside the transaction caches the committed bindings meanwhile; the
        # transaction's own next decision must still see its revocation.
        world = await _world(_cache(), strict_tx=True)
        stale = _Stale(world.resolver)

        assert await world.held() == {"writer"}

        async with world.ctx.tx_ctx.scope("mock"):
            snapshot = await stale.snapshot(world.principal)
            await world.roles.revoke_role(world.principal, "writer")

            outside = asyncio.create_task(
                stale.resolve(world.principal, snapshot), context=contextvars.Context()
            )
            assert await outside == {"writer"}
            assert await world.held() == set()

        assert await world.held() == set()

    async def test_grants_read_in_a_rolled_back_transaction_are_not_kept(self) -> None:
        world = await _world(_cache(), strict_tx=True, assign=False)

        assert await world.held() == set()

        with pytest.raises(RuntimeError):
            async with world.ctx.tx_ctx.scope("mock"):
                await world.roles.assign_role(world.principal, "writer")

                assert await world.held() == {"writer"}  # its own write, read inside

                raise RuntimeError("roll back")

        assert await world.held() == set()

    async def test_a_committed_assignment_is_seen_after_the_commit(self) -> None:
        world = await _world(_cache(), strict_tx=True, assign=False)

        assert await world.held() == set()

        async with world.ctx.tx_ctx.scope("mock"):
            await world.roles.assign_role(world.principal, "writer")

        assert await world.held() == {"writer"}

    async def test_a_reader_of_the_old_bindings_during_the_transaction_is_forgotten_at_commit(
        self,
    ) -> None:
        # Another request, outside the transaction, still reads the committed bindings while
        # the revocation is uncommitted, and caches them; the forget at commit must drop that.
        world = await _world(_cache(), strict_tx=True)
        stale = _Stale(world.resolver)

        assert await world.held() == {"writer"}

        async with world.ctx.tx_ctx.scope("mock"):
            snapshot = await stale.snapshot(world.principal)
            await world.roles.revoke_role(world.principal, "writer")

            outside = asyncio.create_task(
                stale.resolve(world.principal, snapshot), context=contextvars.Context()
            )
            assert await outside == {"writer"}

        assert await world.held() == set()


class TestARaceWithAForget:
    async def test_grants_read_before_a_forget_are_not_stored_after_it(self) -> None:
        world = await _world(_cache())
        gate = asyncio.Event()
        paused = _Paused(world.resolver, gate)

        task = asyncio.create_task(paused.held(world.principal))
        await paused.reached.wait()  # the bindings are read; the grants are not stored yet

        await world.drop_binding()
        assert world.cache is not None
        world.cache.forget(world.principal)
        gate.set()

        assert await task == {"writer"}  # it did read the old bindings
        assert await world.held() == set()  # and they were not kept


# ----------------------- #


class _Stale:
    """A resolver over the same cache that reads a snapshot of the principal-role bindings."""

    def __init__(self, resolver: AuthzGrantResolver) -> None:
        self._resolver = resolver

    async def snapshot(self, principal_id: UUID) -> list[Any]:
        page = await self._resolver.deps.pr_binding_qry.find_many(
            filters={"$values": {"principal_id": principal_id}}
        )

        return list(page.hits)

    async def resolve(self, principal_id: UUID, rows: list[Any]) -> set[str]:
        resolver = attrs.evolve(
            self._resolver,
            deps=attrs.evolve(self._resolver.deps, pr_binding_qry=_Fixed(self._resolver, rows)),
        )
        grants = await resolver.resolve_effective_grants(principal_id)

        return {ref.role_key for ref in grants.roles}


class _Fixed:
    """Serves a fixed set of principal-role binding rows."""

    def __init__(self, resolver: AuthzGrantResolver, rows: list[Any]) -> None:
        self._inner = resolver.deps.pr_binding_qry
        self._rows = rows

    def __getattr__(self, attr: str) -> Any:
        return getattr(self._inner, attr)

    async def find_stream(self, **_kwargs: Any) -> AsyncIterator[list[Any]]:
        yield self._rows


class _Slow:
    """A resolver over the same cache whose read of the role bindings takes *seconds*."""

    def __init__(self, resolver: AuthzGrantResolver, clock: _Clock, *, seconds: float) -> None:
        class _Late:
            def __init__(self, inner: Any) -> None:
                self._inner = inner

            def __getattr__(self, attr: str) -> Any:
                return getattr(self._inner, attr)

            async def find_stream(self, **kwargs: Any) -> AsyncIterator[Any]:
                async for batch in self._inner.find_stream(**kwargs):
                    yield batch

                clock.now += seconds

        self._resolver = attrs.evolve(
            resolver,
            deps=attrs.evolve(resolver.deps, pr_binding_qry=_Late(resolver.deps.pr_binding_qry)),
        )

    async def held(self, principal_id: UUID) -> set[str]:
        grants = await self._resolver.resolve_effective_grants(principal_id)

        return {ref.role_key for ref in grants.roles}


class _Paused:
    """A resolver over the same cache that stops once it has read the role bindings."""

    def __init__(self, resolver: AuthzGrantResolver, gate: asyncio.Event) -> None:
        self.reached = asyncio.Event()
        outer = self

        class _Gate:
            def __init__(self, inner: Any) -> None:
                self._inner = inner

            def __getattr__(self, attr: str) -> Any:
                return getattr(self._inner, attr)

            async def find_stream(self, **kwargs: Any) -> AsyncIterator[Any]:
                batches = [batch async for batch in self._inner.find_stream(**kwargs)]
                outer.reached.set()
                await gate.wait()

                for batch in batches:
                    yield batch

        self._resolver = attrs.evolve(
            resolver,
            deps=attrs.evolve(resolver.deps, pr_binding_qry=_Gate(resolver.deps.pr_binding_qry)),
        )

    async def held(self, principal_id: UUID) -> set[str]:
        grants = await self._resolver.resolve_effective_grants(principal_id)

        return {ref.role_key for ref in grants.roles}
