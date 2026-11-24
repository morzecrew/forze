"""Tenant-held role bindings (RFC 0082): resolution reads the bound tenant only, and writes
follow the assign and revoke table of §5.3."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import attrs
import pytest

from forze.application.contracts.authz import AuthzScope
from forze.application.contracts.authz.specs import AuthzSpec, AuthzTenancyMode
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze_identity.authz.adapters.role_assignment import RoleAssignmentAdapter
from forze_identity.authz.domain.models.bindings import (
    CreatePrincipalRoleBindingCmd,
    ReadPrincipalRoleBinding,
    ReadRolePermissionBinding,
)
from forze_identity.authz.domain.models.policy_principal import ReadPolicyPrincipal
from forze_identity.authz.services.grants import AuthzGrantResolver
from forze_mock import MockDepsModule
from tests.support.authz_grants import seeded_role_assignment, tenant_held_roles
from tests.support.execution_context import context_from_modules
from tests.unit.test_forze_authz.test_grant_resolver_service import (
    FakeDocQuery,
    _doc_kwargs,
    _empty_deps,
    _perm,
    _role,
)

pytestmark = pytest.mark.unit

T, U = uuid4(), uuid4()

# ----------------------- #


def _binding(principal_id: UUID, role_id: UUID, tenant_id: UUID | None) -> ReadPrincipalRoleBinding:
    return ReadPrincipalRoleBinding(
        **_doc_kwargs(uuid4()),
        principal_id=principal_id,
        role_id=role_id,
        scope_tenant_id=tenant_id,
    )


class _Bindings(FakeDocQuery):
    """The principal-role binding table, as both its query and its command port."""

    def __init__(
        self, rows: list[ReadPrincipalRoleBinding] | None = None, *, tenant_aware: bool = False
    ) -> None:
        super().__init__(rows=rows if rows is not None else [])
        self.spec = DocumentSpec(name="pr_bindings", read=ReadPrincipalRoleBinding)
        self.tenant_aware = tenant_aware

    @property
    def rows(self) -> list[ReadPrincipalRoleBinding]:
        return self._rows

    def tenants_of(self, role_id: UUID) -> list[UUID | None]:
        return sorted((r.scope_tenant_id for r in self._rows if r.role_id == role_id), key=str)

    async def create(self, cmd: CreatePrincipalRoleBindingCmd, *, return_new: bool = True) -> None:
        _ = return_new
        self._rows.append(_binding(cmd.principal_id, cmd.role_id, cmd.scope_tenant_id))

    async def kill(self, doc_id: UUID) -> None:
        self._rows = [r for r in self._rows if r.id != doc_id]


def _port(name: str, *, tenant_aware: bool = False) -> Any:
    port = MagicMock()
    port.spec = DocumentSpec(name=name, read=ReadPolicyPrincipal)
    port.tenant_aware = tenant_aware

    return port


# ....................... #
# Resolution


class TestResolution:
    """A principal holding a global role, a role in T and a role in U."""

    def _resolver(self, bound: UUID | None) -> tuple[AuthzGrantResolver, UUID]:
        pid = uuid4()
        everywhere, in_t, in_u = (
            _role(uuid4(), "everywhere"),
            _role(uuid4(), "in_t"),
            _role(uuid4(), "in_u"),
        )
        perms = {
            role.id: _perm(uuid4(), f"{role.role_key}.do") for role in (everywhere, in_t, in_u)
        }
        deps = _empty_deps(
            role_qry=FakeDocQuery(by_id={r.id: r for r in (everywhere, in_t, in_u)}),
            permission_qry=FakeDocQuery(by_id={p.id: p for p in perms.values()}),
            pr_binding_qry=_Bindings(
                [
                    _binding(pid, everywhere.id, None),
                    _binding(pid, in_t.id, T),
                    _binding(pid, in_u.id, U),
                ],
            ),
            rp_binding_qry=FakeDocQuery(
                rows=[
                    ReadRolePermissionBinding(
                        **_doc_kwargs(uuid4()), role_id=rid, permission_id=p.id
                    )
                    for rid, p in perms.items()
                ],
            ),
        )

        return AuthzGrantResolver(deps=deps, invocation_tenant_id=bound), pid

    @pytest.mark.parametrize(
        ("bound", "expected"),
        [(T, {"everywhere", "in_t"}), (U, {"everywhere", "in_u"}), (None, {"everywhere"})],
        ids=["in-T", "in-U", "none-bound"],
    )
    async def test_a_binding_holds_globally_or_in_the_bound_tenant(
        self, bound: UUID | None, expected: set[str]
    ) -> None:
        resolver, pid = self._resolver(bound)

        grants = await resolver.resolve_effective_grants(pid)
        roles = await resolver.list_assigned_roles(pid)

        assert {r.role_key for r in grants.roles} == expected
        assert {p.permission_key for p in grants.permissions} == {f"{k}.do" for k in expected}
        assert {r.role_key for r in roles} == expected

    async def test_with_nothing_bound_a_scope_never_selects_a_tenants_bindings(self) -> None:
        # Untenanted work passing an explicit scope would otherwise read U's roles without
        # the membership check ingress performs.
        resolver, pid = self._resolver(None)
        scope = AuthzScope(tenant_id=U)

        grants = await resolver.resolve_effective_grants(pid, scope=scope)
        roles = await resolver.list_assigned_roles(pid, scope=scope)

        assert {r.role_key for r in grants.roles} == {"everywhere"}
        assert {p.permission_key for p in grants.permissions} == {"everywhere.do"}
        assert {r.role_key for r in roles} == {"everywhere"}

    async def test_a_scope_naming_another_tenant_is_refused(self) -> None:
        resolver, pid = self._resolver(T)

        with pytest.raises(CoreException) as caught:
            await resolver.resolve_effective_grants(pid, scope=AuthzScope(tenant_id=U))

        assert caught.value.code == "authz.scope_tenant_mismatch"


# ....................... #
# Assignment


class _Wiring:
    def __init__(
        self,
        bound: UUID | None,
        *,
        rows: list[ReadPrincipalRoleBinding] | None = None,
        mode: AuthzTenancyMode = "global",
        principal_known: bool = True,
    ) -> None:
        tenant_aware = mode == "require_invocation_tenant"
        self.pid = uuid4()
        self.role = _role(uuid4(), "analyst")
        self.bindings = _Bindings(rows, tenant_aware=tenant_aware)

        principal_qry = _port("principals", tenant_aware=tenant_aware)
        principal_qry.find = AsyncMock(return_value=MagicMock() if principal_known else None)
        role_qry = _port("roles", tenant_aware=tenant_aware)
        role_qry.find = AsyncMock(return_value=self.role)

        deps = _empty_deps(pr_binding_qry=self.bindings)
        for name in (
            "permission_qry",
            "group_qry",
            "rp_binding_qry",
            "pp_binding_qry",
            "gp_binding_qry",
            "gr_binding_qry",
            "gperm_binding_qry",
        ):
            port = getattr(deps, name)
            port.spec = DocumentSpec(name=name, read=ReadPolicyPrincipal)
            port.tenant_aware = tenant_aware

        self.adapter = RoleAssignmentAdapter(
            spec=AuthzSpec(name="main", tenancy_mode=mode),
            principal_qry=principal_qry,
            role_qry=role_qry,
            pr_binding_qry=self.bindings,
            pr_binding_cmd=self.bindings,  # type: ignore[arg-type]
            resolver=AuthzGrantResolver(deps=deps, invocation_tenant_id=bound),
        )

    def tenants(self) -> list[UUID | None]:
        return self.bindings.tenants_of(self.role.id)

    def adapter_in(self, bound: UUID | None) -> RoleAssignmentAdapter:
        """The same stores, seen from an invocation bound to *bound*."""

        resolver = attrs.evolve(self.adapter.resolver, invocation_tenant_id=bound)

        return attrs.evolve(self.adapter, resolver=resolver)

    def row(self, tenant_id: UUID | None) -> ReadPrincipalRoleBinding:
        return _binding(self.pid, self.role.id, tenant_id)


def _sorted(*tenants: UUID | None) -> list[UUID | None]:
    return sorted(tenants, key=str)


class TestTheAssignTable:
    @pytest.mark.parametrize(
        ("bound", "scope", "written"),
        [(T, None, T), (T, T, T), (None, T, T), (None, None, None)],
        ids=["bound-T-unscoped", "bound-T-scope-T", "none-bound-scope-T", "none-bound-unscoped"],
    )
    async def test_the_tenant_written(
        self, bound: UUID | None, scope: UUID | None, written: UUID | None
    ) -> None:
        w = _Wiring(bound)

        await w.adapter.assign_role(
            w.pid, "analyst", scope=AuthzScope(tenant_id=scope) if scope else None
        )

        assert w.tenants() == [written]

    @pytest.mark.parametrize("write", ["assign_role", "revoke_role"])
    async def test_a_scope_naming_another_tenant_is_refused(self, write: str) -> None:
        w = _Wiring(T)

        with pytest.raises(CoreException) as caught:
            await getattr(w.adapter, write)(w.pid, "analyst", scope=AuthzScope(tenant_id=U))

        assert caught.value.code == "authz.scope_tenant_mismatch"
        assert w.tenants() == []

    async def test_assigning_twice_in_a_tenant_writes_one_row(self) -> None:
        w = _Wiring(T)

        await w.adapter.assign_role(w.pid, "analyst")
        await w.adapter.assign_role(w.pid, "analyst")

        assert w.tenants() == [T]

    async def test_an_unscoped_assign_in_a_tenant_scoped_route_takes_the_bound_tenant(self) -> None:
        # Before RFC 0082 this raised tenant_required inside a tenant-bound request.
        w = _Wiring(T, mode="require_invocation_tenant")

        await w.adapter.assign_role(w.pid, "analyst")

        assert w.tenants() == [T]


class TestTheRevokeTable:
    @pytest.mark.parametrize(
        "bound, scope", [(T, None), (None, T)], ids=["bound-T", "none-bound-scope-T"]
    )
    async def test_in_a_tenant_a_global_row_refuses_the_revoke(
        self, bound: UUID | None, scope: UUID | None
    ) -> None:
        w = _Wiring(bound)
        w.bindings.rows.extend([w.row(None), w.row(T)])

        with pytest.raises(CoreException) as caught:
            await w.adapter.revoke_role(
                w.pid, "analyst", scope=AuthzScope(tenant_id=scope) if scope else None
            )

        assert caught.value.code == "authz.role_held_globally"
        assert w.tenants() == _sorted(None, T)

    async def test_in_a_tenant_only_that_tenants_row_goes(self) -> None:
        w = _Wiring(T)
        w.bindings.rows.extend([w.row(T), w.row(U)])

        await w.adapter.revoke_role(w.pid, "analyst")

        assert w.tenants() == [U]

    async def test_with_no_tenant_the_role_goes_everywhere(self) -> None:
        w = _Wiring(None)
        w.bindings.rows.extend([w.row(None), w.row(T), w.row(U)])

        await w.adapter.revoke_role(w.pid, "analyst")

        assert w.tenants() == []

    @pytest.mark.parametrize("first", ["global", "tenant"])
    async def test_global_and_tenant_rows_side_by_side(self, first: str) -> None:
        # Whichever comes first, the other write is not skipped, a revoke in T is refused,
        # and a revoke with no tenant removes both.
        w = _Wiring(T)
        everywhere = w.adapter_in(None)
        order = (everywhere, w.adapter) if first == "global" else (w.adapter, everywhere)

        for adapter in order:
            await adapter.assign_role(w.pid, "analyst")

        assert w.tenants() == _sorted(None, T)

        with pytest.raises(CoreException):
            await w.adapter.revoke_role(w.pid, "analyst")

        await everywhere.revoke_role(w.pid, "analyst")

        assert w.tenants() == []


class TestRevokeTenantRoles:
    async def test_it_removes_the_tenants_rows_and_keeps_the_rest(self) -> None:
        w = _Wiring(T)
        other = _role(uuid4(), "viewer")
        w.bindings.rows.extend([w.row(None), w.row(T), w.row(U), _binding(w.pid, other.id, T)])

        await w.adapter.revoke_tenant_roles(w.pid, tenant_id=T)

        assert _sorted(*(r.scope_tenant_id for r in w.bindings.rows)) == _sorted(None, U)

    async def test_it_runs_from_inside_another_tenant(self) -> None:
        # A member leaves T while bound to U; an operator removes members of any tenant.
        w = _Wiring(U)
        w.bindings.rows.extend([w.row(T), w.row(U)])

        await w.adapter.revoke_tenant_roles(w.pid, tenant_id=T)

        assert w.tenants() == [U]

    async def test_a_principal_the_authz_plane_never_saw_is_no_error(self) -> None:
        w = _Wiring(T, principal_known=False)

        await w.adapter.revoke_tenant_roles(w.pid, tenant_id=T)

    async def test_the_whole_table_on_mock_document_storage(self) -> None:
        # The same walk the Postgres, Mongo and Firestore suites run, over the mock's stores.
        ctx = context_from_modules(MockDepsModule())
        ra, principal_id = await seeded_role_assignment(ctx)

        await tenant_held_roles(ctx, ra, principal_id)

    async def test_over_a_tenant_aware_store_built_as_global_another_tenant_is_refused(
        self,
    ) -> None:
        # Tenant management builds its port from AuthzSpec(name=authz_route), global mode.
        # Over a tenant-aware store the read sees only U's rows, so T's would silently stay.
        w = _Wiring(U)
        w.bindings.tenant_aware = True
        w.bindings.rows.append(w.row(T))

        with pytest.raises(CoreException) as caught:
            await w.adapter.revoke_tenant_roles(w.pid, tenant_id=T)

        assert caught.value.code == "authz.scope_tenant_mismatch"

    async def test_on_a_tenant_scoped_route_another_tenant_is_refused(self) -> None:
        # Storage there confines rows to the bound tenant, so T's rows are out of reach.
        w = _Wiring(U, mode="require_invocation_tenant")

        with pytest.raises(CoreException):
            await w.adapter.revoke_tenant_roles(w.pid, tenant_id=T)

    async def test_a_failed_kill_still_forgets_the_principal(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Outside a transaction the kills before the failure stay, so the cache must not.
        forget = AsyncMock()
        monkeypatch.setattr(AuthzGrantResolver, "forget", forget)
        w = _Wiring(T)
        w.bindings.rows.extend([w.row(T), _binding(w.pid, uuid4(), T)])
        down = RuntimeError("down")
        w.bindings.kill = AsyncMock(side_effect=[None, down, down])  # type: ignore[method-assign]

        with pytest.raises(RuntimeError):
            await w.adapter.revoke_tenant_roles(w.pid, tenant_id=T)

        with pytest.raises(RuntimeError):
            await w.adapter.revoke_role(w.pid, "analyst")  # its one kill fails

        assert forget.await_count == 2


class _Partitioned(_Bindings):
    """A store that follows the bound tenant, as a tenant column or a per-tenant relation
    does: each row lives in the tenant bound when it was written, and only that tenant's rows
    are read."""

    def __init__(self, ctx: Any) -> None:
        super().__init__(tenant_aware=True)
        self.ctx = ctx
        self.partition: dict[UUID, UUID | None] = {}

    def _bound(self) -> UUID | None:
        tenant = self.ctx.inv_ctx.get_tenant()

        return tenant.tenant_id if tenant is not None else None

    def put(self, row: ReadPrincipalRoleBinding, partition: UUID) -> None:
        self._rows.append(row)
        self.partition[row.id] = partition

    async def find_stream(self, *, filters: Any, chunk_size: int) -> Any:
        async for batch in super().find_stream(filters=filters, chunk_size=chunk_size):
            yield [r for r in batch if self.partition.get(r.id) == self._bound()]

    async def create(self, cmd: CreatePrincipalRoleBindingCmd, *, return_new: bool = True) -> None:
        await super().create(cmd, return_new=return_new)
        self.partition[self._rows[-1].id] = self._bound()


class TestOverAPartitionedStore:
    """A store that confines rows to the bound tenant, holding rows written before
    ``scope_tenant_id`` existed, which read as ``None`` without being held everywhere."""

    def _wiring(self) -> tuple[_Wiring, Any, _Partitioned]:
        ctx = context_from_modules(MockDepsModule())
        w = _Wiring(None)
        store = _Partitioned(ctx)
        resolver = attrs.evolve(
            w.adapter.resolver,
            deps=attrs.evolve(w.adapter.resolver.deps, pr_binding_qry=store),
            ctx=ctx,
        )
        w.bindings = store
        w.adapter = attrs.evolve(
            w.adapter, pr_binding_qry=store, pr_binding_cmd=store, resolver=resolver
        )

        return w, ctx, store

    @staticmethod
    def _in(ctx: Any, tenant: UUID) -> Any:
        return ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant))

    async def test_a_revoke_in_the_tenant_removes_a_row_written_before_the_column(self) -> None:
        w, ctx, store = self._wiring()
        store.put(w.row(None), T)

        with self._in(ctx, T):
            await w.adapter.revoke_role(w.pid, "analyst")  # not refused as held everywhere

        assert store.rows == []

    async def test_an_assign_in_the_tenant_counts_that_row_as_there(self) -> None:
        w, ctx, store = self._wiring()
        store.put(w.row(None), T)

        with self._in(ctx, T):
            await w.adapter.assign_role(w.pid, "analyst")

        assert len(store.rows) == 1

    async def test_leaving_from_inside_another_tenant_reaches_the_tenants_rows(self) -> None:
        w, ctx, store = self._wiring()
        store.put(w.row(None), T)
        store.put(w.row(T), T)
        store.put(w.row(U), U)

        with self._in(ctx, U):
            await w.adapter.revoke_tenant_roles(w.pid, tenant_id=T)

            assert ctx.inv_ctx.get_tenant().tenant_id == U  # the caller's tenant is back

        assert [store.partition[r.id] for r in store.rows] == [U]
