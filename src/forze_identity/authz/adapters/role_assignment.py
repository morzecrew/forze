from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, final
from uuid import UUID

import attrs

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz import (
    AuthzScope,
    AuthzSubject,
    PrincipalRef,
    RoleAssignmentPort,
    RoleRef,
    resolve_policy_scope,
    subject_for_grant_query,
)
from forze.application.contracts.authz.specs import AuthzSpec
from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.application.contracts.tenancy import TenantIdentity, effective_tenant
from forze.base.exceptions import exc

from ..domain.models.bindings import (
    CreatePrincipalRoleBindingCmd,
    PrincipalRoleBinding,
    ReadPrincipalRoleBinding,
)
from ..domain.models.policy_principal import ReadPolicyPrincipal
from ..domain.models.role_definition import ReadRoleDefinition
from ..services.grants import AuthzGrantResolver, fetch_all_document_hits
from ._utils import find_policy_principal_by_id, validate_authz_query_ports

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RoleAssignmentAdapter(RoleAssignmentPort):
    """Principal-role bindings backed by junction documents.

    A binding may name the one tenant it holds in (``scope_tenant_id``). Writes take that
    tenant from the bound invocation, or from ``scope`` when nothing is bound, so a forgotten
    scope inside a tenant narrows a grant to that tenant and never widens it to all of them.
    """

    spec: AuthzSpec
    principal_qry: DocumentQueryPort[ReadPolicyPrincipal]
    role_qry: DocumentQueryPort[ReadRoleDefinition]
    pr_binding_qry: DocumentQueryPort[ReadPrincipalRoleBinding]
    pr_binding_cmd: DocumentCommandPort[
        ReadPrincipalRoleBinding,
        PrincipalRoleBinding,
        CreatePrincipalRoleBindingCmd,
        Any,
    ]
    resolver: AuthzGrantResolver
    """Also the source of the invocation tenant, which it reads from ``ctx`` on every call."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        validate_authz_query_ports(
            self.spec,
            (
                self.principal_qry,
                self.role_qry,
                self.pr_binding_qry,
                self.pr_binding_cmd,
                self.resolver.deps.permission_qry,
                self.resolver.deps.group_qry,
                self.resolver.deps.rp_binding_qry,
                self.resolver.deps.pp_binding_qry,
                self.resolver.deps.gp_binding_qry,
                self.resolver.deps.gr_binding_qry,
                self.resolver.deps.gperm_binding_qry,
            ),
        )

    # ....................... #

    async def assign_role(
        self,
        subject: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        role_key: str,
        *,
        scope: AuthzScope | None = None,
    ) -> None:
        tenant_id = self._write_tenant(scope)
        pid = subject_for_grant_query(subject)
        role = await self._role_for(pid, role_key, action="role assignment")

        if await self._find_principal_role_binding(pid, role.id, tenant_id) is None:
            await self.pr_binding_cmd.create(
                CreatePrincipalRoleBindingCmd(
                    principal_id=pid,
                    role_id=role.id,
                    scope_tenant_id=tenant_id,
                ),
                return_new=False,
            )

        # Even when another process bound it first: this process may have cached the absence.
        await self.resolver.forget(pid)

    # ....................... #

    async def revoke_role(
        self,
        subject: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        role_key: str,
        *,
        scope: AuthzScope | None = None,
    ) -> None:
        tenant_id = self._write_tenant(scope)
        pid = subject_for_grant_query(subject)
        role = await self._role_for(pid, role_key, action="role revocation")
        rows = await self._principal_role_bindings(pid, role.id)

        if tenant_id is None:
            # No tenant: everywhere, as an unscoped revoke always meant. The global row and
            # every tenant row of the role.
            doomed = rows

        elif not self._partitioned and any(row.scope_tenant_id is None for row in rows):
            # Removing only this tenant's row would leave the global one granting here.
            raise exc.conflict(
                f"Role {role_key!r} is held in every tenant; revoke it with no tenant bound.",
                code="authz.role_held_globally",
            )

        else:
            doomed = [row for row in rows if self._held_in(row, tenant_id)]

        try:
            for row in doomed:
                await self.pr_binding_cmd.kill(row.id)

        finally:
            # Even when another process removed it first: this process may still cache the
            # role. Also after a kill fails partway, when the earlier ones may have committed.
            await self.resolver.forget(pid)

    # ....................... #

    async def revoke_tenant_roles(
        self,
        subject: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        *,
        tenant_id: UUID,
    ) -> None:
        pid = subject_for_grant_query(subject)

        # Not checked against the bound tenant, unlike the other writes: it only removes grants,
        # and leaving a tenant happens from wherever the caller is bound (a member leaves T
        # from inside U; an operator removes members of any tenant). So it runs with T bound:
        # a store that follows the bound tenant, by a tenant column or by a relation resolved
        # per tenant, then reaches T's rows rather than the caller's.
        with self._bound_to(tenant_id):
            _ = resolve_policy_scope(
                spec=self.spec,
                explicit=AuthzScope(tenant_id=tenant_id),
                invocation_tenant_id=self.resolver.invocation_tenant(),
            )

            # No policy-principal lookup: leaving a tenant must not fail for a principal the
            # authz plane never saw, and such a principal holds no rows to remove.
            rows = await fetch_all_document_hits(
                self.pr_binding_qry,
                filters={"$values": {"principal_id": pid}},
            )

            try:
                for row in rows:
                    if self._held_in(row, tenant_id):
                        await self.pr_binding_cmd.kill(row.id)

            finally:
                await self.resolver.forget(pid)

    # ....................... #

    async def list_roles(
        self,
        subject: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        *,
        scope: AuthzScope | None = None,
    ) -> frozenset[RoleRef]:
        resolved = resolve_policy_scope(
            spec=self.spec,
            explicit=scope,
            invocation_tenant_id=self.resolver.invocation_tenant(),
        )
        pid = subject_for_grant_query(subject)
        principal_row = await find_policy_principal_by_id(self.principal_qry, pid)

        if principal_row is None:
            raise exc.internal("Policy principal not found when listing roles")

        # The bound tenant selects the bindings: global ones plus that tenant's.
        return await self.resolver.list_assigned_roles(pid, scope=resolved)

    # ....................... #

    def _write_tenant(self, scope: AuthzScope | None) -> UUID | None:
        """The tenant a write binds in: the bound one, else the scope's, else none (global).

        A scope naming another tenant than the bound one is refused, as a decision refuses it.
        """

        bound = self.resolver.invocation_tenant()
        self.resolver.require_scope_matches_invocation(scope)
        _ = resolve_policy_scope(spec=self.spec, explicit=scope, invocation_tenant_id=bound)

        return effective_tenant(
            bound=bound,
            requested=scope.tenant_id if scope is not None else None,
        )

    # ....................... #

    async def _role_for(
        self, principal_id: UUID, role_key: str, *, action: str
    ) -> ReadRoleDefinition:
        if await find_policy_principal_by_id(self.principal_qry, principal_id) is None:
            raise exc.internal(f"Policy principal not found for {action}")

        role = await self.role_qry.find(filters={"$values": {"role_key": role_key}})

        if role is None:
            raise exc.internal(f"Unknown role key: {role_key!r}")

        return role

    # ....................... #

    async def _principal_role_bindings(
        self,
        principal_id: UUID,
        role_id: UUID,
    ) -> list[ReadPrincipalRoleBinding]:
        """Every row binding *role_id* to the principal, in any tenant or none."""

        rows = await fetch_all_document_hits(
            self.pr_binding_qry,
            filters={"$values": {"principal_id": principal_id}},
        )

        return [row for row in rows if row.role_id == role_id]

    # ....................... #

    async def _find_principal_role_binding(
        self,
        principal_id: UUID,
        role_id: UUID,
        tenant_id: UUID | None,
    ) -> ReadPrincipalRoleBinding | None:
        """The row for exactly this tenant: ``None`` matches only a global row."""

        for row in await self._principal_role_bindings(principal_id, role_id):
            if self._held_in(row, tenant_id):
                return row

        return None

    # ....................... #

    @property
    def _partitioned(self) -> bool:
        """Whether the binding store itself confines rows to the bound tenant.

        There every row read belongs to the bound tenant, including one written before
        ``scope_tenant_id`` existed, which reads as ``None`` without being held everywhere.
        """

        return (
            self.pr_binding_qry.tenant_aware
            or self.spec.tenancy_mode == "require_invocation_tenant"
        )

    def _held_in(self, row: ReadPrincipalRoleBinding, tenant_id: UUID | None) -> bool:
        """Whether *row* is the binding for exactly *tenant_id* (``None``: every tenant)."""

        if tenant_id is not None and self._partitioned:
            return row.scope_tenant_id in (None, tenant_id)

        return row.scope_tenant_id == tenant_id

    @contextmanager
    def _bound_to(self, tenant_id: UUID) -> Iterator[None]:
        """Bind *tenant_id* for the body, keeping the caller's identity, unless it already is.

        Without a context the resolver's fixed tenant stands in, and nothing can be rebound:
        a partitioned store bound to another tenant would reach none of *tenant_id*'s rows,
        so that is refused rather than left to keep them.
        """

        ctx = self.resolver.ctx
        bound = self.resolver.invocation_tenant()

        if bound == tenant_id:
            yield
            return

        if ctx is None:
            if self._partitioned and bound is not None:
                raise exc.internal(
                    "Revoking a tenant's roles needs that tenant bound: the principal-role "
                    "binding store reaches only the bound tenant's rows.",
                    code="authz.scope_tenant_mismatch",
                )

            yield
            return

        with ctx.inv_ctx.bind_identity(
            authn=ctx.inv_ctx.get_authn(),
            tenant=TenantIdentity(tenant_id=tenant_id),
        ):
            yield
