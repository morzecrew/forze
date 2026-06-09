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
    """Principal-role bindings backed by junction documents."""

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
        _ = resolve_policy_scope(
            spec=self.spec,
            explicit=scope,
            invocation_tenant_id=scope.tenant_id if scope is not None else None,
        )
        pid = subject_for_grant_query(subject)
        principal_row = await find_policy_principal_by_id(self.principal_qry, pid)

        if principal_row is None:
            raise exc.internal("Policy principal not found for role assignment")

        role = await self.role_qry.find(
            filters={"$values": {"role_key": role_key}},
        )

        if role is None:
            raise exc.internal(f"Unknown role key: {role_key!r}")

        existing = await self._find_principal_role_binding(pid, role.id)

        if existing is not None:
            return

        await self.pr_binding_cmd.create(
            CreatePrincipalRoleBindingCmd(principal_id=pid, role_id=role.id),
            return_new=False,
        )

    # ....................... #

    async def revoke_role(
        self,
        subject: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        role_key: str,
        *,
        scope: AuthzScope | None = None,
    ) -> None:
        _ = resolve_policy_scope(
            spec=self.spec,
            explicit=scope,
            invocation_tenant_id=scope.tenant_id if scope is not None else None,
        )
        pid = subject_for_grant_query(subject)
        principal_row = await find_policy_principal_by_id(self.principal_qry, pid)

        if principal_row is None:
            raise exc.internal("Policy principal not found for role revocation")

        role = await self.role_qry.find(
            filters={"$values": {"role_key": role_key}},
        )

        if role is None:
            raise exc.internal(f"Unknown role key: {role_key!r}")

        binding = await self._find_principal_role_binding(pid, role.id)

        if binding is None:
            return

        await self.pr_binding_cmd.kill(binding.id)

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
            invocation_tenant_id=scope.tenant_id if scope is not None else None,
        )
        pid = subject_for_grant_query(subject)
        principal_row = await find_policy_principal_by_id(self.principal_qry, pid)

        if principal_row is None:
            raise exc.internal("Policy principal not found when listing roles")

        return await self.resolver.list_assigned_roles(pid, scope=resolved)

    # ....................... #

    async def _find_principal_role_binding(
        self,
        principal_id: UUID,
        role_id: UUID,
    ) -> ReadPrincipalRoleBinding | None:
        rows = await fetch_all_document_hits(
            self.pr_binding_qry,
            filters={"$values": {"principal_id": principal_id}},
        )

        for row in rows:
            if row.role_id == role_id:
                return row

        return None
