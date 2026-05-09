from typing import Any, final
from uuid import UUID

import attrs

from forze.application.contracts.authz import RoleAssignmentPort
from forze.application.contracts.authz.value_objects import (
    PrincipalRef,
    RoleRef,
    coalesce_authz_tenant_id,
)
from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.base.errors import CoreError

from ..domain.models.bindings import (
    CreatePrincipalRoleBindingCmd,
    PrincipalRoleBinding,
    ReadPrincipalRoleBinding,
)
from ..domain.models.policy_principal import ReadPolicyPrincipal
from ..domain.models.role_definition import ReadRoleDefinition
from ..services.grants import AuthzGrantResolver, fetch_all_document_hits
from ._utils import find_policy_principal_by_id, validate_secure_authz_document_spec

# ----------------------- #


def _principal_id(principal: PrincipalRef | UUID) -> UUID:
    return principal.principal_id if isinstance(principal, PrincipalRef) else principal


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RoleAssignmentAdapter(RoleAssignmentPort):
    """Principal-role bindings backed by junction documents."""

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
        validate_secure_authz_document_spec(self.principal_qry.spec)
        validate_secure_authz_document_spec(self.role_qry.spec)
        validate_secure_authz_document_spec(self.pr_binding_qry.spec)
        validate_secure_authz_document_spec(self.pr_binding_cmd.spec)

        for qry in (
            self.resolver.deps.permission_qry,
            self.resolver.deps.group_qry,
            self.resolver.deps.rp_binding_qry,
            self.resolver.deps.pp_binding_qry,
            self.resolver.deps.gp_binding_qry,
            self.resolver.deps.gr_binding_qry,
            self.resolver.deps.gperm_binding_qry,
        ):
            validate_secure_authz_document_spec(qry.spec)

    # ....................... #

    async def assign_role(
        self,
        principal: PrincipalRef | UUID,
        role_key: str,
        *,
        tenant_id: UUID | None = None,
    ) -> None:
        _ = coalesce_authz_tenant_id(principal, tenant_id=tenant_id)
        pid = _principal_id(principal)
        principal_row = await find_policy_principal_by_id(self.principal_qry, pid)

        if principal_row is None:
            raise CoreError("Policy principal not found for role assignment")

        _ = principal_row

        role = await self.role_qry.find(
            filters={"$fields": {"role_key": role_key}},
        )

        if role is None:
            raise CoreError(f"Unknown role key: {role_key!r}")

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
        principal: PrincipalRef | UUID,
        role_key: str,
        *,
        tenant_id: UUID | None = None,
    ) -> None:
        _ = coalesce_authz_tenant_id(principal, tenant_id=tenant_id)
        pid = _principal_id(principal)
        principal_row = await find_policy_principal_by_id(self.principal_qry, pid)

        if principal_row is None:
            raise CoreError("Policy principal not found for role revocation")

        _ = principal_row

        role = await self.role_qry.find(
            filters={"$fields": {"role_key": role_key}},
        )

        if role is None:
            raise CoreError(f"Unknown role key: {role_key!r}")

        binding = await self._find_principal_role_binding(pid, role.id)

        if binding is None:
            return

        await self.pr_binding_cmd.delete(binding.id, binding.rev)

    # ....................... #

    async def list_roles(
        self,
        principal: PrincipalRef | UUID,
        *,
        tenant_id: UUID | None = None,
    ) -> frozenset[RoleRef]:
        scope_tid = coalesce_authz_tenant_id(principal, tenant_id=tenant_id)
        pid = _principal_id(principal)
        principal_row = await find_policy_principal_by_id(self.principal_qry, pid)

        if principal_row is None:
            raise CoreError("Policy principal not found when listing roles")

        _ = principal_row

        return await self.resolver.list_assigned_roles(pid, tenant_id=scope_tid)

    # ....................... #

    async def _find_principal_role_binding(
        self,
        principal_id: UUID,
        role_id: UUID,
    ) -> ReadPrincipalRoleBinding | None:
        rows = await fetch_all_document_hits(
            self.pr_binding_qry,
            filters={"$fields": {"principal_id": principal_id}},
        )

        for row in rows:
            if row.role_id == role_id:
                return row

        return None
