"""Resolve effective grants from catalog documents and binding edges."""

from dataclasses import dataclass
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.authz import EffectiveGrants, PermissionRef, RoleRef
from forze.application.contracts.document import DocumentQueryPort
from forze.application.contracts.query import QueryFilterExpression

from ..domain.models.bindings import (
    ReadGroupPermissionBinding,
    ReadGroupPrincipalBinding,
    ReadGroupRoleBinding,
    ReadPrincipalPermissionBinding,
    ReadPrincipalRoleBinding,
    ReadRolePermissionBinding,
)
from ..domain.models.group import ReadGroup
from ..domain.models.permission_definition import ReadPermissionDefinition
from ..domain.models.role_definition import ReadRoleDefinition

# ----------------------- #


@dataclass(frozen=True, slots=True)
class AuthzGrantResolverDeps:
    """Document query ports required to compute grants."""

    permission_qry: DocumentQueryPort[ReadPermissionDefinition]
    role_qry: DocumentQueryPort[ReadRoleDefinition]
    group_qry: DocumentQueryPort[ReadGroup]
    rp_binding_qry: DocumentQueryPort[ReadRolePermissionBinding]
    pr_binding_qry: DocumentQueryPort[ReadPrincipalRoleBinding]
    pp_binding_qry: DocumentQueryPort[ReadPrincipalPermissionBinding]
    gp_binding_qry: DocumentQueryPort[ReadGroupPrincipalBinding]
    gr_binding_qry: DocumentQueryPort[ReadGroupRoleBinding]
    gperm_binding_qry: DocumentQueryPort[ReadGroupPermissionBinding]


# ....................... #


async def fetch_all_document_hits[R: BaseModel](
    qry: DocumentQueryPort[R],
    *,
    filters: QueryFilterExpression,  # type: ignore[valid-type]
    page_size: int = 500,
) -> list[R]:
    hits: list[R] = []
    offset = 0

    while True:
        page = await qry.find_many(
            filters=filters,
            pagination={"limit": page_size, "offset": offset},
        )
        hits.extend(page.hits)

        if len(page.hits) < page_size:
            break

        offset += page_size

    return hits


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthzGrantResolver:
    """Computes effective grants from bindings (principal, group, role inheritance)."""

    deps: AuthzGrantResolverDeps

    # ....................... #

    async def _expand_role_lineage(self, root_role_id: UUID) -> frozenset[UUID]:
        """Include ``root_role_id`` and ancestors via ``parent_role_id``."""

        lineage: set[UUID] = set()
        visited: set[UUID] = set()
        cur: UUID | None = root_role_id

        while cur is not None and cur not in visited:
            visited.add(cur)
            lineage.add(cur)

            row = await self.deps.role_qry.get(cur)
            cur = row.parent_role_id

        return frozenset(lineage)

    # ....................... #

    async def list_assigned_roles(
        self,
        principal_id: UUID,
        *,
        tenant_id: UUID | None = None,
    ) -> frozenset[RoleRef]:
        """Roles from principal-role and group-role bindings (no lineage expansion)."""

        _ = tenant_id

        direct_ids = await self._direct_role_ids(principal_id)

        refs: dict[UUID, RoleRef] = {}

        for rid in direct_ids:
            row = await self.deps.role_qry.get(rid)
            refs[rid] = RoleRef(role_id=row.id, role_key=row.role_key)

        return frozenset(refs.values())

    # ....................... #

    async def resolve_effective_grants(
        self,
        principal_id: UUID,
        *,
        tenant_id: UUID | None = None,
    ) -> EffectiveGrants:
        """Union permissions from expanded roles, direct principal and group grants."""

        _ = tenant_id

        deps = self.deps

        direct_role_ids = await self._direct_role_ids(principal_id)

        expanded_role_ids: set[UUID] = set()

        for rid in direct_role_ids:
            lineage = await self._expand_role_lineage(rid)
            expanded_role_ids.update(lineage)

        permission_ids: set[UUID] = set()

        for rid in expanded_role_ids:
            rp_rows = await fetch_all_document_hits(
                deps.rp_binding_qry,
                filters={"$fields": {"role_id": rid}},
            )

            for rb in rp_rows:
                permission_ids.add(rb.permission_id)

        pp_rows = await fetch_all_document_hits(
            deps.pp_binding_qry,
            filters={"$fields": {"principal_id": principal_id}},
        )

        for pb in pp_rows:
            permission_ids.add(pb.permission_id)

        group_ids = await self._active_member_group_ids(principal_id)

        for gid in group_ids:
            gp_rows = await fetch_all_document_hits(
                deps.gperm_binding_qry,
                filters={"$fields": {"group_id": gid}},
            )

            for gb in gp_rows:
                permission_ids.add(gb.permission_id)

        perm_refs: dict[UUID, PermissionRef] = {}

        for pid in permission_ids:
            perm_row = await deps.permission_qry.get(pid)

            perm_refs[pid] = PermissionRef(
                permission_id=perm_row.id,
                permission_key=perm_row.permission_key,
            )

        role_refs: dict[UUID, RoleRef] = {}

        for rid in direct_role_ids:
            role_row = await deps.role_qry.get(rid)

            role_refs[rid] = RoleRef(role_id=role_row.id, role_key=role_row.role_key)

        return EffectiveGrants(
            roles=frozenset(role_refs.values()),
            permissions=frozenset(perm_refs.values()),
        )

    # ....................... #

    async def _direct_role_ids(self, principal_id: UUID) -> set[UUID]:
        """Role ids from principal-role bindings plus group-role bindings for member groups."""

        deps = self.deps

        out: set[UUID] = set()

        pr_rows = await fetch_all_document_hits(
            deps.pr_binding_qry,
            filters={"$fields": {"principal_id": principal_id}},
        )

        for pr in pr_rows:
            out.add(pr.role_id)

        group_ids = await self._active_member_group_ids(principal_id)

        for gid in group_ids:
            gr_rows = await fetch_all_document_hits(
                deps.gr_binding_qry,
                filters={"$fields": {"group_id": gid}},
            )

            for gr in gr_rows:
                out.add(gr.role_id)

        return out

    # ....................... #

    async def _active_member_group_ids(self, principal_id: UUID) -> list[UUID]:
        deps = self.deps

        gp_rows = await fetch_all_document_hits(
            deps.gp_binding_qry,
            filters={"$fields": {"principal_id": principal_id}},
        )

        active: list[UUID] = []

        for row in gp_rows:
            g = await deps.group_qry.get(row.group_id)

            if g.is_active:
                active.append(row.group_id)

        return active
