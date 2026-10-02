"""Authorization catalogs for grant-resolution tests, and the row-by-row resolution as an oracle.

The resolver reads each kind of row in one batch. The oracle is the resolution it replaced — one
read per role, group and permission — so a test can ask both the same question on any backend.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from forze.application.contracts.authz import PermissionRef, RoleRef
from forze_identity.authz.application.specs import (
    group_permission_binding_spec,
    group_principal_binding_spec,
    group_role_binding_spec,
    group_spec,
    permission_definition_spec,
    principal_permission_binding_spec,
    principal_role_binding_spec,
    role_definition_spec,
    role_permission_binding_spec,
)
from forze_identity.authz.domain.models.bindings import (
    CreateGroupPermissionBindingCmd,
    CreateGroupPrincipalBindingCmd,
    CreateGroupRoleBindingCmd,
    CreatePrincipalPermissionBindingCmd,
    CreatePrincipalRoleBindingCmd,
    CreateRolePermissionBindingCmd,
)
from forze_identity.authz.domain.models.group import CreateGroupCmd, ReadGroup, UpdateGroupCmd
from forze_identity.authz.domain.models.permission_definition import (
    CreatePermissionDefinitionCmd,
)
from forze_identity.authz.domain.models.role_definition import CreateRoleDefinitionCmd
from forze_identity.authz.services.grants import (
    AuthzGrantResolver,
    AuthzGrantResolverDeps,
    fetch_all_document_hits,
)

GRANT_SPECS = (
    permission_definition_spec,
    role_definition_spec,
    group_spec,
    role_permission_binding_spec,
    principal_role_binding_spec,
    principal_permission_binding_spec,
    group_principal_binding_spec,
    group_role_binding_spec,
    group_permission_binding_spec,
)
"""The documents grant resolution reads."""


def grant_deps(ctx: Any) -> AuthzGrantResolverDeps:
    return AuthzGrantResolverDeps(
        permission_qry=ctx.doc.query(permission_definition_spec),
        role_qry=ctx.doc.query(role_definition_spec),
        group_qry=ctx.doc.query(group_spec),
        rp_binding_qry=ctx.doc.query(role_permission_binding_spec),
        pr_binding_qry=ctx.doc.query(principal_role_binding_spec),
        pp_binding_qry=ctx.doc.query(principal_permission_binding_spec),
        gp_binding_qry=ctx.doc.query(group_principal_binding_spec),
        gr_binding_qry=ctx.doc.query(group_role_binding_spec),
        gperm_binding_qry=ctx.doc.query(group_permission_binding_spec),
    )


# ----------------------- #
# The row-by-row resolution, as it was: one read per role, group and permission.


async def oracle_groups(deps: AuthzGrantResolverDeps, principal_id: UUID) -> list[UUID]:
    rows = await fetch_all_document_hits(
        deps.gp_binding_qry, filters={"$values": {"principal_id": principal_id}}
    )
    active: list[UUID] = []

    for row in rows:
        if (await deps.group_qry.get(row.group_id)).is_active:
            active.append(row.group_id)

    return active


async def oracle_direct_roles(deps: AuthzGrantResolverDeps, principal_id: UUID) -> set[UUID]:
    out = {
        row.role_id
        for row in await fetch_all_document_hits(
            deps.pr_binding_qry, filters={"$values": {"principal_id": principal_id}}
        )
    }

    for gid in await oracle_groups(deps, principal_id):
        out |= {
            row.role_id
            for row in await fetch_all_document_hits(
                deps.gr_binding_qry, filters={"$values": {"group_id": gid}}
            )
        }

    return out


async def oracle_lineage(deps: AuthzGrantResolverDeps, root: UUID) -> set[UUID]:
    lineage: set[UUID] = set()
    cur: UUID | None = root

    while cur is not None and cur not in lineage:
        lineage.add(cur)
        cur = (await deps.role_qry.get(cur)).parent_role_id

    return lineage


async def oracle_grants(
    deps: AuthzGrantResolverDeps, principal_id: UUID
) -> tuple[frozenset[RoleRef], frozenset[PermissionRef]]:
    direct = await oracle_direct_roles(deps, principal_id)
    expanded: set[UUID] = set()

    for rid in direct:
        expanded |= await oracle_lineage(deps, rid)

    perm_ids: set[UUID] = set()

    for rid in expanded:
        perm_ids |= {
            row.permission_id
            for row in await fetch_all_document_hits(
                deps.rp_binding_qry, filters={"$values": {"role_id": rid}}
            )
        }

    perm_ids |= {
        row.permission_id
        for row in await fetch_all_document_hits(
            deps.pp_binding_qry, filters={"$values": {"principal_id": principal_id}}
        )
    }

    for gid in await oracle_groups(deps, principal_id):
        perm_ids |= {
            row.permission_id
            for row in await fetch_all_document_hits(
                deps.gperm_binding_qry, filters={"$values": {"group_id": gid}}
            )
        }

    perms = set()

    for pid in perm_ids:
        row = await deps.permission_qry.get(pid)
        perms.add(PermissionRef(permission_id=row.id, permission_key=row.permission_key))

    roles = set()

    for rid in direct:
        row = await deps.role_qry.get(rid)
        roles.add(RoleRef(role_id=row.id, role_key=row.role_key))

    return frozenset(roles), frozenset(perms)


async def oracle_assigned(deps: AuthzGrantResolverDeps, principal_id: UUID) -> frozenset[RoleRef]:
    refs = set()

    for rid in await oracle_direct_roles(deps, principal_id):
        row = await deps.role_qry.get(rid)
        refs.add(RoleRef(role_id=row.id, role_key=row.role_key))

    return frozenset(refs)


# ----------------------- #


async def create_group(ctx: Any, key: str, *, active: bool, id: UUID | None = None) -> ReadGroup:
    """A group, deactivated after it is made when *active* is false.

    A create command carries no ``is_active``: one passed to it is dropped, and the group is
    active.
    """

    group = await ctx.doc.command(group_spec).create(CreateGroupCmd(group_key=key), id=id)

    if not active:
        group = await ctx.doc.command(group_spec).update(
            group.id, group.rev, UpdateGroupCmd(is_active=False)
        )

    return group


async def wide_catalog(ctx: Any) -> tuple[UUID, set[str], set[str]]:
    """A principal past one ``$in`` batch on every axis, and the role and permission keys it holds.

    35 roles under 3 parents under one root; 33 groups, 2 of them inactive, each holding a role
    and granting a permission of its own; a second principal whose bindings must not leak in.
    Every role, group and permission is the only source of something held, so a read that drops
    any batch on any axis — roles, lineage levels, groups, their roles or permissions — changes
    the answer. What only an inactive group grants is not held.
    """

    cmd = ctx.doc.command
    principal_id, someone = uuid4(), uuid4()
    roles: set[str] = set()
    perms: set[str] = set()

    async def role(key: str, parent: UUID | None = None, *, held: bool = True) -> UUID:
        """A role granting a permission of its own."""

        row = await cmd(role_definition_spec).create(
            CreateRoleDefinitionCmd(role_key=key, parent_role_id=parent)
        )
        await cmd(role_permission_binding_spec).create(
            CreateRolePermissionBindingCmd(
                role_id=row.id, permission_id=await permission(f"{key}-perm", held=held)
            )
        )

        return row.id

    async def permission(key: str, *, held: bool = True) -> UUID:
        row = await cmd(permission_definition_spec).create(
            CreatePermissionDefinitionCmd(permission_key=key)
        )
        perms.update({key} if held else set())

        return row.id

    root = await role("root")
    parents = [await role(f"parent-{i}", root) for i in range(3)]

    for i in range(35):
        await cmd(principal_role_binding_spec).create(
            CreatePrincipalRoleBindingCmd(
                principal_id=principal_id, role_id=await role(f"role-{i}", parents[i % 3])
            )
        )
        roles.add(f"role-{i}")

    # Granted directly, and to someone else only.
    await cmd(principal_permission_binding_spec).create(
        CreatePrincipalPermissionBindingCmd(
            principal_id=principal_id, permission_id=await permission("direct")
        )
    )
    await cmd(principal_permission_binding_spec).create(
        CreatePrincipalPermissionBindingCmd(
            principal_id=someone, permission_id=await permission("someone-elses", held=False)
        )
    )

    for i in range(33):
        active = i < 31
        key = f"group-{i}" if active else f"inactive-group-{i}"
        group = await create_group(ctx, key, active=active)
        await cmd(group_principal_binding_spec).create(
            CreateGroupPrincipalBindingCmd(group_id=group.id, principal_id=principal_id)
        )
        await cmd(group_role_binding_spec).create(
            CreateGroupRoleBindingCmd(
                group_id=group.id, role_id=await role(f"{key}-role", held=active)
            )
        )
        await cmd(group_permission_binding_spec).create(
            CreateGroupPermissionBindingCmd(
                group_id=group.id, permission_id=await permission(f"{key}-perm", held=active)
            )
        )
        roles.update({f"{key}-role"} if active else set())

    return principal_id, roles, perms


async def resolve_both_ways(
    ctx: Any, principal_id: UUID
) -> tuple[set[str], set[str]]:
    """The role and permission keys *principal_id* holds, after checking that the batched and
    the row-by-row resolution agree on them."""

    deps = grant_deps(ctx)
    grants = await AuthzGrantResolver(deps=deps).resolve_effective_grants(principal_id)
    roles, permissions = await oracle_grants(deps, principal_id)

    if (grants.roles, grants.permissions) != (roles, permissions):
        raise AssertionError("the batched and the row-by-row resolution disagree")

    return {r.role_key for r in roles}, {p.permission_key for p in permissions}
