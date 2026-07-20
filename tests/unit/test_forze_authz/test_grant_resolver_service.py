"""Unit tests for :mod:`forze_identity.authz.services.grants`.

Exercises ``AuthzGrantResolver`` and the ``fetch_all_document_hits`` helper
against in-memory fake document query ports (no database), covering role
lineage expansion, principal/group permission union, inactive-group filtering,
and the pagination helper's boundaries.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.authz import AuthzScope
from forze.application.contracts.base.value_objects import CountlessPage
from forze.base.exceptions import CoreException
from forze_identity.authz.domain.models.bindings import (
    ReadGroupPermissionBinding,
    ReadGroupPrincipalBinding,
    ReadGroupRoleBinding,
    ReadPrincipalPermissionBinding,
    ReadPrincipalRoleBinding,
    ReadRolePermissionBinding,
)
from forze_identity.authz.domain.models.group import ReadGroup
from forze_identity.authz.domain.models.permission_definition import (
    ReadPermissionDefinition,
)
from forze_identity.authz.domain.models.role_definition import ReadRoleDefinition
from forze_identity.authz.services.grants import (
    AuthzGrantResolver,
    AuthzGrantResolverDeps,
    fetch_all_document_hits,
)

# ----------------------- #

_NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _doc_kwargs(_id: UUID) -> dict:
    return {"id": _id, "rev": 1, "created_at": _NOW, "last_update_at": _NOW}


# ....................... #


class FakeDocQuery:
    """In-memory document query port: filters rows by the ``$values`` map."""

    def __init__(
        self,
        rows: list | None = None,
        by_id: dict[UUID, object] | None = None,
    ) -> None:
        self._rows = rows or []
        self._by_id = by_id or {}

    async def get(self, doc_id: UUID):
        return self._by_id[doc_id]

    async def find_many(self, *, filters, pagination):
        values: dict = filters["$values"]
        limit = pagination["limit"]
        offset = pagination["offset"]

        matched = [
            r
            for r in self._rows
            if all(getattr(r, k) == v for k, v in values.items())
        ]
        return CountlessPage(hits=matched[offset : offset + limit], page=1, size=limit)


# ----------------------- #
# fetch_all_document_hits


class TestFetchAllDocumentHits:
    async def test_rejects_non_positive_page_size(self) -> None:
        with pytest.raises(CoreException, match="page_size must be positive"):
            await fetch_all_document_hits(
                FakeDocQuery(),
                filters={"$values": {}},
                page_size=0,
            )

    async def test_collects_across_pages(self) -> None:
        rows = [
            ReadPrincipalPermissionBinding(
                **_doc_kwargs(uuid4()),
                principal_id=UUID(int=1),
                permission_id=uuid4(),
            )
            for _ in range(5)
        ]
        qry = FakeDocQuery(rows=rows)

        out = await fetch_all_document_hits(
            qry,
            filters={"$values": {"principal_id": UUID(int=1)}},
            page_size=2,
        )
        assert len(out) == 5

    async def test_single_partial_page_stops_immediately(self) -> None:
        rows = [
            ReadPrincipalPermissionBinding(
                **_doc_kwargs(uuid4()),
                principal_id=UUID(int=1),
                permission_id=uuid4(),
            )
        ]
        out = await fetch_all_document_hits(
            FakeDocQuery(rows=rows),
            filters={"$values": {"principal_id": UUID(int=1)}},
            page_size=10,
        )
        assert len(out) == 1

    async def test_respects_max_pages(self) -> None:
        # 4 full pages of size 2 would loop; max_pages=1 trips on the 2nd iteration.
        rows = [
            ReadPrincipalPermissionBinding(
                **_doc_kwargs(uuid4()),
                principal_id=UUID(int=1),
                permission_id=uuid4(),
            )
            for _ in range(8)
        ]
        with pytest.raises(CoreException, match="exceeded max_pages"):
            await fetch_all_document_hits(
                FakeDocQuery(rows=rows),
                filters={"$values": {"principal_id": UUID(int=1)}},
                page_size=2,
                max_pages=1,
            )


# ----------------------- #
# AuthzGrantResolver fixtures


def _role(role_id: UUID, key: str, parent: UUID | None = None) -> ReadRoleDefinition:
    return ReadRoleDefinition(
        **_doc_kwargs(role_id),
        role_key=key,
        parent_role_id=parent,
    )


def _perm(perm_id: UUID, key: str) -> ReadPermissionDefinition:
    return ReadPermissionDefinition(**_doc_kwargs(perm_id), permission_key=key)


def _group(group_id: UUID, key: str, *, is_active: bool) -> ReadGroup:
    return ReadGroup(**_doc_kwargs(group_id), group_key=key, is_active=is_active)


def _empty_deps(**overrides) -> AuthzGrantResolverDeps:
    base = {
        "permission_qry": FakeDocQuery(),
        "role_qry": FakeDocQuery(),
        "group_qry": FakeDocQuery(),
        "rp_binding_qry": FakeDocQuery(),
        "pr_binding_qry": FakeDocQuery(),
        "pp_binding_qry": FakeDocQuery(),
        "gp_binding_qry": FakeDocQuery(),
        "gr_binding_qry": FakeDocQuery(),
        "gperm_binding_qry": FakeDocQuery(),
    }
    base.update(overrides)
    return AuthzGrantResolverDeps(**base)


# ----------------------- #


class TestAuthzGrantResolver:
    async def test_no_bindings_yields_empty_grants(self) -> None:
        resolver = AuthzGrantResolver(deps=_empty_deps())
        grants = await resolver.resolve_effective_grants(uuid4())
        assert grants.roles == frozenset()
        assert grants.permissions == frozenset()

    async def test_scope_tenant_mismatch_is_refused(self) -> None:
        """A scope naming a different tenant than the invocation is refused (the queries
        would otherwise run against the ambient tenant's bindings)."""

        invocation_tenant = uuid4()
        other_tenant = uuid4()
        resolver = AuthzGrantResolver(
            deps=_empty_deps(), invocation_tenant_id=invocation_tenant
        )

        with pytest.raises(CoreException) as ei:
            await resolver.resolve_effective_grants(
                uuid4(), scope=AuthzScope(tenant_id=other_tenant)
            )
        assert ei.value.code == "authz.scope_tenant_mismatch"

        with pytest.raises(CoreException) as ei2:
            await resolver.list_assigned_roles(
                uuid4(), scope=AuthzScope(tenant_id=other_tenant)
            )
        assert ei2.value.code == "authz.scope_tenant_mismatch"

    async def test_scope_tenant_match_is_allowed(self) -> None:
        tenant = uuid4()
        resolver = AuthzGrantResolver(
            deps=_empty_deps(), invocation_tenant_id=tenant
        )

        grants = await resolver.resolve_effective_grants(
            uuid4(), scope=AuthzScope(tenant_id=tenant)
        )
        assert grants.permissions == frozenset()

    async def test_no_invocation_tenant_skips_check(self) -> None:
        """Backward-compatible: without a bound invocation tenant the check is a no-op."""

        resolver = AuthzGrantResolver(deps=_empty_deps())  # invocation_tenant_id=None
        grants = await resolver.resolve_effective_grants(
            uuid4(), scope=AuthzScope(tenant_id=uuid4())
        )
        assert grants.roles == frozenset()

    async def test_direct_role_permissions_with_lineage(self) -> None:
        principal_id = uuid4()
        parent_role = _role(uuid4(), "admin")
        child_role = _role(uuid4(), "editor", parent=parent_role.id)
        parent_perm = _perm(uuid4(), "delete")
        child_perm = _perm(uuid4(), "write")

        deps = _empty_deps(
            role_qry=FakeDocQuery(
                by_id={parent_role.id: parent_role, child_role.id: child_role},
            ),
            permission_qry=FakeDocQuery(
                by_id={parent_perm.id: parent_perm, child_perm.id: child_perm},
            ),
            pr_binding_qry=FakeDocQuery(
                rows=[
                    ReadPrincipalRoleBinding(
                        **_doc_kwargs(uuid4()),
                        principal_id=principal_id,
                        role_id=child_role.id,
                    ),
                ],
            ),
            rp_binding_qry=FakeDocQuery(
                rows=[
                    ReadRolePermissionBinding(
                        **_doc_kwargs(uuid4()),
                        role_id=child_role.id,
                        permission_id=child_perm.id,
                    ),
                    ReadRolePermissionBinding(
                        **_doc_kwargs(uuid4()),
                        role_id=parent_role.id,
                        permission_id=parent_perm.id,
                    ),
                ],
            ),
        )

        grants = await AuthzGrantResolver(deps=deps).resolve_effective_grants(
            principal_id,
        )

        # Only the directly-assigned role is returned as a RoleRef...
        assert {r.role_key for r in grants.roles} == {"editor"}
        # ...but permissions come from the full lineage (child + parent role).
        assert {p.permission_key for p in grants.permissions} == {"write", "delete"}

    async def test_role_lineage_handles_cycle(self) -> None:
        # a -> b -> a : the resolver must terminate via its visited set.
        a_id, b_id = uuid4(), uuid4()
        role_a = _role(a_id, "a", parent=b_id)
        role_b = _role(b_id, "b", parent=a_id)

        deps = _empty_deps(role_qry=FakeDocQuery(by_id={a_id: role_a, b_id: role_b}))
        lineage = await AuthzGrantResolver(deps=deps)._expand_role_lineage(a_id)
        assert lineage == frozenset({a_id, b_id})

    async def test_direct_principal_permissions(self) -> None:
        principal_id = uuid4()
        perm = _perm(uuid4(), "read")

        deps = _empty_deps(
            permission_qry=FakeDocQuery(by_id={perm.id: perm}),
            pp_binding_qry=FakeDocQuery(
                rows=[
                    ReadPrincipalPermissionBinding(
                        **_doc_kwargs(uuid4()),
                        principal_id=principal_id,
                        permission_id=perm.id,
                    ),
                ],
            ),
        )
        grants = await AuthzGrantResolver(deps=deps).resolve_effective_grants(
            principal_id,
        )
        assert {p.permission_key for p in grants.permissions} == {"read"}

    async def test_inactive_group_grants_are_ignored(self) -> None:
        principal_id = uuid4()
        active_group = _group(uuid4(), "active", is_active=True)
        inactive_group = _group(uuid4(), "inactive", is_active=False)
        perm_active = _perm(uuid4(), "from-active")
        perm_inactive = _perm(uuid4(), "from-inactive")

        deps = _empty_deps(
            group_qry=FakeDocQuery(
                by_id={
                    active_group.id: active_group,
                    inactive_group.id: inactive_group,
                },
            ),
            permission_qry=FakeDocQuery(
                by_id={perm_active.id: perm_active, perm_inactive.id: perm_inactive},
            ),
            gp_binding_qry=FakeDocQuery(
                rows=[
                    ReadGroupPrincipalBinding(
                        **_doc_kwargs(uuid4()),
                        group_id=active_group.id,
                        principal_id=principal_id,
                    ),
                    ReadGroupPrincipalBinding(
                        **_doc_kwargs(uuid4()),
                        group_id=inactive_group.id,
                        principal_id=principal_id,
                    ),
                ],
            ),
            gperm_binding_qry=FakeDocQuery(
                rows=[
                    ReadGroupPermissionBinding(
                        **_doc_kwargs(uuid4()),
                        group_id=active_group.id,
                        permission_id=perm_active.id,
                    ),
                    ReadGroupPermissionBinding(
                        **_doc_kwargs(uuid4()),
                        group_id=inactive_group.id,
                        permission_id=perm_inactive.id,
                    ),
                ],
            ),
        )

        grants = await AuthzGrantResolver(deps=deps).resolve_effective_grants(
            principal_id,
        )
        assert {p.permission_key for p in grants.permissions} == {"from-active"}

    async def test_group_role_contributes_to_roles_and_perms(self) -> None:
        principal_id = uuid4()
        group = _group(uuid4(), "team", is_active=True)
        role = _role(uuid4(), "member")
        perm = _perm(uuid4(), "view")

        deps = _empty_deps(
            group_qry=FakeDocQuery(by_id={group.id: group}),
            role_qry=FakeDocQuery(by_id={role.id: role}),
            permission_qry=FakeDocQuery(by_id={perm.id: perm}),
            gp_binding_qry=FakeDocQuery(
                rows=[
                    ReadGroupPrincipalBinding(
                        **_doc_kwargs(uuid4()),
                        group_id=group.id,
                        principal_id=principal_id,
                    ),
                ],
            ),
            gr_binding_qry=FakeDocQuery(
                rows=[
                    ReadGroupRoleBinding(
                        **_doc_kwargs(uuid4()),
                        group_id=group.id,
                        role_id=role.id,
                    ),
                ],
            ),
            rp_binding_qry=FakeDocQuery(
                rows=[
                    ReadRolePermissionBinding(
                        **_doc_kwargs(uuid4()),
                        role_id=role.id,
                        permission_id=perm.id,
                    ),
                ],
            ),
        )

        resolver = AuthzGrantResolver(deps=deps)
        grants = await resolver.resolve_effective_grants(principal_id)
        assert {r.role_key for r in grants.roles} == {"member"}
        assert {p.permission_key for p in grants.permissions} == {"view"}

        # list_assigned_roles reports the same group-derived role.
        assigned = await resolver.list_assigned_roles(principal_id)
        assert {r.role_key for r in assigned} == {"member"}

    async def test_list_assigned_roles_empty(self) -> None:
        resolver = AuthzGrantResolver(deps=_empty_deps())
        assert await resolver.list_assigned_roles(uuid4()) == frozenset()
