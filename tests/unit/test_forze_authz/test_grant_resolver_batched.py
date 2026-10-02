"""Grant resolution reads in batches and answers exactly as the row-by-row resolution did.

The resolver used to read every role, group and permission on its own, so one decision for an
administrator holding one role under a parent and 18 permissions ran 27 reads; it now runs 7.
The row-by-row algorithm is the oracle (``tests.support.authz_grants``): on random catalogs —
role hierarchies with shared ancestors, diamonds and cycles, active and inactive groups, and
bindings to rows that do not exist — both must grant the same roles and permissions, or fail
with the same error.
"""

from __future__ import annotations

import random
from typing import Any
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.authz import EffectiveGrants
from forze.base.exceptions import CoreException
from forze.testing import context_from_modules
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
from forze_identity.authz.domain.models.group import CreateGroupCmd
from forze_identity.authz.domain.models.permission_definition import (
    CreatePermissionDefinitionCmd,
)
from forze_identity.authz.domain.models.role_definition import CreateRoleDefinitionCmd
from forze_identity.authz.services.grants import (
    AuthzGrantResolver,
    AuthzGrantResolverDeps,
    fetch_all_document_hits,
)
from forze_mock import MockDepsModule
from tests.support.authz_grants import (
    create_group,
    grant_deps,
    oracle_assigned,
    oracle_grants,
    resolve_both_ways,
    wide_catalog,
)

pytestmark = pytest.mark.unit

# ----------------------- #
# A random catalog in the in-memory backend.


def _some(rnd: random.Random, pool: list[UUID], *, dangling: float = 0.0) -> list[UUID]:
    """A random subset of *pool*, now and then with an id nothing defines."""

    picked = rnd.sample(pool, rnd.randint(0, len(pool))) if pool else []

    if rnd.random() < dangling:
        picked.append(uuid4())

    return picked


async def _catalog(rnd: random.Random, ctx: Any, principal_id: UUID) -> None:
    """Roles (up to 45, past one ``$in`` batch), groups, permissions and the bindings among them.

    A dangling binding — to a role, parent, group or permission that does not exist — is rare
    on purpose, so most catalogs resolve and the rest fail.
    """

    cmd = ctx.doc.command
    big = rnd.random() < 0.2
    role_ids = [uuid4() for _ in range(rnd.randint(0, 45 if big else 10))]
    perm_ids = [uuid4() for _ in range(rnd.randint(0, 40 if big else 12))]
    group_ids = [uuid4() for _ in range(rnd.randint(0, 35 if big else 5))]

    for i, rid in enumerate(role_ids):
        roll = rnd.random()
        # Earlier roles make a hierarchy; any role a cycle or a diamond; rarely a parent no
        # role is.
        parent = (
            None
            if roll < 0.4
            else rnd.choice(role_ids[: i or 1])
            if roll < 0.85
            else rnd.choice(role_ids)
            if roll < 0.97
            else uuid4()
        )
        await cmd(role_definition_spec).create(
            CreateRoleDefinitionCmd(role_key=f"role-{i}", parent_role_id=parent), id=rid
        )

    for i, pid in enumerate(perm_ids):
        await cmd(permission_definition_spec).create(
            CreatePermissionDefinitionCmd(permission_key=f"perm-{i}"), id=pid
        )

    for i, gid in enumerate(group_ids):
        await create_group(ctx, f"group-{i}", active=rnd.random() < 0.7, id=gid)

    # Someone else's bindings, which the principal must not inherit.
    for who in (principal_id, uuid4()):
        for rid in _some(rnd, role_ids, dangling=0.03):
            await cmd(principal_role_binding_spec).create(
                CreatePrincipalRoleBindingCmd(principal_id=who, role_id=rid)
            )
        for pid in _some(rnd, perm_ids, dangling=0.03):
            await cmd(principal_permission_binding_spec).create(
                CreatePrincipalPermissionBindingCmd(principal_id=who, permission_id=pid)
            )
        for gid in _some(rnd, group_ids, dangling=0.03):
            await cmd(group_principal_binding_spec).create(
                CreateGroupPrincipalBindingCmd(group_id=gid, principal_id=who)
            )

    for rid in role_ids:
        for pid in _some(rnd, perm_ids[: rnd.randint(0, len(perm_ids))], dangling=0.02):
            await cmd(role_permission_binding_spec).create(
                CreateRolePermissionBindingCmd(role_id=rid, permission_id=pid)
            )

    for gid in group_ids:
        for rid in _some(rnd, role_ids[: rnd.randint(0, len(role_ids))], dangling=0.02):
            await cmd(group_role_binding_spec).create(
                CreateGroupRoleBindingCmd(group_id=gid, role_id=rid)
            )
        for pid in _some(rnd, perm_ids[: rnd.randint(0, len(perm_ids))], dangling=0.02):
            await cmd(group_permission_binding_spec).create(
                CreateGroupPermissionBindingCmd(group_id=gid, permission_id=pid)
            )


async def _in_a_granting_inactive_group(ctx: Any, principal_id: UUID) -> bool:
    """Whether *principal_id* belongs to an inactive group that grants a role or permission."""

    deps = grant_deps(ctx)

    for row in await fetch_all_document_hits(
        deps.gp_binding_qry, filters={"$values": {"principal_id": principal_id}}
    ):
        group = await deps.group_qry.find({"$values": {"id": row.group_id}})

        if group is None or group.is_active:
            continue

        for binding in (deps.gr_binding_qry, deps.gperm_binding_qry):
            if await binding.find({"$values": {"group_id": group.id}}) is not None:
                return True

    return False


async def _outcome(call: Any) -> Any:
    try:
        return await call
    except CoreException as error:
        return ("raised", error.kind, error.code)


# ----------------------- #


class TestTheBatchedResolutionAnswersAsTheRowByRowOne:
    async def test_on_random_catalogs(self) -> None:
        raised = resolved = inactive = 0

        for seed in range(300):
            rnd = random.Random(seed)
            ctx = context_from_modules(MockDepsModule())
            principal_id = uuid4()
            await _catalog(rnd, ctx, principal_id)
            deps = grant_deps(ctx)
            resolver = AuthzGrantResolver(deps=deps)

            got = await _outcome(resolver.resolve_effective_grants(principal_id))
            want = await _outcome(oracle_grants(deps, principal_id))

            if isinstance(got, EffectiveGrants):
                got = (got.roles, got.permissions)
                resolved += 1
                inactive += await _in_a_granting_inactive_group(ctx, principal_id)
            else:
                raised += 1

            assert got == want, f"seed {seed}"

            assigned = await _outcome(resolver.list_assigned_roles(principal_id))
            assert assigned == await _outcome(oracle_assigned(deps, principal_id)), f"seed {seed}"

        # Every leg ran: catalogs that resolve, ones that fail, and ones where an inactive
        # group's grants had to be left out.
        assert resolved > 100 and raised > 50 and inactive > 30, (resolved, raised, inactive)


# ----------------------- #


class _Counting:
    """A query port that counts the reads it serves: a ``get``, a non-empty ``get_many``, and a
    scan with each batch past its first (an empty scan still reads once)."""

    def __init__(self, inner: Any, reads: list[str], name: str) -> None:
        self._inner, self._reads, self._name = inner, reads, name

    async def get(self, pk: UUID, **kwargs: Any) -> Any:
        self._reads.append(f"{self._name}.get")
        return await self._inner.get(pk, **kwargs)

    async def get_many(self, pks: Any, **kwargs: Any) -> Any:
        if pks:
            self._reads.append(f"{self._name}.get_many")

        return await self._inner.get_many(pks, **kwargs)

    async def find_stream(self, **kwargs: Any) -> Any:
        self._reads.append(f"{self._name}.scan")
        first = True

        async for batch in self._inner.find_stream(**kwargs):
            if not first:
                self._reads.append(f"{self._name}.scan")

            first = False
            yield batch


def _counting(deps: AuthzGrantResolverDeps, reads: list[str]) -> AuthzGrantResolverDeps:
    return AuthzGrantResolverDeps(
        **{
            field: _Counting(getattr(deps, field), reads, field.removesuffix("_qry"))
            for field in (
                "permission_qry",
                "role_qry",
                "group_qry",
                "rp_binding_qry",
                "pr_binding_qry",
                "pp_binding_qry",
                "gp_binding_qry",
                "gr_binding_qry",
                "gperm_binding_qry",
            )
        }
    )


async def _admin(roles: int, permissions: int, groups: int) -> tuple[Any, UUID]:
    """A principal holding *roles* roles under one shared parent, which between them grant
    *permissions*, and a member of *groups* active groups that each hold a role and grant a
    permission."""

    ctx = context_from_modules(MockDepsModule())
    cmd = ctx.doc.command
    principal_id = uuid4()
    parent = await cmd(role_definition_spec).create(CreateRoleDefinitionCmd(role_key="root"))
    perm_ids = [
        (
            await cmd(permission_definition_spec).create(
                CreatePermissionDefinitionCmd(permission_key=f"perm-{i}")
            )
        ).id
        for i in range(permissions)
    ]
    role_ids = []

    for i in range(roles):
        role = await cmd(role_definition_spec).create(
            CreateRoleDefinitionCmd(role_key=f"role-{i}", parent_role_id=parent.id)
        )
        role_ids.append(role.id)
        await cmd(principal_role_binding_spec).create(
            CreatePrincipalRoleBindingCmd(principal_id=principal_id, role_id=role.id)
        )

        for pid in perm_ids[i::roles]:
            await cmd(role_permission_binding_spec).create(
                CreateRolePermissionBindingCmd(role_id=role.id, permission_id=pid)
            )

    for i in range(groups):
        group = await cmd(group_spec).create(CreateGroupCmd(group_key=f"group-{i}"))
        await cmd(group_principal_binding_spec).create(
            CreateGroupPrincipalBindingCmd(group_id=group.id, principal_id=principal_id)
        )
        await cmd(group_role_binding_spec).create(
            CreateGroupRoleBindingCmd(group_id=group.id, role_id=role_ids[i % len(role_ids)])
        )
        await cmd(group_permission_binding_spec).create(
            CreateGroupPermissionBindingCmd(group_id=group.id, permission_id=perm_ids[0])
        )

    return ctx, principal_id


class TestTheReadsDoNotGrowWithTheCatalog:
    @pytest.mark.parametrize(
        ("roles", "permissions", "groups"),
        [(1, 1, 0), (1, 18, 0), (25, 29, 0), (1, 1, 1), (25, 29, 20)],
    )
    async def test_one_read_per_kind_and_per_level(
        self, roles: int, permissions: int, groups: int
    ) -> None:
        ctx, principal_id = await _admin(roles, permissions, groups)
        reads: list[str] = []
        resolver = AuthzGrantResolver(deps=_counting(grant_deps(ctx), reads))

        grants = await resolver.resolve_effective_grants(principal_id)

        assert len(grants.permissions) == permissions
        assert sorted(reads) == sorted(
            [
                "gp_binding.scan",
                "pr_binding.scan",
                "role.get_many",  # the roles the principal holds
                "role.get_many",  # their parent
                "rp_binding.scan",
                "pp_binding.scan",
                "permission.get_many",
            ]
            + (["group.get_many", "gr_binding.scan", "gperm_binding.scan"] if groups else [])
        )

    async def test_the_row_by_row_resolution_read_once_per_row(self) -> None:
        # The baseline the batching replaces: the same administrator — one role under one
        # parent, 18 permissions — cost 27 reads where it now costs 7.
        ctx, principal_id = await _admin(1, 18, 0)
        reads: list[str] = []

        await oracle_grants(_counting(grant_deps(ctx), reads), principal_id)

        assert reads.count("permission.get") == 18
        assert len(reads) == 27

    async def test_past_one_in_batch_the_scans_split(self) -> None:
        # 31 roles: one more than an `$in` may name on Firestore.
        ctx, principal_id = await _admin(31, 1, 0)
        reads: list[str] = []

        grants = await AuthzGrantResolver(deps=_counting(grant_deps(ctx), reads)).resolve_effective_grants(
            principal_id
        )

        assert len(grants.roles) == 31
        assert reads.count("rp_binding.scan") == 2


class TestAWideCatalog:
    async def test_past_one_in_batch_on_every_axis(self) -> None:
        # The catalog the backend integration tests resolve, here on the in-memory backend.
        ctx = context_from_modules(MockDepsModule())
        principal_id, roles, permissions = await wide_catalog(ctx)

        assert await resolve_both_ways(ctx, principal_id) == (roles, permissions)
