"""Resolve effective grants from catalog documents and binding edges."""

from collections.abc import Collection, Sequence
from contextlib import aclosing
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.authz import (
    AuthzScope,
    DerivedPermissionRef,
    EffectiveGrants,
    PermissionProvider,
    PermissionRef,
    RoleRef,
)
from forze.application.contracts.document import DocumentQueryPort
from forze.application.contracts.querying import QueryFilterExpression
from forze.application.integrations.authz import (
    DEFAULT_PROVIDER_TIMEOUT,
    ConfigGrantsProvider,
    derive_permissions,
)
from forze.application.integrations.document._limits import (
    DEFAULT_MAX_FETCH_ALL_PAGES,
    check_page_limit,
)
from forze.base.exceptions import exc

from .._logger import logger
from ..domain.models.bindings import (
    ReadGroupPermissionBinding,
    ReadGroupPrincipalBinding,
    ReadGroupRoleBinding,
    ReadPrincipalPermissionBinding,
    ReadPrincipalRoleBinding,
    ReadRolePermissionBinding,
)
from ..domain.models.group import ReadGroup
from .grants_cache import CatalogGrants, GrantsCache, GrantsCacheKey

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext
from ..domain.models.permission_definition import ReadPermissionDefinition
from ..domain.models.role_definition import ReadRoleDefinition

# ----------------------- #


@attrs.define(frozen=True, slots=True)
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
    max_pages: int | None = DEFAULT_MAX_FETCH_ALL_PAGES,
) -> list[R]:
    """Every row of *qry* matching *filters*, read in keyset batches of *page_size*.

    By cursor, not offset: Firestore refuses an offset past the first page, and an offset scan
    over rows written meanwhile skips or repeats some.

    *max_pages* is this scan's own limit, checked as each batch arrives; ``None`` sets none.
    The query adapter's stream limit (its ``max_stream_pages``) applies either way.

    :raises CoreException: ``precondition`` for a non-positive *page_size*, or once more than
        *max_pages* batches have been read.
    """

    if page_size < 1:
        raise exc.precondition("page_size must be positive")

    hits: list[R] = []
    page_num = 0

    async with aclosing(qry.find_stream(filters=filters, chunk_size=page_size)) as batches:
        async for batch in batches:
            check_page_limit(
                pages=page_num,
                max_pages=max_pages,
                label="fetch_all_document_hits",
            )
            hits.extend(batch)
            page_num += 1

    return hits


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthzGrantResolver:
    """Computes effective grants from bindings (principal, group, role inheritance)."""

    deps: AuthzGrantResolverDeps

    invocation_tenant_id: UUID | None = None
    """The tenant bound on the invocation context, for a resolver built without :attr:`ctx`
    (the tenant the storage layer auto-scopes binding queries to). Used only as a
    defense-in-depth cross-check: a caller-supplied :class:`AuthzScope` naming a *different*
    tenant is refused rather than silently resolved against the ambient tenant's bindings.
    ``None`` disables the check (the historical behavior, and correct for untenanted /
    single-tenant use). With :attr:`ctx`, the tenant is read from it on every call instead."""

    providers: tuple[PermissionProvider, ...] = ()
    """Permission providers run after the catalog grants, in declaration order."""

    ctx: "ExecutionContext | None" = None
    """The execution context a provider reads through; required when there are providers."""

    provider_timeout: timedelta | None = DEFAULT_PROVIDER_TIMEOUT
    """How long one provider's ``derive`` may take before it counts as failed."""

    key_check: "ProviderKeyCheck | None" = None
    """Checks the providers' declared keys against the catalog once per tenant."""

    cache: GrantsCache | None = None
    """Remembers the catalog part of :meth:`resolve_effective_grants`; ``None`` caches nothing.
    Needs :attr:`ctx`, which the key's tenant and the transaction state are read from."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        # Without a context the key's tenant would be a fixed one while the reads follow the
        # request's tenant: one tenant's entry would answer for another.
        if self.cache is not None and self.ctx is None:
            raise exc.configuration("AuthzGrantResolver.cache needs a ctx to key entries by tenant")

    # ....................... #

    def _invocation_tenant(self) -> UUID | None:
        # Read on every call: the runtime caches a built resolver for the whole process, so
        # the tenant bound when it was built is only the first request's.
        if self.ctx is None:
            return self.invocation_tenant_id

        tenant = self.ctx.inv_ctx.get_tenant()

        return tenant.tenant_id if tenant is not None else None

    # ....................... #

    def _require_scope_matches_invocation(self, scope: AuthzScope | None) -> None:
        """Fail closed when a requested scope tenant disagrees with the invocation tenant.

        Tenant isolation of grant resolution is enforced by the storage layer scoping
        binding queries to the ambient invocation tenant. This adds a second, independent
        layer: if the caller passes a scope for a tenant other than the one the queries
        will actually run under, refuse — the grants would otherwise come from the ambient
        tenant while appearing to answer for the requested one. Only fires when both
        tenants are present; a bare scope or an untenanted invocation is left untouched.
        """

        if scope is None or scope.tenant_id is None:
            return

        invocation_tenant_id = self._invocation_tenant()

        if invocation_tenant_id is not None and scope.tenant_id != invocation_tenant_id:
            raise exc.internal(
                "AuthzScope.tenant_id disagrees with the invocation tenant; refusing to "
                "resolve grants against a different tenant's bindings.",
                code="authz.scope_tenant_mismatch",
            )

    # ....................... #

    async def _expand_role_lineage(
        self, role_ids: Collection[UUID]
    ) -> dict[UUID, ReadRoleDefinition]:
        """Each of *role_ids* and its ancestors via ``parent_role_id``, keyed by id.

        One read per level of the hierarchy, whatever the number of roles: an ancestor two roles
        share is read once.
        """

        rows: dict[UUID, ReadRoleDefinition] = {}
        level = set(role_ids)

        while level:
            batch = await _get_many(self.deps.role_qry, level)
            rows.update((row.id, row) for row in batch)
            level = {row.parent_role_id for row in batch if row.parent_role_id is not None}
            level -= rows.keys()

        return rows

    # ....................... #

    async def list_assigned_roles(
        self,
        principal_id: UUID,
        *,
        scope: AuthzScope | None = None,
    ) -> frozenset[RoleRef]:
        """Roles from principal-role and group-role bindings (no lineage expansion)."""

        self._require_scope_matches_invocation(scope)

        group_ids = await self._active_member_group_ids(principal_id)
        direct_ids = await self._direct_role_ids(principal_id, group_ids)

        return frozenset(
            RoleRef(role_id=row.id, role_key=row.role_key)
            for row in await _get_many(self.deps.role_qry, direct_ids)
        )

    # ....................... #

    async def resolve_effective_grants(
        self,
        principal_id: UUID,
        *,
        scope: AuthzScope | None = None,
    ) -> EffectiveGrants:
        """Union permissions from expanded roles, direct principal and group grants.

        Each kind of row is read in batches rather than one at a time: the reads grow with the
        depth of the role hierarchy, and with the number of roles or groups only past 30 of them.
        With :attr:`cache`, the roles and permissions come from it when present, outside a
        transaction; the derived permissions are asked of the providers every time.
        """

        self._require_scope_matches_invocation(scope)

        cache = self.cache

        # Inside a transaction the bindings may include its own uncommitted writes, which a
        # savepoint can still roll back: read them, and neither serve nor keep an entry.
        if cache is None or self.ctx is None or self.ctx.tx_ctx.depth() > 0:
            catalog = await self._catalog_grants(principal_id)

        else:
            # The tenant is read on every call: one entry never answers for another tenant.
            key: GrantsCacheKey = (principal_id, self._invocation_tenant(), scope)
            cached = cache.get(key)

            if cached is None:
                epoch = cache.epoch
                catalog = await self._catalog_grants(principal_id)
                cache.put(key, catalog, epoch=epoch)

            else:
                catalog = cached

        roles, permissions = catalog

        return EffectiveGrants(
            roles=roles,
            permissions=permissions,
            derived=await self._derive(principal_id),
        )

    # ....................... #

    async def forget(self, principal_id: UUID) -> None:
        """Drop *principal_id*'s cached grants when the current transaction commits, or now.

        Until the change commits, the committed bindings are still the truth a decision outside
        the transaction may read and cache; decisions inside it never use the cache. A decision
        that read before the forget does not store what it read. No-op without :attr:`cache`.
        """

        cache = self.cache

        if cache is None or self.ctx is None:
            return

        async def _forget() -> None:
            cache.forget(principal_id)

        await self.ctx.tx_ctx.run_or_defer(_forget)

    # ....................... #

    async def _catalog_grants(self, principal_id: UUID) -> CatalogGrants:
        """Roles and permissions from the catalog bindings, read in batches."""

        deps = self.deps

        group_ids = await self._active_member_group_ids(principal_id)
        direct_role_ids = await self._direct_role_ids(principal_id, group_ids)
        roles = await self._expand_role_lineage(direct_role_ids)

        permission_ids = {
            row.permission_id
            for row in await _fetch_where_in(deps.rp_binding_qry, "role_id", sorted(roles))
        }
        permission_ids.update(
            row.permission_id
            for row in await fetch_all_document_hits(
                deps.pp_binding_qry,
                filters={"$values": {"principal_id": principal_id}},
            )
        )
        permission_ids.update(
            row.permission_id
            for row in await _fetch_where_in(deps.gperm_binding_qry, "group_id", group_ids)
        )

        permissions = await _get_many(deps.permission_qry, permission_ids)

        return (
            frozenset(
                RoleRef(role_id=roles[rid].id, role_key=roles[rid].role_key)
                for rid in direct_role_ids
            ),
            frozenset(
                PermissionRef(permission_id=row.id, permission_key=row.permission_key)
                for row in permissions
            ),
        )

    # ....................... #

    async def _derive(self, principal_id: UUID) -> frozenset[DerivedPermissionRef]:
        if self.providers and self.key_check is not None:
            await self.key_check.ensure(self.deps, self._invocation_tenant())

        return await derive_permissions(
            self.providers, principal_id, self.ctx, timeout=self.provider_timeout
        )

    # ....................... #

    async def _direct_role_ids(self, principal_id: UUID, group_ids: Sequence[UUID]) -> set[UUID]:
        """Role ids from principal-role bindings plus group-role bindings of *group_ids*."""

        deps = self.deps

        out = {
            row.role_id
            for row in await fetch_all_document_hits(
                deps.pr_binding_qry,
                filters={"$values": {"principal_id": principal_id}},
            )
        }
        out.update(
            row.role_id for row in await _fetch_where_in(deps.gr_binding_qry, "group_id", group_ids)
        )

        return out

    # ....................... #

    async def _active_member_group_ids(self, principal_id: UUID) -> list[UUID]:
        """The active groups *principal_id* belongs to, sorted.

        Every group a binding names is read, active or not, so a binding to a missing group
        fails the resolution.
        """

        rows = await fetch_all_document_hits(
            self.deps.gp_binding_qry,
            filters={"$values": {"principal_id": principal_id}},
        )
        groups = await _get_many(self.deps.group_qry, {row.group_id for row in rows})

        return [group.id for group in groups if group.is_active]


# ....................... #

_IN_BATCH: Final = 30
"""Values in one ``$in``: Firestore's limit, the smallest of any backend, so a batch runs on
every one."""


async def _get_many[R: BaseModel](
    query: DocumentQueryPort[R], ids: Collection[UUID]
) -> Sequence[R]:
    # Sorted, so the same ids make the same statement; a missing id raises not-found.
    return await query.get_many(sorted(ids))


async def _fetch_where_in[R: BaseModel](
    query: DocumentQueryPort[R], field: str, values: Sequence[Any]
) -> list[R]:
    # In batches: a query may name at most _IN_BATCH values in one $in. One size for every
    # backend costs Postgres a read per 30 values; ask the port for its own limit if a
    # principal with hundreds of roles or groups ever makes that matter.
    rows: list[R] = []

    for first in range(0, len(values), _IN_BATCH):
        rows += await fetch_all_document_hits(
            query,
            filters={"$values": {field: {"$in": list(values[first : first + _IN_BATCH])}}},
        )

    return rows


async def check_declared_keys(
    query: DocumentQueryPort[ReadPermissionDefinition],
    providers: Sequence[PermissionProvider],
) -> None:
    """Refuse when a provider declares a key the permission catalog behind *query* lacks.

    A misspelt declared key would make the provider's denial of it do nothing, while the
    catalog's grant of the real key stands.

    :raises CoreException: ``configuration`` (``authz_provider_unknown_keys``), naming each
        provider and its missing keys.
    """

    declared = sorted({key for provider in providers for key in provider.keys})

    if not declared:
        return

    known = {row.permission_key for row in await _fetch_where_in(query, "permission_key", declared)}

    missing = {
        provider.name: sorted(provider.keys - known)
        for provider in providers
        if provider.keys - known
    }

    if missing:
        raise exc.configuration(
            f"Permission providers declare keys the permission catalog does not define: "
            f"{missing}. A typo here would deny forever; define the permissions or fix "
            "the keys.",
            code="authz_provider_unknown_keys",
        )


async def find_config_grant_overlap(
    query: DocumentQueryPort[ReadPermissionDefinition],
    bindings: Sequence[DocumentQueryPort[Any]],
    providers: Sequence[PermissionProvider],
) -> list[str]:
    """The keys a :class:`~forze.application.integrations.authz.ConfigGrantsProvider` owns that
    any of the permission *bindings* (role, principal or group) also grants, sorted."""

    owned = sorted(
        {
            key
            for provider in providers
            if isinstance(provider, ConfigGrantsProvider)
            for key in provider.keys
        }
    )

    if not owned:
        return []

    keys = {
        row.id: row.permission_key for row in await _fetch_where_in(query, "permission_key", owned)
    }
    ids = sorted(keys, key=str)
    bound: set[str] = set()

    for binding in bindings:
        bound |= {
            keys[row.permission_id] for row in await _fetch_where_in(binding, "permission_id", ids)
        }

    return sorted(bound)


async def check_config_grant_overlap(
    query: DocumentQueryPort[ReadPermissionDefinition],
    bindings: Sequence[DocumentQueryPort[Any]],
    providers: Sequence[PermissionProvider],
) -> None:
    """Refuse when :func:`find_config_grant_overlap` finds a key.

    Configuration and the catalog would otherwise be two sources of truth for one permission.
    The config provider denies its keys to every principal it does not list, so the binding grants
    nothing; this makes the contradiction a startup error instead of a silent no-op.

    :raises CoreException: ``configuration`` (``authz_config_grant_overlap``), naming the keys.
    """

    if bound := await find_config_grant_overlap(query, bindings, providers):
        raise exc.configuration(
            f"Permission keys {bound} are granted by configuration and also through the "
            "permission catalog. A key the configuration owns has one source: remove its catalog "
            "bindings, or drop it from the configuration.",
            code="authz_config_grant_overlap",
        )


async def check_provider_catalog(
    query: DocumentQueryPort[ReadPermissionDefinition],
    bindings: Sequence[DocumentQueryPort[Any]],
    providers: Sequence[PermissionProvider],
) -> None:
    """:func:`check_declared_keys`, then :func:`check_config_grant_overlap`."""

    await check_declared_keys(query, providers)
    await check_config_grant_overlap(query, bindings, providers)


@attrs.define(slots=True, kw_only=True)
class ProviderKeyCheck:
    """:func:`check_declared_keys`, run per tenant per process until it first succeeds.

    The lifecycle step fails at boot, but only where a deployment registers it; this makes the
    check impossible to leave out. A failure is not remembered, so every decision refuses until
    the keys are fixed. A key a config provider owns that a binding also grants is logged
    (``authz.config_grant_overlap``) rather than refused, when the tenant's check runs; a binding
    written afterwards is not seen until the next start. First decisions that
    overlap may each run the check: it is a read, so
    running it twice only costs a query, where a lock would serialize them.
    """

    providers: tuple[PermissionProvider, ...]

    _verified: set[UUID | None] = attrs.field(factory=set[UUID | None], init=False)

    async def ensure(self, deps: AuthzGrantResolverDeps, tenant_id: UUID | None) -> None:
        if not self.providers or tenant_id in self._verified:
            return

        await check_declared_keys(deps.permission_qry, self.providers)

        # Logged, not refused: the config provider's denial already makes such a binding grant
        # nothing, and refusing here would let anyone who may write a binding stop every
        # decision in the tenant. The startup step refuses it.
        if bound := await find_config_grant_overlap(
            deps.permission_qry,
            (deps.rp_binding_qry, deps.pp_binding_qry, deps.gperm_binding_qry),
            self.providers,
        ):
            logger.error("authz.config_grant_overlap", keys=bound, tenant_id=tenant_id)

        self._verified.add(tenant_id)
