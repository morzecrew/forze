from uuid import UUID

import attrs

from forze.base.exceptions import exc

from .ports import TenantProviderPort

# ----------------------- #


def effective_tenant(*, bound: UUID | None, requested: UUID | None) -> UUID | None:
    """Reconcile a caller-supplied tenant with a bound one, or refuse the contradiction.

    The rule itself, free of how either value was obtained, so an adapter that carries its
    own provider (the mock stores) applies the same one as every adapter built on
    :class:`TenancyMixin`. See :meth:`TenancyMixin._effective_tenant` for why refusing beats
    preferring either side.
    """

    if requested is None:
        return bound

    if bound is not None and requested != bound:
        raise exc.authentication(
            "Requested tenant does not match the bound tenant.",
            code="tenant_mismatch",
        )

    return requested


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TenancyMixin:
    """Mixin to handle multi-tenancy."""

    tenant_aware: bool = False
    """Whether tenant ID is required for the class."""

    tenant_provider: TenantProviderPort | None = attrs.field(default=None)
    """Callable to provide the tenant ID."""

    # ....................... #

    def require_tenant_if_aware(self) -> UUID | None:
        if not self.tenant_aware:
            return None

        if self.tenant_provider is None:
            raise exc.configuration("Tenant provider is required")

        tenant = self.tenant_provider()

        if tenant is None:
            # Missing tenant on a tenant-aware adapter mirrors the
            # ``TenantRequired`` before-hook: the caller context lacks a bound
            # tenant identity, so it egresses as an authentication failure.
            raise exc.authentication("Tenant ID is required", code="tenant_required")

        return tenant.tenant_id

    # ....................... #

    def _effective_tenant(self, requested: UUID | None) -> UUID | None:
        """The one tenant an operation resolves against — its relation, its tag, its ids.

        An adapter method that accepts a caller-supplied tenant has two answers available
        to it, and the defect this exists to prevent is using each for a different half of
        the same write: tagging a row with the requested tenant while resolving the
        relation from the binding puts the row where nobody looks for it.

        A *contradiction* — an explicit tenant that differs from a bound one — is refused
        rather than resolved, because either resolution is wrong: preferring the request
        sanctions a cross-tenant write, and preferring the binding silently discards what
        the caller asked for. Where only one answer exists, that answer is used, so an
        unbound caller may still name the tenant it is writing for.

        Note the order: the bound tenant is read **first**, so a ``tenant_aware`` adapter
        with nothing bound fails closed on the missing binding before an explicitly passed
        tenant is ever considered. That read is the canonical contract and this does not
        carve an exception into it.
        """

        return effective_tenant(bound=self._tenant_id_for_resolve(), requested=requested)

    # ....................... #

    def _tenant_id_for_resolve(self) -> UUID | None:
        """Tenant id for per-tenant namespace / relation resolution.

        Returns the bound tenant id whenever one is present — so a dynamic per-tenant
        resolver (bucket / queue / index / collection) can scope itself even *without*
        tagged-tier ``tenant_aware`` (namespace-level isolation). When ``tenant_aware`` and no
        tenant is bound it fails closed with the same ``authentication`` /
        ``tenant_required`` error as :meth:`require_tenant_if_aware` — so every enforcement
        site is consistent. The single canonical implementation; adapters inherit it.
        """

        if self.tenant_aware:
            return self.require_tenant_if_aware()

        if self.tenant_provider is None:
            return None

        tenant = self.tenant_provider()

        return tenant.tenant_id if tenant is not None else None
