"""Postgres governed dynamic-read execution config."""

from datetime import timedelta

import attrs

from forze.application.contracts.dynamic_read import DynamicReadProvenance
from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_optional_named_resource_spec,
)
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresDynamicReadConfig(TenantAwareIntegrationConfig):
    """Physical Postgres mapping for one governed dynamic-read route.

    The route, not the statement: every statement executed through it runs in a ``READ ONLY``
    transaction, under this timeout, in this schema, as this role.

    Safe-by-default is a design requirement here rather than a preference — an unsafe default
    on the plane that executes runtime-authored SQL is the kind of thing found three months
    later. So :attr:`provenance` has **no default** (an author must name their threat tier to
    wire the route at all) and :attr:`statement_timeout` ships on with a real value. The tier
    guards themselves live on :class:`~forze_postgres.execution.deps.module.PostgresDepsModule`,
    where the client's routing is known — a dedicated-tier deployment satisfies them with
    credentials rather than with anything written here.
    """

    provenance: DynamicReadProvenance
    """Who authored the statements this route executes. Mandatory; see
    :data:`~forze.application.contracts.dynamic_read.DynamicReadProvenance`."""

    query_schema: NamedResourceSpec | None = attrs.field(
        default=None,
        converter=coerce_optional_named_resource_spec,
    )
    """Per-tenant query schema (namespace tier) — a static name or ``(tenant_id) -> str``
    resolver. Applied as ``SET LOCAL search_path`` so an unqualified relation resolves in the
    tenant's own schema.

    Routing, not confinement: a statement can always schema-qualify ``other.table`` explicitly.
    It is the *correctness* mechanism for trusted statements (unqualified names land in the
    right project schema), and it is why the namespace tier is admissible here at all — the
    same authoring mistake that silently leaks on the tagged tier either fails loudly
    (undefined relation) or stays inside the tenant's container."""

    role: NamedResourceSpec | None = attrs.field(
        default=None,
        converter=coerce_optional_named_resource_spec,
    )
    """Per-route (or per-tenant) role applied as ``SET LOCAL ROLE`` before the statement.

    **Mistake-proofing plus defense in depth, not an adversarial boundary.** A ``NOLOGIN`` role
    holding ``USAGE`` on exactly the tenant's schema blocks cross-schema reads for any statement
    that simply *references* the wrong relation — the entire mistake class a non-adversarial
    generator produces. Against a deliberately crafted statement it is porous, and the reason is
    structural rather than a missing feature: on a shared connection the statement and the
    adapter wield the same identity, so a statement can ``set_config('role', …)`` mid-query and
    reach whatever the connection user is a member of — which the adapter's own ``SET LOCAL
    ROLE`` requires it to be a member of. What survives every such gadget is the read-only
    transaction, which is sticky for the transaction's lifetime. An author who must withstand a
    hostile statement uses a routed (dedicated) client instead.

    The connection user must be a member of the role; a missing role or membership surfaces as
    a configuration error rather than a raw ``42704``/``42501``."""

    statement_timeout: timedelta = attrs.field(default=timedelta(seconds=5))
    """``SET LOCAL statement_timeout`` for every statement on this route. Always on; a per-call
    option may clamp it down, never up."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.statement_timeout.total_seconds() <= 0:
            raise exc.configuration(
                "Postgres dynamic-read statement_timeout must be positive.",
                code="dynamic_read_timeout_invalid",
                details={"statement_timeout": str(self.statement_timeout)},
            )
