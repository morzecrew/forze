"""What an export covers: one tenant, or the whole system."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal
from uuid import UUID

import attrs

from forze.base.exceptions import exc
from forze_kits.integrations.quiesce import QuiesceReport

# ----------------------- #

UNTENANTED: Literal["untenanted"] = "untenanted"
"""The explicit declaration that a full-system walk has no tenant dimension at all."""


# ....................... #


@attrs.frozen(kw_only=True)
class TenantScope:
    """Export one tenant's rows. The loop runs inside ``bind_identity(tenant=…)``, so tenancy's
    own fail-closed scoping does the filtering; consistency is the operator's claim that the
    tenant's traffic is drained or suspended, recorded in the manifest as ``consistency: tenant``.
    """

    tenant_id: UUID


# ....................... #


def _normalize_tenants(
    value: Sequence[UUID] | Literal["untenanted"],
) -> tuple[UUID, ...] | Literal["untenanted"]:
    return UNTENANTED if value == UNTENANTED else tuple(value)


@attrs.frozen(kw_only=True)
class FullScope:
    """Export the whole system, carrying the attestation that it was still while captured.

    The *procedure* that makes the attestation mean anything — stop the fleet, run a dedicated
    exporter, drain its own relay, quiesce (RFC 0017 §4.1) — is operational. This type carries the
    resulting :class:`QuiesceReport`, and export refuses to stamp ``consistency: quiesced`` from a
    report that did not attest.

    **The tenant dimension must be declared** — there is no default. A tenant-aware deployment
    resolves each tenant's partition only under that tenant's bound identity: an unbound
    full-system walk on the namespace or dedicated tier silently reads the *default* partition
    only (and raises on the tagged tier), so "walk unbound and hope" is exactly the
    looks-complete-and-is-not artifact this feature exists to prevent. Pass:

    - ``tenants=[...]`` — the complete tenant set (e.g. from
      ``TenantManagementPort.list_tenants``). The walk runs once per tenant, bound, and each
      tenant's rows land in their own archive section — complete on every tenancy tier.
    - ``tenants=UNTENANTED`` (the literal ``"untenanted"``) — the operator's declaration that the
      deployment runs without tenancy, so a single unbound walk is the whole system.
    """

    quiesce: QuiesceReport

    tenants: tuple[UUID, ...] | Literal["untenanted"] = attrs.field(converter=_normalize_tenants)
    """The full tenant set to walk (one bound pass each), or :data:`UNTENANTED` — the explicit
    declaration that the deployment has no tenants and one unbound pass covers everything."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.tenants != UNTENANTED and not self.tenants:
            raise exc.configuration(
                "FullScope was given an empty tenant set. Pass the complete tenant list "
                "(e.g. from TenantManagementPort.list_tenants), or declare "
                "tenants=UNTENANTED for a deployment that genuinely runs without tenancy — "
                "an empty list would export nothing while reporting success."
            )


# ....................... #

ExportScope = TenantScope | FullScope
"""What an export covers. The union is the stable API."""
