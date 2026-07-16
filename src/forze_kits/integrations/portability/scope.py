"""What an export covers: one tenant, or the whole system."""

from __future__ import annotations

from uuid import UUID

import attrs

from forze_kits.integrations.quiesce import QuiesceReport

# ----------------------- #


@attrs.frozen(kw_only=True)
class TenantScope:
    """Export one tenant's rows. The loop runs inside ``bind_identity(tenant=…)``, so tenancy's
    own fail-closed scoping does the filtering; consistency is the operator's claim that the
    tenant's traffic is drained or suspended, recorded in the manifest as ``consistency: tenant``.
    """

    tenant_id: UUID


# ....................... #


@attrs.frozen(kw_only=True)
class FullScope:
    """Export the whole system, carrying the attestation that it was still while captured.

    The *procedure* that makes the attestation mean anything — stop the fleet, run a dedicated
    exporter, drain its own relay, quiesce (RFC 0017 §4.1) — is operational. This type carries the
    resulting :class:`QuiesceReport`, and export refuses to stamp ``consistency: quiesced`` from a
    report that did not attest. The full-system walk lands with the blob plane (RFC §10 P2).
    """

    quiesce: QuiesceReport


# ....................... #

ExportScope = TenantScope | FullScope
"""What an export covers. The union is the stable API; P1 implements :class:`TenantScope`."""
