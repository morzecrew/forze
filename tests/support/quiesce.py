"""Honest minimal quiesce attestations for portability tests.

The export/migrate gate no longer accepts a bare two-field token: an attested report
must have observed at least one plane, carry its timestamp, and name the tenant
partitions it probed (``None`` = the single unbound pass, matching ``UNTENANTED``
scopes). These helpers build the smallest report that honestly clears that bar.
"""

from __future__ import annotations

from uuid import UUID

from forze.base.primitives import utcnow
from forze_kits.integrations.quiesce import QuiescePlane, QuiesceReport

# ----------------------- #


def attested_report(*, tenants: tuple[UUID, ...] | None = None) -> QuiesceReport:
    """An attested report covering *tenants*: one settled plane, stamped now."""

    return QuiesceReport(
        planes=(QuiescePlane(name="operations", state="settled"),),
        admission_held=True,
        taken_at=utcnow(),
        tenants=tenants,
    )


def unattested_report(*planes: QuiescePlane) -> QuiesceReport:
    """A report that must NOT clear the gate — residual planes (or none at all)."""

    return QuiesceReport(planes=planes, admission_held=True, taken_at=utcnow())
