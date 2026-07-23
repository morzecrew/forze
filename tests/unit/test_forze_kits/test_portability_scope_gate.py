"""The export/migrate attestation gate: what a quiesce report must actually prove.

# covers: forze_kits.integrations.portability._core.assert_scope_permitted
# covers: forze_kits.integrations.quiesce.report (non-vacuous attestation)

The gate used to accept a hand-built two-field token — ``QuiesceReport(planes=(),
admission_held=True)`` attested vacuously (``all(())``) and carried no timestamp or
tenant set to cross-check, so an export of three tenants could stamp ``consistency:
quiesced`` on a sweep that probed one partition, or none. These tests pin the two
halves of the fix: an empty report never attests, and attestation must **cover** the
scope's tenant set.
"""

from __future__ import annotations

from uuid import UUID

import pytest

from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow
from forze_kits.integrations.portability import UNTENANTED, FullScope, TenantScope
from forze_kits.integrations.portability._core import assert_scope_permitted
from forze_kits.integrations.quiesce import QuiescePlane, QuiesceReport
from tests.support.quiesce import attested_report

# ----------------------- #

_T1 = UUID(int=1)
_T2 = UUID(int=2)
_T3 = UUID(int=3)


def test_an_empty_report_never_attests() -> None:
    # the forgeable token: two fields, zero observations — ``all(())`` is vacuously
    # true, but a sweep that saw nothing has nothing to attest
    forged = QuiesceReport(planes=(), admission_held=True, taken_at=utcnow())

    assert forged.settled is False
    assert forged.attested is False

    with pytest.raises(CoreException, match="observed no planes"):
        forged.raise_if_unattested()


def test_an_unbound_sweep_does_not_cover_a_tenanted_scope() -> None:
    # tenants=None probes only the default partition: an export of named tenants
    # gated on it would stamp "quiesced" on backlogs nobody looked at
    scope = FullScope(quiesce=attested_report(), tenants=[_T1, _T2])

    with pytest.raises(CoreException, match="unbound"):
        assert_scope_permitted(scope, allow_fuzzy=False)


def test_missing_tenant_partitions_are_refused_by_name() -> None:
    scope = FullScope(quiesce=attested_report(tenants=(_T1,)), tenants=[_T1, _T3])

    with pytest.raises(CoreException, match=str(_T3)):
        assert_scope_permitted(scope, allow_fuzzy=False)


def test_a_covering_attestation_passes() -> None:
    # probing a superset is fine — the scope's set must be inside the probed set
    scope = FullScope(quiesce=attested_report(tenants=(_T1, _T2, _T3)), tenants=[_T1, _T2])

    assert_scope_permitted(scope, allow_fuzzy=False)


def test_a_tenant_probed_sweep_does_not_cover_an_untenanted_scope() -> None:
    # the unbound partition an UNTENANTED walk exports was never watched
    scope = FullScope(quiesce=attested_report(tenants=(_T1,)), tenants=UNTENANTED)

    with pytest.raises(CoreException, match="untenanted"):
        assert_scope_permitted(scope, allow_fuzzy=False)


def test_an_unbound_sweep_covers_an_untenanted_scope() -> None:
    assert_scope_permitted(
        FullScope(quiesce=attested_report(), tenants=UNTENANTED), allow_fuzzy=False
    )


def test_allow_fuzzy_bypasses_attestation_and_coverage() -> None:
    # the explicit opt-out — the manifest then records consistency: fuzzy
    residual = QuiesceReport(
        planes=(QuiescePlane(name="outbox:events", state="residual", detail="3 pending"),),
        admission_held=True,
        taken_at=utcnow(),
    )

    assert_scope_permitted(FullScope(quiesce=residual, tenants=[_T1]), allow_fuzzy=True)


def test_a_tenant_scope_is_the_operators_claim() -> None:
    # unchanged contract: per-tenant scopes carry no attestation to cross-check
    assert_scope_permitted(TenantScope(tenant_id=_T1), allow_fuzzy=False)
