"""RFC 0017 §9 / decision #10: a per-tenant export excludes identity/credential specs by default.

# covers: forze_kits.integrations.portability (identity exclusion)

A full-system archive is a credential store — the 19 ``forze_identity`` document specs carry live
sessions, API-key records and unexpired invite/reset tokens. A per-tenant (GDPR data-portability)
export must not: it wants the tenant's business data, not their session tokens. So identity specs
carry an inventory marker, a per-tenant export skips them by default, ``include_identity=True`` opts
in, and a full-system export always carries them (a live system needs its sessions). The manifest
records which — telling a business-data archive from a credential store — and the marker stays out
of the fingerprint, so a per-tenant archive still imports into the full application.
"""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from forze import build_runtime
from forze.application.contracts.inventory import SpecRegistry
from forze.application.execution import ExecutionRuntime
from forze.base.exceptions import CoreException
from forze_identity.inventory import spec_contributions
from forze_kits.integrations.portability import (
    UNTENANTED,
    ExportReport,
    FullScope,
    Manifest,
    TenantScope,
    export_archive,
    import_archive,
    migrate,
)
from forze_mock import MockDepsModule
from forze_mock.state import MockState
from tests.support.portability_corpus import ORDER_SPEC, order_corpus, read_orders, seed_orders
from tests.support.quiesce import attested_report

# ----------------------- #

_ATTESTED = attested_report()

# The 19 identity document files a full export writes and a per-tenant export must not.
_IDENTITY_FILES = {f"{entry.name}.jsonl.gz" for entry in spec_contributions().freeze().entries}


def _runtime(state: MockState) -> ExecutionRuntime:
    # A business spec plus the whole forze_identity contribution — the shape of a real app.
    registry = SpecRegistry().register(ORDER_SPEC).merge(spec_contributions())
    return build_runtime(MockDepsModule(state=state), specs=registry, allow_unregistered=True)


def _doc_files(archive: Path) -> set[str]:
    return {path.name for path in (archive / "documents").glob("*.jsonl.gz")}


def _manifest(archive: Path) -> Manifest:
    return Manifest.model_validate_json((archive / "manifest.json").read_text())


async def _export(
    runtime: ExecutionRuntime, dest: Path, scope: object, **kwargs: object
) -> ExportReport:
    async with runtime.scope():
        return await export_archive(runtime, dest, scope=scope, **kwargs)  # type: ignore[arg-type]


# ....................... #


@pytest.mark.asyncio
async def test_per_tenant_export_excludes_identity_by_default(tmp_path: Path) -> None:
    runtime = _runtime(MockState())
    archive = tmp_path / "archive"

    report = await _export(runtime, archive, TenantScope(tenant_id=uuid4()))

    files = _doc_files(archive)
    assert "orders.jsonl.gz" in files, "the tenant's business data is carried"
    assert files & _IDENTITY_FILES == set(), "no identity/credential spec is carried per-tenant"
    assert {doc.name for doc in report.documents} == {"orders"}
    assert _manifest(archive).identity_included is False


@pytest.mark.asyncio
async def test_full_system_export_includes_identity(tmp_path: Path) -> None:
    runtime = _runtime(MockState())
    archive = tmp_path / "archive"

    await _export(
        runtime,
        archive,
        FullScope(quiesce=_ATTESTED, tenants=UNTENANTED),
        acknowledge_plaintext=True,
    )

    files = _doc_files(archive)
    assert "orders.jsonl.gz" in files
    assert files >= _IDENTITY_FILES, "a full-system archive carries every identity spec"
    assert _manifest(archive).identity_included is True


@pytest.mark.asyncio
async def test_per_tenant_include_identity_opts_in(tmp_path: Path) -> None:
    runtime = _runtime(MockState())
    archive = tmp_path / "archive"

    await _export(
        runtime,
        archive,
        TenantScope(tenant_id=uuid4()),
        include_identity=True,
        acknowledge_plaintext=True,
    )

    assert _doc_files(archive) >= _IDENTITY_FILES, "opting in carries identity per-tenant"
    assert _manifest(archive).identity_included is True


@pytest.mark.asyncio
async def test_migrate_per_tenant_excludes_identity(tmp_path: Path) -> None:
    """The direct migrate applies the same exclusion — else it would copy sessions and tokens
    into the target under a per-tenant move."""

    source = _runtime(MockState())
    target = _runtime(MockState())

    async with source.scope(), target.scope():
        report = await migrate(source, target, scope=TenantScope(tenant_id=uuid4()))

    assert {doc.name for doc in report.documents} == {"orders"}


@pytest.mark.asyncio
async def test_per_tenant_archive_still_imports_into_the_full_application(tmp_path: Path) -> None:
    """The identity marker is out of the fingerprint, so a business-data-only per-tenant archive
    imports into the same application — whose registry *does* carry the identity specs — without a
    fingerprint mismatch. The excluded specs are simply absent, not incompatible."""

    tenant = uuid4()
    source = _runtime(MockState())
    async with source.scope():
        seeded = await seed_orders(source.get_context(), order_corpus(2), tenant=tenant)

    archive = tmp_path / "archive"
    await _export(source, archive, TenantScope(tenant_id=tenant))

    target = _runtime(MockState())
    async with target.scope():
        result = await import_archive(target, archive, tenant=tenant)

    assert result.total_imported == 2  # business data landed; fingerprints matched

    async with target.scope():
        restored = await read_orders(target.get_context(), list(seeded), tenant=tenant)
    assert set(restored) == set(seeded)


@pytest.mark.asyncio
async def test_unsealed_identity_export_is_refused_without_acknowledgement(
    tmp_path: Path,
) -> None:
    # A full-system export always carries identity — sessions, API keys — so an unsealed
    # one is a credential store and must not be producible by default.
    runtime = _runtime(MockState())

    with pytest.raises(CoreException, match="PLAINTEXT"):
        await _export(runtime, tmp_path / "a", FullScope(quiesce=_ATTESTED, tenants=UNTENANTED))
