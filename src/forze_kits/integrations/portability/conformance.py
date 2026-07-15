"""The portability trust story: a backend-agnostic export → import → re-export round-trip.

RFC 0017 §8. This is the ``forze_dst`` conformance doctrine — a *sharp* equivalence observable
plus an allowed-divergence catalog — applied to portability. It cannot live under
``forze_dst.conformance`` like the isolation and delivery families do, because those validate
**core** contracts (``forze.application.contracts.*``) while a round-trip drives the export/import
verbs, which are ``forze_kits`` code, and ``forze_dst`` is not a sanctioned ``forze_kits`` importer
(only ``forze_fastapi`` is). So the family is co-located with the code it validates, which is the
right home anyway: the trust harness ships *with* the feature.

The equivalence observable is the export format **itself**. Seed a governed corpus into backend A,
export it, import into a fresh backend B, then **re-export from B** and compare the two archives'
row projections. Whatever survives that comparison is exactly what the round-trip preserves, and
whatever the format cannot represent (``rev`` — RFC §7) is honestly outside the claim rather than a
field an assertion forgot to check. Legs the tests wire: mock↔mock (oracle), mock→Postgres,
Postgres→Mongo, Mongo→Postgres (real backends, two wired in one process).

:data:`PORTABILITY_DIVERGENCES` is the false-positive firewall — the differences two *correct*
backends legitimately produce (a coarser datetime precision, a re-rendered float, a stream ordering)
that the sharp observable would otherwise read as data loss. Each is reviewed data: a reason, a
citation, and — where one exists — the name of a probe that pins it on both backends.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path

import attrs

from forze.application.contracts.inventory import FrozenSpecRegistry
from forze.application.execution.context import ExecutionContext
from forze.base.primitives import JsonDict

from .export import ArchiveExporter
from .format import read_rows
from .import_ import ArchiveImporter
from .manifest import Manifest
from .scope import ExportScope

# ----------------------- #

Seed = Callable[[ExecutionContext], Awaitable[None]]
"""How the corpus is written into the source backend. The caller owns it because only the caller
knows its specs and create models; the family owns everything after (export, import, re-export,
compare). Mirrors the delivery family taking its events and staging them itself."""


# ....................... #


@attrs.frozen(kw_only=True)
class RoundTripOutcome:
    """The observable result of an export → import → re-export round-trip, compared across backends.

    The counts let a test assert the corpus actually moved (``exported == imported == reexported``);
    the two ``*_match`` flags are the equivalence itself — the target's re-export projects to
    exactly the source's export, plane by plane.
    """

    exported: int
    """Document rows the source export carried."""

    imported: int
    """Document rows the target import inserted (a fresh target, so this equals ``exported``)."""

    reexported: int
    """Document rows the target re-export carried — equal again if nothing was lost or gained."""

    documents_match: bool
    """Whether the target's re-exported document rows equal the source's, keyed by id."""

    blobs_match: bool
    """Whether the target's re-exported blob index equals the source's, keyed by object key."""

    # ....................... #

    @property
    def lossless(self) -> bool:
        """The headline verdict: the re-export projects to exactly the export, so by the format's
        own definition nothing it can represent was lost across the backend change."""

        return self.documents_match and self.blobs_match


# ....................... #


async def run_export_import_roundtrip(
    source: ExecutionContext,
    target: ExecutionContext,
    registry: FrozenSpecRegistry,
    *,
    seed: Seed,
    workdir: Path,
    scope: ExportScope,
) -> RoundTripOutcome:
    """Seed *source*, export it, import into *target*, re-export *target*, compare the projections.

    *source* and *target* are live contexts over two backends (mock or real) wired in one process;
    *registry* is the shared inventory both bind (same fingerprint); *seed* writes the corpus into
    *source*; *workdir* holds the two throwaway archives; *scope* selects what to carry on both
    exports (a :class:`TenantScope`, or a :class:`FullScope` with an attested report). *target* must
    be empty for the imported set, so its re-export is exactly the imported rows — the caller's
    precondition. Returns the :class:`RoundTripOutcome` a differential asserts identically on every
    backend that honours the fidelity contract.
    """

    await seed(source)

    archive_a = workdir / "a"
    archive_b = workdir / "b"

    export_a = await ArchiveExporter()(source, registry, archive_a, scope=scope)
    import_b = await ArchiveImporter()(target, registry, archive_a)
    export_b = await ArchiveExporter()(target, registry, archive_b, scope=scope)

    proj_a = await _archive_projection(archive_a)
    proj_b = await _archive_projection(archive_b)

    return RoundTripOutcome(
        exported=export_a.total_rows,
        imported=import_b.total_imported,
        reexported=export_b.total_rows,
        documents_match=proj_a.documents == proj_b.documents,
        blobs_match=proj_a.blobs == proj_b.blobs,
    )


# ....................... #


@attrs.frozen(kw_only=True)
class _ArchiveProjection:
    """An archive reduced to the row maps the round-trip compares, keyed for order-independence."""

    documents: dict[str, dict[str, JsonDict]]
    """spec name → {document id → canonical row}."""

    blobs: dict[str, dict[str, JsonDict]]
    """storage route → {object key → index row (sha256, size, content_type, tags)}."""


async def _archive_projection(archive: Path) -> _ArchiveProjection:
    """Reduce an archive to its comparable projection.

    Keyed by id / key rather than compared by file position, so ``find_stream`` /​ ``list`` ordering
    differences across backends (the ``stream-order-normalized`` divergence) cannot surface as a
    false loss. The manifest drives which files are documents and which are blob indexes.
    """

    manifest = Manifest.model_validate_json((archive / "manifest.json").read_text())

    documents: dict[str, dict[str, JsonDict]] = {}
    blobs: dict[str, dict[str, JsonDict]] = {}

    for archive_file in manifest.files:
        if archive_file.path.startswith("documents/"):
            name = Path(archive_file.path).name.removesuffix(".jsonl.gz")
            documents[name] = {
                str(row["id"]): row async for row in read_rows(archive / archive_file.path)
            }

        elif archive_file.path.startswith("blobs/"):
            route = Path(archive_file.path).parent.name
            blobs[route] = {
                str(row["key"]): row async for row in read_rows(archive / archive_file.path)
            }

    return _ArchiveProjection(documents=documents, blobs=blobs)


# ....................... #


@attrs.frozen(kw_only=True)
class PortabilityDivergence:
    """A legitimate cross-backend difference the round-trip differential must expect, not flag.

    The export projection is a sharp equality observable, which makes it unforgiving: a difference
    two *correct* backends produce (a datetime rounded to a coarser precision, a float re-rendered
    in its last digit, a stream ordering) would read as data loss unless it is catalogued. Each
    entry names the difference, why it is legitimate, and its source; the checked ones carry the
    name of a probe that pins it on both backends, so the catalog stays reviewed data.
    """

    name: str
    reason: str
    source: str

    probe: str | None = None
    """The test that pins this divergence on both backends, when one exists — the entry is then a
    checked fact, not a claim. ``None`` for a difference the projection normalizes structurally, so
    there is nothing to lose and nothing to pin."""


# ....................... #


PORTABILITY_DIVERGENCES: tuple[PortabilityDivergence, ...] = (
    PortabilityDivergence(
        name="datetime-subsecond-precision",
        reason=(
            "A BSON datetime is millisecond-precision (int64 ms since epoch); a Postgres "
            "timestamptz is microsecond; a Python datetime carries microseconds. A timestamp with "
            "sub-millisecond precision therefore round-trips losslessly on Postgres but is "
            "truncated on Mongo, so the two backends' re-exports of a µs-precision value "
            "legitimately differ. The conformance corpus uses whole-second timestamps, keeping the "
            "round-trip about type fidelity (does a datetime survive at all) rather than a "
            "precision no backend promises to preserve. Not a portability bug: the field survives "
            "as a datetime; only the sub-millisecond digits the target cannot store are dropped."
        ),
        source="BSON spec (UTC datetime = int64 ms since epoch); PostgreSQL timestamp precision",
        probe="test_subsecond_datetime_truncates_on_mongo",
    ),
    PortabilityDivergence(
        name="decimal-is-string-canonical",
        reason=(
            'The canonical row renders a Decimal as a JSON *string* (mode="json"), so its exact '
            "value survives verbatim across Postgres numeric and Mongo Decimal128 and the two "
            "re-exports agree byte-for-byte. A float renders as a JSON *number* and can differ in "
            "its last ULP after a store-and-reload or a cross-backend re-encode — the classic "
            "binary-float portability trap. The corpus uses Decimal for any exact value; a model "
            "that stores money as float is a modelling choice the round-trip cannot make lossless, "
            "and the catalog says so rather than the differential flagging every float as loss."
        ),
        source="IEEE 754 double round-trip; forze codec Decimal-as-string policy",
        probe="test_decimal_field_round_trips_exactly",
    ),
    PortabilityDivergence(
        name="stream-order-normalized",
        reason=(
            "find_stream / list make no cross-backend ordering promise — keyset order, and where "
            "NULLs sort within it, are engine-specific. The export projection is compared **keyed "
            "by id** (documents) and **by object key** (blobs), never by file position, so a "
            "backend's ORDER BY / null-placement difference is normalized away by construction. "
            "There is nothing to pin: the observable does not depend on order, so no ordering "
            "difference can surface as a false loss."
        ),
        source="DocumentQueryPort.find_stream / StorageQueryPort.list ordering contract",
        probe=None,
    ),
)
"""Cross-backend differences the round-trip differential must normalize, not flag (RFC §8)."""
