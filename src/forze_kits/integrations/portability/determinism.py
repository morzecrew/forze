"""Logical determinism for archives, and the run manifest that records how one was made.

Two builds of an archive can be *the same content* without being the same bytes: row order
follows whatever the backend's cursors returned, compression and sealing differ per run, and
byte identity holds only inside a pinned build environment while carrying no semantic
information. The split is deliberate:

- **Byte determinism** is :attr:`~.manifest.ArchiveFile.sha256` — what import verifies.
- **Logical determinism** is :attr:`~.manifest.ArchiveFile.content_digest` — equal whenever
  the same rows were written, in any order. :func:`compare_content` renders the verdict for
  two manifests, refusing to call archives equal on files it cannot compare.

Beside it, :class:`RunManifest` binds one export or import run to the inputs that make it
reproducible — the code identity and lockfile digest that per-plane counts alone cannot
carry — as a plain JSON-serializable record the application persists as a row, a file, or
both.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import attrs
import orjson
from pydantic import BaseModel, Field

from forze._version import __version__
from forze.base.exceptions import exc
from forze.base.primitives import utcnow

from .format import file_sha256
from .manifest import Manifest
from .report import ExportReport, ImportReport, MigrateReport

# ----------------------- #
# Logical determinism


@attrs.frozen(kw_only=True)
class FileDifference:
    """One path whose two sides disagree — by content digest, row count, or both."""

    path: str
    rows_a: int
    rows_b: int
    content_digest_a: str | None
    content_digest_b: str | None


@attrs.frozen(kw_only=True)
class ContentComparison:
    """The logical-determinism verdict over two archive manifests.

    :attr:`same_content` is true only when the verdict is *complete*: the registries
    match, both sides carry the same file paths, and every pair compared equal by content
    digest and row count. A file missing a digest on either side lands in
    :attr:`unknown` and makes the verdict ``False`` — "could not compare" is not "equal".
    """

    registry_match: bool
    matching: tuple[str, ...]
    differing: tuple[FileDifference, ...]
    unknown: tuple[str, ...]
    """Paths present on both sides where at least one carries no content digest (sealed
    archives, blob indexes from older writers) — uncomparable, so never counted equal."""

    only_in_a: tuple[str, ...]
    only_in_b: tuple[str, ...]

    # ....................... #

    @property
    def same_content(self) -> bool:
        return (
            self.registry_match
            and not self.differing
            and not self.unknown
            and not self.only_in_a
            and not self.only_in_b
        )


def compare_content(a: Manifest, b: Manifest) -> ContentComparison:
    """Compare two archives *logically*: same rows, whatever the bytes.

    Byte identity (:attr:`~.manifest.ArchiveFile.sha256`) holds only inside a pinned
    build environment and says nothing about meaning; this comparison says whether two
    builds carry the same content. It reads the manifests alone — files are matched by
    archive path, so it never opens a data file. A reproducibility signal, not a
    security check: manifests are unauthenticated claims, and integrity verification
    stays with :func:`~.format.verify_file` on import.
    """

    files_a = {f.path: f for f in a.files}
    files_b = {f.path: f for f in b.files}

    if len(files_a) != len(a.files) or len(files_b) != len(b.files):
        raise exc.configuration("Archive manifest lists a duplicate file path")

    matching: list[str] = []
    differing: list[FileDifference] = []
    unknown: list[str] = []

    for path in sorted(files_a.keys() & files_b.keys()):
        fa, fb = files_a[path], files_b[path]

        if fa.content_digest is None or fb.content_digest is None:
            unknown.append(path)
        elif fa.content_digest == fb.content_digest and fa.rows == fb.rows:
            matching.append(path)
        else:
            differing.append(
                FileDifference(
                    path=path,
                    rows_a=fa.rows,
                    rows_b=fb.rows,
                    content_digest_a=fa.content_digest,
                    content_digest_b=fb.content_digest,
                )
            )

    return ContentComparison(
        registry_match=a.registry_fingerprint == b.registry_fingerprint,
        matching=tuple(matching),
        differing=tuple(differing),
        unknown=tuple(unknown),
        only_in_a=tuple(sorted(files_a.keys() - files_b.keys())),
        only_in_b=tuple(sorted(files_b.keys() - files_a.keys())),
    )


# ----------------------- #
# Run manifest


RunStatus = Literal["succeeded", "failed"]
"""A run manifest records terminal outcomes only — a manifest for a run still in flight
would pin reproducibility inputs to counts that are not final."""


class RunManifest(BaseModel):
    """One export, import or migrate run, bound to the inputs that make it reproducible.

    The per-plane counts already live in the run's report; what the report cannot carry is
    *which code produced it* — the application version and commit, the framework version,
    the dependency lockfile's digest. This record holds both halves as one JSON document,
    for the application to persist as a row, a file, or both. It complements
    :func:`compare_content`: the comparison says two builds are the same content, the
    manifest says what to rebuild with when they are not.
    """

    run_id: str
    kind: Literal["export", "import", "migrate"]
    status: RunStatus
    error: str | None = None
    """The failure, for a ``failed`` run; a ``succeeded`` run refuses one."""

    started_at: datetime
    finished_at: datetime = Field(default_factory=utcnow)

    counts: dict[str, int]
    """Per-plane totals from the run's report."""

    detail: dict[str, Any]
    """The full report, JSON-flattened — per-spec counts included, so the manifest stands
    alone once the report object is gone."""

    forze_version: str = __version__
    app_version: str | None = None
    git_sha: str | None = None
    """The application code commit — pass what the deployment already knows (e.g. its
    ``RuntimeSettings.git_sha``); nothing here shells out to git."""

    build_id: str | None = None
    lockfile_sha256: str | None = None
    """Digest of the dependency lockfile the run executed under, when one was given."""


def run_manifest(
    report: ExportReport | ImportReport | MigrateReport,
    *,
    run_id: str,
    started_at: datetime,
    status: RunStatus = "succeeded",
    error: str | None = None,
    app_version: str | None = None,
    git_sha: str | None = None,
    build_id: str | None = None,
    lockfile: Path | None = None,
) -> RunManifest:
    """Build a :class:`RunManifest` from a finished run's report.

    ``kind`` and the counts derive from the report's own type and totals; ``lockfile`` is
    hashed here (SHA-256 of its bytes) so the manifest records the digest, never a path
    that may later point at a different file. A ``failed`` run must say what failed, and
    a ``succeeded`` one must not carry an ``error``.
    """

    if (status == "failed") == (error is None):
        raise exc.configuration(
            "A failed run manifest requires an error, and a succeeded one refuses it "
            f"(status={status!r}, error={error!r})",
        )

    if isinstance(report, ExportReport):
        kind: Literal["export", "import", "migrate"] = "export"
        counts = {
            "rows": report.total_rows,
            "blobs": report.total_blobs,
            "vertices": report.total_vertices,
            "edges": report.total_edges,
            "counters": report.total_counters,
        }
    else:
        kind = "import" if isinstance(report, ImportReport) else "migrate"
        counts = {
            "imported": report.total_imported,
            "skipped_existing": sum(doc.skipped_existing for doc in report.documents),
            "blobs": report.total_blobs,
            "vertices": report.total_vertices,
            "edges": report.total_edges,
            "counters": report.total_counters,
        }

    return RunManifest(
        run_id=run_id,
        kind=kind,
        status=status,
        error=error,
        started_at=started_at,
        counts=counts,
        # JSON-native from the start (tuples become lists), so the persisted document
        # and the in-memory one are the same value.
        detail=orjson.loads(orjson.dumps(attrs.asdict(report))),
        app_version=app_version,
        git_sha=git_sha,
        build_id=build_id,
        lockfile_sha256=file_sha256(lockfile) if lockfile is not None else None,
    )
