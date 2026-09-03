"""Logical determinism: content digests, the comparison verdict, and run manifests."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest

from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import JsonDict
from forze_kits.integrations.portability import (
    ArchiveFile,
    DocumentExport,
    DocumentImport,
    ExportReport,
    ImportReport,
    Manifest,
    MigrateReport,
    RunManifest,
    ScopeManifest,
    compare_content,
    run_manifest,
)
from forze_kits.integrations.portability.format import JsonlWriter

pytestmark = pytest.mark.unit

# ----------------------- #
# Content digest on the writer


_ROWS: list[JsonDict] = [{"id": "a", "n": 1}, {"id": "b", "n": 2}, {"id": "c", "n": 3}]


def _write(tmp_path: Path, name: str, rows: list[JsonDict]) -> JsonlWriter:
    writer = JsonlWriter(tmp_path / name, compression="gzip")

    with writer:
        for row in rows:
            writer.write(row)

    return writer


class TestContentDigest:
    def test_order_independent(self, tmp_path: Path) -> None:
        # The same rows in reversed order are the same content — a re-export whose
        # cursor walked differently must fingerprint identically.
        forward = _write(tmp_path, "f.jsonl.gz", _ROWS)
        backward = _write(tmp_path, "b.jsonl.gz", list(reversed(_ROWS)))

        assert forward.content_digest == backward.content_digest
        assert len(forward.content_digest or "") == 64

    def test_different_rows_differ(self, tmp_path: Path) -> None:
        a = _write(tmp_path, "a.jsonl.gz", _ROWS)
        b = _write(tmp_path, "b.jsonl.gz", [*_ROWS[:2], {"id": "c", "n": 4}])

        assert a.content_digest != b.content_digest

    def test_multiplicity_counts(self, tmp_path: Path) -> None:
        # A multiset digest: one row versus the same row twice must differ.
        once = _write(tmp_path, "once.jsonl.gz", [_ROWS[0]])
        twice = _write(tmp_path, "twice.jsonl.gz", [_ROWS[0], _ROWS[0]])

        assert once.content_digest != twice.content_digest

    def test_key_order_is_canonicalized(self, tmp_path: Path) -> None:
        # Canonical JSON sorts keys, so dict insertion order is not content.
        a = _write(tmp_path, "a.jsonl.gz", [{"x": 1, "y": 2}])
        b = _write(tmp_path, "b.jsonl.gz", [{"y": 2, "x": 1}])

        assert a.content_digest == b.content_digest

    def test_sealed_writer_carries_no_digest(self, tmp_path: Path) -> None:
        # A plaintext-derived digest in the plaintext manifest would let anyone confirm
        # a guessed row set against the ciphertext.
        class _FakeCipher:
            def sealing_sink(self, sink: object, *, base_aad: str) -> object:
                raise AssertionError("never entered")

        writer = JsonlWriter(
            tmp_path / "sealed.jsonl.gz",
            compression="gzip",
            cipher=_FakeCipher(),  # type: ignore[arg-type]
        )

        assert writer.content_digest is None


# ----------------------- #
# The comparison verdict


def _manifest(files: list[ArchiveFile], *, registry: str = "reg-1") -> Manifest:
    return Manifest(
        forze_version="0",
        registry_fingerprint=registry,
        scope=ScopeManifest(kind="full"),
        consistency="quiesced",
        files=files,
    )


def _file(path: str, *, rows: int = 3, digest: str | None = "d" * 64) -> ArchiveFile:
    return ArchiveFile(path=path, sha256="s" * 64, rows=rows, content_digest=digest)


class TestCompareContent:
    def test_same_content(self) -> None:
        a = _manifest([_file("documents/orders.jsonl.gz")])
        b = _manifest([_file("documents/orders.jsonl.gz")])

        verdict = compare_content(a, b)

        assert verdict.same_content
        assert verdict.matching == ("documents/orders.jsonl.gz",)

    def test_differing_digest_or_rows(self) -> None:
        a = _manifest([_file("d.jsonl.gz", digest="a" * 64)])
        b = _manifest([_file("d.jsonl.gz", digest="b" * 64)])

        verdict = compare_content(a, b)

        assert not verdict.same_content
        assert verdict.differing[0].path == "d.jsonl.gz"
        assert verdict.differing[0].content_digest_a == "a" * 64

        rows_only = compare_content(
            _manifest([_file("d.jsonl.gz", rows=3)]),
            _manifest([_file("d.jsonl.gz", rows=4)]),
        )

        assert not rows_only.same_content

    def test_unknown_is_never_equal(self) -> None:
        # "Could not compare" must not read as "equal": a sealed side has no digest.
        a = _manifest([_file("d.jsonl.gz", digest=None)])
        b = _manifest([_file("d.jsonl.gz")])

        verdict = compare_content(a, b)

        assert not verdict.same_content
        assert verdict.unknown == ("d.jsonl.gz",)
        assert not verdict.differing

    def test_file_set_mismatch(self) -> None:
        verdict = compare_content(
            _manifest([_file("a.jsonl.gz"), _file("b.jsonl.gz")]),
            _manifest([_file("b.jsonl.gz"), _file("c.jsonl.gz")]),
        )

        assert not verdict.same_content
        assert verdict.only_in_a == ("a.jsonl.gz",)
        assert verdict.only_in_b == ("c.jsonl.gz",)

    def test_registry_mismatch_is_not_same_content(self) -> None:
        a = _manifest([_file("d.jsonl.gz")], registry="reg-1")
        b = _manifest([_file("d.jsonl.gz")], registry="reg-2")

        verdict = compare_content(a, b)

        assert not verdict.registry_match
        assert not verdict.same_content

    def test_duplicate_path_is_refused(self) -> None:
        a = _manifest([_file("d.jsonl.gz"), _file("d.jsonl.gz")])

        with pytest.raises(CoreException) as ei:
            compare_content(a, _manifest([_file("d.jsonl.gz")]))

        assert ei.value.kind is ExceptionKind.CONFIGURATION


# ----------------------- #
# Run manifests


_STARTED = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)

_EXPORT = ExportReport(
    documents=(DocumentExport(name="orders", rows=7), DocumentExport(name="users", rows=3)),
    rebuild=("search",),
)

_IMPORT = ImportReport(
    documents=(DocumentImport(name="orders", imported=5, skipped_existing=2),),
    rebuild=(),
)


class TestRunManifest:
    def test_export_manifest(self, tmp_path: Path) -> None:
        lockfile = tmp_path / "uv.lock"
        lockfile.write_bytes(b"pinned")

        manifest = run_manifest(
            _EXPORT,
            run_id="run-1",
            started_at=_STARTED,
            git_sha="abc123",
            lockfile=lockfile,
        )

        assert manifest.kind == "export"
        assert manifest.status == "succeeded"
        assert manifest.counts["rows"] == 10
        assert manifest.detail["documents"][0] == {"name": "orders", "rows": 7}
        assert manifest.git_sha == "abc123"
        assert manifest.lockfile_sha256 == hashlib.sha256(b"pinned").hexdigest()
        assert manifest.forze_version

    def test_import_and_migrate_kinds(self) -> None:
        imported = run_manifest(_IMPORT, run_id="r", started_at=_STARTED)

        assert imported.kind == "import"
        assert imported.counts == {
            "imported": 5,
            "skipped_existing": 2,
            "blobs": 0,
            "vertices": 0,
            "edges": 0,
            "counters": 0,
        }

        migrated = run_manifest(
            MigrateReport(documents=_IMPORT.documents, rebuild=()),
            run_id="r",
            started_at=_STARTED,
        )

        assert migrated.kind == "migrate"

    def test_failed_requires_error_and_succeeded_refuses_one(self) -> None:
        with pytest.raises(CoreException) as no_error:
            run_manifest(_EXPORT, run_id="r", started_at=_STARTED, status="failed")

        assert no_error.value.kind is ExceptionKind.CONFIGURATION

        with pytest.raises(CoreException):
            run_manifest(_EXPORT, run_id="r", started_at=_STARTED, error="boom")

        failed = run_manifest(
            _EXPORT, run_id="r", started_at=_STARTED, status="failed", error="boom"
        )

        assert failed.error == "boom"

    def test_missing_lockfile_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(CoreException, match="lockfile does not exist") as ei:
            run_manifest(_EXPORT, run_id="r", started_at=_STARTED, lockfile=tmp_path / "gone.lock")

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_json_roundtrip(self) -> None:
        manifest = run_manifest(_EXPORT, run_id="r", started_at=_STARTED)

        assert RunManifest.model_validate_json(manifest.model_dump_json()) == manifest
