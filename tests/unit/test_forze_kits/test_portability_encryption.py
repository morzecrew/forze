"""Envelope encryption for the archive at rest (RFC 0017 §9).

# covers: forze_kits.integrations.portability._crypt
# covers: forze_kits.integrations.portability.export
# covers: forze_kits.integrations.portability.import_

The file artifact is plaintext by construction — a full-system one is therefore a credential store.
A sealer wraps it: a single per-archive data key (DEK) seals every data file and blob object as
``FZEc`` frames, and the DEK rides in the manifest wrapped under a KEK whose plaintext never leaves
the KMS. Export with a sealer writes ciphertext on disk plus an ``encryption`` record; import with a
sealer unwraps the DEK once and decrypts. It fails **closed**: an encrypted archive handed to an
importer with no sealer is a refusal, never a fall-through to raw bytes; a manifest whose wrapped key
or named AEAD no longer matches is a refusal too. ``MockKeyManagement`` stands in for a real KMS (its
KEK is derived from the public key ref — test/dev only, never production), while the AEAD is the real
one, so the crypto path is faithful.
"""

from __future__ import annotations

import inspect
import json
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.crypto import KeyRef
from forze.application.execution import ExecutionRuntime
from forze.base.exceptions import CoreException
from forze_kits.integrations.portability import (
    UNTENANTED,
    ArchiveExporter,
    ArchiveImporter,
    ArchiveMigrator,
    ArchiveSealer,
    ExportReport,
    FullScope,
    ImportReport,
    Manifest,
    TenantScope,
    export_archive,
    migrate,
)
from forze_kits.integrations.portability.format import Compression, data_suffix
from forze_mock import MockKeyManagement
from forze_mock.state import MockState
from tests.support.portability_corpus import (
    ATTACHMENTS,
    OrderRead,
    assert_orders_faithful,
    download_attachment,
    mock_runtime,
    order_corpus,
    read_orders,
    seed_attachments,
    seed_orders,
)
from tests.support.quiesce import attested_report

# ----------------------- #

_KEY_ID = "archive-kek"
_ATTESTED = attested_report()


def _sealer(*, chunk_size: int | None = None) -> ArchiveSealer:
    """An export-side sealer: it names the KEK to wrap the archive's data key under."""

    kwargs: dict[str, object] = {"kms": MockKeyManagement(), "key_ref": KeyRef(key_id=_KEY_ID)}
    if chunk_size is not None:
        kwargs["chunk_size"] = chunk_size

    return ArchiveSealer(**kwargs)  # type: ignore[arg-type]


def _reader_sealer() -> ArchiveSealer:
    """An import-side sealer: no key ref — the archive names its own KEK in the manifest."""

    return ArchiveSealer(kms=MockKeyManagement())


async def _seed(runtime: ExecutionRuntime, tenant: UUID, count: int) -> dict[UUID, OrderRead]:
    async with runtime.scope():
        return await seed_orders(runtime.get_context(), order_corpus(count), tenant=tenant)


async def _export(
    runtime: ExecutionRuntime,
    dest: Path,
    tenant: UUID,
    *,
    sealer: ArchiveSealer | None,
    compression: Compression = "gzip",
) -> ExportReport:
    async with runtime.scope():
        assert runtime.spec_registry is not None
        return await ArchiveExporter(compression=compression, sealer=sealer)(
            runtime.get_context(), runtime.spec_registry, dest, scope=TenantScope(tenant_id=tenant)
        )


async def _import(
    runtime: ExecutionRuntime,
    src: Path,
    *,
    sealer: ArchiveSealer | None = None,
    tenant: UUID | None = None,
) -> ImportReport:
    async with runtime.scope():
        assert runtime.spec_registry is not None
        return await ArchiveImporter(sealer=sealer, tenant=tenant)(
            runtime.get_context(), runtime.spec_registry, src
        )


async def _read(runtime: ExecutionRuntime, tenant: UUID, ids: list[UUID]) -> dict[UUID, OrderRead]:
    async with runtime.scope():
        return await read_orders(runtime.get_context(), ids, tenant=tenant)


def _manifest(archive: Path) -> Manifest:
    return Manifest.model_validate_json((archive / "manifest.json").read_text())


# ....................... #
# Round-trip


@pytest.mark.parametrize("compression", ["gzip", "zstd", "none"])
@pytest.mark.asyncio
async def test_encrypted_round_trip_under_each_codec(
    tmp_path: Path, compression: Compression
) -> None:
    """A sealed export writes ciphertext on disk and an ``encryption`` record; a sealed import
    unwraps the data key once and restores every field faithfully — under every codec, since the
    seal wraps the *compressed* stream whatever the codec is."""

    tenant = uuid4()
    source = mock_runtime(MockState())
    seeded = await _seed(source, tenant, 4)

    archive = tmp_path / "archive"
    report = await _export(source, archive, tenant, sealer=_sealer(), compression=compression)
    assert report.total_rows == 4

    data_file = archive / "documents" / f"orders{data_suffix(compression)}"
    raw = data_file.read_bytes()
    assert raw[:1] in (b"\x00", b"\x01")  # first byte is an FZEc frame's is_final flag
    assert not raw.startswith(b"\x1f\x8b")  # not a gzip stream — the seal is the outer layer
    assert b"order-0" not in raw  # the plaintext label never appears in the sealed bytes

    manifest = _manifest(archive)
    assert manifest.encryption is not None
    assert manifest.encryption.algorithm == "AES-256-GCM"
    assert manifest.encryption.key_id == _KEY_ID
    assert manifest.encryption.wrapped_dek  # base64, non-empty

    target = mock_runtime(MockState())
    result = await _import(target, archive, sealer=_reader_sealer(), tenant=tenant)
    assert result.total_imported == 4

    assert_orders_faithful(await _read(target, tenant, list(seeded)), seeded)


@pytest.mark.asyncio
async def test_encrypted_multi_frame_round_trip(tmp_path: Path) -> None:
    """A tiny chunk size forces each data file into many ``FZEc`` frames, exercising the streaming
    reassembly through the real writer and reader (not just the single-frame happy path)."""

    tenant = uuid4()
    source = mock_runtime(MockState())
    seeded = await _seed(source, tenant, 6)

    archive = tmp_path / "archive"
    # ``none`` + a 64-byte chunk: ~150 bytes per uncompressed row over six rows is many frames.
    await _export(source, archive, tenant, sealer=_sealer(chunk_size=64), compression="none")

    target = mock_runtime(MockState())
    # The reader needs no matching chunk size — the frames are self-describing.
    result = await _import(target, archive, sealer=_reader_sealer(), tenant=tenant)
    assert result.total_imported == 6

    assert_orders_faithful(await _read(target, tenant, list(seeded)), seeded)


@pytest.mark.asyncio
async def test_two_sealed_exports_use_distinct_data_keys(tmp_path: Path) -> None:
    """Each export mints its own random DEK, so the same corpus sealed twice yields different
    ciphertext — the wrapped keys in the two manifests differ. (A sealed archive is *not* a
    deterministic equality observable; the plaintext one is.)"""

    tenant = uuid4()
    source = mock_runtime(MockState())
    await _seed(source, tenant, 3)

    await _export(source, tmp_path / "a", tenant, sealer=_sealer())
    await _export(source, tmp_path / "b", tenant, sealer=_sealer())

    a, b = _manifest(tmp_path / "a"), _manifest(tmp_path / "b")
    assert a.encryption is not None and b.encryption is not None
    assert a.encryption.wrapped_dek != b.encryption.wrapped_dek

    file_a = (tmp_path / "a" / "documents" / "orders.jsonl.gz").read_bytes()
    file_b = (tmp_path / "b" / "documents" / "orders.jsonl.gz").read_bytes()
    assert file_a != file_b  # fresh DEK + per-chunk nonces


# ....................... #
# Fail-closed


@pytest.mark.asyncio
async def test_import_without_sealer_on_encrypted_archive_is_refused(tmp_path: Path) -> None:
    """An encrypted archive is unreadable without its KEK, so an importer given no sealer must
    refuse up front — never silently decode raw ciphertext into corrupt rows."""

    tenant = uuid4()
    source = mock_runtime(MockState())
    await _seed(source, tenant, 2)

    archive = tmp_path / "archive"
    await _export(source, archive, tenant, sealer=_sealer())

    target = mock_runtime(MockState())
    with pytest.raises(CoreException, match="encrypted"):
        await _import(target, archive, sealer=None, tenant=tenant)


@pytest.mark.asyncio
async def test_plaintext_archive_imports_without_a_sealer(tmp_path: Path) -> None:
    """The fail-closed guard fires only for a sealed archive: a plaintext one has no ``encryption``
    record and imports with no sealer, exactly as before this feature existed."""

    tenant = uuid4()
    source = mock_runtime(MockState())
    seeded = await _seed(source, tenant, 2)

    archive = tmp_path / "archive"
    await _export(source, archive, tenant, sealer=None)

    assert _manifest(archive).encryption is None

    target = mock_runtime(MockState())
    result = await _import(target, archive, sealer=None, tenant=tenant)
    assert result.total_imported == 2
    assert_orders_faithful(await _read(target, tenant, list(seeded)), seeded)


@pytest.mark.asyncio
async def test_tampered_wrapped_key_is_refused(tmp_path: Path) -> None:
    """Corrupt the wrapped data key in the manifest and the unwrapped DEK is wrong, so the AEAD
    fails to authenticate the first frame — the data key genuinely gates the plaintext, and a
    forged manifest does not yield readable rows."""

    tenant = uuid4()
    source = mock_runtime(MockState())
    await _seed(source, tenant, 2)

    archive = tmp_path / "archive"
    await _export(source, archive, tenant, sealer=_sealer())

    manifest_path = archive / "manifest.json"
    data = json.loads(manifest_path.read_text())
    # Flip the wrapped DEK to a different 32-byte value (keeps the length the mock expects, so the
    # failure is an authentication failure on decrypt, not a length rejection at unwrap).
    data["encryption"]["wrapped_dek"] = "A" * 44  # 32 bytes, base64
    manifest_path.write_text(json.dumps(data))

    target = mock_runtime(MockState())
    with pytest.raises(CoreException) as excinfo:
        await _import(target, archive, sealer=_reader_sealer(), tenant=tenant)

    assert "crypto" in (excinfo.value.code or "")


@pytest.mark.asyncio
async def test_algorithm_mismatch_is_refused(tmp_path: Path) -> None:
    """If the manifest names an AEAD the provided sealer does not use, import refuses up front with
    a clear message rather than an opaque authentication failure on the first frame."""

    tenant = uuid4()
    source = mock_runtime(MockState())
    await _seed(source, tenant, 1)

    archive = tmp_path / "archive"
    await _export(source, archive, tenant, sealer=_sealer())

    manifest_path = archive / "manifest.json"
    data = json.loads(manifest_path.read_text())
    data["encryption"]["algorithm"] = "XChaCha20-Poly1305"
    manifest_path.write_text(json.dumps(data))

    target = mock_runtime(MockState())
    with pytest.raises(CoreException, match="AEAD"):
        await _import(target, archive, sealer=_reader_sealer(), tenant=tenant)


# ....................... #
# Blob plane


def _blob_runtime(state: MockState) -> ExecutionRuntime:
    return mock_runtime(state, with_blobs=True)


async def _export_full(
    runtime: ExecutionRuntime, dest: Path, *, sealer: ArchiveSealer | None
) -> ExportReport:
    async with runtime.scope():
        return await export_archive(
            runtime, dest, scope=FullScope(quiesce=_ATTESTED, tenants=UNTENANTED), sealer=sealer
        )


@pytest.mark.asyncio
async def test_encrypted_blob_round_trip(tmp_path: Path) -> None:
    """Under a sealer each blob object is sealed (bound to its ``blobs/<route>|<key>`` identity) and
    the index — which carries keys and tags — is sealed too, so nothing about the object is left in
    the clear. Import restores every blob byte-for-byte under its own key, with its tags."""

    source = _blob_runtime(MockState())
    blobs = [
        (b"%PDF-1.4 confidential", {"kind": "invoice"}),
        (b"\x00\x01\x02 binary", {"kind": "avatar"}),
        (b"", {}),  # a zero-byte object still seals to a final frame
    ]

    async with source.scope():
        seeded = await seed_attachments(source.get_context(), blobs)

    archive = tmp_path / "archive"
    report = await _export_full(source, archive, sealer=_sealer())
    assert report.total_blobs == 3

    index = archive / "blobs" / "attachments" / "index.jsonl.gz"
    assert b"invoice" not in index.read_bytes()  # tags are sealed, not in the clear

    objects = archive / "blobs" / "attachments" / "objects"
    for obj in objects.iterdir():
        raw = obj.read_bytes()
        assert b"%PDF" not in raw and b"confidential" not in raw  # object bytes are ciphertext

    target = _blob_runtime(MockState())
    result = await _import(target, archive, sealer=_reader_sealer())
    assert result.total_blobs == 3

    for key, (content, tags) in seeded.items():
        async with target.scope():
            assert await download_attachment(target.get_context(), key) == content
            head = (
                await target.get_context().storage.query(ATTACHMENTS).head(key, include_tags=True)
            )
        assert dict(head.tags) == tags


@pytest.mark.asyncio
async def test_encrypted_blob_import_without_sealer_is_refused(tmp_path: Path) -> None:
    """The fail-closed guard covers the blob plane too — a sealed blob archive without a sealer is
    a refusal, read before any object is touched."""

    source = _blob_runtime(MockState())
    async with source.scope():
        await seed_attachments(source.get_context(), [(b"secret bytes", {})])

    archive = tmp_path / "archive"
    await _export_full(source, archive, sealer=_sealer())

    target = _blob_runtime(MockState())
    with pytest.raises(CoreException, match="encrypted"):
        await _import(target, archive, sealer=None)


# ....................... #
# migrate needs no sealer — nothing is ever at rest


@pytest.mark.asyncio
async def test_migrate_carries_no_artifact_and_no_sealer(tmp_path: Path) -> None:
    """``migrate`` is ports-to-ports: it writes nothing to disk, so there is no artifact to seal and
    it exposes no sealer. Its round-trip is unaffected by this feature. (Encryption at rest is the
    file path's concern; the direct migration never rests a plaintext credential store on disk.)"""

    assert "sealer" not in inspect.signature(migrate).parameters
    assert "sealer" not in {f.name for f in ArchiveMigrator.__attrs_attrs__}  # type: ignore[attr-defined]

    tenant = uuid4()
    source = mock_runtime(MockState())
    seeded = await _seed(source, tenant, 3)

    target = mock_runtime(MockState())
    async with source.scope(), target.scope():
        report = await migrate(source, target, scope=TenantScope(tenant_id=tenant))

    assert report.total_imported == 3
    assert not any(tmp_path.iterdir())  # migrate created no files whatsoever

    assert_orders_faithful(await _read(target, tenant, list(seeded)), seeded)
