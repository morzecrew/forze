"""The archive's manifest and the scope that produced it — the plain, first, human surface.

``manifest.json`` is never compressed and always written last: it names every other file, its
checksum and row count, the registry fingerprint the whole archive is valid against, and what an
importer must rebuild rather than expect to find. An importer reads it before touching a single
row, so a corrupt payload, an incompatible target, or a missing file is a loud refusal up front,
never a scatter of silently absent documents.
"""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field

from .format import Compression

# ----------------------- #

FORMAT_VERSION = "1"
"""The archive layout version. An importer refuses a format it does not know."""

Consistency = Literal["tenant", "quiesced", "fuzzy"]
"""What the artifact claims about the moment it captured:

- ``tenant`` — a per-tenant export; consistency is the operator's claim that the tenant was quiet.
- ``quiesced`` — a full-system export taken under an *attested* quiesce (nothing moving, admission
  held). The strong claim.
- ``fuzzy`` — a full-system export whose quiesce did not attest; importable, but the manifest says
  it is not point-consistent. Opt-in to even produce.
"""


# ....................... #


class ArchiveFile(BaseModel):
    """One data file, as the manifest records it — enough to find and verify it without opening
    it. ``rows`` is the line count for a JSONL data file."""

    path: str
    sha256: str
    rows: int


# ....................... #


class ScopeManifest(BaseModel):
    """The scope, flattened to JSON: what the artifact covers, and — for a tenant export — whom."""

    kind: Literal["tenant", "full"]
    tenant_id: UUID | None = None


# ....................... #


class ArchiveEncryption(BaseModel):
    """Envelope-encryption record — present only for a sealed archive (RFC §9).

    Every data file and blob object is an ``FZEc`` chunked-AEAD stream sealed under one per-archive
    data key; that key is itself wrapped under a key-encryption key (a KMS/CMK) whose plaintext never
    left the KMS. An importer resolves :attr:`key_id` through its own KMS, unwraps the data key once,
    and decrypts. This record is plaintext — it is what *bootstraps* decryption — but it reveals only
    which key sealed the archive, never a byte of its contents."""

    algorithm: str
    """The AEAD that sealed each chunk (e.g. ``"AES-256-GCM"``)."""

    key_id: str
    """The key-encryption key the data key is wrapped under — resolved by the importer's KMS."""

    key_version: str | None = None
    wrapped_dek: str
    """The KEK-wrapped data key, base64-encoded (bytes do not live in JSON)."""

    chunk_size: int
    """The plaintext chunk size the writer used — informational; the frames carry their own sizes."""


# ....................... #


class Manifest(BaseModel):
    """The archive's table of contents, integrity record, and compatibility gate."""

    format_version: str = FORMAT_VERSION
    forze_version: str
    """The framework version that wrote the archive — diagnostic, not a compatibility gate."""

    registry_fingerprint: str
    """The exporting application's `FrozenSpecRegistry.fingerprint()`. Import refuses a target
    whose own fingerprint differs: same shapes in, or nothing."""

    compression: Compression = "gzip"
    scope: ScopeManifest
    consistency: Consistency

    encryption: ArchiveEncryption | None = None
    """Envelope-encryption metadata when the archive is sealed at rest; ``None`` for a plaintext
    archive. Import fails closed if this is set and no sealer is provided — an encrypted archive is
    unreadable without its KEK, and reading it must not silently fall through to raw bytes."""

    identity_included: bool = True
    """Whether the archive carries identity/credential specs (sessions, API keys, invite/reset
    tokens, roles, grants). A **full-system** export always includes them (``True``); a
    **per-tenant** export excludes them by default (``False``) unless ``include_identity=True`` was
    passed. Read it before treating an archive as safe: ``True`` means it is a credential store (RFC
    §9). Defaults ``True`` so an archive whose producer never considered the question is treated as
    the more dangerous case."""

    files: list[ArchiveFile] = Field(default_factory=list)
    """Every data file, checksummed. Import verifies all of them before decoding any row."""

    rebuild: list[str] = Field(default_factory=list)
    """Derived planes (search indexes, projected analytics) the target must recompute — they were
    never exported. A promise the import report echoes, not a warning it buries."""

    quiesce_attestation: dict[str, Any] | None = None
    """A full-system export embeds its :class:`QuiesceReport` here as a JSON snapshot; absent for a
    per-tenant export. Kept JSON-native (not the attrs report itself) so the manifest stays a pure
    document — the report is dumped in when full-system scope lands (RFC §10 P2)."""

    # ....................... #

    def file_for(self, path: str) -> ArchiveFile | None:
        """The recorded entry for a data file, or ``None`` when the manifest never listed it."""

        return next((f for f in self.files if f.path == path), None)
