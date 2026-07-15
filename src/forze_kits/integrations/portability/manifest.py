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

# ----------------------- #

FORMAT_VERSION = "1"
"""The archive layout version. An importer refuses a format it does not know."""

Compression = Literal["gzip"]
"""Compression codec for the data files. ``gzip`` is stdlib and needs no extra; ``zstd`` is a
later, manifest-declared option (RFC §5). Declared so an importer fails closed on one it lacks."""

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
