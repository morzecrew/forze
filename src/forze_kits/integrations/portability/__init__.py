"""Portable export/import: an application's system-of-record state, through the ports and out.

Export walks the runtime's spec inventory and streams each plane's state into a backend-agnostic
archive — decrypted on read, so it never depends on the source's keys; import replays it into any
other wired backend, re-sealing under the target's. The trust story is the plane-completeness
doctrine (RFC 0016): every plane an application binds declares itself *exportable*, *rebuildable*,
or *drained*, and an export **refuses anything it cannot account for** rather than ship an artifact
that looks complete and is not.

This is a **portability** plane, not backup — durability stays your backend's job (WAL / PITR /
snapshots). The artifact is **plaintext by construction**; treat it as credential-adjacent (RFC
0017 §9). P1 implements the document plane under a per-tenant scope; blobs, counters, graph, the
full-system scope, and the direct ``migrate`` mode arrive in later phases.
"""

from .export import ArchiveExporter, export_archive
from .import_ import ArchiveImporter, OnConflict, import_archive
from .manifest import FORMAT_VERSION, ArchiveFile, Manifest, ScopeManifest
from .report import (
    DocumentExport,
    DocumentImport,
    ExportReport,
    ImportReport,
    StorageExport,
    StorageImport,
)
from .scope import ExportScope, FullScope, TenantScope

# ----------------------- #

__all__ = [
    "FORMAT_VERSION",
    "ArchiveExporter",
    "ArchiveFile",
    "ArchiveImporter",
    "DocumentExport",
    "DocumentImport",
    "ExportReport",
    "ExportScope",
    "FullScope",
    "ImportReport",
    "Manifest",
    "OnConflict",
    "ScopeManifest",
    "StorageExport",
    "StorageImport",
    "TenantScope",
    "export_archive",
    "import_archive",
]
