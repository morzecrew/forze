"""Portable export/import: an application's system-of-record state, through the ports and out.

Export walks the runtime's spec inventory and streams each plane's state into a backend-agnostic
archive — decrypted on read, so it never depends on the source's keys; import replays it into any
other wired backend, re-sealing under the target's. The trust story is the plane-completeness
doctrine (RFC 0016): every plane an application binds declares itself *exportable*, *rebuildable*,
or *drained*, and an export **refuses anything it cannot account for** rather than ship an artifact
that looks complete and is not.

This is a **portability** plane, not backup — durability stays your backend's job (WAL / PITR /
snapshots). A file artifact is **plaintext by construction** (treat it as credential-adjacent, RFC
0017 §9); the direct ``migrate`` mode fuses export and import ports-to-ports so nothing plaintext is
ever written to disk — the recommended path for a backend migration. Documents, blobs, per-tenant
and full-system scope ship today; counters and graph arrive with later phases.
"""

from ._core import OnConflict
from .export import ArchiveExporter, export_archive
from .import_ import ArchiveImporter, import_archive
from .manifest import FORMAT_VERSION, ArchiveFile, Manifest, ScopeManifest
from .migrate import ArchiveMigrator, migrate
from .report import (
    DocumentExport,
    DocumentImport,
    ExportReport,
    GraphExport,
    GraphImport,
    ImportReport,
    MigrateReport,
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
    "ArchiveMigrator",
    "DocumentExport",
    "DocumentImport",
    "ExportReport",
    "ExportScope",
    "FullScope",
    "GraphExport",
    "GraphImport",
    "ImportReport",
    "Manifest",
    "MigrateReport",
    "OnConflict",
    "ScopeManifest",
    "StorageExport",
    "StorageImport",
    "TenantScope",
    "export_archive",
    "import_archive",
    "migrate",
]
