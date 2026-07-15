"""What an export or import did, per plane — enough for an operator to trust or to retry."""

from __future__ import annotations

import attrs

# ----------------------- #


@attrs.frozen(kw_only=True)
class DocumentExport:
    """One document spec's contribution to an archive."""

    name: str
    rows: int


@attrs.frozen(kw_only=True)
class StorageExport:
    """One storage route's contribution to an archive."""

    name: str
    blobs: int


@attrs.frozen(kw_only=True)
class GraphExport:
    """One graph module's contribution to an archive — every node and edge kind walked."""

    name: str
    vertices: int
    edges: int


@attrs.frozen(kw_only=True)
class ExportReport:
    """The outcome of an :func:`export_archive` run."""

    documents: tuple[DocumentExport, ...]
    """Per-spec row counts, in the order the specs were walked."""

    storage: tuple[StorageExport, ...] = ()
    """Per-route blob counts."""

    graph: tuple[GraphExport, ...] = ()
    """Per-module vertex + edge counts."""

    rebuild: tuple[str, ...]
    """Derived planes the target will recompute rather than receive — carried into the manifest so
    the import side can echo the same promise."""

    # ....................... #

    @property
    def total_rows(self) -> int:
        return sum(doc.rows for doc in self.documents)

    @property
    def total_blobs(self) -> int:
        return sum(route.blobs for route in self.storage)

    @property
    def total_vertices(self) -> int:
        return sum(module.vertices for module in self.graph)

    @property
    def total_edges(self) -> int:
        return sum(module.edges for module in self.graph)


# ....................... #


@attrs.frozen(kw_only=True)
class DocumentImport:
    """One document spec's outcome on import.

    ``imported`` + ``skipped_existing`` = the rows the archive carried for this spec. They are kept
    apart because ``ensure`` semantics make a re-run land entirely in ``skipped_existing`` — which
    is convergence, not a no-op that lost data, and an operator reading the report must be able to
    tell the difference.
    """

    name: str
    imported: int
    skipped_existing: int

    # ....................... #

    @property
    def rows(self) -> int:
        return self.imported + self.skipped_existing


@attrs.frozen(kw_only=True)
class StorageImport:
    """One storage route's outcome on import."""

    name: str
    uploaded: int


@attrs.frozen(kw_only=True)
class GraphImport:
    """One graph module's outcome on import — vertices then edges, both ``ensure``-idempotent."""

    name: str
    vertices: int
    edges: int


@attrs.frozen(kw_only=True)
class ImportReport:
    """The outcome of an :func:`import_archive` run."""

    documents: tuple[DocumentImport, ...]
    storage: tuple[StorageImport, ...] = ()
    graph: tuple[GraphImport, ...] = ()
    rebuild: tuple[str, ...]
    """Derived planes the caller must now rebuild on the target (search indexes, projected
    analytics). Import does not rebuild them itself in P1; it reports what the manifest declared so
    nothing is silently missing."""

    # ....................... #

    @property
    def total_imported(self) -> int:
        return sum(doc.imported for doc in self.documents)

    @property
    def total_blobs(self) -> int:
        return sum(route.uploaded for route in self.storage)

    @property
    def total_vertices(self) -> int:
        return sum(module.vertices for module in self.graph)

    @property
    def total_edges(self) -> int:
        return sum(module.edges for module in self.graph)


# ....................... #


@attrs.frozen(kw_only=True)
class MigrateReport:
    """The outcome of a direct :func:`migrate` — what landed in the target, per plane.

    A migration is an export and an import fused per chunk, so its result is the *import* half's
    shape: the same :class:`DocumentImport` / :class:`StorageImport` outcomes, reporting what the
    target received. ``rebuild`` comes from the source plan (not a manifest — there is none), so the
    operator still learns which derived planes to recompute on the target after the copy.
    """

    documents: tuple[DocumentImport, ...]
    storage: tuple[StorageImport, ...] = ()
    graph: tuple[GraphImport, ...] = ()
    rebuild: tuple[str, ...]

    # ....................... #

    @property
    def total_imported(self) -> int:
        return sum(doc.imported for doc in self.documents)

    @property
    def total_blobs(self) -> int:
        return sum(route.uploaded for route in self.storage)

    @property
    def total_vertices(self) -> int:
        return sum(module.vertices for module in self.graph)

    @property
    def total_edges(self) -> int:
        return sum(module.edges for module in self.graph)
