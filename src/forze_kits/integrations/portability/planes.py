"""Which planes an export of this version carries, rebuilds, or must refuse.

The plane-completeness doctrine (RFC 0016) says every plane an application binds declares itself
*exportable*, *rebuildable*, *drained*, or *refused*, and an export **refuses anything it cannot
account for** — silence is never read as "nothing to carry". This module is where that doctrine
meets a *partial* implementation: P1 carries the document plane, and it must refuse a plane it
*could* carry in principle but has not built yet, exactly as loudly as it refuses one that can
never be carried. Shipping a half-complete artifact that looks whole is the one outcome the whole
feature exists to prevent.
"""

from __future__ import annotations

from typing import Any, cast

import attrs

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.inventory import (
    FrozenSpecRegistry,
    PlaneDisposition,
    SpecPlane,
    SpecRegistryEntry,
    assert_exportable,
)
from forze.base.exceptions import exc

# ----------------------- #

_CARRIED_PLANES = frozenset({SpecPlane.DOCUMENT})
"""The exportable planes this version writes into an archive. Grows with each phase: blobs and
counters (RFC §10 P2), graph (P4)."""


# ....................... #


@attrs.frozen(kw_only=True)
class ExportPlan:
    """What a registry resolves to for this export version."""

    documents: tuple[SpecRegistryEntry, ...]
    """Document entries to stream out, in catalogue order."""

    rebuild: tuple[str, ...]
    """Rebuildable plane routes the target recomputes (search, cache, projected analytics) —
    named in the manifest, never exported."""


# ....................... #


def plan_export(registry: FrozenSpecRegistry) -> ExportPlan:
    """Resolve *registry* into an :class:`ExportPlan`, or refuse.

    Three refusals, in order, each a fail-closed guard rather than a silent skip:

    - ``assert_exportable`` first — a ``REFUSED`` plane (an analytics table nobody declared
      recomputable, a graph holding an unwalkable kind) stops the export before it writes a byte.
    - An ``EXPORTABLE`` plane this version does not yet carry (storage, counters, graph) — refused
      by name, because skipping it would ship an archive that looks complete and is not.
    - A read-only document (no ``write`` model) — refused, because it cannot be imported, so
      exporting it produces a file no target can consume.
    """

    assert_exportable(registry)

    documents: list[SpecRegistryEntry] = []
    rebuild: list[str] = []
    unsupported: list[str] = []

    for entry in registry.entries:
        if entry.disposition is PlaneDisposition.REBUILDABLE:
            rebuild.append(entry.ref.label())
            continue

        if entry.disposition is not PlaneDisposition.EXPORTABLE:
            # DRAINED planes (outbox, inbox, durable, offsets) hold operational, in-flight work;
            # quiesce (or, per-tenant, tenant quiet) brings them to empty and they are never
            # carried. REFUSED was already handled by assert_exportable.
            continue

        if entry.plane not in _CARRIED_PLANES:
            unsupported.append(entry.ref.label())
            continue

        # A DOCUMENT-plane entry always holds a ``DocumentSpec`` (the inventory maps the type to
        # the plane), so narrow without re-checking — the same idiom ``export``/``quiesce`` use.
        if cast("DocumentSpec[Any, Any, Any, Any]", entry.spec).write is None:
            raise exc.precondition(
                f"Cannot export {entry.ref.label()!r}: it is a read-only document with no write "
                f"model, so no target could import it. Exclude it, or give it a create model."
            )

        documents.append(entry)

    if unsupported:
        raise exc.precondition(
            "This export version carries the document plane only; it cannot yet carry "
            f"{', '.join(sorted(unsupported))}. Exporting anyway would ship an archive that "
            f"looks complete and is not. Support arrives in a later phase (RFC 0017 §10)."
        )

    return ExportPlan(documents=tuple(documents), rebuild=tuple(rebuild))
