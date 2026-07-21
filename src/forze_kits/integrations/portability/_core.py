"""Pipeline primitives shared by the three portability verbs — export, import, and migrate.

These are the load-bearing, fidelity-critical steps: how a read model becomes a backend-agnostic
row, how a row becomes a keyed create, how a batch lands in a target under the existence +
``on_conflict`` rule, how a scope binds an identity and gates on attestation. They live here, once,
so a file **export**, a file **import**, and a direct **migrate** cannot drift on them — a fix to
the ingest rule or the row projection is a fix to every path at the same time. The verb modules
(:mod:`export`, :mod:`import_`, :mod:`migrate`) are thin orchestrators over exactly this set.

The module is private (``_core``); its members carry no leading underscore because *this file* is
the boundary, and the verb modules import them as a clean internal API.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AbstractContextManager, nullcontext
from typing import Any, Literal, cast
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.counter import CounterEntry
from forze.application.contracts.document import KeyedCreate
from forze.application.contracts.inventory import FrozenSpecRegistry
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionRuntime
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from .report import DocumentImport
from .scope import ExportScope, FullScope, TenantScope

# ----------------------- #

DEFAULT_CHUNK = 500
"""Keyset page size for ``find_stream`` — one page in memory per spec, whatever the size."""

DEFAULT_BATCH = 500
"""Rows per ``ensure_many`` on import/migrate — one batch of creates in memory at a time."""

BLOB_PAGE = 100
"""Objects per ``list`` page while enumerating a storage route's keys."""


# ....................... #


OnConflict = Literal["fail", "skip"]
"""What import/migrate does when a document id already exists in the target:

- ``fail`` — refuse the whole run (a collision means the target was not the empty destination the
  caller assumed).
- ``skip`` — leave the existing row untouched (``ensure`` semantics), so a crashed run re-runs to
  convergence. The default.

Blobs and the graph plane are not gated by this: a blob is always written to its archived key
(``overwrite_stream``), and graph vertices/edges go in through the idempotent ``ensure_vertex`` /
``ensure_edge``, so a re-run of either converges without a conflict check."""


# ....................... #


def require_registry(runtime: ExecutionRuntime) -> FrozenSpecRegistry:
    if runtime.spec_registry is None:
        raise exc.precondition(
            "export/import needs the runtime's spec inventory and found none. Build the runtime "
            "with build_runtime(specs=…) so it knows what it is carrying."
        )

    return runtime.spec_registry


# ....................... #


def portable_row(doc: BaseModel) -> JsonDict:
    """The backend-agnostic JSON for one read model.

    Typed ``BaseModel`` on purpose: a document spec's read type is bound to it, so ``model_dump``
    and ``model_computed_fields`` are guaranteed, not assumed. Model field names, not storage
    columns — the archive must not carry one backend's column layout into another. ``rev`` is
    dropped (optimistic-concurrency lineage resets to 1 on import, RFC §7, so carrying it would
    only make a re-export diverge) and so are computed fields (they recompute on write from the
    fields that *are* carried).
    """

    excluded = {"rev", *type(doc).model_computed_fields}

    return doc.model_dump(mode="json", exclude=excluded)


# ....................... #


def keyed_create(row: JsonDict, create_codec: Any) -> KeyedCreate[Any]:
    """Reconstruct one create payload at its archived id.

    The row decodes through the create codec with ``forbid_extra=False``, so the fields the create
    model does not know — ``id`` (carried on the :class:`KeyedCreate` instead) and any ``rev`` a
    future format leaves behind — are dropped, while ``created_at`` / ``last_update_at`` flow
    through **only if** the create model mixes in ``ImportTimestamps``. Without it, the timestamps
    fall back to the server's write-time stamp: ids and data are always faithful, timestamps are
    faithful when the aggregate opted in.
    """

    raw_id = row.get("id")

    if not isinstance(raw_id, str):
        raise exc.precondition(f"Archive row is missing its 'id': {row!r}")

    return KeyedCreate(id=UUID(raw_id), payload=create_codec.decode_mapping(row))


# ....................... #


async def ingest_documents(
    *,
    query: Any,
    command: Any,
    name: str,
    rows: AsyncIterator[KeyedCreate[Any]],
    on_conflict: OnConflict,
    batch_size: int,
) -> DocumentImport:
    """Batch keyed creates into the target under the soft-existence + ``on_conflict`` rule.

    The single document-write path both a **file import** and a **direct** ``migrate`` go through,
    so the two can never drift on the fidelity-critical ingest. Each batch runs a *soft* membership
    check — ``find_many`` id-in, because ``get_many`` raises on any absent id and most ids are
    absent against a fresh target — so it can tell convergence (all skipped) from a no-op that
    silently dropped rows. ``on_conflict='fail'`` refuses the moment any id already exists;
    otherwise ``ensure_many`` inserts the new rows and leaves existing ones untouched, so a crashed
    or repeated run converges. ``imported`` and ``skipped_existing`` are kept apart for exactly that
    reason — a re-run landing entirely in ``skipped_existing`` is convergence, not lost data.
    """

    imported = 0
    skipped = 0
    batch: list[KeyedCreate[Any]] = []

    async def flush() -> None:
        nonlocal imported, skipped

        if not batch:
            return

        ids = [item.id for item in batch]
        page = await query.find_many({"$values": {"id": {"$in": ids}}})
        existing = {doc.id for doc in page.hits}

        if on_conflict == "fail" and existing:
            raise exc.conflict(
                f"{len(existing)} document(s) for {name!r} already exist in the target; import "
                f"was asked to fail on conflict. Use on_conflict='skip' to converge onto an "
                f"existing target instead."
            )

        await command.ensure_many(batch, return_new=False)
        imported += len(batch) - len(existing)
        skipped += len(existing)
        batch.clear()

    async for item in rows:
        batch.append(item)

        if len(batch) >= batch_size:
            await flush()

    await flush()

    return DocumentImport(name=name, imported=imported, skipped_existing=skipped)


# ....................... #


async def list_keys(query: Any) -> list[str]:
    """Every object key on a route, enumerated to exhaustion before any is streamed.

    Paging and streaming must not interleave (the ``reencrypt_objects`` rule): ``list`` makes no
    ordering promise, so a backend that orders by anything a read touches could reshuffle objects
    under an advancing offset and silently skip some. Only the keys are held; the bytes stream one
    object at a time by the caller.

    ``missing_ok``: a storage route an app declares but has never written to has no bucket yet,
    and exporting such an app must yield *no blobs*, not abort — the archive records the route
    as empty, which import re-provisions on first write.
    """

    keys: list[str] = []
    offset = 0

    while True:
        page, _ = await query.list(BLOB_PAGE, offset, missing_ok=True)

        if not page:
            return keys

        keys.extend(obj.key for obj in page)
        offset += len(page)


# ....................... #


def assert_scope_permitted(scope: ExportScope, *, allow_fuzzy: bool) -> None:
    """Refuse a full-system walk whose quiesce did not attest, unless *allow_fuzzy* opts in.

    A no-op for a :class:`TenantScope` — its consistency is the operator's claim that the tenant is
    quiet. Shared by export and the direct ``migrate`` so both refuse an unattested whole-system
    capture identically: an artifact that only *looks* point-consistent, or a migration that copied
    a still-moving source, is the same "looks complete and is not" failure either way.
    """

    if isinstance(scope, FullScope) and not scope.quiesce.attested and not allow_fuzzy:
        scope.quiesce.raise_if_unattested()


# ....................... #


@attrs.frozen(kw_only=True)
class ScopeSection:
    """One partition of a scoped walk: the identity it binds, and where its files live.

    A scope resolves to **sections**, and every verb (export, import, migrate) walks each
    section under its bound identity — the shape that makes a full-system walk complete on a
    tenant-aware deployment. A :class:`TenantScope` is one section; a :class:`FullScope` is one
    per declared tenant (files under ``tenants/<uuid>/``) or a single unbound one when the
    operator declared :data:`~forze_kits.integrations.portability.scope.UNTENANTED`.
    """

    tenant_id: UUID | None
    """The tenant this section binds; ``None`` for the unbound (untenanted) walk."""

    prefix: str
    """Archive path prefix for the section's files (``""`` or ``tenants/<uuid>/``)."""

    aad_prefix: str
    """Prefix bound into every sealed frame's AAD. For a per-tenant archive the tenant is not in
    the file path, so it is bound here (``tenant:<uuid>|``) — a sealed archive whose manifest is
    edited to name another tenant then fails frame authentication instead of importing into the
    wrong partition. Full-scope sections carry the tenant in the path itself, so this stays empty.
    """

    # ....................... #

    def file_aad(self, rel: str) -> str:
        """The AAD a sealed data file at archive path *rel* is bound to."""

        return f"{self.aad_prefix}{rel}"

    def blob_aad(self, route: str, key: str) -> str:
        """The AAD a sealed blob object is bound to — its route + key identity, section-scoped."""

        return f"{self.aad_prefix}{self.prefix}blobs/{route}|{key}"


# ....................... #


def scope_sections(scope: ExportScope) -> tuple[ScopeSection, ...]:
    """The sections a scope walks — the one place the scope → partition mapping exists.

    Shared by export and the direct ``migrate`` (and mirrored by import from the manifest), so
    a full-system export and a full-system migration cover exactly the same partitions by
    construction.
    """

    if isinstance(scope, TenantScope):
        return (
            ScopeSection(
                tenant_id=scope.tenant_id,
                prefix="",
                aad_prefix=f"tenant:{scope.tenant_id}|",
            ),
        )

    if scope.tenants == "untenanted":
        return (ScopeSection(tenant_id=None, prefix="", aad_prefix=""),)

    return tuple(
        ScopeSection(tenant_id=tenant, prefix=f"tenants/{tenant}/", aad_prefix="")
        for tenant in scope.tenants
    )


# ....................... #


def section_binding(ctx: ExecutionContext, section: ScopeSection) -> AbstractContextManager[Any]:
    """The identity one section's walk runs under against *ctx*.

    A tenant section binds its tenant, so tenancy's own fail-closed scoping does the filtering
    (and a tenant-aware backend resolves that tenant's partition); the untenanted section binds
    nothing. ``migrate`` binds one on the source and one on the target from the same section, so
    reads and writes land in the same partition.
    """

    if section.tenant_id is not None:
        return ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=section.tenant_id))

    return nullcontext()


# ....................... #


def section_label(section: ScopeSection, name: str) -> str:
    """A report label that stays unambiguous when a full-system walk repeats per tenant."""

    return name if section.prefix == "" else f"{section.tenant_id}/{name}"


# ....................... #


def counter_row(entry: CounterEntry) -> JsonDict:
    """One counter partition as an archive row: its ``suffix`` and its current ``value``.

    ``suffix`` is kept as-is — ``None`` is a real, distinct counter (the unsuffixed one), not
    "no counter", and dropping it would export every partition of a sequence except the one most
    applications actually use.
    """

    return {"suffix": entry.suffix, "value": entry.value}


def counter_reset_args(row: JsonDict) -> tuple[int, str | None]:
    """The ``(value, suffix)`` a counter row restores through ``CounterPort.reset`` — read here, in
    one place, so export's row shape and import's read cannot drift apart."""

    return cast("int", row["value"]), cast("str | None", row["suffix"])
