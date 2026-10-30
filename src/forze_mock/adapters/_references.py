"""Declared references, applied when the in-memory store hard-deletes a referenced row.

The in-memory store has no foreign keys, so a parent's ``kill`` used to leave the rows naming it
behind: still listed, and colliding with their replacement on a declared uniqueness. A
:class:`~forze.application.contracts.guarantees.Reference` on the child spec says what Postgres's
``ON DELETE`` does, and this applies it.

The parent knows nothing about its children, so each child spec's references are recorded on the
state (:attr:`~forze_mock.state.MockState.document_references`), keyed by the spec each one names,
when the child's adapter is built and again on each of its writes. The record is persisted with
the rows. A row whose spec has had no adapter built against this state, or since its last
``clear``, is not reached: nothing here knows that spec exists. The adapter's guarantee check is
kept beside it, for the rows a ``set_null`` clears; it lives only as long as the process.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from typing import TYPE_CHECKING, Any
from uuid import UUID

from forze.application.contracts.guarantees import Reference
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze_mock.adapters._mvcc import current_mvcc_tx
from forze_mock.tenancy import partition_namespace

if TYPE_CHECKING:
    from forze.application.contracts.document import DocumentSpec
    from forze_mock.state import MockState

# ----------------------- #


GuaranteeCheck = Callable[..., None]
"""A spec's write-time guarantee check, ``(store, pk, row, *, deferred)``, raising on a
violation: the bound ``_check_guarantees`` of one of its adapters."""


def record_references(
    state: MockState,
    spec: DocumentSpec[Any, Any, Any, Any],
    *,
    namespace: str,
    tenant_scoped: bool,
    check: GuaranteeCheck,
) -> None:
    """Record *spec*'s references on *state*, keyed by the spec each one names.

    :param namespace: The namespace *spec*'s rows live in, before any tenant partition.
    :param tenant_scoped: Whether those rows are partitioned by tenant.
    :param check: *spec*'s guarantee check, which a ``set_null`` runs on the row it clears.
    """

    references = [g for g in spec.guarantees if isinstance(g, Reference)]

    if not references:
        return

    with state.lock:
        state.reference_checks[str(spec.name)] = check

        for reference in references:
            state.document_references.setdefault(reference.spec, set()).add(
                (str(spec.name), namespace, tenant_scoped, reference.field, reference.on_delete)
            )


# ....................... #


def _partitions(state: MockState, namespace: str) -> Iterator[tuple[str, UUID]]:
    """Every tenant partition of *namespace* in *state*, with its tenant."""

    for ns in list(state.documents):
        prefix, _, rest = ns.partition("/")

        if rest != namespace:
            continue

        try:
            yield ns, UUID(prefix)

        except ValueError:
            continue


# ....................... #


def apply_references(
    state: MockState,
    *,
    parent: str,
    namespace: str,
    pks: Iterable[UUID],
    tenant: UUID | None,
    store_for: Callable[[str], dict[UUID, JsonDict]],
) -> None:
    """Apply each declared ``on_delete`` to the rows naming *pks*, and to theirs in turn.

    The whole cascade is planned before anything changes, so a ``restrict`` anywhere in it
    refuses the delete with nothing written, as one Postgres statement does. Rows are read and
    written through *store_for*, the active transaction's view, so a rollback undoes the cascade
    with the parent. Each row is visited once, so a cycle of references ends.

    A ``restrict`` reference refuses even from a row the same cascade deletes, whatever the
    order. Postgres refuses or allows that delete depending on which key was created first, so
    this takes the stricter reading. A row among *pks* never refuses: Postgres deletes them all
    before any key is checked.

    A tenant-scoped referencing spec is searched in the partition of the row it references, the
    one its own adapter reads under that tenant. Below an unscoped row it is searched in every
    tenant's partition, since a foreign key reaches every tenant's rows.

    A ``set_null`` is an update of the child, so it keeps the child's guarantees, checked as a
    write of the child checks them against what the whole cascade leaves, before anything
    changes. A violation refuses the delete with the write's ``conflict``, as Postgres checks
    the child's constraints after the action. The revision is left as it was, as under
    application bookkeeping; a trigger keeping it (``database`` bookkeeping) would bump it.

    The caller holds ``state.lock`` and deletes the parent rows itself.

    :param parent: The name of the spec *pks* belong to.
    :param namespace: The parent rows' namespace, tenant partition included.
    :param tenant: The parent rows' tenant, or ``None`` when the parent is not tenant-scoped.
    :param store_for: The rows of a namespace, through the transaction's view.
    :raises CoreException: ``conflict``, as Postgres's restrict and foreign-key violations map
        on a hard delete, when a ``restrict`` reference still names a row, and as the child's
        own write raises it when a cleared row breaks one of its guarantees; ``configuration``
        when a row to clear belongs to a spec with no adapter built in this process, whose
        guarantees therefore cannot be checked.
    """

    pending = [(parent, pk, tenant) for pk in pks]
    killed = frozenset((namespace, pk) for _, pk, _ in pending)
    seen = set(killed)
    doomed: list[tuple[dict[UUID, JsonDict], UUID]] = []
    # (namespace, row) -> (store, spec, fields to clear)
    nulled: dict[tuple[str, UUID], tuple[dict[UUID, JsonDict], str, set[str]]] = {}

    while pending:
        name, pk, row_tenant = pending.pop()

        # Sorted, so a run reaches the same refusal first whatever the set's hash order.
        for child, child_ns, tenant_scoped, field, on_delete in sorted(
            state.document_references.get(name, ())
        ):
            stores: list[tuple[str, UUID | None]]

            if not tenant_scoped:
                stores = [(child_ns, None)]

            elif row_tenant is not None:
                stores = [(partition_namespace(row_tenant, child_ns), row_tenant)]

            else:
                stores = list(_partitions(state, child_ns))

            # shortcut: a scan per referenced row; index the reference field if cascades get big.
            for ns, child_tenant in stores:
                store = store_for(ns)

                for child_pk, row in store.items():
                    if (ns, child_pk) in killed or str(row.get(field)) != str(pk):
                        continue

                    if on_delete == "restrict":
                        refusal = exc.conflict(
                            f"A row of {name!r} cannot be deleted while rows of {child!r} "
                            f"reference it through {field!r} (on_delete='restrict').",
                            details={"spec": child, "field": field, "references": name},
                        )
                        # For the logs only, as Postgres's: below an unscoped parent the child
                        # may belong to another tenant.
                        refusal.expose_details = False

                        raise refusal

                    if (ns, child_pk) in seen:
                        continue

                    if on_delete == "set_null":
                        nulled.setdefault((ns, child_pk), (store, child, set()))[2].add(field)
                        continue

                    seen.add((ns, child_pk))
                    doomed.append((store, child_pk))
                    pending.append((child, child_pk, child_tenant))

    cleared = _check_cleared(state, nulled, gone=seen)

    for store, child_pk in doomed:
        del store[child_pk]

    mvcc = current_mvcc_tx()

    for (ns, child_pk), (store, row, check) in cleared.items():
        store[child_pk] = row

        if mvcc is not None:
            mvcc.mark_guarantee_recheck(ns, child_pk, check)


# ....................... #


def _check_cleared(
    state: MockState,
    nulled: dict[tuple[str, UUID], tuple[dict[UUID, JsonDict], str, set[str]]],
    *,
    gone: set[tuple[str, UUID]],
) -> dict[tuple[str, UUID], tuple[dict[UUID, JsonDict], JsonDict, GuaranteeCheck]]:
    """Each row a ``set_null`` clears, checked against its namespace as the cascade leaves it.

    :param gone: The rows the delete removes, left out of what a cleared row is checked against.
    :returns: Per row, its store, the cleared row and the check it passed.
    """

    staged: dict[str, dict[UUID, JsonDict]] = {}
    cleared: dict[tuple[str, UUID], tuple[dict[UUID, JsonDict], JsonDict, GuaranteeCheck]] = {}

    for (ns, child_pk), (store, child, fields) in nulled.items():
        if (ns, child_pk) in gone:
            continue

        check = state.reference_checks.get(child)

        if check is None:
            raise exc.configuration(
                f"A delete would clear rows of {child!r}, whose guarantees cannot be checked: no "
                f"adapter for {child!r} has been built in this process. Resolve its port first.",
                code="mock.document.reference_unchecked",
                details={"spec": child},
            )

        rows = staged.get(ns)

        if rows is None:
            rows = staged[ns] = {pk: row for pk, row in store.items() if (ns, pk) not in gone}

        rows[child_pk] = {**rows[child_pk], **dict.fromkeys(fields)}
        cleared[(ns, child_pk)] = (store, rows[child_pk], check)

    deferred = current_mvcc_tx() is None

    for (ns, child_pk), (_, row, check) in cleared.items():
        check(staged[ns], child_pk, row, deferred=deferred)

    return cleared
