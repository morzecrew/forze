"""Declared references, applied when the in-memory store hard-deletes a referenced row.

The in-memory store has no foreign keys, so a parent's ``kill`` used to leave the rows naming it
behind: still listed, and colliding with their replacement on a declared uniqueness. A
:class:`~forze.application.contracts.guarantees.Reference` on the child spec says what Postgres's
``ON DELETE`` does, and this applies it.

The parent knows nothing about its children, so each child spec's references are recorded on the
state (:attr:`~forze_mock.state.MockState.document_references`), keyed by the spec each one names,
when the child's adapter is built and again on each of its writes. The record is persisted with
the rows. A row whose spec has had no adapter built against this state, or since its last
``clear``, is not reached: nothing here knows that spec exists.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from typing import TYPE_CHECKING, Any
from uuid import UUID

from forze.application.contracts.guarantees import Reference
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze_mock.tenancy import partition_namespace

if TYPE_CHECKING:
    from forze.application.contracts.document import DocumentSpec
    from forze_mock.state import MockState

# ----------------------- #


def record_references(
    state: MockState,
    spec: DocumentSpec[Any, Any, Any, Any],
    *,
    namespace: str,
    tenant_scoped: bool,
) -> None:
    """Record *spec*'s references on *state*, keyed by the spec each one names.

    :param namespace: The namespace *spec*'s rows live in, before any tenant partition.
    :param tenant_scoped: Whether those rows are partitioned by tenant.
    """

    references = [g for g in spec.guarantees if isinstance(g, Reference)]

    if not references:
        return

    with state.lock:
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

    The caller holds ``state.lock`` and deletes the parent rows itself.

    :param parent: The name of the spec *pks* belong to.
    :param namespace: The parent rows' namespace, tenant partition included.
    :param tenant: The parent rows' tenant, or ``None`` when the parent is not tenant-scoped.
    :param store_for: The rows of a namespace, through the transaction's view.
    :raises CoreException: ``conflict``, as Postgres's restrict and foreign-key violations map
        on a hard delete, when a ``restrict`` reference still names a row.
    """

    pending = [(parent, pk, tenant) for pk in pks]
    killed = frozenset((namespace, pk) for _, pk, _ in pending)
    seen = set(killed)
    doomed: list[tuple[dict[UUID, JsonDict], UUID]] = []
    nulled: list[tuple[dict[UUID, JsonDict], UUID, str]] = []

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
                        nulled.append((store, child_pk, field))
                        continue

                    seen.add((ns, child_pk))
                    doomed.append((store, child_pk))
                    pending.append((child, child_pk, child_tenant))

    for store, child_pk in doomed:
        del store[child_pk]

    # shortcut: the field is cleared without a revision bump, as under application bookkeeping
    # (a Postgres trigger keeping the revision would bump it), and the child's own guarantees
    # are not rechecked; recheck them if a NULLS NOT DISTINCT uniqueness over the field matters.
    for store, child_pk, field in nulled:
        if child_pk in store:
            store[child_pk] = {**store[child_pk], field: None}
