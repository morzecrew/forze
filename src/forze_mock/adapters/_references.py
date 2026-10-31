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

from collections.abc import Callable, Iterator, Mapping
from typing import TYPE_CHECKING, Any
from uuid import UUID

from forze.application.contracts.guarantees import Reference
from forze.application.contracts.querying.internal.matching import instant_key
from forze.base.exceptions import CoreException, exc
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
                (
                    str(spec.name),
                    namespace,
                    tenant_scoped,
                    reference.fields,
                    reference.references,
                    reference.on_delete,
                    reference.set_null_fields or reference.fields,
                )
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


def _key(row: JsonDict, names: tuple[str, ...]) -> tuple[Any, ...] | None:
    """*row*'s values under *names*, compared as a foreign key compares them, or ``None`` when
    one is null: a null matches nothing, as a key leaves a row holding one unchecked
    (``MATCH SIMPLE``)."""

    values = [row.get(name) for name in names]

    if any(value is None for value in values):
        return None

    # A UUID meets its JSON string, as a seeded row holds it; an aware datetime is the instant
    # it is, as `timestamptz` compares, whatever offset it was written in.
    return tuple(str(value) if isinstance(value, UUID) else instant_key(value) for value in values)


def _stores(
    state: MockState, namespace: str, tenant_scoped: bool, tenant: UUID | None
) -> list[tuple[str, UUID | None]]:
    """Where a referencing spec's rows naming a row of *tenant* live, with their tenant."""

    if not tenant_scoped:
        return [(namespace, None)]

    if tenant is not None:
        return [(partition_namespace(tenant, namespace), tenant)]

    return list(_partitions(state, namespace))


def _restricted(summary: str, *, child: str, fields: tuple[str, ...], parent: str) -> CoreException:
    """The ``conflict`` a restricted delete raises, as Postgres's restrict violation maps."""

    refusal = exc.conflict(
        summary, details={"spec": child, "fields": list(fields), "references": parent}
    )
    # For the logs only, as Postgres's: below an unscoped parent the child may belong to
    # another tenant.
    refusal.expose_details = False

    return refusal


def _through(fields: tuple[str, ...]) -> str | tuple[str, ...]:
    return fields[0] if len(fields) == 1 else fields


def _refuse_stranded(
    state: MockState,
    *,
    name: str,
    row: JsonDict,
    cleared: set[str],
    tenant: UUID | None,
    gone: set[tuple[str, UUID]],
    store_for: Callable[[str], dict[UUID, JsonDict]],
) -> None:
    """Refuse clearing *cleared* on *row*, of spec *name*, while a row another reference keys
    on those fields still names its old values.

    No key here updates its referencing rows, so Postgres refuses the ``SET NULL``'s update
    under that key (``ON UPDATE NO ACTION``). A row the same delete removes, in *gone*, does
    not count.

    :raises CoreException: ``conflict``, as a restricted delete.
    """

    for child, child_ns, tenant_scoped, fields, references, _, _ in sorted(
        state.document_references.get(name, ())
    ):
        if not cleared & set(references) or (wanted := _key(row, references)) is None:
            continue

        for ns, _ in _stores(state, child_ns, tenant_scoped, tenant):
            for pk, stranded in store_for(ns).items():
                if (ns, pk) not in gone and _key(stranded, fields) == wanted:
                    raise _restricted(
                        f"A delete would clear {sorted(cleared & set(references))} on a row of "
                        f"{name!r} while rows of {child!r} reference it through "
                        f"{_through(fields)!r}.",
                        child=child,
                        fields=fields,
                        parent=name,
                    )


# ....................... #


def apply_references(
    state: MockState,
    *,
    parent: str,
    namespace: str,
    rows: Mapping[UUID, JsonDict],
    tenant: UUID | None,
    store_for: Callable[[str], dict[UUID, JsonDict]],
) -> None:
    """Apply each declared ``on_delete`` to the rows naming *rows*, and to theirs in turn.

    A row names a parent row when its reference's fields hold, pairwise, the values of the
    parent's referenced fields; a row holding a null in any of them names nothing.

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

    A ``set_null`` clears the reference's ``set_null_fields``, every field by default. It is an
    update of the child, so it keeps the child's guarantees, checked as a write of the child
    checks them against what the whole cascade leaves, before anything changes. A violation
    refuses the delete with the write's ``conflict``, as Postgres checks the child's constraints
    after the action. The revision is left as it was, as under application bookkeeping; a
    trigger keeping it (``database`` bookkeeping) would bump it. A cleared field that another
    reference names on the cleared row's side refuses the delete while rows reference the old
    value, as Postgres refuses the update under their key (``ON UPDATE NO ACTION``).

    The caller holds ``state.lock`` and deletes the parent rows itself.

    :param parent: The name of the spec *rows* belong to.
    :param namespace: The parent rows' namespace, tenant partition included.
    :param rows: The parent rows, by primary key.
    :param tenant: The parent rows' tenant, or ``None`` when the parent is not tenant-scoped.
    :param store_for: The rows of a namespace, through the transaction's view.
    :raises CoreException: ``conflict``, as Postgres's restrict and foreign-key violations map
        on a hard delete, when a ``restrict`` reference still names a row, and as the child's
        own write raises it when a cleared row breaks one of its guarantees or leaves rows
        referencing a value it cleared; ``configuration``
        when a row to clear belongs to a spec with no adapter built in this process, whose
        guarantees therefore cannot be checked.
    """

    pending = [(parent, row, tenant) for row in rows.values()]
    killed = frozenset((namespace, pk) for pk in rows)
    seen = set(killed)
    doomed: list[tuple[dict[UUID, JsonDict], UUID]] = []
    # (namespace, row) -> (store, spec, fields to clear)
    nulled: dict[tuple[str, UUID], tuple[dict[UUID, JsonDict], str, set[str]]] = {}
    tenants: dict[tuple[str, UUID], UUID | None] = {}

    while pending:
        name, parent_row, row_tenant = pending.pop()

        # Sorted, so a run reaches the same refusal first whatever the set's hash order.
        for child, child_ns, tenant_scoped, fields, references, on_delete, nullable in sorted(
            state.document_references.get(name, ())
        ):
            wanted = _key(parent_row, references)

            if wanted is None:
                continue

            # shortcut: a scan per referenced row; index the reference field if cascades get big.
            for ns, child_tenant in _stores(state, child_ns, tenant_scoped, row_tenant):
                store = store_for(ns)

                for child_pk, row in store.items():
                    if (ns, child_pk) in killed or _key(row, fields) != wanted:
                        continue

                    if on_delete == "restrict":
                        raise _restricted(
                            f"A row of {name!r} cannot be deleted while rows of {child!r} "
                            f"reference it through {_through(fields)!r} (on_delete='restrict').",
                            child=child,
                            fields=fields,
                            parent=name,
                        )

                    if (ns, child_pk) in seen:
                        continue

                    if on_delete == "set_null":
                        nulled.setdefault((ns, child_pk), (store, child, set()))[2].update(nullable)
                        tenants[(ns, child_pk)] = child_tenant
                        continue

                    seen.add((ns, child_pk))
                    doomed.append((store, child_pk))
                    pending.append((child, row, child_tenant))

    for (ns, child_pk), (store, child, fields_cleared) in nulled.items():
        if (ns, child_pk) not in seen:
            _refuse_stranded(
                state,
                name=child,
                row=store[child_pk],
                cleared=fields_cleared,
                tenant=tenants[(ns, child_pk)],
                gone=seen,
                store_for=store_for,
            )

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
