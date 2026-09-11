"""Resolving a spec's derived read fields against the in-memory store.

A real backend reads a derived field straight from its relation, because the view
already joined it. The mock has no view — but it holds every row of every spec, so
it can perform the join itself, which is the only reason a view-backed aggregate is
reachable in tests at all.

Resolution is deliberately narrow: one hop, by primary key, no predicates and no
ordering. That bound is what keeps the mock's answer comparable to the real one
rather than turning it into a small ORM whose divergences are its own.

A *marked* derived field never reaches here. Its value is whatever the stored row
carries, so the marker needs no adapter code at all — the deps factory resolves only
the fields that declare a join.
"""

from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze_mock.tenancy import partition_namespace

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ResolvedDerivedRead:
    """One derived read field, with its source located at wiring time.

    The namespace is resolved by the deps factory rather than looked up here: the
    factory holds the spec registry and the routing config, and an adapter resolving
    specs at read time would be re-deriving what freezing already settled.
    """

    namespace: str
    """The source's namespace, before tenant partitioning."""

    via: str
    """Field on the reading document carrying the source row's primary key."""

    field: str
    """Field on the source row whose value lands on the reading document."""

    optional: bool = False
    """Whether an absent source row (or an unset key) yields ``None``."""

    tenant_scoped: bool = False
    """Whether the source's namespace is partitioned by tenant."""


# ....................... #


def hydrate_derived(
    doc: JsonDict,
    *,
    derived: Mapping[str, ResolvedDerivedRead],
    store_for: Callable[[str], Mapping[UUID, JsonDict]],
    tenant_id: UUID | None,
    spec_name: object,
) -> JsonDict:
    """Return *doc* with every derived field resolved from its source row.

    Idempotent: a document already carrying resolved values is hydrated to the same
    result, which is what lets the single hydration point sit on a path that is
    sometimes reached twice. An empty *derived* returns a copy — callers guard on the
    spec having any, so this is not the path a read without derived fields takes.

    *store_for* takes a namespace and returns the rows in it — the adapter's own
    accessor, so a derived read observes the same transaction snapshot the reading
    document does rather than the live store beneath it.

    :raises exc.internal: when a set key resolves to no row and the field is not
        declared ``optional``. In a store that holds every row, a dangling key is a
        seeding bug, and silence would surface it as a confusing assertion later.
    """

    out = dict(doc)

    for name, spec in derived.items():
        raw_key = out.get(spec.via)

        if raw_key is None:
            if not spec.optional:
                raise exc.internal(
                    f"Derived read field {name!r} on spec {spec_name!r} joins on "
                    f"{spec.via!r}, which is unset; declare optional=True if the key "
                    f"is nullable.",
                )

            out[name] = None
            continue

        try:
            key = raw_key if isinstance(raw_key, UUID) else UUID(str(raw_key))
        except (ValueError, AttributeError, TypeError) as e:
            raise exc.internal(
                f"Derived read field {name!r} on spec {spec_name!r} joins on "
                f"{spec.via!r}, whose value {raw_key!r} is not a primary key.",
            ) from e

        rows = store_for(_namespaced(spec, tenant_id))
        row = rows.get(key)

        if row is None:
            if not spec.optional:
                raise exc.internal(
                    f"Derived read field {name!r} on spec {spec_name!r} joins "
                    f"{spec.via!r}={key} against {spec.namespace!r}, which holds no "
                    f"such row; seed the source before reading, or declare "
                    f"optional=True.",
                )

            out[name] = None
            continue

        out[name] = row.get(spec.field)

    return out


# ....................... #


def _namespaced(spec: ResolvedDerivedRead, tenant_id: UUID | None) -> str:
    """The source namespace to read, partitioned only where the source is tenant-scoped.

    A tenant-scoped source read without the partition would reach whatever sits in the
    unpartitioned namespace — the wrong row, or another tenant's. The wiring refuses the
    one pairing that cannot supply a tenant here, so an unset *tenant_id* on a scoped
    source is unreachable rather than silently resolved.
    """

    if not spec.tenant_scoped:
        return spec.namespace

    return partition_namespace(tenant_id, spec.namespace)


# ....................... #


def require_marked(
    doc: JsonDict,
    *,
    marked: frozenset[str],
    read_model: type[BaseModel],
    spec_name: object,
    requested: Sequence[str] | None = None,
) -> None:
    """Refuse a row missing a *required* marked derived field, naming how to supply it.

    A marked field's value comes from the stored row, so an unsupplied one reaches pydantic
    as a missing required field and escapes as a raw ``ValidationError`` — which says
    nothing about the declaration that put it there. Refusing here turns a pydantic
    traceback into the one sentence a reader needs.

    Optional fields are left alone: absence is what ``None`` is for, and so are fields
    outside *requested* — a projection that does not name a marked field cannot hand an
    unsupplied value to anyone, so refusing it would reject a read that is fine.

    :raises exc.configuration: when a required marked field is absent from the row.
    """

    fields = read_model.model_fields
    # A projection names top-level fields, dotted for nested paths; the marked name is
    # what the first segment has to match.
    asked = None if requested is None else {name.split(".", 1)[0] for name in requested}
    missing = sorted(
        name
        for name in marked
        if name not in doc
        and name in fields
        and fields[name].is_required()
        and (asked is None or name in asked)
    )

    if missing:
        raise exc.configuration(
            f"Spec {spec_name!r} declares {missing} derived, and the stored row carries "
            "no value for them. A marked derived field is produced by the relation, so "
            "in the mock it has to be supplied — seed it with SpecSeed(derived={...}) — "
            "or give it a join to resolve, or make it optional on the read model.",
            code="mock.document.derived_unsupplied",
        )


# ....................... #

_STAGED: ContextVar[Mapping[tuple[str, UUID], JsonDict] | None] = ContextVar(
    "forze_mock_staged_derived",
    default=None,
)
"""Derived values visible to reads before they are written onto the row."""


@contextmanager
def staged_derived(values: Mapping[tuple[str, UUID], JsonDict]) -> Iterator[None]:
    """Make *values* readable for the duration, keyed by ``(spec name, primary key)``.

    ``create`` stores the row and then awaits ``drain_domain_events`` **before it
    returns**, so a handler that reads the created aggregate runs before a seeder can
    write its derived values — and sees a required marked field as missing. Staging
    closes that window: during the create the values read exactly as though they were
    already on the row, and the seeder persists them immediately afterwards.
    """

    token = _STAGED.set(values)

    try:
        yield
    finally:
        _STAGED.reset(token)


# ....................... #


def staged_for(spec_name: object, pk: object) -> JsonDict:
    """Staged derived values for one row, or nothing staged."""

    staged = _STAGED.get()

    if not staged or pk is None:
        return {}

    try:
        # A stored row carries its id as a string; the staging key is the UUID the
        # seeder minted, so the two have to be brought to one type.
        key = pk if isinstance(pk, UUID) else UUID(str(pk))
    except (ValueError, AttributeError, TypeError):
        return {}

    return dict(staged.get((str(spec_name), key), {}))
