"""Resolving a spec's derived read fields against the in-memory store.

A real backend reads a derived field straight from its relation, because the view
already joined it. The mock has no view — but it holds every row of every spec, so
it can perform the join itself, which is the only reason a view-backed aggregate is
reachable in tests at all.

Resolution is deliberately narrow: one hop, by primary key, no predicates and no
ordering. That bound is what keeps the mock's answer comparable to the real one
rather than turning it into a small ORM whose divergences are its own.
"""

from collections.abc import Callable, Mapping
from uuid import UUID

import attrs

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
