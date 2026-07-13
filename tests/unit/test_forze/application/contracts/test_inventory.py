"""Unit tests for the spec inventory and its reconciliation against the wired dependencies.

# covers: forze.application.contracts.inventory.SpecRegistry
# covers: forze.application.contracts.inventory.reconcile_specs

The inventory exists because a spec is passed to a port at *resolve* time and stored nowhere,
so nothing could answer "what does this application consist of?". The reconciliation exists
because completeness is exactly what an author cannot verify by eye: most of an app's specs
are not written by its author.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.deps import Deps, frame_for
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.contracts.inventory import (
    PlaneDisposition,
    SpecEdgeKind,
    SpecPlane,
    SpecRegistry,
    SpecSource,
    reconcile_specs,
)
from forze.application.contracts.outbox import OutboxSpec
from forze.application.contracts.procedure import ProcedureSpec
from forze.application.contracts.search import SearchSpec
from forze.application.execution import DepsRegistry, ExecutionRuntime, build_runtime
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec

# ----------------------- #


class _Model(BaseModel):
    id: str


def _document(name: str) -> DocumentSpec[_Model, _Model, _Model, _Model]:
    return DocumentSpec(
        name=name,
        read=_Model,
        write={"domain": _Model, "create_cmd": _Model, "update_cmd": _Model},
    )


def _bound(*names: str) -> Deps:
    """Deps that bind each *name* as a routed document."""

    return Deps.routed(
        {
            DocumentQueryDepKey: dict.fromkeys(names, object()),
            DocumentCommandDepKey: dict.fromkeys(names, object()),
        }
    )


def _frames(*names: str) -> frozenset:
    return frozenset(frame_for(DocumentQueryDepKey, name) for name in names)


# ----------------------- #
# The registry


def test_planes_and_dispositions_are_inferred_from_the_spec_type() -> None:
    registry = SpecRegistry().register(
        _document("orders"),
        SearchSpec(name="orders_idx", model_type=_Model, fields=["id"]),
        OutboxSpec(name="events", codec=PydanticModelCodec(_Model)),
    )

    by_plane = {entry.plane: entry.disposition for entry in registry.freeze().entries}

    # The plane-completeness doctrine, as data: a document travels, a search index is rebuilt
    # from it, an outbox must be empty before anything is copied at all.
    assert by_plane[SpecPlane.DOCUMENT] is PlaneDisposition.EXPORTABLE
    assert by_plane[SpecPlane.SEARCH] is PlaneDisposition.REBUILDABLE
    assert by_plane[SpecPlane.OUTBOX] is PlaneDisposition.DRAINED


def test_counter_and_analytics_default_to_refused() -> None:
    # Neither can be carried faithfully today, and skipping either in silence corrupts the
    # target — a counter with no read path makes a migrated app reissue sequence numbers it
    # already handed out. "We didn't think about it" must not look like "there was nothing
    # there", so the default is a refusal rather than a shrug.
    from forze.application.contracts.counter import CounterSpec

    registry = SpecRegistry().register(CounterSpec(name="invoice_no")).freeze()

    assert registry.of_disposition(PlaneDisposition.REFUSED)[0].name == "invoice_no"


def test_re_registering_an_equal_spec_is_benign() -> None:
    # Load-bearing: several specs are *rebuilt on every access* — StoredFileKitSpec.document
    # mints a fresh DocumentSpec per read, and a kit re-derives its search-sync trio each time
    # it is asked. Identity dedup would emit four copies of one route; value equality is the
    # only thing that works (and a `set` of specs would raise, since DocumentSpec is unhashable).
    registry = SpecRegistry().register(_document("orders")).register(_document("orders"))

    assert len(registry.freeze().entries) == 1


def test_registering_a_different_spec_under_a_taken_name_fails() -> None:
    registry = SpecRegistry().register(_document("orders"))

    with pytest.raises(CoreException, match="identifies exactly one spec"):
        registry.register(
            DocumentSpec(
                name="orders",
                read=_Model,
                write={"domain": _Model, "create_cmd": _Model},  # a different shape
            )
        )


def test_a_spec_with_no_inventoried_plane_is_rejected_not_dropped() -> None:
    # A procedure holds no state, derived data, or in-flight work. Silently ignoring it would
    # leave the caller believing they had contributed something.
    with pytest.raises(CoreException, match="not an inventoried plane"):
        SpecRegistry().register(ProcedureSpec(name="refresh", params=_Model))


def test_entries_are_ordered_deterministically() -> None:
    forward = SpecRegistry().register(_document("b"), _document("a")).freeze()
    backward = SpecRegistry().register(_document("a"), _document("b")).freeze()

    assert [e.name for e in forward.entries] == [e.name for e in backward.entries] == ["a", "b"]


# ----------------------- #
# Reconciliation


def test_a_bound_route_missing_from_the_inventory_is_a_failure() -> None:
    # THE check. Every other guarantee rests on the inventory being complete, and this is the
    # failure an export cannot detect and cannot recover from: a missing plane and an empty
    # one look identical in the artifact.
    registry = SpecRegistry().register(_document("orders")).freeze()

    with pytest.raises(CoreException, match="would silently omit it"):
        reconcile_specs(registry, _frames("orders", "invoices"))


def test_a_catalogued_spec_nothing_binds_is_a_failure() -> None:
    registry = SpecRegistry().register(_document("orders"), _document("ghost")).freeze()

    with pytest.raises(CoreException, match="no dependency binds it"):
        reconcile_specs(registry, _frames("orders"))


def test_allow_unregistered_downgrades_only_the_missing_direction() -> None:
    registry = SpecRegistry().register(_document("orders")).freeze()

    warnings = reconcile_specs(registry, _frames("orders", "invoices"), allow_unregistered=True)

    assert len(warnings) == 1
    assert "document:invoices" in warnings[0]

    # ...but a catalogued-but-unbound spec is a plain wiring bug, and stays fatal.
    unbound = SpecRegistry().register(_document("ghost")).freeze()

    with pytest.raises(CoreException, match="no dependency binds it"):
        reconcile_specs(unbound, _frames(), allow_unregistered=True)


def test_a_plain_registration_satisfies_every_route() -> None:
    # The mock backend registers one provider per key, serving all routes (route=None). Reading
    # that as "binds nothing" would make every mock-backed runtime fail reconciliation.
    registry = SpecRegistry().register(_document("orders"), _document("invoices")).freeze()

    assert reconcile_specs(registry, frozenset({frame_for(DocumentQueryDepKey, None)})) == ()


def test_uninventoried_keys_are_ignored() -> None:
    # A transaction route is an *engine* label the app invents, not a spec name; resilience,
    # crypto and authn routes are likewise nothing an inventory could catalogue. Reconciling
    # them would demand an entry for a thing that has no spec.
    from forze.application.contracts.transaction import TransactionManagerDepKey

    registry = SpecRegistry().register(_document("orders")).freeze()
    frames = _frames("orders") | {frame_for(TransactionManagerDepKey, "default")}

    assert reconcile_specs(registry, frames) == ()


def test_a_dangling_edge_is_a_failure() -> None:
    orders = _document("orders")
    index = SearchSpec(name="orders_idx", model_type=_Model, fields=["id"])

    registry = SpecRegistry()
    registry.register(index).link(SpecEdgeKind.REBUILDS_FROM, source=index, target=orders)

    with pytest.raises(CoreException, match="not in the inventory"):
        reconcile_specs(registry.freeze(), _frames("orders_idx"))


# ----------------------- #
# The runtime seam


def test_the_runtime_reconciles_at_construction() -> None:
    # Construction is the only place that holds both halves — the frozen deps know every
    # (key, route) the app wired, the inventory knows every spec it declared, and neither can
    # see the other anywhere else.
    with pytest.raises(CoreException, match="would silently omit it"):
        build_runtime(
            deps=[_bound("orders", "invoices")],
            specs=SpecRegistry().register(_document("orders")),
        )


def test_no_inventory_means_no_check() -> None:
    # Existing apps are untouched: the field is optional and its absence skips the check.
    runtime = build_runtime(deps=[_bound("orders")])

    assert runtime.spec_registry is None


def test_a_complete_inventory_reaches_the_runtime() -> None:
    runtime = build_runtime(
        deps=[_bound("orders", "invoices")],
        specs=SpecRegistry().register(_document("orders"), _document("invoices")),
    )

    assert runtime.spec_registry is not None
    assert [e.name for e in runtime.spec_registry.entries] == ["invoices", "orders"]


def test_an_execution_runtime_built_by_hand_reconciles_too() -> None:
    registry = DepsRegistry.from_deps(_bound("orders")).freeze()

    with pytest.raises(CoreException, match="no dependency binds it"):
        ExecutionRuntime(
            deps=registry,
            spec_registry=SpecRegistry().register(_document("ghost")).freeze(),
        )


# ----------------------- #
# Framework contributions


def test_identity_contributes_the_nineteen_specs_no_app_declares() -> None:
    # The single most consequential "forgot a spec" case in the codebase — and it is the
    # framework's fault, not the author's. An inventory built from author declarations alone
    # would omit every credential, session and grant in the application.
    import forze_identity

    entries = forze_identity.spec_contributions().freeze().entries
    names = {entry.name for entry in entries}

    assert len(entries) == 19
    assert all(entry.source is SpecSource.FRAMEWORK for entry in entries)
    assert {"authn_token_sessions", "authz_delegation_grants", "tenancy_tenants"} <= names


def test_registries_merge() -> None:
    app = SpecRegistry().register(_document("orders"))
    merged = app.merge(SpecRegistry().register(_document("audit"), source=SpecSource.FRAMEWORK))

    entries = merged.freeze().entries

    assert [e.name for e in entries] == ["audit", "orders"]
    assert entries[0].source is SpecSource.FRAMEWORK
    assert entries[1].source is SpecSource.AUTHOR
