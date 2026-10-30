"""What an adapter can enforce, declared per adapter, and the reconciliation against it.

The mirror of the query capabilities: there a backend publishes which filter operators its
renderer compiles and a validator refuses a query that strays outside them. Here a backend
publishes which guarantees it can keep, and the same kind of refusal fires when a spec asks for
one it cannot.

Two differences from that convention are deliberate.

The declaration is **read off the port**, not held as a module constant inside a renderer. Every
shipped capability declaration is private to the code that consults it, which works because that
code is the only consumer — a query never leaves the renderer that checked it. A guarantee's
consumer is dependency resolution, which holds a spec and a port and nothing else, so the
declaration has to be somewhere resolution can see.

And the default is **nothing**. A port that declares no guarantees is read as enforcing none, so
a spec that asks for one is refused rather than served by a store that quietly does not keep it.
Fail-closed is the whole point: a skipped guarantee turns a declared property into a comment,
which is the failure every consumer of this vocabulary was written to escape. It costs nothing
today, because a spec with no guarantees asks for nothing and every adapter satisfies it.
"""

from typing import Final, Protocol, runtime_checkable

import attrs

from forze.base.exceptions import exc

from ..deps import backend_of
from .value_objects import (
    GuaranteeKind,
    GuaranteeMoment,
    NonOverlapping,
    Reference,
    SerializedBy,
    StorageGuarantee,
    StorageGuarantees,
    UniqueTogether,
)

# ----------------------- #

GUARANTEE_UNSUPPORTED: Final[str] = "storage_guarantee_unsupported"
"""Code on the refusal, so a caller can tell "this backend cannot keep that" from "something
broke" — the reason :data:`query_feature_unsupported` exists in the sibling convention."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class StorageGuaranteeCapabilities:
    """Which guarantees a store can enforce.

    Its own value rather than flags on an existing plane's capabilities, because a guarantee is
    not a query feature and the planes that can carry one are not the planes that render filters.

    Every flag defaults to ``False``. A backend states what it can keep; silence is not consent.
    """

    unique_together: bool = False
    """Whether uniqueness over a field tuple is enforced."""

    unique_together_filtered: bool = False
    """Whether uniqueness over a *subset* of rows is enforced.

    Separate from :attr:`unique_together` because the two are genuinely different capabilities —
    a store can have unfiltered uniqueness and no way to restrict it — and because the filtered
    form is the one consumers actually declare, so a backend that had only the unfiltered kind
    would otherwise pass reconciliation and fail at the first write."""

    unique_together_skip_null: bool = False
    """Whether rows holding a null in the tuple can be exempted."""

    non_overlapping: bool = False
    """Whether non-overlap of periods per key is enforced."""

    serialized_by: bool = False
    """Whether writes for one key can be kept from interleaving."""

    non_overlapping_filtered: bool = False
    """Whether non-overlap can be restricted to a *subset* of rows.

    Separate from :attr:`non_overlapping` for the reason :attr:`unique_together_filtered` is
    separate: they are different capabilities, and a store with the unfiltered kind and no way
    to restrict it would otherwise pass reconciliation and fail at the first write."""

    checks_deferred_to_commit: bool = False
    """Whether a guarantee can hold at commit (``holds="commit"``) rather than after every write.

    A store without it checks every write as it lands, so a transaction passing through a state
    the guarantee forbids fails there — refusing work the declaration says is legal."""

    unique_together_partial_at_commit: bool = False
    """Whether a uniqueness holding at commit can also be partial — restricted by ``where`` or
    exempting nulls by ``skip_null``.

    Separate because a store can have both halves and not the pair: in Postgres deferral is a
    property of constraints, a partial uniqueness is only an index, and a unique constraint
    cannot be partial. Non-overlap has no such flag: its constraint can be both."""

    unique_together_skip_null_at_commit: bool = False
    """Whether a uniqueness exempting nulls (``skip_null``) can hold at commit even where a
    filtered one cannot. Consulted only when :attr:`unique_together_partial_at_commit` is off,
    since that flag already covers ``skip_null``.

    Separate because the exemption is partial only in a store whose plain uniqueness counts
    nulls as values. A Postgres unique constraint already treats a tuple holding a null as
    distinct, so it keeps ``skip_null`` without a predicate, and a deferred one keeps it at
    commit."""

    reference: bool = False
    """Whether a hard delete of a referenced row applies a :class:`Reference`'s ``on_delete``
    to the rows naming it."""

    # ....................... #

    def unmet(self, guarantee: StorageGuarantee) -> tuple[str, ...]:
        """Which of *guarantee*'s requirements this store cannot keep.

        A tuple rather than a bool so the refusal can say *which* part is missing: "cannot
        enforce a filtered uniqueness" sends a reader to a different fix than "cannot enforce
        uniqueness at all", and a caller reading one and acting on the other is a wasted
        deployment.

        Matched on the member's type rather than its :attr:`kind`, so each arm reads that
        member's own fields and a member added to the union without an arm here is a type
        error rather than a guarantee that silently reconciles against nothing.
        """

        match guarantee:
            case UniqueTogether():
                missing = [] if self.unique_together else ["uniqueness over a field tuple"]
                partial = guarantee.where is not None or (
                    guarantee.skip_null and not self.unique_together_skip_null_at_commit
                )

                # Each axis is consulted only where the guarantee asks for it: a declaration
                # with no `where` must not be refused by a store lacking the filtered form.
                if guarantee.where is not None and not self.unique_together_filtered:
                    missing.append("uniqueness restricted to a subset of rows (`where`)")

                if guarantee.skip_null and not self.unique_together_skip_null:
                    missing.append("exempting rows whose tuple holds a null (`skip_null`)")

                missing.extend(self._unmet_moment(guarantee.holds))

                # Only for a store that can defer at all: one that cannot is already refused
                # above, and naming the narrower gap as well would send a reader two ways.
                if (
                    guarantee.holds == "commit"
                    and partial
                    and self.checks_deferred_to_commit
                    and not self.unique_together_partial_at_commit
                ):
                    missing.append(
                        "a partial uniqueness (`where` or `skip_null`) that holds only at commit"
                    )

                return tuple(missing)

            case NonOverlapping():
                missing = [] if self.non_overlapping else ["non-overlap of periods per key"]

                if guarantee.where is not None and not self.non_overlapping_filtered:
                    missing.append("non-overlap restricted to a subset of rows (`where`)")

                missing.extend(self._unmet_moment(guarantee.holds))

                return tuple(missing)

            case SerializedBy():
                return () if self.serialized_by else ("serializing writes per key",)

            case Reference():
                return () if self.reference else ("applying a reference's `on_delete`",)

    def _unmet_moment(self, holds: GuaranteeMoment) -> tuple[str, ...]:
        if holds == "commit" and not self.checks_deferred_to_commit:
            return ('holding only at commit (`holds="commit"`)',)

        return ()


# ....................... #

FULL_STORAGE_GUARANTEES: Final[StorageGuaranteeCapabilities] = StorageGuaranteeCapabilities(
    unique_together=True,
    unique_together_filtered=True,
    unique_together_skip_null=True,
    non_overlapping=True,
    non_overlapping_filtered=True,
    serialized_by=True,
    checks_deferred_to_commit=True,
    unique_together_partial_at_commit=True,
    unique_together_skip_null_at_commit=True,
    reference=True,
)
"""Every guarantee that is enforced anywhere today — the in-memory store's declaration.

The mock is the canonical superset, so this is both what it enforces and the ceiling on what a
spec can usefully declare. It is the declaration most easily wrong in the optimistic direction —
a mock claiming more than a backend keeps would pass a simulation a deployment fails — so the
batteries compare the two stores' refusals rather than trusting this value.

A member arrives here when a store maps it, never before: a capability that reconciles and then
does not enforce is worse than one that refuses."""


# ....................... #


@runtime_checkable
class GuaranteeDeclaring(Protocol):
    """A spec that asks something of its store.

    Structural for the same reason as :class:`GuaranteeEnforcing`: most specs have no
    guarantees field and should not need one to be resolvable.
    """

    guarantees: StorageGuarantees
    """What this spec's store must enforce."""


# ....................... #


@runtime_checkable
class GuaranteeImplying(Protocol):
    """A domain model whose fields carry a guarantee of their own, checked on the model class.

    Structural so a mixin outside the core can say what its fields need without the core
    importing it: a number handed out once by a counter is a number no two rows may hold. A
    document spec over the model adds these to its declared guarantees.
    """

    implied_guarantees: StorageGuarantees
    """What any store holding this model's rows must enforce, declared or not."""


# ....................... #


@runtime_checkable
class GuaranteeEnforcing(Protocol):
    """A port that says which guarantees it enforces.

    Structural, and checked with :func:`isinstance` at the resolution seam: a port that has not
    heard of guarantees is not a port that must be edited to say so, it is one whose silence
    reads as ``StorageGuaranteeCapabilities()`` — nothing.
    """

    storage_guarantees: StorageGuaranteeCapabilities
    """What this port's store enforces."""


# ....................... #


def guarantees_of(spec: object) -> StorageGuarantees:
    """What *spec* requires of its store, or nothing when it requires nothing."""

    return spec.guarantees if isinstance(spec, GuaranteeDeclaring) else ()


def implied_guarantees_of(model: type) -> StorageGuarantees:
    """What *model*'s fields require of its store, or nothing when they require nothing.

    :raises CoreException: ``configuration`` (``implied_guarantees_invalid``) when the model's
        ``implied_guarantees`` is not a tuple of storage guarantees.
    """

    if not isinstance(model, GuaranteeImplying):
        return ()

    implied = model.implied_guarantees

    # Checked rather than trusted: a bare guarantee or a stray item would otherwise fail deep in
    # spec validation as a TypeError or an UnboundLocalError that names no model.
    if not isinstance(implied, tuple) or not all(isinstance(g, StorageGuarantee) for g in implied):
        raise exc.configuration(
            f"{model.__qualname__}.implied_guarantees is {implied!r}; it must be a tuple of "
            "storage guarantees (UniqueTogether, NonOverlapping, SerializedBy), or () for none.",
            code="implied_guarantees_invalid",
            details={"model": f"{model.__module__}.{model.__qualname__}"},
        )

    return implied


# ....................... #


def capabilities_of(port: object) -> StorageGuaranteeCapabilities:
    """*port*'s declaration, or the empty one when it makes none."""

    # The backend's declaration: a port resolution wrapped forwards it, but a protocol check
    # of the wrapper never sees it.
    backend = backend_of(port)

    if isinstance(backend, GuaranteeEnforcing):
        return backend.storage_guarantees

    return StorageGuaranteeCapabilities()


# ....................... #


def validate_storage_guarantees(
    guarantees: StorageGuarantees,
    port: object,
    *,
    spec_name: str,
    backend: str,
    declared: StorageGuarantees | None = None,
) -> None:
    """Refuse *spec_name*'s guarantees that *port* cannot keep.

    Every unmet guarantee is reported in one refusal rather than the first one found, so a
    deployment learns its whole gap in a single pass — the reason
    :meth:`~forze.application.execution.operations.wiring.WiringReport.raise_if_failed`
    aggregates too.

    :param declared: What the spec's author declared, when *guarantees* also holds what its
        domain model implies. An unmet implied guarantee is marked, and the refusal names the
        opt-out on the model, since there is no declaration to drop.
    :raises CoreException: ``precondition``, code ``storage_guarantee_unsupported``, naming each
        guarantee, the spec and the backend.
    """

    if not guarantees:
        return

    capabilities = capabilities_of(port)
    unmet: list[str] = []
    implied = False

    for guarantee in guarantees:
        missing = capabilities.unmet(guarantee)

        if not missing:
            continue

        entry = f"{guarantee.kind} needs {', and '.join(missing)}"

        if declared is not None and guarantee not in declared:
            entry += " (implied by the domain model)"
            implied = True

        unmet.append(entry)

    if not unmet:
        return

    lines = "\n".join(f"  - {entry}" for entry in unmet)
    opt_out = (
        " A guarantee the domain model implies is dropped on the model, with "
        "`implied_guarantees = ()`."
        if implied
        else ""
    )

    raise exc.precondition(
        f"Backend {backend!r} cannot enforce {len(unmet)} guarantee(s) that spec "
        f"{spec_name!r} declares:\n{lines}\n"
        "A guarantee is refused rather than skipped: a store that does not keep it leaves the "
        "declaration reading as a promise. Either wire a backend that enforces it, or drop the "
        "guarantee and accept that nothing prevents the violation." + opt_out,
        code=GUARANTEE_UNSUPPORTED,
        details={"spec": spec_name, "backend": backend},
    )


__all__ = [
    "FULL_STORAGE_GUARANTEES",
    "GUARANTEE_UNSUPPORTED",
    "GuaranteeDeclaring",
    "GuaranteeEnforcing",
    "GuaranteeImplying",
    "GuaranteeKind",
    "StorageGuaranteeCapabilities",
    "capabilities_of",
    "guarantees_of",
    "implied_guarantees_of",
    "validate_storage_guarantees",
]
