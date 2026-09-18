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

from .value_objects import (
    GuaranteeKind,
    NonOverlapping,
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

                # Each axis is consulted only where the guarantee asks for it: a declaration
                # with no `where` must not be refused by a store lacking the filtered form.
                if guarantee.where is not None and not self.unique_together_filtered:
                    missing.append("uniqueness restricted to a subset of rows (`where`)")

                if guarantee.skip_null and not self.unique_together_skip_null:
                    missing.append("exempting rows whose tuple holds a null (`skip_null`)")

                return tuple(missing)

            case NonOverlapping():
                return () if self.non_overlapping else ("non-overlap of periods per key",)


# ....................... #

FULL_STORAGE_GUARANTEES: Final[StorageGuaranteeCapabilities] = StorageGuaranteeCapabilities(
    unique_together=True,
    unique_together_filtered=True,
    unique_together_skip_null=True,
    non_overlapping=True,
)
"""Every guarantee the vocabulary defines.

The in-memory store enforces all of it, which is what makes a guarantee testable under simulation
instead of only against a live database. It is also the declaration most easily wrong in the
optimistic direction — a mock that enforced more than a backend would pass a simulation a
deployment fails — so the batteries compare the two refusals rather than trusting this flag."""


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


# ....................... #


def capabilities_of(port: object) -> StorageGuaranteeCapabilities:
    """*port*'s declaration, or the empty one when it makes none."""

    if isinstance(port, GuaranteeEnforcing):
        return port.storage_guarantees

    return StorageGuaranteeCapabilities()


# ....................... #


def validate_storage_guarantees(
    guarantees: StorageGuarantees,
    port: object,
    *,
    spec_name: str,
    backend: str,
) -> None:
    """Refuse *spec_name*'s guarantees that *port* cannot keep.

    Every unmet guarantee is reported in one refusal rather than the first one found, so a
    deployment learns its whole gap in a single pass — the reason
    :meth:`~forze.application.execution.operations.wiring.WiringReport.raise_if_failed`
    aggregates too.

    :raises CoreException: ``precondition``, code ``storage_guarantee_unsupported``, naming each
        guarantee, the spec and the backend.
    """

    if not guarantees:
        return

    declared = capabilities_of(port)
    unmet: list[str] = []

    for guarantee in guarantees:
        missing = declared.unmet(guarantee)

        if missing:
            unmet.append(f"{guarantee.kind} needs {', and '.join(missing)}")

    if not unmet:
        return

    lines = "\n".join(f"  - {entry}" for entry in unmet)

    raise exc.precondition(
        f"Backend {backend!r} cannot enforce {len(unmet)} guarantee(s) that spec "
        f"{spec_name!r} declares:\n{lines}\n"
        "A guarantee is refused rather than skipped: a store that does not keep it leaves the "
        "declaration reading as a promise. Either wire a backend that enforces it, or drop the "
        "guarantee and accept that nothing prevents the violation.",
        code=GUARANTEE_UNSUPPORTED,
        details={"spec": spec_name, "backend": backend},
    )


__all__ = [
    "FULL_STORAGE_GUARANTEES",
    "GUARANTEE_UNSUPPORTED",
    "GuaranteeDeclaring",
    "GuaranteeEnforcing",
    "GuaranteeKind",
    "StorageGuaranteeCapabilities",
    "capabilities_of",
    "guarantees_of",
    "validate_storage_guarantees",
]
