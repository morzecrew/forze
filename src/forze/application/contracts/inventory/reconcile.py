"""Cross-check the inventory against what the runtime actually binds.

The inventory is only as good as its completeness, and completeness is exactly what an author
cannot verify by eye — most of an application's specs are not written by its author.
``forze_identity`` binds nineteen document specs an app inherits without declaring; a kit
derives an outbox, a queue and an inbox for every synced search index. An export that walked a
hand-maintained list would quietly leave out every credential, session and sync route in the
system, and the artifact would look complete.

So the inventory is reconciled, in both directions, against the dependency registry's own
static frame list — the one thing that already knows every ``(key, route)`` the app bound.
"""

from collections.abc import Iterable

from forze.base.exceptions import exc

from ..deps import ResolutionFrame
from .planes import PLANE_DEP_KEYS, plane_of_key
from .registry import FrozenSpecRegistry

# ----------------------- #


def _binds(entry_name: str, plane_keys: frozenset[str], frames: Iterable[ResolutionFrame]) -> bool:
    """Whether any frame binds *entry_name* on this plane.

    A frame with **no route** is a *plain* registration — one provider serving every route of
    that key, which is what the mock backend does for essentially everything — so it satisfies
    any name. Treating it otherwise would make every mock-backed runtime fail reconciliation.
    """

    return any(
        frame.key_name in plane_keys and frame.route in (None, entry_name) for frame in frames
    )


# ....................... #


def reconcile_specs(
    registry: FrozenSpecRegistry,
    frames: Iterable[ResolutionFrame],
    *,
    allow_unregistered: bool = False,
) -> tuple[str, ...]:
    """Check the inventory against the bound dependency frames, both ways.

    Raises ``exc.configuration`` naming every mismatch:

    - **A spec is catalogued but nothing binds it.** The app declared a route it never wired,
      so the port cannot be resolved — a latent failure that would otherwise surface at the
      first request rather than at startup.
    - **A route is bound but nothing catalogues it.** *This is the one that matters.* It is
      the forgotten identity plane, the derived sync route, the spec added last month — the
      failure mode an export cannot detect and cannot recover from, because a missing plane
      and an empty one look identical in the artifact.
    - **An edge points at a spec that is not in the inventory.**

    Only the dependency keys in :data:`PLANE_DEP_KEYS` participate; everything else a runtime
    binds (transaction engines, resilience policies, crypto singletons, authn/authz routes) is
    outside the inventory by design and is ignored here.

    *allow_unregistered* downgrades the second check to a returned warning, for incremental
    adoption: an app can turn the inventory on, see what it is missing, and fix it without its
    runtime refusing to start. It does **not** downgrade the others — a catalogued-but-unbound
    spec is a plain wiring bug, and a dangling edge is a bug in whoever built the inventory.

    :returns: warnings (empty unless *allow_unregistered* suppressed something).
    """

    frames = tuple(frames)
    problems: list[str] = []
    warnings: list[str] = []

    problems.extend(
        f"  - {entry.ref.label()}: catalogued (by {entry.source.value}) but no dependency binds it — the port cannot be resolved"
        for entry in registry.entries
        if not _binds(entry.name, PLANE_DEP_KEYS[entry.plane], frames)
    )

    catalogued = {(entry.plane, entry.name) for entry in registry.entries}

    for frame in frames:
        plane = plane_of_key(frame.key_name)

        # Not an inventoried key, or a plain provider with no route to attribute to a spec.
        if plane is None or frame.route is None:
            continue

        if (plane, frame.route) in catalogued:
            continue

        message = (
            f"  - {plane.value}:{frame.route}: bound (as {frame.label()}) but missing from "
            f"the spec inventory — an export would silently omit it"
        )

        if allow_unregistered:
            warnings.append(message)

        else:
            problems.append(message)

    for edge in registry.edges:
        problems.extend(
            f"  - {edge.label()}: points at {end.label()}, which is not in the inventory"
            for end in (edge.source, edge.target)
            if (end.plane, end.name) not in catalogued
        )

    if problems:
        raise exc.configuration(
            f"Spec inventory does not match the wired dependencies "
            f"({len(problems)} problem(s)):\n" + "\n".join(sorted(problems))
        )

    return tuple(sorted(warnings))
