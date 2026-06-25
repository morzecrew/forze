"""Enforce the deployment-wide ``required_reach`` floor at messaging resolve points.

A ``CryptoDepsModule`` may register a minimum encryption *reach* under
:data:`RequiredReachDepKey`. Outbox and direct-transport (queue/stream/pub-sub) wiring
calls :func:`enforce_required_reach` as it resolves a route, so a route whose declared
reach is weaker than the floor is refused before it can stage or publish — the encrypting
wrappers themselves stay free of policy.
"""

from forze.application.contracts.base import EncryptionReach
from forze.application.contracts.crypto import (
    RequiredReachDepKey,
    validate_required_reach,
)

from ..deps import FrozenDeps

# ----------------------- #


def resolve_required_reach(deps: FrozenDeps) -> EncryptionReach | None:
    """Return the declared ``required_reach`` floor, or ``None`` when none is wired."""

    if not deps.exists(RequiredReachDepKey):
        return None

    reach: EncryptionReach = deps.provide(RequiredReachDepKey)
    return reach


# ....................... #


def enforce_required_reach(
    deps: FrozenDeps,
    *,
    route: str,
    declared: EncryptionReach,
    kind: str,
) -> None:
    """Refuse a *kind* route whose *declared* reach is weaker than the wired floor.

    No-op when no floor is wired. *kind* (``outbox`` / ``queue`` / ``stream`` / ``pubsub``)
    names the resource in the error and its ``code`` (``core.<kind>.reach_floor``).
    """

    required = resolve_required_reach(deps)

    if required is None:
        return

    validate_required_reach(
        integration=f"{kind} route {route!r}",
        declared=declared,
        required=required,
        code=f"core.{kind}.reach_floor",
    )
