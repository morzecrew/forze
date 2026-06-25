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
from forze.base.exceptions import exc

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
    supports_at_rest: bool = True,
) -> None:
    """Validate a *kind* route's encryption reach at resolve.

    Two checks, both fail-closed. First, when ``supports_at_rest`` is ``False`` (a direct
    transport has no store of its own, so ``at_rest`` is inapplicable), a ``declared`` reach
    of ``at_rest`` is rejected regardless of any floor — the ``Literal`` on the transport
    spec is not enforced at runtime, so an invalid route must be caught here rather than be
    treated as encrypted by the command wrapper. Second, when a ``required_reach`` floor is
    wired, a ``declared`` reach weaker than it is refused.

    *kind* (``outbox`` / ``queue`` / ``stream`` / ``pubsub``) names the resource in the
    error and its ``code`` (``core.<kind>.invalid_reach`` / ``core.<kind>.reach_floor``).
    """

    if not supports_at_rest and declared == "at_rest":
        raise exc.configuration(
            f"{kind} route {route!r} declares encryption='at_rest', but a direct transport "
            "has no store of its own to protect — use 'end_to_end' (sealed through the broker, "
            "consumer decrypts) or 'none'.",
            code=f"core.{kind}.invalid_reach",
        )

    required = resolve_required_reach(deps)

    if required is None:
        return

    validate_required_reach(
        integration=f"{kind} route {route!r}",
        declared=declared,
        required=required,
        code=f"core.{kind}.reach_floor",
    )
