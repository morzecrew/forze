"""Shared encryption wiring validation for integration deps modules.

The encryption analog of :mod:`forze.application.contracts.tenancy.wiring`: a
deployment declares the *minimum* encryption it accepts and this refuses to wire
any weaker combination — failing closed at boot rather than leaking plaintext at
runtime. Two orthogonal floors live here:

- ``required_encryption`` — the storage *coverage* floor (how much of a stored
  value is protected: ``none < field < envelope``).
- ``required_reach`` — the messaging *reach* floor (where a whole-payload envelope
  is decrypted: ``none < at_rest < end_to_end``), applied to outbox and transport
  routes.
"""

from typing import Literal

from ..base import EncryptionReach
from ..tiers import TierLattice

# ----------------------- #

EncryptionTier = Literal["none", "field", "envelope"]
"""Derived encryption-coverage tier and the ``required_encryption`` floor.

The coverage ladder (weakest → strongest) is ``none < field < envelope``. The
ordering is by *how much of a stored value is protected*, not by cipher strength:

- ``none`` — no application-level encryption wired; values are stored as written.
- ``field`` — selected fields are encrypted while the rest stay plaintext so the
  backend can still index/route/query them. The canonical mode for databases.
- ``envelope`` — the whole serialized value is encrypted as one opaque blob,
  exposing nothing. The canonical mode for messages, outbox payloads and blobs,
  where nothing inside the value is queried.

``envelope`` coverage is a superset of ``field`` (it hides everything ``field``
hides and more), so it satisfies a ``field`` floor; the reverse does not hold.
This is a coarse coverage floor — *which* fields are encrypted is per-field
configuration, not expressible here.
"""

# ....................... #

_ENCRYPTION_LATTICE: TierLattice[EncryptionTier] = TierLattice(
    field="encryption",
    validation_label="encryption",
    wired_noun="coverage",
    ceiling_noun="encryption",
    floor_remediation=(
        "Wire a key manager and mark the value (or its sensitive fields) for "
        "encryption, or lower the declared requirement."
    ),
    ranks={"none": 0, "field": 1, "envelope": 2},
)
"""Coverage ordering for encryption tiers (weakest → strongest), with its floor check."""

# ....................... #


def encryption_satisfies(
    *,
    derived: EncryptionTier,
    required: EncryptionTier,
) -> bool:
    """Return whether *derived* coverage is at least as strong as *required*."""

    return _ENCRYPTION_LATTICE.satisfies(derived=derived, required=required)


# ....................... #


def validate_required_encryption(
    *,
    integration: str,
    derived: EncryptionTier,
    required: EncryptionTier | None,
    code: str,
    max_supported: EncryptionTier | None = None,
) -> None:
    """Fail closed when wired encryption coverage is weaker than required.

    A deployment declares the *minimum* coverage it accepts (``required``); this
    refuses to wire any combination whose ``derived`` tier is weaker. Pass
    ``required=None`` to opt out (no declared floor — the historical behavior).

    ``max_supported`` is the strongest tier the integration can ever provide (its
    capability ceiling — e.g. a queue can only ever do whole-payload
    ``envelope``, never per-``field``). When ``required`` exceeds it, the failure
    is reported as a capability mismatch (the floor is unreachable by
    configuration) rather than a wiring gap.
    """

    _ENCRYPTION_LATTICE.validate(
        integration=integration,
        derived=derived,
        required=required,
        code=code,
        max_supported=max_supported,
    )


# ----------------------- #
# Reach floor (messaging: outbox + direct transports)


_REACH_LATTICE: TierLattice[EncryptionReach] = TierLattice(
    field="reach",
    validation_label="encryption reach",
    wired_noun="reach",
    ceiling_noun="reach",
    floor_remediation=(
        "Raise the route's encryption (a transport with no store reaches the floor only by "
        "'end_to_end'; the outbox can also use 'at_rest') or lower the declared requirement."
    ),
    ranks={"none": 0, "at_rest": 1, "end_to_end": 2},
)
"""Reach ordering for messaging encryption (weakest → strongest), with its floor check.

No capability ceiling: every messaging backend can reach ``end_to_end`` (forwarding a
sealed payload is strictly less work than decrypting it), so the floor compares two
*declared* reaches — the deployment's minimum against the route's."""

# ....................... #


def validate_required_reach(
    *,
    integration: str,
    declared: EncryptionReach,
    required: EncryptionReach | None,
    code: str,
) -> None:
    """Fail closed when a route's declared reach is weaker than the required floor.

    A deployment declares the *minimum* reach it accepts (``required``); this refuses any
    outbox/transport route whose own ``declared`` reach is weaker. Pass ``required=None`` to
    opt out (no declared floor — the historical behavior). A floor of ``at_rest`` is
    satisfied by an ``end_to_end`` route; a transport (no ``at_rest`` level) satisfies an
    ``at_rest`` floor only by being ``end_to_end``.
    """

    _REACH_LATTICE.validate(
        integration=integration,
        derived=declared,
        required=required,
        code=code,
    )
