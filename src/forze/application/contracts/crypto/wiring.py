"""Shared encryption wiring validation for integration deps modules.

The encryption analog of :mod:`forze.application.contracts.tenancy.wiring`: a
deployment declares the *minimum* encryption coverage it accepts
(``required_encryption``) and this refuses to wire any combination whose derived
coverage is weaker — failing closed at boot rather than leaking plaintext at
runtime.
"""

from typing import Literal

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
