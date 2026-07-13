"""A generic strength lattice for fail-closed wiring floors.

Several wiring concerns share one shape: an ordered ladder of tiers (weakest →
strongest), a deployment-declared *minimum* it accepts, and a boot-time refusal
to wire any combination weaker than that floor — failing closed rather than
degrading silently at runtime. Three concerns are instances of it: tenant
isolation (``none < tagged < namespace < dedicated``), encryption coverage
(``none < field < envelope``), and messaging encryption reach (``none < at_rest <
end_to_end``, via ``_REACH_LATTICE`` / ``validate_required_reach`` in
:mod:`forze.application.contracts.crypto.wiring`).

:class:`TierLattice` captures that mechanism once. Each concern constructs one
instance with its own ranks and remediation wording; the per-concern
``*_satisfies`` / ``validate_required_*`` helpers stay as thin wrappers so their
error messages and ``details`` keys are unchanged.
"""

from collections.abc import Mapping
from typing import final

import attrs

from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TierLattice[T]:
    """An ordered tier ladder plus its fail-closed floor check.

    *ranks* maps each tier to its strength (higher = stronger). The remaining
    fields shape the diagnostics so a generic lattice still produces the
    concern's own wording:

    - *field* — the noun used in the declared-requirement kwarg and the
      ``details`` keys (``required_<field>`` / ``derived_<field>`` /
      ``max_supported_<field>``), e.g. ``"isolation"`` or ``"encryption"``.
    - *validation_label* — the concern name in the floor-failure message
      (``"{integration} {validation_label} validation failed: ..."``).
    - *wired_noun* — how the derived tier is described (``the wired {wired_noun}
      is ...``), e.g. ``"isolation"`` or ``"coverage"``.
    - *ceiling_noun* — how the ceiling is described (``supports at most X
      {ceiling_noun}``), e.g. ``"tenant isolation"`` or ``"encryption"``.
    - *floor_remediation* — the trailing sentence telling the operator how to fix
      a floor failure.
    """

    field: str
    validation_label: str
    wired_noun: str
    ceiling_noun: str
    floor_remediation: str
    ranks: Mapping[T, int]

    # ....................... #

    def satisfies(self, *, derived: T, required: T) -> bool:
        """Return whether *derived* is at least as strong as *required*."""

        return self.ranks[derived] >= self.ranks[required]

    # ....................... #

    def validate(
        self,
        *,
        integration: str,
        derived: T,
        required: T | None,
        code: str,
        max_supported: T | None = None,
    ) -> None:
        """Fail closed when the *derived* tier is weaker than the declared floor.

        A deployment declares the *minimum* tier it accepts (*required*); this
        refuses any combination whose *derived* tier is weaker. Pass
        ``required=None`` to opt out (no declared floor).

        *max_supported* is the strongest tier the integration can ever provide
        (its capability ceiling). When *required* exceeds it, the failure is
        reported as a capability mismatch (the floor is unreachable by
        configuration) rather than a wiring gap.
        """

        if required is None:
            return

        if max_supported is not None and not self.satisfies(
            derived=max_supported, required=required
        ):
            raise exc.configuration(
                f"{integration} supports at most {max_supported!r} {self.ceiling_noun}, "
                f"but the deployment declares required_{self.field}={required!r}, which it "
                "cannot provide. Lower the declared requirement or use a backend that "
                "supports it.",
                code=code,
                details={
                    f"required_{self.field}": required,
                    f"max_supported_{self.field}": max_supported,
                },
            )

        if self.satisfies(derived=derived, required=required):
            return

        raise exc.configuration(
            f"{integration} {self.validation_label} validation failed: deployment declares "
            f"required_{self.field}={required!r} but the wired {self.wired_noun} is "
            f"{derived!r}, which is weaker. {self.floor_remediation}",
            code=code,
            details={
                f"required_{self.field}": required,
                f"derived_{self.field}": derived,
            },
        )
