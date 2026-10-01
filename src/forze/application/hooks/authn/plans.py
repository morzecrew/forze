"""Wire authentication requirements into operation plans."""

from typing import Any, Final, final

import attrs

from forze.application.contracts.execution import Before, BeforeFactory, BeforeStep
from forze.application.execution.context import ExecutionContext
from forze.base.primitives import StrKey

from .._base import authentication_guard_before, required_guard_step

# ----------------------- #

AUTHN_PRINCIPAL: Final = "authn.principal"
"""The canonical authentication step id, and the capability that step provides."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnRequired(BeforeFactory):
    """Before-hook factory that requires a bound :class:`~forze.application.contracts.authn.AuthnIdentity`."""

    def __call__(self, ctx: ExecutionContext) -> Before[Any]:
        return authentication_guard_before(
            ctx,
            lambda c: c.inv_ctx.get_authn(),
            message="Authentication required",
            code="auth_required",
        )

    # ....................... #

    def requires_authn(self) -> bool:
        """Marker (:class:`~forze.application.contracts.execution.DeclaresAuthn`):
        this hook demands a bound principal."""

        return True

    # ....................... #

    def to_step(
        self,
        *,
        step_id: StrKey = AUTHN_PRINCIPAL,
        requires: tuple[StrKey, ...] = (),
        provides: tuple[StrKey, ...] | None = None,
        depends_on: tuple[StrKey, ...] = (),
        priority: int = 10,
    ) -> BeforeStep:
        """Build a :class:`BeforeStep` using this factory.

        The canonical step (the default *step_id*) provides the ``authn.principal``
        capability, which :meth:`~forze.application.hooks.authz.AuthzBeforeAuthorize.to_step`
        requires by default, so an authorization step in the same plan runs after it. A step
        under any other id provides nothing unless *provides* says so, so a kit's step and an
        app's own can share a plan without both claiming the capability.
        """

        if provides is None:
            provides = (AUTHN_PRINCIPAL,) if step_id == AUTHN_PRINCIPAL else ()

        return required_guard_step(
            self,
            step_id=step_id,
            requires=requires,
            provides=provides,
            depends_on=depends_on,
            priority=priority,
        )
