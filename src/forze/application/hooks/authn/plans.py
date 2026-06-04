"""Wire authentication requirements into operation plans."""

from typing import Any, final

import attrs

from forze.application.contracts.execution import Before, BeforeFactory, BeforeStep
from forze.application.execution.context import ExecutionContext
from forze.base.primitives import StrKey

from .._base import authentication_guard_before, required_guard_step

# ----------------------- #


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

    def to_step(
        self,
        *,
        step_id: StrKey = "authn.principal",
        requires: tuple[StrKey, ...] = (),
        depends_on: tuple[StrKey, ...] = (),
        priority: int = 10,
    ) -> BeforeStep:
        """Build a :class:`BeforeStep` using this factory."""

        return required_guard_step(
            self,
            step_id=step_id,
            requires=requires,
            depends_on=depends_on,
            priority=priority,
        )
